import test from 'node:test';
import assert from 'node:assert/strict';
import {
  DaemonConnection,
  NotSentError,
  OverflowError,
  UncertainError,
} from '../dist/index.js';
import { startMock, tmpSocket } from './helpers.mjs';
const GET_CONNECTION_STATE = { type: 'get_connection_state', activeSessionId: 'wire' };

test('a full frame queue tears the connection down with a sticky Overflow', async () => {
  const socketPath = tmpSocket('overflow');
  const stop = await startMock(socketPath, 'flood');
  const connection = await DaemonConnection.connect(socketPath, { frameBuffer: 2 });
  // The mock floods; the pump fills the bounded queue and fails closed.
  // Frames queued before the terminal mark may drain first — then the
  // sticky OverflowError surfaces and no further frame is observable.
  let sawOverflow = false;
  for (let i = 0; i < 512; i += 1) {
    try {
      await connection.nextFrame();
    } catch (error) {
      if (!(error instanceof OverflowError)) throw error;
      sawOverflow = true;
      break;
    }
  }
  assert.ok(sawOverflow, 'the terminal overflow error never surfaced');
  // Sticky: every later pull observes the terminal error, not a stream.
  await assert.rejects(connection.nextFrame(), (error) => error instanceof OverflowError);
  assert.equal(connection.currentStatus().type, 'overflow');
  connection.close();
  await stop();
});

test('a PARKED nextFrame pull observes the sticky overflow branch', async () => {
  const socketPath = tmpSocket('overflow-parked');
  const stop = await startMock(socketPath, 'flood-trigger');
  const connection = await DaemonConnection.connect(socketPath, { frameBuffer: 1 });
  // Readiness barrier: the gated mock sent hello ONLY; the queue is empty
  // and stays empty until the trigger, so every parked pull below parks
  // with a PROVABLY empty queue.
  await connection.request({
    type: 'get_connection_state',
    activeSessionId: 'wire',
    floodTrigger: true,
  }).catch((error) => error);
  let witnessed = 0;
  for (let attempt = 0; attempt < 512 && witnessed === 0; attempt += 1) {
    const pull = connection.nextFrame();
    // The single-consumer probe proves the pull holds the pending poll:
    // a concurrent pull MUST reject while the first is parked.
    const probe = connection
      .nextFrame()
      .then(
        () => {
          throw new Error('the probe should have rejected');
        },
        (error) => error,
      );
    const probeError = await probe;
    assert.ok(/single-consumer/.test(probeError.message));
    const outcome = await pull.then(
      (frame) => ({ frame }),
      (error) => ({ error }),
    );
    if (outcome.error !== undefined) {
      assert.ok(outcome.error instanceof OverflowError);
      witnessed += 1;
    }
    // A pre-mark frame is a legal interleave: repark with a fresh
    // proven-pending pull and keep witnessing.
  }
  assert.ok(witnessed > 0, 'no proven-pending pull ever witnessed the overflow');
  // Sticky thereafter.
  await assert.rejects(connection.nextFrame(), (error) => error instanceof OverflowError);
  assert.equal(connection.currentStatus().type, 'overflow');
  connection.close();
  await stop();
});

test('in-flight requests at overflow stay Uncertain with cause overflow', async () => {
  const socketPath = tmpSocket('overflow-inflight');
  const stop = await startMock(socketPath, 'flood-trigger');
  const connection = await DaemonConnection.connect(socketPath, { frameBuffer: 2 });
  // The gated trigger request is in flight when the flood fills the queue
  // (the mock delays the response): the in-flight line was queued and must
  // settle Uncertain with cause overflow — never re-classified.
  const inflight = connection
    .request({
      type: 'get_connection_state',
      activeSessionId: 'wire',
      floodTrigger: true,
    })
    .then(
      (value) => value,
      (error) => error,
    );
  const settled = await inflight;
  assert.ok(
    settled instanceof UncertainError && settled.cause === 'overflow',
    `unexpected class: ${settled?.kind}:${settled?.cause}`,
  );
  connection.close();
  await stop();
});

test('new requests after overflow are NotSent with the overflow cause surfaced', async () => {
  const socketPath = tmpSocket('overflow-notsent');
  const stop = await startMock(socketPath, 'flood');
  const connection = await DaemonConnection.connect(socketPath, { frameBuffer: 2 });
  await assert.rejects(connection.nextFrame(), (error) => error instanceof OverflowError);
  await assert.rejects(
    connection.request(GET_CONNECTION_STATE),
    (error) => error instanceof NotSentError && error.cause === 'overflow',
  );
  connection.close();
  await stop();
});
