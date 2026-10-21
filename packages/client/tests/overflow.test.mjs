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
  // Bounded fresh gated cycles: a legitimate pre-mark frame interleave can
  // end a cycle without the pending-Overflow witness; each cycle gets a
  // fresh gated connection and its cleanup is AWAITED. At least one
  // proven-pending pull must witness the terminal Overflow across the
  // cycles.
  let witnessed = 0;
  // 12 cycles: under a fully loaded parallel battery the pre-mark frame
  // interleave wins most cycles; the witness stays a bounded certainty.
  for (let cycle = 0; cycle < 12 && witnessed === 0; cycle += 1) {
    const socketPath = tmpSocket(`overflow-parked-${cycle}`);
    const stop = await startMock(socketPath, 'flood-trigger');
    const connection = await DaemonConnection.connect(socketPath, { frameBuffer: 1 });
    // The gated mock sent hello ONLY: one pull parks against an empty
    // queue — provably pending before any flood. The two native calls may
    // acquire the pending poll in EITHER order, so the proof is symmetric:
    // the loser rejects single-consumer promptly, the owner stays parked.
    const pulls = [
      connection.nextFrame().then(
        (frame) => ({ index: 0, frame }),
        (error) => ({ index: 0, error }),
      ),
      connection.nextFrame().then(
        (frame) => ({ index: 1, frame }),
        (error) => ({ index: 1, error }),
      ),
    ];
    const loser = await Promise.race(pulls);
    assert.ok(loser.error && /single-consumer/.test(loser.error.message));
    const parked = pulls[1 - loser.index];
    // Only NOW the trigger command starts the flood: the in-flight
    // trigger settles Uncertain-cause-overflow; the PROVEN-pending
    // parked pull settles the sticky terminal branch or a legal frame.
    const trigger = connection
      .request({
        type: 'get_connection_state',
        activeSessionId: 'wire',
        floodTrigger: true,
      })
      .then(
        (value) => value,
        (error) => error,
      );
    const triggerSettled = await trigger;
    assert.ok(
      triggerSettled instanceof UncertainError && triggerSettled.cause === 'overflow',
      `trigger class: ${triggerSettled?.kind}:${triggerSettled?.cause}`,
    );
    const parkedOutcome = await parked;
    if (parkedOutcome.error !== undefined) {
      assert.ok(parkedOutcome.error instanceof OverflowError);
      witnessed += 1;
      await connection.close();
    } else {
      // A pre-mark frame interleave: await the cycle's cleanup and retry.
      await connection.close();
    }
    await stop();
  }
  assert.ok(witnessed > 0, 'no proven-pending pull ever witnessed the overflow');
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
  // Frames queued before the mark may drain first — bounded drain until
  // the terminal OverflowError surfaces, then the NEW request must be the
  // provable NotSent with the overflow cause.
  let sawOverflow = false;
  for (let i = 0; i < 512 && !sawOverflow; i += 1) {
    try {
      await connection.nextFrame();
    } catch (error) {
      if (!(error instanceof OverflowError)) throw error;
      sawOverflow = true;
    }
  }
  assert.ok(sawOverflow, 'the terminal overflow error never surfaced');
  await assert.rejects(
    connection.request(GET_CONNECTION_STATE),
    (error) => error instanceof NotSentError && error.cause === 'overflow',
  );
  connection.close();
  await stop();
});
