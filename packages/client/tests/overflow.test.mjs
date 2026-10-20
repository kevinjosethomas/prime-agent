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
  // No frames exist yet: the pull parks INSIDE the select with the queue
  // empty — provably pending before any overflow mark.
  const parked = connection.nextFrame();
  // The readiness barrier: the trigger command is answered AND the flood
  // starts only now, so the parked pull was pending pre-mark.
  await connection.request({
    type: 'get_connection_state',
    activeSessionId: 'wire',
    floodTrigger: true,
  });
  // The flood fills the one-slot queue and tears the connection down:
  // the PARKED pull settles through the sticky terminal branch.
  await assert.rejects(parked, (error) => error instanceof OverflowError);
  // Sticky thereafter.
  await assert.rejects(connection.nextFrame(), (error) => error instanceof OverflowError);
  assert.equal(connection.currentStatus().type, 'overflow');
  connection.close();
  await stop();
});

test('in-flight requests at overflow stay Uncertain with cause overflow', async () => {
  const socketPath = tmpSocket('overflow-inflight');
  const stop = await startMock(socketPath, 'flood');
  const connection = await DaemonConnection.connect(socketPath, { frameBuffer: 2 });
  const inFlight = connection.request(GET_CONNECTION_STATE, 60_000);
  await assert.rejects(
    inFlight,
    (error) => error instanceof UncertainError && error.cause === 'overflow',
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
