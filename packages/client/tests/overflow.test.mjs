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
  // Do NOT drain: the mock floods; the pump fills the bounded queue and
  // fails closed. The overflow surfaces to the first pull.
  await assert.rejects(
    connection.nextFrame(),
    (error) => error instanceof OverflowError,
  );
  // Sticky: every later pull observes the terminal error, not a stream.
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
    (error) => error instanceof NotSentError && /overflow/.test(error.message),
  );
  connection.close();
  await stop();
});
