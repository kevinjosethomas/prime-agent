import test from 'node:test';
import assert from 'node:assert/strict';
import { DaemonConnection, NotSentError, UncertainError } from '../dist/index.js';
import { startMock, tmpSocket } from './helpers.mjs';
const GET_CONNECTION_STATE = { type: 'get_connection_state', activeSessionId: 'wire' };

test('close resolves after pending request, upgrade, and pull are all torn down', async () => {
  const socketPath = tmpSocket('close-full');
  const stop = await startMock(socketPath, 'silent');
  const connection = await DaemonConnection.connect(socketPath);
  // Handlers attach at creation: the teardown rejections must not pass
  // through an unhandled window.
  const pendingRequest = connection
    .request(GET_CONNECTION_STATE, 60_000)
    .then(
      () => {
        throw new Error('the request should have been torn down');
      },
      (error) => error,
    );
  const pendingUpgrade = connection.upgradeDirect('active-1').then(
    (upgraded) => upgraded,
    (error) => error,
  );
  const pendingFrame = connection.nextFrame();
  // The initial 'open' is delivered exactly once: consume it, then hold a
  // REAL pending change that close must resolve with 'closed'.
  assert.deepEqual(await connection.nextStatus(), { type: 'open' });
  const pendingStatus = connection.nextStatus();
  await connection.close();
  // The close promise resolved: the registry drained and the core task set
  // joined for real.
  // The teardown race is structural: if close marked before the request
  // line was queued, the honest class is NotSent (provably never sent);
  // after the first poll it is conservatively Uncertain. Both carry the
  // closed cause — same rule as the request battery's close-inflight test.
  const requestError = await pendingRequest;
  assert.ok(
    (requestError instanceof UncertainError || requestError instanceof NotSentError) &&
      requestError.cause === 'closed',
    `unexpected class: ${requestError?.kind}:${requestError?.cause}`,
  );
  assert.equal(await pendingUpgrade, false);
  assert.equal(await pendingFrame, null);
  assert.equal((await pendingStatus).type, 'closed');
  await stop();
});

test('concurrent close calls all resolve on the same completion', async () => {
  const socketPath = tmpSocket('close-concurrent');
  const stop = await startMock(socketPath);
  const connection = await DaemonConnection.connect(socketPath);
  const first = connection.close();
  const second = connection.close();
  const third = connection.close();
  await Promise.all([first, second, third]);
  await stop();
});

test('a canceled JS close await does not cancel the cleanup', async () => {
  const socketPath = tmpSocket('close-cancel');
  const stop = await startMock(socketPath);
  const connection = await DaemonConnection.connect(socketPath);
  const canceled = connection.close();
  canceled.catch(() => {});
  // The runner keeps going regardless of the abandoned promise.
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.equal(connection.currentStatus().type, 'closed');
  // A later close observes the settled completion immediately.
  await connection.close();
  await stop();
});
