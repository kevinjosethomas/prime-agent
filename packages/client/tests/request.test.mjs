import test from 'node:test';
import assert from 'node:assert/strict';
import {
  DaemonConnection,
  NotSentError,
  RejectedError,
  UncertainError,
} from '../dist/index.js';
import { startMock, tmpSocket } from './helpers.mjs';
const GET_CONNECTION_STATE = { type: 'get_connection_state', activeSessionId: 'wire' };

test('request round-trips a command, unknown fields preserved', async () => {
  const socketPath = tmpSocket('echo');
  const stop = await startMock(socketPath, 'echo');
  const connection = await DaemonConnection.connect(socketPath);
  const response = await connection.request({
    ...GET_CONNECTION_STATE,
    mysteryField: { nested: [1, 2, 3] },
  });
  assert.equal(response.success, true);
  assert.deepEqual(response.data.echoed.mysteryField, { nested: [1, 2, 3] });
  connection.close();
  await stop();
});

test('a malformed command is a provable NotSent (deserialize failure)', async () => {
  const socketPath = tmpSocket('deserialize');
  const stop = await startMock(socketPath, 'echo');
  const connection = await DaemonConnection.connect(socketPath);
  await assert.rejects(
    connection.request({ type: 'no_such_command_type', data: 1 }),
    (error) => error instanceof NotSentError && /deserialize/.test(error.message),
  );
  connection.close();
  await stop();
});

test('a timeout on a queued line is Uncertain — never resend', async () => {
  const socketPath = tmpSocket('silent');
  const stop = await startMock(socketPath, 'silent');
  const connection = await DaemonConnection.connect(socketPath);
  await assert.rejects(
    connection.request(GET_CONNECTION_STATE, 150),
    (error) => error instanceof UncertainError && error.kind === 'Uncertain',
  );
  connection.close();
  await stop();
});

test('a daemon refusal is Rejected with errorInfo preserved', async () => {
  const socketPath = tmpSocket('refuse');
  const stop = await startMock(socketPath, 'refuse');
  const connection = await DaemonConnection.connect(socketPath);
  await assert.rejects(
    connection.requestOk(GET_CONNECTION_STATE),
    (error) =>
      error instanceof RejectedError &&
      error.errorInfo.code === 'session_already_active' &&
      error.errorInfo.sessionPath === '/tmp/refused-session.jsonl',
  );
  connection.close();
  await stop();
});

test('socket death under a sent line is Uncertain; later sends are NotSent', async () => {
  const socketPath = tmpSocket('die');
  const stop = await startMock(socketPath, 'die');
  const connection = await DaemonConnection.connect(socketPath);
  const underDeath = connection.request(GET_CONNECTION_STATE, 5000);
  await assert.rejects(underDeath, (error) => error instanceof UncertainError);
  await assert.rejects(
    connection.request(GET_CONNECTION_STATE, 5000),
    (error) => error instanceof NotSentError,
  );
  connection.close();
  await stop();
});

test('the pending cap refuses as a provable NotSent', async () => {
  const socketPath = tmpSocket('cap');
  const stop = await startMock(socketPath, 'silent');
  const connection = await DaemonConnection.connect(socketPath, { maxPending: 1 });
  const first = connection.request(GET_CONNECTION_STATE, 500);
  const second = connection.request(GET_CONNECTION_STATE, 500);
  await assert.rejects(second, (error) => error instanceof NotSentError);
  await first.catch(() => {});
  connection.close();
  await stop();
});

test('a resolved answer never downgrades under a racing close (biased select proof)', async () => {
  // The select is biased with the core future first: when a completed
  // answer and the teardown wake are both ready, the core class wins.
  // Structural stress: the daemon answered the request (the mock echoes
  // immediately); a close racing the poll must yield only the honest
  // classes — a completed success, or NotSent/Uncertain with cause.
  for (let i = 0; i < 25; i += 1) {
    const socketPath = tmpSocket(`race-${i}`);
    const stop = await startMock(socketPath, 'echo');
    const connection = await DaemonConnection.connect(socketPath);
    const pending = connection.request(GET_CONNECTION_STATE);
    connection.close();
    const outcome = await pending.then(
      () => 'success',
      (error) => error,
    );
    if (outcome !== 'success') {
      assert.ok(
        (NotSentError.is(outcome) || UncertainError.is(outcome)) && outcome.cause === 'closed',
        `unexpected class: ${outcome.kind}`,
      );
    }
    await stop();
  }
});

test('close tears down an in-flight request with the honest class', async () => {
  const socketPath = tmpSocket('close-inflight');
  const stop = await startMock(socketPath, 'silent');
  const connection = await DaemonConnection.connect(socketPath);
  const inFlight = connection.request(GET_CONNECTION_STATE, 60_000);
  connection.close();
  // Structural, not timing-dependent: if close raced before the line was
  // queued the provable NotSent wins; after the first poll the line may be
  // queued and the class is conservatively Uncertain. Both carry cause.
  await assert.rejects(inFlight, (error) => {
    if (error instanceof NotSentError) {
      return error.cause === 'closed';
    }
    return error instanceof UncertainError && error.cause === 'closed';
  });
  await stop();
});
