import test from 'node:test';
import assert from 'node:assert/strict';
import {
  DaemonConnection,
  NotSentError,
  RejectedError,
  UncertainError,
} from '../dist/index.js';
import { startMock, tmpSocket } from './helpers.mjs';

test('request round-trips a command, unknown fields preserved', async () => {
  const socketPath = tmpSocket('echo');
  const stop = await startMock(socketPath, 'echo');
  const connection = await DaemonConnection.connect(socketPath);
  const response = await connection.request({
    type: 'get_connection_state',
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
    connection.request({ type: 'get_connection_state' }, 150),
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
    connection.requestOk({ type: 'get_connection_state' }),
    (error) =>
      error instanceof RejectedError &&
      error.errorInfo.code === 'session_already_active' &&
      error.errorInfo.session_path === '/tmp/refused-session.jsonl',
  );
  connection.close();
  await stop();
});

test('socket death under a sent line is Uncertain; later sends are NotSent', async () => {
  const socketPath = tmpSocket('die');
  const stop = await startMock(socketPath, 'die');
  const connection = await DaemonConnection.connect(socketPath);
  const underDeath = connection.request({ type: 'get_connection_state' }, 5000);
  await assert.rejects(underDeath, (error) => error instanceof UncertainError);
  await assert.rejects(
    connection.request({ type: 'get_connection_state' }, 5000),
    (error) => error instanceof NotSentError,
  );
  connection.close();
  await stop();
});

test('the pending cap refuses as a provable NotSent', async () => {
  const socketPath = tmpSocket('cap');
  const stop = await startMock(socketPath, 'silent');
  const connection = await DaemonConnection.connect(socketPath, { maxPending: 1 });
  const first = connection.request({ type: 'get_connection_state' }, 500);
  const second = connection.request({ type: 'get_connection_state' }, 500);
  await assert.rejects(second, (error) => error instanceof NotSentError);
  await first.catch(() => {});
  connection.close();
  await stop();
});

test('close tears down an in-flight request as Uncertain with cause closed', async () => {
  const socketPath = tmpSocket('close-inflight');
  const stop = await startMock(socketPath, 'silent');
  const connection = await DaemonConnection.connect(socketPath);
  const inFlight = connection.request({ type: 'get_connection_state' }, 60_000);
  connection.close();
  await assert.rejects(
    inFlight,
    (error) => error instanceof UncertainError && error.cause === 'closed',
  );
  await stop();
});
