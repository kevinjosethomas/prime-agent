import test from 'node:test';
import assert from 'node:assert/strict';
import { DaemonConnection, NotSentError, TransportError } from '../dist/index.js';
import { startMock, tmpSocket } from './helpers.mjs';

test('connect completes the hello handshake', async () => {
  const socketPath = tmpSocket('connect');
  const stop = await startMock(socketPath);
  const connection = await DaemonConnection.connect(socketPath);
  const hello = connection.hello();
  assert.equal(hello.type, 'daemon_hello');
  assert.deepEqual(hello.protocol, { name: 'prime-agent.daemon', version: 7 });
  assert.equal(connection.protocol().name, 'prime-agent.daemon');
  assert.equal(connection.supportsServerCapability('direct_peer_transport'), true);
  assert.equal(connection.supportsServerCapability('made_up_capability'), false);
  assert.equal(connection.socketPath(), socketPath);
  const caps = connection.caps();
  assert.equal(caps.frameBuffer, 1024);
  assert.equal('maxFrameBytes' in caps, false);
  assert.equal('maxPending' in caps, false);
  assert.equal(connection.currentStatus().type, 'open');
  connection.close();
  await stop();
});

test('connect rejects invalid option values', async () => {
  const socketPath = tmpSocket('invalid');
  const stop = await startMock(socketPath);
  await assert.rejects(
    DaemonConnection.connect(socketPath, { frameBuffer: 0 }),
    (error) => error instanceof TransportError && error.kind === 'Transport',
  );
  await stop();
});

test('requests after close fail as provable NotSent', async () => {
  const socketPath = tmpSocket('closed');
  const stop = await startMock(socketPath);
  const connection = await DaemonConnection.connect(socketPath);
  connection.close();
  await assert.rejects(
    connection.request({ type: 'get_connection_state', activeSessionId: 'active-1' }),
    (error) => error instanceof NotSentError && error.kind === 'NotSent',
  );
  await stop();
});
