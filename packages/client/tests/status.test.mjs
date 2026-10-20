import test from 'node:test';
import assert from 'node:assert/strict';
import { DaemonConnection } from '../dist/index.js';
import { startMock, tmpSocket } from './helpers.mjs';

test('nextStatus returns open immediately, then the close change', async () => {
  const socketPath = tmpSocket('status');
  const stop = await startMock(socketPath);
  const connection = await DaemonConnection.connect(socketPath);
  assert.deepEqual(await connection.nextStatus(), { type: 'open' });
  const change = connection.nextStatus();
  connection.close();
  const status = await change;
  assert.equal(status.type, 'closed');
  connection.close();
  await stop();
});

test('a terminal status returned to a late caller without waiting', async () => {
  const socketPath = tmpSocket('status-late');
  const stop = await startMock(socketPath);
  const connection = await DaemonConnection.connect(socketPath);
  connection.close();
  await connection.nextStatus();
  const late = await connection.nextStatus();
  assert.equal(late.type, 'closed');
  await stop();
});
