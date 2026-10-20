import test from 'node:test';
import assert from 'node:assert/strict';
import { DaemonConnection } from '../dist/index.js';
import { startMock, tmpSocket } from './helpers.mjs';

test('nextFrame yields raw frames verbatim, unknown types included, in order', async () => {
  const socketPath = tmpSocket('events');
  const stop = await startMock(socketPath, 'events');
  const connection = await DaemonConnection.connect(socketPath);
  const sessionEvent = await connection.nextFrame();
  assert.deepEqual(sessionEvent, {
    type: 'session_event',
    activeSessionId: 'active-1',
    event: { type: 'assistant_text', text: 'hello' },
    meta: { sequence: 1 },
  });
  const listItem = await connection.nextFrame();
  assert.deepEqual(listItem, {
    type: 'session_list_item',
    id: 'daemon_list',
    session: { sessionId: 'saved-1', name: 'Saved session' },
  });
  const roster = await connection.nextFrame();
  assert.deepEqual(roster, { type: 'roster_update', changed: [], removed: [], resync: true });
  const unknown = await connection.nextFrame();
  assert.deepEqual(unknown, {
    type: 'a_future_frame_the_tui_does_not_know',
    payload: { anything: { nested: [1, 2] } },
    meta: { sequence: 2, source: 'worker' },
  });
  connection.close();
  const end = await connection.nextFrame();
  assert.equal(end, null);
  await stop();
});

test('nextFrame is single-consumer: a concurrent pull rejects', async () => {
  const socketPath = tmpSocket('single');
  const stop = await startMock(socketPath, 'events');
  const connection = await DaemonConnection.connect(socketPath);
  const pending = connection.nextFrame();
  await assert.rejects(connection.nextFrame(), (error) => /single-consumer/.test(error.message));
  await pending;
  connection.close();
  await stop();
});

test('close wakes a pending nextFrame with end-of-stream', async () => {
  const socketPath = tmpSocket('wake');
  const stop = await startMock(socketPath, 'echo');
  const connection = await DaemonConnection.connect(socketPath);
  const pending = connection.nextFrame();
  connection.close();
  assert.equal(await pending, null);
  await stop();
});
