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
  // An echo mock pushes no frames: the first pull stays genuinely pending
  // so the concurrent pull hits the single-consumer gate.
  const stop = await startMock(socketPath, 'echo');
  const connection = await DaemonConnection.connect(socketPath);
  // Handlers attach at creation: the two native calls may acquire the
  // pending-poll slot in EITHER order, so whichever loses must never
  // reject through an unhandled window.
  const first = connection.nextFrame().then((frame) => ({ frame }), (error) => ({ error }));
  const second = connection.nextFrame().then((frame) => ({ frame }), (error) => ({ error }));
  // The echo mock pushes no frames: the owner parks, so the first settle
  // is always the loser's prompt single-consumer rejection.
  const loser = await Promise.race([first, second]);
  assert.ok(loser.error && /single-consumer/.test(loser.error.message));
  // The echo mock never sends a frame: close is what ends the owned pull.
  connection.close();
  const [a, b] = await Promise.all([first, second]);
  const owner = a.error === undefined ? a : b;
  assert.equal(owner.frame, null);
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
