import test from 'node:test';
import assert from 'node:assert/strict';
import { ManagedDaemonSession } from '../dist/index.js';
import { startMock, tmpSocket } from './helpers.mjs';

const OPTIONS = { restoreAttemptTimeoutMs: 30_000 };
const ATTACH = { type: 'attach', activeSessionId: 'active-1' };

async function untilEvent(session, predicate, capMs = 20_000) {
  const timer = new Promise((resolve) => setTimeout(() => resolve('timeout'), capMs));
  for (;;) {
    const pull = session.nextEvent();
    const event = await Promise.race([pull, timer]);
    if (event === 'timeout') {
      throw new Error(`no matching event arrived within ${capMs}ms`);
    }
    if (predicate(event)) return event;
  }
}

test('a loss reconnects and restores the registered attachment', async () => {
  const socketPath = tmpSocket('managed-reattach');
  // recycle: the first connection dies ~250ms after its first answered
  // command; the driver reconnects and RESTORES.
  const stop = await startMock(socketPath, 'recycle');
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  await untilEvent(
    session,
    (event) => event.type === 'status' && event.status.state === 'connected',
  );
  // Register and adopt through the request path: the restore needs the
  // registry entry.
  session.registerAttachment('active-1');
  await session.requestOk(ATTACH);
  // The scripted death; the driver reconnects and restores.
  const disconnected = await untilEvent(
    session,
    (event) => event.type === 'status' && event.status.state === 'disconnected',
  );
  assert.equal(disconnected.status.state, 'disconnected');
  const begin = await untilEvent(session, (event) => event.type === 'reattachBegin');
  assert.equal(begin.activeSessionId, 'active-1');
  const reattached = await untilEvent(session, (event) => event.type === 'reattached');
  assert.equal(reattached.activeSessionId, 'active-1');
  assert.equal(reattached.data.echoed.type, 'attach');
  await untilEvent(
    session,
    (event) => event.type === 'status' && event.status.state === 'connected',
  );
  await session.stop();
  await stop();
});

test('a forgotten attachment is not restored after a loss', async () => {
  const socketPath = tmpSocket('managed-forget-restore');
  const stop = await startMock(socketPath, 'recycle');
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  await untilEvent(
    session,
    (event) => event.type === 'status' && event.status.state === 'connected',
  );
  session.registerAttachment('active-1');
  await session.requestOk(ATTACH);
  session.forgetAttachment('active-1');
  await untilEvent(
    session,
    (event) => event.type === 'status' && event.status.state === 'disconnected',
  );
  await untilEvent(
    session,
    (event) => event.type === 'status' && event.status.state === 'connected',
  );
  // No reattach events for the forgotten id may arrive: drain a bounded
  // window of quiet events and assert none matched.
  const quiet = [];
  for (let i = 0; i < 4; i += 1) {
    const event = await session.nextEvent();
    if (event === null) break;
    quiet.push(event);
  }
  assert.ok(
    quiet.every(
      (event) => event.type !== 'reattachBegin' && event.type !== 'reattached',
    ),
    `a forgotten attachment was restored: ${JSON.stringify(quiet)}`,
  );
  await session.stop();
  await stop();
});

test('replayed session events are deduped for the registered attachment', async () => {
  const socketPath = tmpSocket('managed-dedupe');
  // replay: the trigger command answers, then seq 1, a DUPLICATE seq 1,
  // and seq 2 stream down.
  const stop = await startMock(socketPath, 'replay');
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  await untilEvent(
    session,
    (event) => event.type === 'status' && event.status.state === 'connected',
  );
  session.registerAttachment('active-1');
  await session.requestOk({ type: 'get_connection_state', activeSessionId: 'active-1', replayTrigger: true });
  const sequences = [];
  for (let i = 0; i < 2; i += 1) {
    const event = await untilEvent(session, (event) => event.type === 'frame');
    sequences.push(event.frame.meta.sequence);
  }
  // The duplicate was suppressed: exactly seq 1 and seq 2 delivered.
  assert.deepEqual(sequences, [1, 2]);
  await session.stop();
  await stop();
});
