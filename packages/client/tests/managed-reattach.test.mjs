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

/**
 * Mock + session scope: both teardowns run even when the body throws, so
 * a failing assertion can never leak the mock server into a hanging file.
 */
async function withManaged({ name, mode }, body) {
  const socketPath = tmpSocket(name);
  const stop = await startMock(socketPath, mode);
  try {
    const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
    try {
      await body(session);
    } finally {
      await session.stop();
    }
  } finally {
    await stop();
  }
}

test('a loss reconnects and restores the registered attachment', async () => {
  // recycle: the first connection dies ~250ms after its first answered
  // command; the driver reconnects and RESTORES.
  await withManaged({ name: 'managed-reattach', mode: 'recycle' }, async (session) => {
    await untilEvent(
      session,
      (event) => event.type === 'status' && event.status.state === 'connected',
    );
    // Register and adopt through the request path: the restore needs the
    // registry entry.
    session.registerAttachment('active-1');
    await session.requestOk(ATTACH);
    // The scripted death; the core arms the reconnect WITHOUT a
    // disconnected status (that state is the connect-failure witness):
    // the loss shows as the next connecting status, then the restore.
    await untilEvent(
      session,
      (event) => event.type === 'status' && event.status.state === 'connecting',
    );
    await untilEvent(
      session,
      (event) => event.type === 'status' && event.status.state === 'connected',
    );
    const begin = await untilEvent(session, (event) => event.type === 'reattachBegin');
    assert.equal(begin.activeSessionId, 'active-1');
    const reattached = await untilEvent(session, (event) => event.type === 'reattached');
    assert.equal(reattached.activeSessionId, 'active-1');
    assert.equal(reattached.data.echoed.type, 'attach');
  });
});

test('a forgotten attachment is not restored after a loss', async () => {
  await withManaged({ name: 'managed-forget-restore', mode: 'recycle' }, async (session) => {
    await untilEvent(
      session,
      (event) => event.type === 'status' && event.status.state === 'connected',
    );
    session.registerAttachment('active-1');
    await session.requestOk(ATTACH);
    session.forgetAttachment('active-1');
    // The loss shows as connecting (no disconnected status on an armed
    // reconnect), then the fresh connection with nothing to restore.
    await untilEvent(
      session,
      (event) => event.type === 'status' && event.status.state === 'connecting',
    );
    await untilEvent(
      session,
      (event) => event.type === 'status' && event.status.state === 'connected',
    );
    // No reattach events for the forgotten id may arrive: drain a bounded
    // QUIET window (the idle session emits nothing further, so the last
    // pull stays pending and is OWNED through the stop below — never a
    // dangling concurrent pull).
    let pending = null;
    const quiet = [];
    for (let i = 0; i < 4; i += 1) {
      if (pending === null) pending = session.nextEvent();
      const event = await Promise.race([
        pending,
        new Promise((resolve) => setTimeout(() => resolve('quiet'), 1_500)),
      ]);
      if (event === 'quiet') break;
      pending = null;
      if (event === null) break;
      quiet.push(event);
    }
    assert.ok(
      quiet.every(
        (event) => event.type !== 'reattachBegin' && event.type !== 'reattached',
      ),
      `a forgotten attachment was restored: ${JSON.stringify(quiet)}`,
    );
    if (pending !== null) {
      // The teardown mark ends the stream: the owned pull settles null.
      const [ended] = await Promise.all([pending, session.stop()]);
      assert.equal(ended, null);
    }
  });
});

test('replayed session events are deduped for the registered attachment', async () => {
  // replay: the trigger command answers, then seq 1, a DUPLICATE seq 1,
  // and seq 2 stream down.
  await withManaged({ name: 'managed-dedupe', mode: 'replay' }, async (session) => {
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
  });
});
