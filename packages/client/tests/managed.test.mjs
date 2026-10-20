import test from 'node:test';
import assert from 'node:assert/strict';
import {
  ManagedDaemonSession,
  NotSentError,
  RejectedError,
  TransportError,
} from '../dist/index.js';
import { startMock, startMockReporting, tmpSocket } from './helpers.mjs';

const OPTIONS = { restoreAttemptTimeoutMs: 30_000 };
const ATTACH = { type: 'attach', activeSessionId: 'active-1' };

/** Pull events until one matches; bounded so a broken stream fails fast. */
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

async function untilConnected(session) {
  return untilEvent(
    session,
    (event) => event.type === 'status' && event.status.state === 'connected',
  );
}

/**
 * Mock + session scope: both teardowns run even when the body throws, so
 * a failing assertion can never leak the mock server (a leaked server
 * holds the event loop and turns one red test into a hanging battery).
 */
async function withManaged({ name, mode = 'echo', options = OPTIONS }, body) {
  const socketPath = tmpSocket(name);
  const stop = await startMock(socketPath, mode);
  try {
    const session = await ManagedDaemonSession.start({ socketPath, ...options });
    try {
      await body(session, socketPath);
    } finally {
      await session.stop();
    }
  } finally {
    await stop();
  }
}

test('start connects and streams the status lifecycle', async () => {
  await withManaged({ name: 'managed-connect' }, async (session, socketPath) => {
    // The deterministic first statuses: connecting (published at start), then
    // connected (the hello install).
    const first = await session.nextEvent();
    assert.equal(first.type, 'status');
    assert.equal(first.status.state, 'connecting');
    const second = await untilConnected(session);
    assert.equal(second.status.socketPath, socketPath);
    assert.deepEqual(second.status.capabilities, ['direct_peer_transport']);
    const snapshot = session.status();
    assert.equal(snapshot.state, 'connected');
    assert.equal(snapshot.errorCode, undefined);
    assert.ok(!('errorCode' in snapshot) && !('error' in snapshot));
  });
});

test('caps reports the effective configuration', async () => {
  await withManaged({ name: 'managed-caps' }, async (session) => {
    const caps = session.caps();
    assert.equal(caps.eventBuffer, 1024);
    assert.equal(caps.maxFrameBytes, 64 * 1024 * 1024);
    assert.equal(caps.maxPending, 128);
    assert.equal(caps.restoreAttemptTimeoutMs, 30_000);
    assert.equal(caps.dedupeSessionEvents, true);
    assert.deepEqual(caps.attachCapabilities, ['attach_snapshot', 'event_sequence']);
    assert.equal(caps.desktopReconnectPolicy, true);
    assert.equal(caps.clientId, undefined);
  });
});

test('a zero restore budget is rejected at construction', async () => {
  const socketPath = tmpSocket('managed-zero-budget');
  const stop = await startMock(socketPath);
  try {
    await assert.rejects(
      ManagedDaemonSession.start({ socketPath, restoreAttemptTimeoutMs: 0 }),
      (error) => error instanceof TransportError && error.kind === 'Transport',
    );
  } finally {
    await stop();
  }
});

test('an explicit non-desktop reconnect policy is honored', async () => {
  await withManaged(
    {
      name: 'managed-explicit-policy',
      options: {
        ...OPTIONS,
        reconnect: {
          lost: { initialDelayMs: 5_000, maxDelayMs: 30_000 },
          update: null,
        },
      },
    },
    async (session) => {
      const caps = session.caps();
      assert.equal(caps.desktopReconnectPolicy, false);
    },
  );
});

test('frames stream verbatim, unknown frame types included', async () => {
  await withManaged({ name: 'managed-frames', mode: 'events' }, async (session) => {
    await untilConnected(session);
    const frames = [];
    for (let i = 0; i < 4; i += 1) {
      const event = await untilEvent(session, (event) => event.type === 'frame');
      frames.push(event.frame);
    }
    assert.equal(frames.length, 4);
    assert.equal(frames[0].type, 'session_event');
    assert.equal(frames[0].event.type, 'assistant_text');
    assert.deepEqual(frames[0].meta, { sequence: 1 });
    assert.equal(frames[3].type, 'a_future_frame_the_tui_does_not_know');
  });
});

test('request and requestOk speak the serde boundary honestly', async () => {
  await withManaged({ name: 'managed-request' }, async (session) => {
    await untilConnected(session);
    const data = await session.requestOk(ATTACH);
    assert.equal(data.echoed.type, 'attach');
    assert.equal(data.echoed.activeSessionId, 'active-1');
    const response = await session.request({ type: 'get_connection_state', activeSessionId: 'active-1' });
    assert.equal(response.success, true);
  });
});

test('a daemon refusal surfaces Rejected with errorInfo preserved', async () => {
  await withManaged({ name: 'managed-rejected', mode: 'refuse' }, async (session) => {
    await untilConnected(session);
    await assert.rejects(
      session.requestOk(ATTACH),
      (error) =>
        error instanceof RejectedError &&
        error.errorInfo.code === 'session_already_active',
    );
  });
});

test('attachments register and forget without transport effect', async () => {
  await withManaged({ name: 'managed-attachments' }, async (session) => {
    await untilConnected(session);
    // Idempotent per id, and forgetting an unknown id is a no-op.
    session.registerAttachment('active-1');
    session.registerAttachment('active-1');
    session.registerAttachment('active-1', ['attach_snapshot']);
    session.forgetAttachment('active-1');
    session.forgetAttachment('never-registered');
    await session.requestOk(ATTACH);
  });
});

test('stop settles honestly: requests after are NotSent, the stream ends null', async () => {
  const socketPath = tmpSocket('managed-stop');
  // The reporting mock OBSERVES the client-side socket EOF and stays
  // alive: the stop's connection release is proven client-driven.
  const mock = await startMockReporting(socketPath);
  try {
    const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
    try {
      await untilConnected(session);
      const stops = [session.stop(), session.stop(), session.stop()];
      await Promise.all(stops);
      await mock.waitForDisconnect();
      assert.equal(session.status().state, 'disconnected');
      // Drain-then-null: events queued BEFORE the teardown mark (like the
      // final disconnected stop status) legally drain first; the stream
      // must end null within that bounded buffer.
      let end = await session.nextEvent();
      for (let i = 0; i < 4 && end !== null; i += 1) {
        assert.equal(end.type, 'status');
        end = await session.nextEvent();
      }
      assert.equal(end, null);
      // Post-stop requests are the provable non-delivery with the cause.
      await assert.rejects(
        session.request(ATTACH),
        (error) => error instanceof NotSentError && error.cause === 'closed',
      );
    } finally {
      // A late stop observes the settled completion instantly.
      await session.stop();
    }
  } finally {
    await mock.stop();
  }
});

test('a connect failure surfaces the structural error code', async () => {
  // A socket path that does not exist: the first connect failure carries
  // the structural not_found code — never a message parse.
  const socketPath = tmpSocket('managed-missing-path');
  const session = await ManagedDaemonSession.start({
    socketPath: `${socketPath}.missing`,
    ...OPTIONS,
  });
  try {
    const status = await untilEvent(
      session,
      (event) =>
        event.type === 'status' &&
        event.status.state === 'disconnected' &&
        typeof event.status.errorCode === 'string',
    );
    assert.equal(status.status.errorCode, 'not_found');
    assert.equal(session.status().errorCode, 'not_found');
  } finally {
    await session.stop();
  }
});

test('a dead socket surfaces connection_refused', async () => {
  const socketPath = tmpSocket('managed-dead-socket');
  // A plain FILE at the socket path: nothing listens.
  const { writeFileSync } = await import('node:fs');
  writeFileSync(socketPath, '');
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  try {
    const status = await untilEvent(
      session,
      (event) =>
        event.type === 'status' &&
        event.status.state === 'disconnected' &&
        typeof event.status.errorCode === 'string',
    );
    assert.equal(status.status.errorCode, 'connection_refused');
  } finally {
    await session.stop();
  }
});
