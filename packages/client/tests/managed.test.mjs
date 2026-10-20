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

test('start connects and streams the status lifecycle', async () => {
  const socketPath = tmpSocket('managed-connect');
  const stop = await startMock(socketPath);
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
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
  await session.stop();
  await stop();
});

test('caps reports the effective configuration', async () => {
  const socketPath = tmpSocket('managed-caps');
  const stop = await startMock(socketPath);
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  const caps = session.caps();
  assert.equal(caps.eventBuffer, 1024);
  assert.equal(caps.maxFrameBytes, 64 * 1024 * 1024);
  assert.equal(caps.maxPending, 128);
  assert.equal(caps.restoreAttemptTimeoutMs, 30_000);
  assert.equal(caps.dedupeSessionEvents, true);
  assert.deepEqual(caps.attachCapabilities, ['attach_snapshot', 'event_sequence']);
  assert.equal(caps.desktopReconnectPolicy, true);
  assert.equal(caps.clientId, undefined);
  await session.stop();
  await stop();
});

test('a zero restore budget is rejected at construction', async () => {
  const socketPath = tmpSocket('managed-zero-budget');
  const stop = await startMock(socketPath);
  await assert.rejects(
    ManagedDaemonSession.start({ socketPath, restoreAttemptTimeoutMs: 0 }),
    (error) => error instanceof TransportError && error.kind === 'Transport',
  );
  await stop();
});

test('an explicit non-desktop reconnect policy is honored', async () => {
  const socketPath = tmpSocket('managed-explicit-policy');
  const stop = await startMock(socketPath);
  const session = await ManagedDaemonSession.start({
    socketPath,
    ...OPTIONS,
    reconnect: {
      lost: { initialDelayMs: 5_000, maxDelayMs: 30_000 },
      update: null,
    },
  });
  const caps = session.caps();
  assert.equal(caps.desktopReconnectPolicy, false);
  await session.stop();
  await stop();
});

test('frames stream verbatim, unknown frame types included', async () => {
  const socketPath = tmpSocket('managed-frames');
  const stop = await startMock(socketPath, 'events');
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
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
  await session.stop();
  await stop();
});

test('request and requestOk speak the serde boundary honestly', async () => {
  const socketPath = tmpSocket('managed-request');
  const stop = await startMock(socketPath);
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  await untilConnected(session);
  const data = await session.requestOk(ATTACH);
  assert.equal(data.echoed.type, 'attach');
  assert.equal(data.echoed.activeSessionId, 'active-1');
  const response = await session.request({ type: 'get_connection_state', activeSessionId: 'active-1' });
  assert.equal(response.success, true);
  await session.stop();
  await stop();
});

test('a daemon refusal surfaces Rejected with errorInfo preserved', async () => {
  const socketPath = tmpSocket('managed-rejected');
  const stop = await startMock(socketPath, 'refuse');
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  await untilConnected(session);
  await assert.rejects(
    session.requestOk(ATTACH),
    (error) =>
      error instanceof RejectedError &&
      error.errorInfo.code === 'session_already_active',
  );
  await session.stop();
  await stop();
});

test('attachments register and forget without transport effect', async () => {
  const socketPath = tmpSocket('managed-attachments');
  const stop = await startMock(socketPath);
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  await untilConnected(session);
  // Idempotent per id, and forgetting an unknown id is a no-op.
  session.registerAttachment('active-1');
  session.registerAttachment('active-1');
  session.registerAttachment('active-1', ['attach_snapshot']);
  session.forgetAttachment('active-1');
  session.forgetAttachment('never-registered');
  await session.requestOk(ATTACH);
  await session.stop();
  await stop();
});

test('stop settles honestly: requests after are NotSent, the stream ends null', async () => {
  const socketPath = tmpSocket('managed-stop');
  // The reporting mock OBSERVES the client-side socket EOF and stays
  // alive: the stop's connection release is proven client-driven.
  const mock = await startMockReporting(socketPath);
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  await untilConnected(session);
  const stops = [session.stop(), session.stop(), session.stop()];
  await Promise.all(stops);
  await mock.waitForDisconnect();
  assert.equal(session.status().state, 'disconnected');
  // The teardown mark ended the stream: null, immediately.
  assert.equal(await session.nextEvent(), null);
  // Post-stop requests are the provable non-delivery with the cause.
  await assert.rejects(
    session.request(ATTACH),
    (error) => error instanceof NotSentError && error.cause === 'closed',
  );
  // A late stop observes the settled completion instantly.
  await session.stop();
  await mock.stop();
});

test('a connect failure surfaces the structural error code', async () => {
  // A socket path that does not exist: the first connect failure carries
  // the structural not_found code — never a message parse.
  const socketPath = tmpSocket('managed-missing-path');
  const session = await ManagedDaemonSession.start({
    socketPath: `${socketPath}.missing`,
    ...OPTIONS,
  });
  const status = await untilEvent(
    session,
    (event) =>
      event.type === 'status' &&
      event.status.state === 'disconnected' &&
      typeof event.status.errorCode === 'string',
  );
  assert.equal(status.status.errorCode, 'not_found');
  assert.equal(session.status().errorCode, 'not_found');
  await session.stop();
});

test('a dead socket surfaces connection_refused', async () => {
  const socketPath = tmpSocket('managed-dead-socket');
  // A plain FILE at the socket path: nothing listens.
  const { writeFileSync } = await import('node:fs');
  writeFileSync(socketPath, '');
  const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS });
  const status = await untilEvent(
    session,
    (event) =>
      event.type === 'status' &&
      event.status.state === 'disconnected' &&
      typeof event.status.errorCode === 'string',
  );
  assert.equal(status.status.errorCode, 'connection_refused');
  await session.stop();
});
