import test from 'node:test';
import assert from 'node:assert/strict';
import {
  ManagedDaemonSession,
  NotSentError,
  OverflowError,
  UncertainError,
} from '../dist/index.js';
import { startMock, tmpSocket } from './helpers.mjs';

const OPTIONS = { restoreAttemptTimeoutMs: 30_000 };

async function untilConnected(session) {
  const timer = new Promise((resolve) => setTimeout(() => resolve('timeout'), 20_000));
  for (;;) {
    const pull = session.nextEvent();
    const event = await Promise.race([pull, timer]);
    if (event === 'timeout') throw new Error('never connected');
    if (event.type === 'status' && event.status.state === 'connected') return;
  }
}

test('a full event queue tears the session down with a sticky Overflow', async () => {
  const socketPath = tmpSocket('managed-overflow');
  const stop = await startMock(socketPath, 'flood');
  const session = await ManagedDaemonSession.start({
    socketPath,
    eventBuffer: 2,
    ...OPTIONS,
  });
  await untilConnected(session);
  // The mock floods; the pump fills the bounded event queue and fails
  // closed. Events queued before the mark may drain first — then the
  // sticky OverflowError surfaces and no further event is observable.
  let sawOverflow = false;
  for (let i = 0; i < 512; i += 1) {
    try {
      const event = await session.nextEvent();
      if (event === null) break;
    } catch (error) {
      if (!(error instanceof OverflowError)) throw error;
      sawOverflow = true;
      break;
    }
  }
  assert.ok(sawOverflow, 'the terminal overflow error never surfaced');
  // Sticky: every later pull observes the terminal error, not a stream.
  await assert.rejects(session.nextEvent(), (error) => error instanceof OverflowError);
  // The overflow scheduled the full stop: awaited, it settles honestly.
  await session.stop();
  assert.equal(session.status().state, 'disconnected');
  await stop();
});

test('a parked nextEvent pull observes the sticky overflow', async () => {
  const socketPath = tmpSocket('managed-overflow-parked');
  const stop = await startMock(socketPath, 'flood-trigger');
  const session = await ManagedDaemonSession.start({
    socketPath,
    eventBuffer: 1,
    ...OPTIONS,
  });
  await untilConnected(session);
  // The gated mock sent hello ONLY: the pull parks against an empty queue
  // — provably pending before any flood.
  const parked = session.nextEvent().then(
    (event) => ({ event }),
    (error) => ({ error }),
  );
  // The single-consumer probe: a concurrent pull MUST reject while the
  // first is parked.
  const probeError = await session.nextEvent().then(
    () => {
      throw new Error('the probe should have rejected');
    },
    (error) => error,
  );
  assert.ok(/single-consumer/.test(probeError.message));
  // Only NOW the trigger command starts the flood: the in-flight trigger
  // settles Uncertain-cause-overflow; the parked pull settles the sticky
  // terminal branch or a legal pre-mark event.
  const trigger = session
    .request({ type: 'get_connection_state', activeSessionId: 'wire', floodTrigger: true })
    .then(
      (value) => value,
      (error) => error,
    );
  const triggerSettled = await trigger;
  assert.ok(
    triggerSettled instanceof UncertainError && triggerSettled.cause === 'overflow',
    `trigger class: ${triggerSettled?.kind}:${triggerSettled?.cause}`,
  );
  const parkedOutcome = await parked;
  if (parkedOutcome.error !== undefined) {
    assert.ok(parkedOutcome.error instanceof OverflowError);
  }
  // The mark happened: every later pull is the sticky terminal error.
  await assert.rejects(session.nextEvent(), (error) => error instanceof OverflowError);
  await session.stop();
  await stop();
});

test('in-flight requests at overflow stay Uncertain with cause overflow', async () => {
  const socketPath = tmpSocket('managed-overflow-inflight');
  const stop = await startMock(socketPath, 'flood-trigger');
  const session = await ManagedDaemonSession.start({
    socketPath,
    eventBuffer: 2,
    ...OPTIONS,
  });
  await untilConnected(session);
  // The gated trigger request is in flight when the flood fills the queue
  // (the mock delays the response): the in-flight line was queued and must
  // settle Uncertain with cause overflow — never re-classified.
  const inflight = session
    .request({ type: 'get_connection_state', activeSessionId: 'wire', floodTrigger: true })
    .then(
      (value) => value,
      (error) => error,
    );
  const settled = await inflight;
  assert.ok(
    settled instanceof UncertainError && settled.cause === 'overflow',
    `unexpected class: ${settled?.kind}:${settled?.cause}`,
  );
  await session.stop();
  await stop();
});

test('new requests after overflow are NotSent with the overflow cause surfaced', async () => {
  const socketPath = tmpSocket('managed-overflow-notsent');
  const stop = await startMock(socketPath, 'flood');
  const session = await ManagedDaemonSession.start({
    socketPath,
    eventBuffer: 2,
    ...OPTIONS,
  });
  await untilConnected(session);
  let sawOverflow = false;
  for (let i = 0; i < 512 && !sawOverflow; i += 1) {
    try {
      const event = await session.nextEvent();
      if (event === null) break;
    } catch (error) {
      if (!(error instanceof OverflowError)) throw error;
      sawOverflow = true;
    }
  }
  assert.ok(sawOverflow, 'the terminal overflow error never surfaced');
  await assert.rejects(
    session.request({ type: 'get_connection_state', activeSessionId: 'wire' }),
    (error) => error instanceof NotSentError && error.cause === 'overflow',
  );
  await session.stop();
  await stop();
});
