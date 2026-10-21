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

/**
 * Mock + session scope: both teardowns run even when the body throws, so
 * a failing assertion can never leak the mock server (or its flood timer)
 * into a hanging file. The late stop on an overflowed session is the
 * settled-completion observation — idempotent by contract.
 */
async function withManaged({ name, mode, options }, body) {
  const socketPath = tmpSocket(name);
  const stop = await startMock(socketPath, mode);
  try {
    const session = await ManagedDaemonSession.start({ socketPath, ...OPTIONS, ...options });
    try {
      await body(session);
    } finally {
      await session.stop();
    }
  } finally {
    await stop();
  }
}

/** Pull until the sticky OverflowError surfaces; pre-mark events may drain first. */
async function pullUntilOverflow(session) {
  for (let i = 0; i < 512; i += 1) {
    try {
      const event = await session.nextEvent();
      if (event === null) return false;
    } catch (error) {
      if (!(error instanceof OverflowError)) throw error;
      return true;
    }
  }
  return false;
}

test('a full event queue tears the session down with a sticky Overflow', async () => {
  await withManaged(
    { name: 'managed-overflow', mode: 'flood', options: { eventBuffer: 2 } },
    async (session) => {
      await untilConnected(session);
      // The mock floods; the pump fills the bounded event queue and fails
      // closed. Events queued before the mark may drain first — then the
      // sticky OverflowError surfaces and no further event is observable.
      assert.ok(await pullUntilOverflow(session), 'the terminal overflow error never surfaced');
      // Sticky: every later pull observes the terminal error, not a stream.
      await assert.rejects(session.nextEvent(), (error) => error instanceof OverflowError);
      // The overflow scheduled the full stop: awaited, it settles honestly.
      await session.stop();
      assert.equal(session.status().state, 'disconnected');
    },
  );
});

test('a parked nextEvent pull observes the sticky overflow', async () => {
  await withManaged(
    // eventBuffer 2, not 1: the managed queue also carries the two
    // initial statuses (connecting, connected) — a 1-slot queue can
    // truthfully overflow before the connect drain ever runs.
    { name: 'managed-overflow-parked', mode: 'flood-trigger', options: { eventBuffer: 2 } },
    async (session) => {
      await untilConnected(session);
      // The gated mock sent hello ONLY: one pull parks against an empty
      // queue — provably pending before any flood. The two native calls
      // may acquire the pending poll in EITHER order, so the proof is
      // symmetric: the loser rejects single-consumer promptly, the owner
      // stays parked.
      const pulls = [
        session.nextEvent().then(
          (event) => ({ index: 0, event }),
          (error) => ({ index: 0, error }),
        ),
        session.nextEvent().then(
          (event) => ({ index: 1, event }),
          (error) => ({ index: 1, error }),
        ),
      ];
      const loser = await Promise.race(pulls);
      assert.ok(loser.error && /single-consumer/.test(loser.error.message));
      const parked = pulls[1 - loser.index];
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
    },
  );
});

test('in-flight requests at overflow stay Uncertain with cause overflow', async () => {
  await withManaged(
    { name: 'managed-overflow-inflight', mode: 'flood-trigger', options: { eventBuffer: 2 } },
    async (session) => {
      await untilConnected(session);
      // The gated trigger request is in flight when the flood fills the queue
      // (the mock delays the response): the in-flight line was queued and must
      // settle Uncertain with cause overflow — never re-classified.
      const settled = await session
        .request({ type: 'get_connection_state', activeSessionId: 'wire', floodTrigger: true })
        .then(
          (value) => value,
          (error) => error,
        );
      assert.ok(
        settled instanceof UncertainError && settled.cause === 'overflow',
        `unexpected class: ${settled?.kind}:${settled?.cause}`,
      );
    },
  );
});

test('new requests after overflow are NotSent with the overflow cause surfaced', async () => {
  await withManaged(
    { name: 'managed-overflow-notsent', mode: 'flood', options: { eventBuffer: 2 } },
    async (session) => {
      await untilConnected(session);
      assert.ok(await pullUntilOverflow(session), 'the terminal overflow error never surfaced');
      await assert.rejects(
        session.request({ type: 'get_connection_state', activeSessionId: 'wire' }),
        (error) => error instanceof NotSentError && error.cause === 'overflow',
      );
    },
  );
});
