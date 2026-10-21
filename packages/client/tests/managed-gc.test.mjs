import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';

// GC-drop-then-mock-EOF for the MANAGED session: the child drops the JS
// handle WITHOUT stop; the napi finalizer schedules the full stop chain
// (core stop + driver join + bridge drain) on the owned runtime; the
// reporting mock OBSERVES the client-side socket EOF and stays alive — the
// release is proven client-driven — and the child process exits naturally,
// proving the runtime threads never hold the Node event loop.
const childScript = `
  globalThis.gc?.();
  const { ManagedDaemonSession } = await import(process.env.CLIENT_DIST);
  const { startMockReporting, tmpSocket } = await import(process.env.CLIENT_HELPERS);
  const socketPath = tmpSocket('managed-gc-child');
  const mock = await startMockReporting(socketPath);
  // The mock stop rides finally: an error exit may not orphan the server.
  try {
    // FUNCTION scope: V8 retains module-level bindings, so the wrapper must
    // die on function return for the napi finalizer to ever run.
    await (async () => {
      const session = await ManagedDaemonSession.start({
        socketPath,
        restoreAttemptTimeoutMs: 30_000,
      });
      // start() returns at CONNECTING: drain to connected first, or the
      // pre-poll barrier truthfully refuses the request as NotSent.
      for (;;) {
        const event = await session.nextEvent();
        if (event === null) throw new Error('the stream ended before connect');
        if (event.type === 'status' && event.status.state === 'connected') break;
      }
      await session.request({ type: 'get_connection_state', activeSessionId: 'wire' });
    })();
    let disconnected = false;
    // 8 bounded windows: GC scheduling under a loaded battery runner is
    // slow, not broken — the proof stays the observed client-side EOF.
    for (let attempt = 0; attempt < 8 && !disconnected; attempt += 1) {
      globalThis.gc();
      disconnected = await Promise.race([
        mock.waitForDisconnect().then(() => true, () => false),
        new Promise((resolve) => setTimeout(() => resolve(false), 2000)),
      ]);
    }
    if (!disconnected) throw new Error('the GC-driven client release was never observed');
  } finally {
    await mock.stop();
  }
  // Natural exit: the loop drains with nothing holding it.
`;

test('managed-session GC drop without stop exits naturally after the mock EOF', async () => {
  const child = spawn(
    process.execPath,
    ['--expose-gc', '--input-type=module', '--eval', childScript],
    {
      env: {
        ...process.env,
        CLIENT_DIST: new URL('../dist/index.js', import.meta.url).href,
        CLIENT_HELPERS: new URL('./helpers.mjs', import.meta.url).href,
      },
      stdio: ['ignore', 'inherit', 'inherit'],
    },
  );
  const outcome = await Promise.race([
    new Promise((resolve) => child.on('exit', (code) => resolve(code))),
    new Promise((resolve) => setTimeout(() => resolve('timeout'), 40_000)),
  ]);
  if (outcome === 'timeout') {
    child.kill('SIGKILL');
    assert.fail('GC-dropped child did not exit within 40s');
  }
  assert.equal(outcome, 0);
});
