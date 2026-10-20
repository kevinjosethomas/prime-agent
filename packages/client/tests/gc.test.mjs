import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';

// GC-drop-then-mock-EOF: the child drops the JS handle WITHOUT close; the
// napi finalizer schedules the cleanup on the owned runtime; the mock exits
// (EOF for the socket); the child process must exit naturally — proving the
// binding's runtime threads and core tasks never hold the Node event loop.
const childScript = `
  globalThis.gc?.();
  const { DaemonConnection } = await import(process.env.CLIENT_DIST);
  const { startMockReporting, tmpSocket } = await import(process.env.CLIENT_HELPERS);
  const socketPath = tmpSocket('gc-child');
  // The reporting mock OBSERVES the client-side socket EOF and stays
  // alive: the GC-driven finalizer cleanup is proven client-driven,
  // never masked by a daemon kill. Bounded repeated GC passes.
  const mock = await startMockReporting(socketPath, 'echo');
  {
    const connection = await DaemonConnection.connect(socketPath);
    await connection.request({ type: 'get_connection_state', activeSessionId: 'wire' });
  }
  let disconnected = false;
  for (let attempt = 0; attempt < 5 && !disconnected; attempt += 1) {
    globalThis.gc();
    disconnected = await Promise.race([
      mock.waitForDisconnect().then(() => true, () => false),
      new Promise((resolve) => setTimeout(() => resolve(false), 2000)),
    ]);
  }
  if (!disconnected) throw new Error('the GC-driven client release was never observed');
  await mock.stop();
  // Natural exit: the loop drains with nothing holding it.
`;

test('GC drop without close exits naturally after the mock EOF', async () => {
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
    new Promise((resolve) => setTimeout(() => resolve('timeout'), 30_000)),
  ]);
  if (outcome === 'timeout') {
    child.kill('SIGKILL');
    assert.fail('GC-dropped child did not exit within 30s');
  }
  assert.equal(outcome, 0);
});
