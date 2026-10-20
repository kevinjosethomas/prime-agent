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
  const { startMock, tmpSocket } = await import(process.env.CLIENT_HELPERS);
  const socketPath = tmpSocket('gc-child');
  const stop = await startMock(socketPath, 'echo');
  {
    // The scoped block lets the handle die on scope exit; the explicit GC
    // pass then drives the napi finalizer's scheduled cleanup.
    const connection = await DaemonConnection.connect(socketPath);
    await connection.request({ type: 'get_connection_state', activeSessionId: 'wire' });
  }
  globalThis.gc();
  await stop();
  // The dropped connection's cleanup runs on the runtime; the socket gets
  // EOF when the mock exits; the loop drains with nothing holding it.
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
