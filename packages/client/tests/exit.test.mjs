import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';

// Natural-exit-after-explicit-close: the child connects, exchanges a
// request and frames, closes, and then simply ENDS. No process.exit in the
// child. If the binding leaked a task, channel, or event-loop handle, the
// child would hang and the timeout below fails the suite.
// The GC-drop-without-close case is deliberately NOT here: pre
// shutdown-PR it leaks the connection by documented interim semantics.
const childScript = `
  const { DaemonConnection } = await import(process.env.CLIENT_DIST);
  const { startMock, tmpSocket } = await import(process.env.CLIENT_HELPERS);
  const socketPath = tmpSocket('exit-child');
  const stop = await startMock(socketPath, 'events');
  const connection = await DaemonConnection.connect(socketPath);
  await connection.request({ type: 'get_connection_state', activeSessionId: 'wire' });
  await connection.nextFrame();
  connection.close();
  await stop();
  // The event loop drains on its own; the process exits naturally.
`;

test('a child process that explicitly closed exits naturally', async () => {
  const child = spawn(
    process.execPath,
    ['--input-type=module', '--eval', childScript],
    {
      env: {
        ...process.env,
        CLIENT_DIST: new URL('../dist/index.js', import.meta.url).href,
        CLIENT_HELPERS: new URL('./helpers.mjs', import.meta.url).href,
      },
      stdio: ['ignore', 'inherit', 'inherit'],
    },
  );
  const exitCode = await Promise.race([
    new Promise((resolve) => child.on('exit', (code) => resolve(code))),
    new Promise((resolve) => setTimeout(() => resolve('timeout'), 30_000)),
  ]);
  if (exitCode === 'timeout') {
    child.kill('SIGKILL');
    assert.fail('child did not exit naturally within 30s after explicit close');
  }
  assert.equal(exitCode, 0);
});
