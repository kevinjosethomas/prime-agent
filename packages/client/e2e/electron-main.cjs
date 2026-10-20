/**
 * Electron 44 main-process PROTOCOL proof (not an exit proof): runs the
 * real battery surface inside Electron's main process — provenance-verified
 * load, connect/handshake, a request round-trip, the overflow fail-closed
 * scenario, status transitions, and close — then quits through the natural
 * app lifecycle. The event-loop natural-exit property is proven by the
 * Node battery's exit test (no app.quit masking there); Electron's own
 * exit here is the app lifecycle, labeled as such.
 */
const { app } = require('electron');
const { join } = require('node:path');
const { pathToFileURL } = require('node:url');

async function main() {
  const dist = pathToFileURL(join(__dirname, '..', 'dist', 'index.js')).href;
  const helpers = pathToFileURL(join(__dirname, '..', 'tests', 'helpers.mjs')).href;
  const { DaemonConnection, OverflowError, UncertainError } = await import(dist);
  const { startMock, tmpSocket } = await import(helpers);

  // 1. Connect + request round-trip against the echo mock.
  const echoPath = tmpSocket('electron-echo');
  const stopEcho = await startMock(echoPath, 'echo');
  const connection = await DaemonConnection.connect(echoPath);
  const response = await connection.request({ type: 'get_connection_state' });
  if (response.success !== true) throw new Error('request failed in Electron main');
  connection.close();

  // 2. The overflow fail-closed scenario in the flood mock.
  const floodPath = tmpSocket('electron-overflow');
  const stopFlood = await startMock(floodPath, 'flood');
  const flooded = await DaemonConnection.connect(floodPath, { frameBuffer: 2 });
  let overflowed = false;
  try {
    while (true) {
      // Drain frames until the terminal overflow error surfaces.
      await flooded.nextFrame();
    }
  } catch (error) {
    if (!(error instanceof OverflowError)) throw error;
    overflowed = true;
  }
  if (!overflowed) throw new Error('overflow never surfaced in Electron main');
  const inflight = flooded.request({ type: 'get_connection_state' }, 60_000);
  await inflight.catch((error) => {
    if (!(error instanceof UncertainError) || error.cause !== 'overflow') throw error;
  });
  flooded.close();
  await stopFlood();
  await stopEcho();

  // The natural-exit proofs run as ELECTRON_RUN_AS_NODE children spawned
  // from the Electron host: each child exits with NO app.quit and NO
  // process.exit - the addon releasing the Node loop is the exit proof.
  // The host itself ends through the app lifecycle (app.quit only here).
  const { spawn } = require('node:child_process');
  const { join } = require('node:path');
  const childScript = (kind) => `
    globalThis.gc?.();
    const { DaemonConnection } = await import(${JSON.stringify(pathToFileURL(join(__dirname, '..', 'dist', 'index.js')).href)});
    const { startMock, tmpSocket } = await import(${JSON.stringify(pathToFileURL(join(__dirname, '..', 'tests', 'helpers.mjs')).href)});
    const socketPath = tmpSocket('electron-${kind}');
    const stop = await startMock(socketPath, 'echo');
    ${
      kind === 'close'
        ? `const connection = await DaemonConnection.connect(socketPath);
           await connection.request({ type: 'get_connection_state', activeSessionId: 'wire' });
           await connection.close();`
        : `{
             const connection = await DaemonConnection.connect(socketPath);
             await connection.request({ type: 'get_connection_state', activeSessionId: 'wire' });
           }
           globalThis.gc();`
    }
    await stop();
  `;
  const runChild = (kind) =>
    new Promise((resolve) => {
      const child = spawn(
        process.execPath,
        ['--expose-gc', '--input-type=module', '--eval', childScript(kind)],
        { env: { ...process.env, ELECTRON_RUN_AS_NODE: '1' }, stdio: ['ignore', 'inherit', 'inherit'] },
      );
      const guard = setTimeout(() => {
        child.kill('SIGKILL');
        resolve('timeout');
      }, 30000);
      child.on('exit', (code) => {
        clearTimeout(guard);
        resolve(code);
      });
    });
  const closeExit = await runChild('close');
  const gcExit = await runChild('gc');
  console.log(
    'ELECTRON_MAIN_PROTOCOL_OK',
    process.versions.electron,
    'napi',
    process.versions.napi,
    'child-exit-code',
    closeExit,
    'gc-exit-code',
    gcExit,
  );
  app.quit();
}

app.whenReady().then(main).catch((error) => {
  console.error(error);
  process.exitCode = 1;
  app.quit();
});
