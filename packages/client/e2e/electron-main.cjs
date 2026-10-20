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
const { appendFileSync } = require('node:fs');

// Line-flushed progress log: every stage marker hits the file BEFORE its
// awaits, so a hang leaves the last completed marker on disk.
const LOG = join(__dirname, 'electron-leg.log');
const mark = (m) => {
  appendFileSync(LOG, `${new Date().toISOString()} ${m}\n`);
};
const SECTION = process.env.E2E_SECTION || 'all';

async function main() {
  const { spawn } = require('node:child_process');
  const { join } = require('node:path');
  mark('main-start');
  const dist = pathToFileURL(join(__dirname, '..', 'dist', 'index.js')).href;
  const helpers = pathToFileURL(join(__dirname, '..', 'tests', 'helpers.mjs')).href;
  mark('imports-done');
  const { DaemonConnection, NotSentError, OverflowError, UncertainError } = await import(dist);
  const { startMock, tmpSocket } = await import(helpers);

  // 1. Connect + request round-trip against the echo mock.
  const echoPath = tmpSocket('electron-echo');
  const stopEcho = await startMock(echoPath, 'echo');
  mark('echo-mock-up');
  const connection = await DaemonConnection.connect(echoPath);
  const response = await connection.request({ type: 'get_connection_state' });
  if (response.success !== true) throw new Error('request failed in Electron main');
  mark('echo-closing');
  connection.close();
  mark('echo-closed');

  // 2. The overflow fail-closed scenario in the flood mock. The in-flight
  // request is issued BEFORE the terminal mark (it must settle Uncertain
  // with cause overflow); a request AFTER the mark is the provable
  // NotSent with the overflow cause.
  if (SECTION !== 'children') {
  const floodPath = tmpSocket('electron-overflow');
  const stopFlood = await startMock(floodPath, 'flood');
  mark('flood-mock-up');
  const flooded = await DaemonConnection.connect(floodPath, { frameBuffer: 2 });
  mark('flood-connected');
  mark('premark-request-started');
  const preMarkRequest = flooded
    .request({ type: 'get_connection_state', activeSessionId: 'wire' }, 60_000)
    .then(
      () => {
        throw new Error('the pre-mark request should have torn down');
      },
      (error) => error,
    );
  mark('flood-drain-start');
  let overflowed = false;
  try {
    while (true) {
      await flooded.nextFrame();
    }
  } catch (error) {
    if (!(error instanceof OverflowError)) throw error;
    overflowed = true;
  }
  if (!overflowed) throw new Error('overflow never surfaced in Electron main');
  mark('flood-overflow-marked');
  const tornDown = await preMarkRequest;
  mark('premark-settled');
  if (!(tornDown instanceof UncertainError) || tornDown.cause !== 'overflow') {
    throw new Error(`pre-mark request class: ${tornDown?.kind}:${tornDown?.cause}`);
  }
  const postMark = await flooded
    .request({ type: 'get_connection_state', activeSessionId: 'wire' }, 60_000)
    .then(
      () => {
        throw new Error('the post-mark request should have refused');
      },
      (error) => error,
    );
  if (!(postMark instanceof NotSentError) || postMark.cause !== 'overflow') {
    throw new Error(`post-mark request class: ${postMark?.kind}:${postMark?.cause}`);
  }
  flooded.close();
  mark('flood-closed');
  await stopFlood();
  mark('flood-mock-stopped');
  await stopEcho();
  mark('echo-mock-stopped');
  }
  if (SECTION !== 'protocol') {

  // The natural-exit proofs run as ELECTRON_RUN_AS_NODE children spawned
  // from the Electron host: each child exits with NO app.quit and NO
  // process.exit - the addon releasing the Node loop is the exit proof.
  // The host itself ends through the app lifecycle (app.quit only here).
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
  mark('children-start');
  const closeExit = await runChild('close');
  mark('close-child-exit');
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
  mark('gc-child-exit');
  }
  app.quit();
}

app.whenReady().then(main).catch((error) => {
  console.error(error);
  process.exitCode = 1;
  mark('gc-child-exit');
  }
  app.quit();
});
