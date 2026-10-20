/**
 * Electron 44 main-process PROTOCOL + natural-exit leg. Stage markers are
 * line-flushed to electron-leg.log BEFORE their awaits; E2E_SECTION gates
 * protocol vs children runs for bisection. The host ends through the app
 * lifecycle (app.quit only here); the natural-exit children prove the
 * addon releases the Node loop with NO app.quit/process.exit of their own.
 */
const { app } = require('electron');
const { join } = require('node:path');
const { pathToFileURL } = require('node:url');
const { appendFileSync } = require('node:fs');

const LOG = join(__dirname, 'electron-leg.log');
const mark = (m) => {
  appendFileSync(LOG, `${new Date().toISOString()} ${m}\n`);
};
const SECTION = process.env.E2E_SECTION || 'all';

async function main() {
  const { spawn } = require('node:child_process');
  mark('main-start');
  const dist = pathToFileURL(join(__dirname, '..', 'dist', 'index.js')).href;
  const helpers = pathToFileURL(join(__dirname, '..', 'tests', 'helpers.mjs')).href;
  const { DaemonConnection, NotSentError, OverflowError, UncertainError } = await import(dist);
  const { startMock, tmpSocket } = await import(helpers);
  mark('imports-done');

  if (SECTION !== 'children') {
    // 1. Connect + request round-trip against the echo mock.
    const echoPath = tmpSocket('electron-echo');
    const stopEcho = await startMock(echoPath, 'echo');
    mark('echo-mock-up');
    const connection = await DaemonConnection.connect(echoPath);
    mark('echo-connected');
    const response = await connection.request({
      type: 'get_connection_state',
      activeSessionId: 'wire',
    });
    mark('echo-request-done');
    if (response.success !== true) throw new Error('request failed in Electron main');
    connection.close();
    mark('echo-closed');

    // 2. The overflow fail-closed scenario in the gated flood mock: the
    // pre-mark request settles Uncertain with cause overflow; the
    // post-mark request is the provable NotSent with the overflow cause.
    const floodPath = tmpSocket('electron-overflow');
    const stopFlood = await startMock(floodPath, 'flood');
    mark('flood-mock-up');
    const flooded = await DaemonConnection.connect(floodPath, { frameBuffer: 2 });
    mark('flood-connected');
    const preMarkRequest = flooded
      .request({ type: 'get_connection_state', activeSessionId: 'wire' }, 60_000)
      .then(
        () => {
          throw new Error('the pre-mark request should have torn down');
        },
        (error) => error,
      );
    mark('premark-request-started');
    let overflowed = false;
    while (!overflowed) {
      try {
        await flooded.nextFrame();
      } catch (error) {
        if (!(error instanceof OverflowError)) throw error;
        overflowed = true;
      }
    }
    mark('flood-overflow-marked');
    const tornDown = await preMarkRequest;
    if (!(tornDown instanceof UncertainError) || tornDown.cause !== 'overflow') {
      throw new Error(`pre-mark request class: ${tornDown?.kind}:${tornDown?.cause}`);
    }
    mark('premark-settled');
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
    // The natural-exit proofs: ELECTRON_RUN_AS_NODE children that end with
    // no app.quit and no process.exit — the addon releasing the Node loop
    // is the exit proof.
    const childScript = (kind) => `
      globalThis.gc?.();
      const { DaemonConnection } = await import(${JSON.stringify(dist)});
      const { startMock, tmpSocket } = await import(${JSON.stringify(helpers)});
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
        child.on('exit', (code) => resolve(code));
      });
    mark('children-start');
    const closeExit = await runChild('close');
    mark('close-child-exit');
    const gcExit = await runChild('gc');
    mark('gc-child-exit');
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
  }
  app.quit();
}

app.whenReady().then(main).catch((error) => {
  console.error(error);
  process.exitCode = 1;
  app.quit();
});
