/**
 * Electron 44 main-process PROTOCOL + resource-release leg. Stage markers
 * are line-flushed to electron-leg.log BEFORE their awaits. Every close is
 * AWAITED to real completion and every mock's client-EOF is OBSERVED via
 * the reporting mock BEFORE the daemon is stopped — no kill can mask a
 * resource leak. The windowless-main host lifecycle (app.quit only at the
 * end) is a reported CONSTRAINT, not a natural-exit claim: Chromium keeps
 * the main alive independent of JS work.
 */
const { app } = require('electron');
const { join } = require('node:path');
const { pathToFileURL } = require('node:url');
const { appendFileSync } = require('node:fs');
const assert = require('node:assert/strict');

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
  const { startMock, startMockReporting, tmpSocket } = await import(helpers);
  mark('imports-done');

  if (SECTION !== 'children') {
    // 1. Connect + request round-trip; the close is AWAITED and the mock's
    // client-EOF observed BEFORE the mock stops.
    const echoPath = tmpSocket('electron-echo');
    const echo = await startMockReporting(echoPath, 'echo');
    mark('echo-mock-up');
    const connection = await DaemonConnection.connect(echoPath);
    mark('echo-connected');
    const response = await connection.request({
      type: 'get_connection_state',
      activeSessionId: 'wire',
    });
    mark('echo-request-done');
    if (response.success !== true) throw new Error('request failed in Electron main');
    await connection.close();
    mark('echo-close-completed');
    await echo.waitForDisconnect();
    mark('echo-eof-observed');
    await echo.stop();
    mark('echo-mock-stopped');

    // 2. The gated overflow fail-closed scenario: pre-mark request ->
    // Uncertain-cause-overflow, post-mark -> NotSent-cause-overflow; the
    // close is AWAITED and the flood mock's EOF observed before stop.
    const floodPath = tmpSocket('electron-overflow');
    const flood = await startMockReporting(floodPath, 'flood-trigger');
    mark('flood-mock-up');
    const flooded = await DaemonConnection.connect(floodPath, { frameBuffer: 2 });
    mark('flood-connected');
    const preMarkRequest = flooded
      .request(
        {
          type: 'get_connection_state',
          activeSessionId: 'wire',
          floodTrigger: true,
        },
        60_000,
      )
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
    assert.ok(
      tornDown instanceof UncertainError && tornDown.cause === 'overflow',
      `pre-mark class: ${tornDown?.kind}:${tornDown?.cause}`,
    );
    mark('premark-settled');
    const postMark = await flooded
      .request({ type: 'get_connection_state', activeSessionId: 'wire' }, 60_000)
      .then(
        () => {
          throw new Error('the post-mark request should have refused');
        },
        (error) => error,
      );
    assert.ok(
      postMark instanceof NotSentError && postMark.cause === 'overflow',
      `post-mark class: ${postMark?.kind}:${postMark?.cause}`,
    );
    await flooded.close();
    mark('flood-close-completed');
    await flood.waitForDisconnect();
    mark('flood-eof-observed');
    await flood.stop();
    mark('flood-mock-stopped');

    // 3. Main-process GC resource proof: a scoped connection drops, the
    // main's GC runs (bounded repeats), and the mock's client-EOF is
    // observed BEFORE any kill — the finalizer's scheduled cleanup is the
    // client-driven release.
    const gcPath = tmpSocket('electron-gc');
    const gcMock = await startMockReporting(gcPath, 'echo');
    mark('gc-mock-up');
    {
      const scoped = await DaemonConnection.connect(gcPath);
      await scoped.request({ type: 'get_connection_state', activeSessionId: 'wire' });
    }
    mark('gc-scope-exited');
    let gcDisconnected = false;
    for (let attempt = 0; attempt < 5 && !gcDisconnected; attempt += 1) {
      globalThis.gc?.();
      gcDisconnected = await Promise.race([
        gcMock.waitForDisconnect().then(
          () => true,
          () => false,
        ),
        new Promise((resolve) => setTimeout(() => resolve(false), 2000)),
      ]);
    }
    assert.ok(gcDisconnected, 'the GC-driven client release was never observed');
    mark('gc-eof-observed');
    await gcMock.stop();
    mark('gc-mock-stopped');
  }

  if (SECTION !== 'protocol') {
    // Node-runtime children (ELECTRON_RUN_AS_NODE): each proves the addon
    // releases the Node loop (no app.quit/process.exit of its own) and
    // OBSERVES the mock's client-EOF before stopping it. These are
    // Node-mode children — NOT an Electron main natural-exit proof.
    const childScript = (kind) => `
      globalThis.gc?.();
      const { DaemonConnection } = await import(${JSON.stringify(dist)});
      const { startMockReporting, tmpSocket } = await import(${JSON.stringify(helpers)});
      const assert = (await import('node:assert/strict')).default;
      const socketPath = tmpSocket('electron-child-${kind}');
      const mock = await startMockReporting(socketPath, 'echo');
      ${
        kind === 'close'
          ? `const connection = await DaemonConnection.connect(socketPath);
             await connection.request({ type: 'get_connection_state', activeSessionId: 'wire' });
             await connection.close();`
          : `{
               const scoped = await DaemonConnection.connect(socketPath);
               await scoped.request({ type: 'get_connection_state', activeSessionId: 'wire' });
             }
             globalThis.gc();`
      }
      await mock.waitForDisconnect();
      await mock.stop();
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
    assert.strictEqual(closeExit, 0, 'the close child must exit 0 naturally');
    assert.strictEqual(gcExit, 0, 'the gc child must exit 0 naturally');
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
