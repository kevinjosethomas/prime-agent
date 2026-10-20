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
  const {
    DaemonConnection,
    ManagedDaemonSession,
    NotSentError,
    OverflowError,
    UncertainError,
  } = await import(dist);
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
    // FUNCTION scope: the wrapper dies on return; V8 retains module-level
    // bindings, which would keep the finalizer from ever running.
    await (async () => {
      const scoped = await DaemonConnection.connect(gcPath);
      await scoped.request({ type: 'get_connection_state', activeSessionId: 'wire' });
    })();
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

    // 4. Managed session: connect through the shared Rust lifecycle, pull
    // the status stream, request round-trip; the STOP is AWAITED and the
    // mock's client-EOF observed BEFORE the mock stops.
    const managedPath = tmpSocket('electron-managed');
    const managedMock = await startMockReporting(managedPath, 'echo');
    mark('managed-mock-up');
    const session = await ManagedDaemonSession.start({
      socketPath: managedPath,
      restoreAttemptTimeoutMs: 30_000,
    });
    mark('managed-started');
    const firstEvent = await session.nextEvent();
    assert.ok(firstEvent.type === 'status' && firstEvent.status.state === 'connected', `unexpected first event: ${JSON.stringify(firstEvent)}`);
    mark('managed-status-streamed');
    const managedResponse = await session.request({
      type: 'get_connection_state',
      activeSessionId: 'wire',
    });
    mark('managed-request-done');
    if (managedResponse.success !== true) throw new Error('managed request failed in Electron main');
    await session.stop();
    mark('managed-stop-completed');
    await managedMock.waitForDisconnect();
    mark('managed-eof-observed');
    await managedMock.stop();
    mark('managed-mock-stopped');

    // 5. Managed overflow fail-closed: pre-mark request settles
    // Uncertain-cause-overflow, post-mark NotSent-cause-overflow; the stop
    // is AWAITED and the flood mock's EOF observed before stop.
    const managedFloodPath = tmpSocket('electron-managed-overflow');
    const managedFlood = await startMockReporting(managedFloodPath, 'flood-trigger');
    mark('managed-flood-mock-up');
    const floodedSession = await ManagedDaemonSession.start({
      socketPath: managedFloodPath,
      eventBuffer: 2,
      restoreAttemptTimeoutMs: 30_000,
    });
    mark('managed-flood-started');
    const managedPreMark = floodedSession
      .request({
        type: 'get_connection_state',
        activeSessionId: 'wire',
        floodTrigger: true,
      })
      .then(
        () => {
          throw new Error('the pre-mark request should have torn down');
        },
        (error) => error,
      );
    mark('managed-premark-request-started');
    let managedOverflowed = false;
    while (!managedOverflowed) {
      try {
        const event = await floodedSession.nextEvent();
        if (event === null) break;
      } catch (error) {
        if (!(error instanceof OverflowError)) throw error;
        managedOverflowed = true;
      }
    }
    mark('managed-overflow-marked');
    const managedTornDown = await managedPreMark;
    assert.ok(
      managedTornDown instanceof UncertainError && managedTornDown.cause === 'overflow',
      `pre-mark class: ${managedTornDown?.kind}:${managedTornDown?.cause}`,
    );
    mark('managed-premark-settled');
    const managedPostMark = await floodedSession
      .request({ type: 'get_connection_state', activeSessionId: 'wire' })
      .then(
        () => {
          throw new Error('the post-mark request should have refused');
        },
        (error) => error,
      );
    assert.ok(
      managedPostMark instanceof NotSentError && managedPostMark.cause === 'overflow',
      `post-mark class: ${managedPostMark?.kind}:${managedPostMark?.cause}`,
    );
    await floodedSession.stop();
    mark('managed-stop-completed');
    await managedFlood.waitForDisconnect();
    mark('managed-flood-eof-observed');
    await managedFlood.stop();
    mark('managed-flood-mock-stopped');

    // 6. Managed GC resource proof: a scoped session drops, the main's GC
    // runs (bounded repeats), and the mock's client-EOF is observed BEFORE
    // any kill — the finalizer's scheduled stop chain is the client-driven
    // release.
    const managedGcPath = tmpSocket('electron-managed-gc');
    const managedGcMock = await startMockReporting(managedGcPath, 'echo');
    mark('managed-gc-mock-up');
    await (async () => {
      const scoped = await ManagedDaemonSession.start({
        socketPath: managedGcPath,
        restoreAttemptTimeoutMs: 30_000,
      });
      await scoped.request({ type: 'get_connection_state', activeSessionId: 'wire' });
    })();
    mark('managed-gc-scope-exited');
    let managedGcDisconnected = false;
    for (let attempt = 0; attempt < 5 && !managedGcDisconnected; attempt += 1) {
      globalThis.gc?.();
      managedGcDisconnected = await Promise.race([
        managedGcMock.waitForDisconnect().then(
          () => true,
          () => false,
        ),
        new Promise((resolve) => setTimeout(() => resolve(false), 2000)),
      ]);
    }
    assert.ok(managedGcDisconnected, 'the managed GC-driven release was never observed');
    mark('managed-gc-eof-observed');
    await managedGcMock.stop();
    mark('managed-gc-mock-stopped');
  }

  if (SECTION !== 'protocol') {
    // Node-runtime children (ELECTRON_RUN_AS_NODE): each proves the addon
    // releases the Node loop (no app.quit/process.exit of its own) and
    // OBSERVES the mock's client-EOF before stopping it. These are
    // Node-mode children — NOT an Electron main natural-exit proof.
    const childScript = (kind) => `
      globalThis.gc?.();
      const { DaemonConnection, ManagedDaemonSession } = await import(${JSON.stringify(dist)});
      const { startMockReporting, tmpSocket } = await import(${JSON.stringify(helpers)});
      const assert = (await import('node:assert/strict')).default;
      const socketPath = tmpSocket('electron-child-${kind}');
      const mock = await startMockReporting(socketPath, 'echo');
      ${
        kind === 'close'
          ? `const connection = await DaemonConnection.connect(socketPath);
             await connection.request({ type: 'get_connection_state', activeSessionId: 'wire' });
             await connection.close();`
          : kind === 'gc'
            ? `await (async () => {
                 const scoped = await DaemonConnection.connect(socketPath);
                 await scoped.request({ type: 'get_connection_state', activeSessionId: 'wire' });
               })();
               globalThis.gc();`
            : kind === 'managed-close'
              ? `const session = await ManagedDaemonSession.start({
                   socketPath,
                   restoreAttemptTimeoutMs: 30_000,
                 });
                 await session.request({ type: 'get_connection_state', activeSessionId: 'wire' });
                 await session.stop();`
              : `await (async () => {
                   const scoped = await ManagedDaemonSession.start({
                     socketPath,
                     restoreAttemptTimeoutMs: 30_000,
                   });
                   await scoped.request({ type: 'get_connection_state', activeSessionId: 'wire' });
                 })();
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
    const managedCloseExit = await runChild('managed-close');
    mark('managed-close-child-exit');
    const managedGcExit = await runChild('managed-gc');
    mark('managed-gc-child-exit');
    assert.strictEqual(closeExit, 0, 'the close child must exit 0 naturally');
    assert.strictEqual(gcExit, 0, 'the gc child must exit 0 naturally');
    assert.strictEqual(managedCloseExit, 0, 'the managed close child must exit 0 naturally');
    assert.strictEqual(managedGcExit, 0, 'the managed gc child must exit 0 naturally');
    console.log(
      'ELECTRON_MAIN_PROTOCOL_OK',
      process.versions.electron,
      'napi',
      process.versions.napi,
      'child-exit-code',
      closeExit,
      'gc-exit-code',
      gcExit,
      'managed-child-exit-code',
      managedCloseExit,
      'managed-gc-exit-code',
      managedGcExit,
    );
  }
  app.quit();
}

app.whenReady().then(main).catch((error) => {
  console.error(error);
  process.exitCode = 1;
  app.quit();
});
