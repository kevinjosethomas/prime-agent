/**
 * Electron 44 main-process LOAD PROOF (not an exit proof): requires the
 * provenance-verified addon through the real loader inside Electron's
 * main process, runs one connect/request/close cycle, then quits through
 * the natural app.quit() lifecycle. The same .node that Node 22 loads
 * loads here without a rebuild.
 */
const { app } = require('electron');

async function main() {
  const { DaemonConnection } = await import(new URL('../dist/index.js', import.meta.url).href);
  const { startMock, tmpSocket } = await import(new URL('../tests/helpers.mjs', import.meta.url).href);
  const socketPath = tmpSocket('electron');
  const stop = await startMock(socketPath, 'echo');
  const connection = await DaemonConnection.connect(socketPath);
  const response = await connection.request({ type: 'get_connection_state' });
  if (response.success !== true) throw new Error('request failed in Electron main');
  connection.close();
  await stop();
  console.log('ELECTRON_ADDON_LOAD_OK', process.versions.electron, 'napi', process.versions.napi);
  app.quit();
}

app.whenReady().then(main).catch((error) => {
  console.error(error);
  process.exitCode = 1;
  app.quit();
});
