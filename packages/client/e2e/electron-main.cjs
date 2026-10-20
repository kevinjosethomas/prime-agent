/**
 * Electron 44 main-process LOAD PROOF (not an exit proof): requires the
 * provenance-verified addon through the real loader inside Electron's
 * main process, runs one connect/request/close cycle, then quits through
 * the natural app.quit() lifecycle. The same .node that Node 22 loads
 * loads here without a rebuild.
 */
const { app } = require('electron');
const { join } = require('node:path');
const { pathToFileURL } = require('node:url');

async function main() {
  const { DaemonConnection } = await import(pathToFileURL(join(__dirname, '..', 'dist', 'index.js')).href);
  const { startMock, tmpSocket } = await import(pathToFileURL(join(__dirname, '..', 'tests', 'helpers.mjs')).href);
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
