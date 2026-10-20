// Electron 44 main-process spike: same surface as spike.mjs inside Electron.
// Natural lifecycle only: app.quit() after the checks. No process.exit.
const { app } = require('electron');
const addon = require(process.argv[process.argv.length - 1]);

async function main() {
  const meta = addon.spikeMetadata();
  if (meta.paClientOptionsLinked !== true) throw new Error('pa-client not linked');
  const slept = await addon.spikeDelay(30);
  if (slept !== 30) throw new Error('async fn broken');
  let ticks = 0;
  const ticker = new addon.SpikeTicker(() => { ticks += 1; });
  await new Promise((resolve) => {
    const t = setInterval(() => { if (ticks >= 5) { clearInterval(t); resolve(); } }, 5);
    setTimeout(() => { clearInterval(t); resolve(); }, 2000);
  });
  ticker.stop();
  if (ticks < 5) throw new Error('TSFN never delivered: ' + ticks);
  console.log('ELECTRON_SPIKE_OK', process.versions.electron, 'node', process.versions.node);
  app.quit();
}
app.whenReady().then(main).catch((error) => { console.error(error); process.exitCode = 1; app.quit(); });
