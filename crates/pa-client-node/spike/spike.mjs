// NAPI spike: exercises the addon surface and then ENDS. No process.exit.
// The runner wraps this with a timeout: natural process exit IS the test.
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);

const addon = require(process.argv[2]);
const meta = addon.spikeMetadata();
if (meta.version !== '0.10.0') throw new Error('bad version: ' + meta.version);
if (meta.defaultRequestTimeoutMs !== 30000) throw new Error('bad timeout const');
if (meta.paClientOptionsLinked !== true) throw new Error('pa-client not linked');
console.log('META_OK', JSON.stringify(meta));

const slept = await addon.spikeDelay(30);
if (slept !== 30) throw new Error('async fn broken');
console.log('ASYNC_OK');

let ticks = 0;
const ticker = new addon.SpikeTicker(() => { ticks += 1; });
await new Promise((resolve) => {
  const t = setInterval(() => { if (ticks >= 5) { clearInterval(t); resolve(); } }, 5);
  setTimeout(() => { clearInterval(t); resolve(); }, 2000);
});
ticker.stop();
if (ticks < 5) throw new Error('TSFN never delivered: ' + ticks);
console.log('TSFN_OK', ticks);
// Natural end: nothing below holds the loop. If the addon leaks, the wrapper
// timeout kills the run and the spike FAILS.
