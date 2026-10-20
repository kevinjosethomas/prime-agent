/**
 * Generates provenance.json for the built addons: sha256 + the addon's own
 * derived metadata (schema id, harness commit, target triple). The loader
 * verifies every load against this manifest — fail-closed.
 */
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const nativeDir = join(root, 'native');
const require = createRequire(import.meta.url);
const artifacts = {};
for (const name of readdirSync(nativeDir)) {
  if (!name.endsWith('.node')) continue;
  const addonPath = join(nativeDir, name);
  const digest = createHash('sha256').update(readFileSync(addonPath)).digest('hex');
  const addon = require(addonPath);
  const metadata = addon.clientMetadata();
  artifacts[name] = {
    sha256: digest,
    harnessCommit: metadata.harnessCommit,
    schemaId: metadata.schemaId,
    targetTriple: metadata.targetTriple,
  };
}
writeFileSync(join(root, 'provenance.json'), JSON.stringify({ artifacts }, null, 2) + '\n');
console.log('provenance.json written for', Object.keys(artifacts).join(', '));
