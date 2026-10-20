/**
 * Provenance verification against the CURRENT tree, so a stale artifact
 * cannot pass tautologically: the manifest's harness commit must equal
 * `git rev-parse HEAD`, the schema id must equal the Rust constant in
 * pa-types, and the target triple must equal the expected platform.
 */
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const manifest = JSON.parse(readFileSync(`${root}/provenance.json`, 'utf8'));
const gitHead = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root })
  .toString()
  .trim();
const schemaLine = readFileSync(
  `${root}/../../crates/pa-types/src/daemon/mod.rs`,
  'utf8',
)
  .split('\n')
  .find((line) => line.includes('DAEMON_SCHEMA_ID:'));
const schemaId = schemaLine?.match(/"([^"]+)"/)?.[1];
if (!schemaId) throw new Error('DAEMON_SCHEMA_ID not found in pa-types');

const expectedTriples = {
  'linux-x64': 'x86_64-unknown-linux-gnu',
  'darwin-arm64': 'aarch64-apple-darwin',
};

const failures = [];
for (const [artifact, entry] of Object.entries(manifest.artifacts)) {
  if (entry.harnessCommit !== gitHead) {
    failures.push(`${artifact}: manifest commit ${entry.harnessCommit} != HEAD ${gitHead}`);
  }
  if (entry.schemaId !== schemaId) {
    failures.push(`${artifact}: manifest schema id ${entry.schemaId} != pa-types ${schemaId}`);
  }
  const triple = expectedTriples[artifact];
  if (!triple || entry.targetTriple !== triple) {
    failures.push(`${artifact}: manifest triple ${entry.targetTriple} != expected ${triple}`);
  }
}
if (failures.length > 0) {
  console.error('PROVENANCE_VERIFY_FAILED\n' + failures.join('\n'));
  process.exit(1);
}
console.log(`provenance verified: ${Object.keys(manifest.artifacts).join(', ')} @ ${gitHead}`);
