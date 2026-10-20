import test from 'node:test';
import assert from 'node:assert/strict';
import { copyFileSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadAddon, platformArtifact } from '../dist/loader.js';
import { clientMetadata } from '../dist/index.js';

const root = fileURLToPath(new URL('..', import.meta.url));

test('the production fixed path loads the platform artifact (before any mock work)', () => {
  const addon = loadAddon();
  const metadata = addon.clientMetadata();
  const expectedTriple =
    process.platform === 'darwin' ? 'aarch64-apple-darwin' : 'x86_64-unknown-linux-gnu';
  assert.equal(metadata.targetTriple, expectedTriple);
  assert.match(metadata.harnessCommit, /^[0-9a-f]{40}$/);
  assert.equal(metadata.schemaId, 'protocol-7-schema-31-5b2e0e3f07aa');
});

test('a tampered test artifact is refused by the sha256 gate', () => {
  const artifact = platformArtifact();
  const tampered = 'tampered-test.node';
  copyFileSync(join(root, 'native', artifact), join(root, 'native', tampered));
  try {
    assert.throws(() => loadAddon({ testArtifact: tampered }), /no entry|sha256/);
  } finally {
    rmSync(join(root, 'native', tampered), { force: true });
  }
});

test('a manifest mismatch fails closed even for the right artifact', () => {
  const artifact = platformArtifact();
  const manifestPath = join(root, 'provenance.json');
  const original = readFileSync(manifestPath, 'utf8');
  const manifest = JSON.parse(original);
  manifest.artifacts[artifact].sha256 = 'deadbeef';
  writeFileSync(manifestPath, JSON.stringify(manifest));
  try {
    assert.throws(() => loadAddon(), /sha256/);
  } finally {
    writeFileSync(manifestPath, original);
  }
});

test('clientMetadata flows through the production loader', () => {
  assert.equal(clientMetadata().protocolName, 'prime-agent.daemon');
});
