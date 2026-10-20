import test from 'node:test';
import assert from 'node:assert/strict';
import { copyFileSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { loadAddon, platformArtifact } from '../dist/loader.js';

const root = fileURLToPath(new URL('..', import.meta.url));

test('the loader verifies the fixed artifact and refuses a tampered addon', () => {
  const artifact = platformArtifact();
  const tampered = 'tampered-test.node';
  copyFileSync(join(root, 'native', artifact), join(root, 'native', tampered));
  process.env.PRIME_AGENT_CLIENT_ADDON = tampered;
  try {
    assert.throws(() => loadAddon(), /no entry|sha256/);
  } finally {
    delete process.env.PRIME_AGENT_CLIENT_ADDON;
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

