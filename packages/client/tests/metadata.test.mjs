import test from 'node:test';
import assert from 'node:assert/strict';
import { clientMetadata } from '../dist/index.js';

test('clientMetadata reports the derived build metadata', () => {
  const meta = clientMetadata();
  assert.equal(meta.schemaId, 'protocol-7-schema-32-0add452274ed');
  assert.equal(meta.schemaRevision, 31);
  assert.equal(meta.protocolName, 'prime-agent.daemon');
  assert.equal(meta.protocolVersion, 7);
  assert.equal(meta.version, '0.10.0');
  assert.match(meta.harnessCommit, /^[0-9a-f]{40}$/);
  assert.equal(meta.targetTriple, `${process.platform === 'darwin' ? 'aarch64-apple-darwin' : 'x86_64-unknown-linux-gnu'}`);
  assert.equal(meta.os, process.platform === 'darwin' ? 'macos' : 'linux');
});
