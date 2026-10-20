/**
 * The fixed-artifact addon loader.
 *
 * The addon loads from a fixed platform path (`native/<platform>.node`)
 * verified against `provenance.json` before use. `PRIME_AGENT_CLIENT_ADDON`
 * is an explicit TEST override: it skips the platform path but still
 * requires the provenance manifest to match. Any mismatch refuses the
 * load — no silent fallback.
 */
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { createRequire } from 'node:module';
import { fileURLToPath, pathToFileURL } from 'node:url';

/** The platform-qualified artifact name this host must load. */
export function platformArtifact(): string {
  return `${process.platform}-${process.arch}.node`;
}

interface ProvenanceEntry {
  sha256: string;
  harnessCommit: string;
  schemaId: string;
  targetTriple: string;
}

interface ProvenanceManifest {
  artifacts: Record<string, ProvenanceEntry>;
}

const require = createRequire(import.meta.url);

function packageRoot(): string {
  return join(dirname(fileURLToPath(import.meta.url)), '..');
}

function sha256(path: string): string {
  return createHash('sha256').update(readFileSync(path)).digest('hex');
}

/** Loads the native addon after verifying its provenance. Fail-closed. */
export function loadAddon(): Record<string, unknown> {
  const root = packageRoot();
  const manifestPath = join(root, 'provenance.json');
  const manifest = JSON.parse(readFileSync(manifestPath, 'utf8')) as ProvenanceManifest;
  const override = process.env.PRIME_AGENT_CLIENT_ADDON;
  const artifact = override ?? platformArtifact();
  const addonPath = join(root, 'native', artifact);
  const digest = sha256(addonPath);
  const entry = manifest.artifacts[artifact];
  if (!entry) {
    throw new Error(`provenance manifest has no entry for artifact "${artifact}"`);
  }
  if (entry.sha256 !== digest) {
    throw new Error(`addon provenance mismatch for "${artifact}": sha256 ${digest}`);
  }
  const addon = require(pathToFileURL(addonPath).href) as {
    clientMetadata: () => {
      schemaId: string;
      harnessCommit: string;
      targetTriple: string;
    };
  };
  const metadata = addon.clientMetadata();
  if (metadata.schemaId !== entry.schemaId) {
    throw new Error(
      `addon schema id mismatch: ${JSON.stringify(metadata.schemaId)} vs manifest ${entry.schemaId}`,
    );
  }
  if (metadata.harnessCommit !== entry.harnessCommit) {
    throw new Error(
      `addon harness commit mismatch: ${metadata.harnessCommit} vs manifest ${entry.harnessCommit}`,
    );
  }
  if (metadata.targetTriple !== entry.targetTriple) {
    throw new Error(
      `addon target triple mismatch: ${metadata.targetTriple} vs manifest ${entry.targetTriple}`,
    );
  }
  return addon;
}
