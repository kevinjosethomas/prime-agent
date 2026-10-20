/**
 * The fixed-artifact addon loader.
 *
 * Production loads `native/<platform>.node` and verifies it against
 * `provenance.json` (sha256 + schema id + harness commit + target triple)
 * before use — fail-closed, no silent fallback. There is deliberately NO
 * ambient environment override: the only escape hatch is the explicit
 * `testArtifact` option, for the loader's own tests.
 */
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);

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

export interface LoadAddonOptions {
  /** Explicit TEST option: load `native/<name>` instead of the platform
   * artifact. Production callers never pass this. */
  testArtifact?: string;
}

function packageRoot(): string {
  return join(dirname(fileURLToPath(import.meta.url)), '..');
}

function sha256(path: string): string {
  return createHash('sha256').update(readFileSync(path)).digest('hex');
}

/** Loads the native addon after verifying its provenance. Fail-closed. */
export function loadAddon(options?: LoadAddonOptions): {
  clientMetadata: () => {
    schemaId: string;
    harnessCommit: string;
    targetTriple: string;
  };
} {
  const root = packageRoot();
  const manifest = JSON.parse(
    readFileSync(join(root, 'provenance.json'), 'utf8'),
  ) as ProvenanceManifest;
  const artifact = options?.testArtifact ?? platformArtifact();
  const addonPath = join(root, 'native', artifact);
  const digest = sha256(addonPath);
  const entry = manifest.artifacts[artifact];
  if (!entry) {
    throw new Error(`provenance manifest has no entry for artifact "${artifact}"`);
  }
  if (entry.sha256 !== digest) {
    throw new Error(`addon provenance mismatch for "${artifact}": sha256 ${digest}`);
  }
  const addon = require(addonPath);
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
