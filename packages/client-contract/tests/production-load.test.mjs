// The production load: the packed tarball, installed WITHOUT dev
// dependencies, must expose a working built-JS entry (no TypeScript at
// runtime — Node 18 imports dist/index.js).
import test from "node:test";
import assert from "node:assert/strict";
// shell-run commands: this npm fails as a direct execFileSync child in
// some environments (exit 1, empty stderr); via the shell it is reliable.
import { execSync } from "node:child_process";
import { mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

function run(command, cwd) {
  try {
    return execSync(command, { cwd, encoding: "utf8" });
  } catch (error) {
    console.error(`command failed: ${command} in ${cwd}`);
    console.error(String(error.stderr ?? error.message));
    throw error;
  }
}

test("packed tarball loads with production dependencies only", async () => {
  // The consumer receives dist built at pack time (documented in README).
  run("npm run build:validators", packageRoot);
  const staging = mkdtempSync(join(tmpdir(), "pr3a-prod-"));
  try {
    run(`npm pack --pack-destination ${staging}`, packageRoot);
    const tarball = readdirSync(staging).find((name) => name.endsWith(".tgz"));
    assert.ok(tarball, "npm pack produced a tarball");
    const consumer = mkdtempSync(join(tmpdir(), "pr3a-consumer-"));
    run("npm init -y", consumer);
    run(
      `npm install --omit=dev --no-audit --no-fund ${join(staging, tarball)}`,
      consumer,
    );
    // The CONSUMER-side load: a separate node process in the consumer
    // directory imports the package by NAME (package.json `main` resolution)
    // and runs real validations — the exact shape PR4's adoption will use.
    const probe = `import { loadContract, loadMetadata, checkInboundFrame } from "prime-agent-client-contract";
const contract = await loadContract();
if (typeof contract.validateCommand !== "function") throw new Error("no validateCommand");
const witness = { type: "get_state", activeSessionId: "wire" };
if (!contract.validateCommand(witness)) throw new Error("accepted command rejected");
const malformedBlock = { role: "user", content: [{ type: "text" }], timestamp: 0 };
if (contract.validateAgentMessage(malformedBlock)) throw new Error("malformed known-kind block accepted");
if (loadMetadata().protocol.name !== "prime-agent.daemon") throw new Error("metadata broken");
const verdict = checkInboundFrame(contract, { type: "brand_new_kind", kept: 1 });
if (!verdict.unknownKind || !verdict.accepted) throw new Error("unknown kind not passed through");
console.log("production load on node " + process.version + " via package main");`;
    writeFileSync(join(consumer, "probe.mjs"), probe);
    const out = run("node probe.mjs", consumer);
    console.log(out.trim());
  } finally {
    rmSync(staging, { recursive: true, force: true });
  }
});
