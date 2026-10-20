// The production load: the packed tarball, installed WITHOUT dev
// dependencies, must expose a working built-JS entry (no TypeScript at
// runtime — Node 18 imports dist/index.js).
import test from "node:test";
import assert from "node:assert/strict";
// shell-run commands: this npm fails as a direct execFileSync child in
// some environments (exit 1, empty stderr); via the shell it is reliable.
import { execSync } from "node:child_process";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
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
    const loaded = await import(
      new URL("./node_modules/prime-agent-client-contract/dist/src/index.js", `file://${consumer}/`).href
    );
    assert.equal(typeof loaded.loadContract, "function");
    assert.equal(typeof loaded.loadMetadata, "function");
    const contract = await loaded.loadContract();
    assert.ok(contract.validateCommand);
    const metadata = loaded.loadMetadata();
    assert.equal(metadata.protocol.name, "prime-agent.daemon");
    console.log(`production load on node ${process.version}`);
    const witness = { type: "get_state", activeSessionId: "wire" };
    assert.ok(contract.validateCommand(witness));
    const malformedBlock = {
      role: "user",
      content: [{ type: "text" }],
      timestamp: 0,
    };
    assert.ok(!contract.validateAgentMessage(malformedBlock));
  } finally {
    rmSync(staging, { recursive: true, force: true });
  }
});
