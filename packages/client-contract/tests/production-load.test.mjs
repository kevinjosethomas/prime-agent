// The production load: the packed tarball, installed WITHOUT dev
// dependencies, must expose a working built-JS entry (no TypeScript at
// runtime — Node 18 imports dist/index.js).
import test from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

function run(cmd, cwd, args) {
  return execFileSync(cmd, { cwd, args, encoding: "utf8" });
}

test("packed tarball loads with production dependencies only", async () => {
  // The consumer receives dist built at pack time (documented in README).
  run("npm", packageRoot, ["run", "build:validators"]);
  const staging = mkdtempSync(join(tmpdir(), "pr3a-prod-"));
  try {
    run("npm", packageRoot, ["pack", "--pack-destination", staging]);
    const tarball = readdirSync(staging).find((name) => name.endsWith(".tgz"));
    assert.ok(tarball, "npm pack produced a tarball");
    const consumer = mkdtempSync(join(tmpdir(), "pr3a-consumer-"));
    run("npm", consumer, ["init", "-y"]);
    run("npm", consumer, [
      "install",
      "--omit=dev",
      "--no-audit",
      "--no-fund",
      join(staging, tarball),
    ]);
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
