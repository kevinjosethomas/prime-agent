// Drift gate for the Node side: regeneration must byte-match the committed
// generated TS. Mirrors the Rust `pa-contract` drift test.
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { generate } from "./gen-ts.mjs";

const root = new URL("..", import.meta.url).pathname;

async function files(dir) {
  const out = [];
  for (const entry of await readdir(dir, { recursive: true })) {
    if (entry.endsWith(".ts")) out.push(join(dir, entry));
  }
  return out;
}

const tsDir = join(root, "generated", "ts");
const typecheckDir = join(root, "generated", "typecheck");
const committed = new Map();
for (const path of [...(await files(tsDir)), ...(await files(typecheckDir))]) {
  committed.set(path, await readFile(path, "utf8"));
}
await generate();
let failed = false;
for (const path of [...(await files(tsDir)), ...(await files(typecheckDir))]) {
  const after = await readFile(path, "utf8");
  if (!committed.has(path)) {
    console.error(`DRIFT: new file ${path}`);
    failed = true;
  } else if (committed.get(path) !== after) {
    console.error(`DRIFT: ${path}`);
    failed = true;
  }
}
if (failed) process.exit(1);
console.log("node-side drift gate: clean");
