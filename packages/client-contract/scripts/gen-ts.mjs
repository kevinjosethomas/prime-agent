// Generate TypeScript types and the corpus type-check module from the
// canonical JSON Schemas (packages/client-contract/generated/schema).
// Output is committed and drift-gated: `npm run verify` regenerates and
// diffs. Do not edit generated output by hand.
import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { compile } from "json-schema-to-typescript";

const root = new URL("..", import.meta.url).pathname;
const schemaDir = join(root, "generated", "schema");
const tsDir = join(root, "generated", "ts");
const typecheckDir = join(root, "generated", "typecheck");

const banner = (stem) => `/* eslint-disable */
/**
 * Generated from ${stem}.schema.json by packages/client-contract/scripts/gen-ts.mjs.
 * Canonical source: the pa-types daemon wire types (crates/pa-types).
 * DO NOT EDIT: regenerate with \`npm run gen:ts\` and commit.
 */
`;

export async function generate() {
  await mkdir(tsDir, { recursive: true });
  await mkdir(typecheckDir, { recursive: true });
  const files = (await readdir(schemaDir)).filter((name) => name.endsWith(".schema.json"));
  const exports = [];
  for (const file of files) {
    const stem = file.replace(/\.schema\.json$/, "");
    const raw = JSON.parse(await readFile(join(schemaDir, file), "utf8"));
    const ts = await compile(raw, stem, {
      bannerComment: "",
      unknownAny: true,
      additionalProperties: true,
      cwd: schemaDir,
    });
    await writeFile(join(tsDir, `${stem}.ts`), banner(stem) + ts + "\n");
    exports.push([stem, raw.title ?? stem]);
  }
  // One barrel: the ROOT type of each schema (the generated modules also
  // re-emit their $defs, so wildcard re-exports would be ambiguous).
  await writeFile(
    join(tsDir, "index.ts"),
    banner("index") +
      exports
        .map(([stem, title]) => `export type { ${title} } from "./${stem}.js";`)
        .join("\n") +
      "\n",
  );
  // The corpus type-check module: every accepted witness, assigned to its
  // root type. `tsc --noEmit` then proves the generated types accept the
  // same wire samples the Rust serde parse accepts.
  const acceptedDir = join(root, "generated", "corpus", "accepted");
  const lines = [];
  const files2 = (await readdir(acceptedDir)).filter((name) => name.endsWith(".jsonl"));
  let n = 0;
  for (const file of files2.sort()) {
    const stem = file.replace(/\.jsonl$/, "");
    const raw = await readFile(join(acceptedDir, file), "utf8");
    for (const line of raw.split("\n")) {
      if (!line.trim()) continue;
      const entry = JSON.parse(line);
      lines.push(`export const sample_${n++}: ${entry.schemaRoot === "client-inbound-frame" ? "unknown" : (exports.find(([s]) => s === entry.schemaRoot)?.[1] ?? "unknown")} = ${JSON.stringify(entry.sample)};`);
    }
  }
  const imports = exports
    .filter(([stem]) => files2.some((file) => file.startsWith(stem)))
    .map(([stem, title]) => `import type { ${title} } from "../ts/${stem}.js";`)
    .join("\n");
  await writeFile(
    join(typecheckDir, "corpus.ts"),
    banner("typecheck") + imports + "\n\n" + lines.join("\n") + "\n",
  );
  return { schemas: files.length, samples: n };
}

if (process.argv[1] === new URL(import.meta.url).pathname) {
  const out = await generate();
  console.log(`generated ${out.schemas} type modules, ${out.samples} typecheck samples`);
}
