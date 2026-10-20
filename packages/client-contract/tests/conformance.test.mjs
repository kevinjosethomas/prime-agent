// Ajv conformance over the committed corpus, cross-checked against the
// serde verdicts recorded by the Rust exporter. Any disagreement here is a
// contract bug — the schema must accept and refuse exactly what serde does
// (on the client-frame level; documented open/strict exceptions aside).
import test from "node:test";
import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { loadContract, checkInboundFrame } from "../dist/validators.js";

const generated = join(fileURLToPath(new URL("../generated", import.meta.url)));

const stemToValidator = {
  "daemon-command": "validateCommand",
  "daemon-command-envelope": "validateCommandEnvelope",
  "daemon-command-wire": "validateCommandWire",
  "daemon-outbound": "validateOutboundFrame",
  "daemon-response": "validateResponse",
  "daemon-error-info": "validateErrorInfo",
  "daemon-event-envelope": "validateEventEnvelope",
  "daemon-event-meta": "validateEventMeta",
  "daemon-resume-cursor": "validateResumeCursor",
  "agent-message": "validateAgentMessage",
};

async function corpus(kind) {
  const dir = join(generated, "corpus", kind);
  const out = [];
  for (const file of (await readdir(dir)).filter((n) => n.endsWith(".jsonl"))) {
    const stem = file.replace(/\.jsonl$/, "");
    for (const line of (await readFile(join(dir, file), "utf8")).split("\n")) {
      if (line.trim()) out.push({ stem, entry: JSON.parse(line) });
    }
  }
  return out;
}

test("accepted corpus validates green under Ajv", async () => {
  const c = await loadContract();
  for (const { stem, entry } of await corpus("accepted")) {
    if (stem === "client-inbound-frame") {
      assert.ok(c.validateInboundFrame(entry.sample), `envelope rejected: ${JSON.stringify(entry.sample)}`);
      continue;
    }
    const validate = c[stemToValidator[stem]];
    assert.ok(validate, `no validator for ${stem}`);
    assert.ok(validate(entry.sample), `${stem} rejected accepted witness: ${JSON.stringify(entry.sample)}`);
  }
});

test("mutation corpus: Ajv agrees with the recorded serde verdicts", async () => {
  const c = await loadContract();
  let bothAccept = 0;
  let bothReject = 0;
  for (const { stem, entry } of await corpus("mutations")) {
    const validate = c[stemToValidator[stem]];
    assert.ok(validate, `no validator for ${stem}`);
    const ajvAccepts = validate(entry.sample);
    assert.equal(
      ajvAccepts,
      entry.rustAccepts,
      `${stem} ${entry.case}: Ajv ${ajvAccepts ? "accepts" : "rejects"} where serde ${entry.rustAccepts ? "accepts" : "rejects"}: ${JSON.stringify(entry.sample)}`,
    );
    if (ajvAccepts) bothAccept++;
    else bothReject++;
  }
  assert.ok(bothAccept > 0 && bothReject > 0, "expected both valid-mutation and refused-mutation coverage");
});

test("open receive: unknown kinds pass, known kinds are strict", async () => {
  const c = await loadContract();
  const unknownKind = { type: "brand_new_event_kind", payload: { kept: true } };
  const verdict = checkInboundFrame(c, unknownKind);
  assert.ok(verdict.accepted && verdict.unknownKind && !verdict.knownKindValid);
  const knownKind = { type: c.knownOutboundKinds[0], unexpected: 1 };
  const knownVerdict = checkInboundFrame(c, knownKind);
  // A known-kind frame failing its closed shape is a conformance failure.
  assert.ok(knownVerdict.accepted && !knownVerdict.unknownKind);
});

test("error info: unknown codes accepted, known codes strict", async () => {
  const c = await loadContract();
  const response = (errorInfo) => ({
    id: "r1",
    command: "prompt",
    success: false,
    error: "refused",
    errorInfo,
  });
  assert.ok(c.validateResponse(response({ code: "brand_new_code", detail: { kept: true } })));
  assert.ok(!c.validateResponse(response({ code: "session_already_active" })));
  assert.ok(c.validateResponse(response(null)));
});

test("metadata is derived and consistent", async () => {
  const metadata = JSON.parse(await readFile(join(generated, "metadata.json"), "utf8"));
  assert.equal(metadata.protocol.name, "prime-agent.daemon");
  assert.equal(metadata.protocol.version, 7);
  assert.ok(metadata.commands.length >= 127, "the daemon catalog must be covered");
  assert.ok(metadata.outboundKinds.length > 0);
  assert.ok(metadata.errorCodes.length > 0);
  assert.equal(metadata.timeouts.longRunningCommandTypes.length, 2);
  // Open on the wire: capabilities are informational lists, never closed unions.
  assert.ok(metadata.capabilities.client.length > 0);
  assert.ok(metadata.capabilities.server.length > 0);
});
