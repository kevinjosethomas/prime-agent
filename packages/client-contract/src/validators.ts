/**
 * Runtime validators compiled from the canonical JSON Schemas.
 *
 * Deserialization-acceptance direction: a green verdict means the wire value
 * is accepted INTO a client. Never mutates input (removeAdditional stays
 * false). Two layers on receive, per the wire contract:
 *  - validateInboundFrame: the OPEN envelope — any object with a string
 *    `type` is a frame; unknown kinds pass through raw.
 *  - for a KNOWN kind (metadata.outboundKinds), validateOutboundFrame must
 *    additionally pass; unknown kinds are forwarded, not refused.
 */
import { createRequire } from "node:module";
import { readFile } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";

// ajv 8's 2020 build is CommonJS; `createRequire` loads it without the
// NodeNext default-interop typing quirk.
type AjvCompiler = (schema: object) => (data: unknown) => boolean;
type Ajv2020Ctor = new (opts?: object) => { compile: AjvCompiler };
const Ajv2020 = createRequire(import.meta.url)("ajv/dist/2020.js") as Ajv2020Ctor;

// The emitted module lives at dist/src/ (tsc mirrors the source layout for
// the generated type imports): the committed `generated` tree is two levels
// up from there — the same relative layout in dev and in a packed install.
const generated = fileURLToPath(new URL("../../generated", import.meta.url));

export interface SchemaValidator {
  /** True when the value is accepted by the schema. Never mutates it. */
  (value: unknown): boolean;
  /** The JSON Schema this validator was compiled from (canonical artifact). */
  schema: object;
}

async function compileSchema(stem: string): Promise<SchemaValidator> {
  const schema = JSON.parse(
    await readFile(join(generated, "schema", `${stem}.schema.json`), "utf8"),
  );
  const ajv = new Ajv2020({ strict: false, validateFormats: false, removeAdditional: false });
  const validate = ajv.compile(schema);
  const fn = (value: unknown) => validate(value) as boolean;
  fn.schema = schema;
  return fn;
}

export interface Contract {
  validateCommand: SchemaValidator;
  validateCommandEnvelope: SchemaValidator;
  validateCommandWire: SchemaValidator;
  validateOutboundFrame: SchemaValidator;
  validateResponse: SchemaValidator;
  validateErrorInfo: SchemaValidator;
  validateEventEnvelope: SchemaValidator;
  validateEventMeta: SchemaValidator;
  validateResumeCursor: SchemaValidator;
  validateAgentMessage: SchemaValidator;
  validateInboundFrame: SchemaValidator;
  /** Outbound frame kinds known to this schema revision (metadata-derived). */
  knownOutboundKinds: string[];
}

let metadata_cache: unknown | null = null;
let metadata_loaded = false;

/** The derived wire metadata (protocol, commands, kinds, errors, capabilities). */
export function loadMetadata(): unknown {
  if (!metadata_loaded) {
    metadata_cache = JSON.parse(
      readFileSync(join(generated, "metadata.json"), "utf8"),
    );
    metadata_loaded = true;
  }
  return metadata_cache;
}

let contract: Contract | null = null;

/** Load and compile the contract (one process may reuse the same instance). */
export async function loadContract(): Promise<Contract> {
  if (contract) return contract;
  const [
    validateCommand,
    validateCommandEnvelope,
    validateCommandWire,
    validateOutboundFrame,
    validateResponse,
    validateErrorInfo,
    validateEventEnvelope,
    validateEventMeta,
    validateResumeCursor,
    validateAgentMessage,
    validateInboundFrame,
  ] = await Promise.all(
    [
      "daemon-command",
      "daemon-command-envelope",
      "daemon-command-wire",
      "daemon-outbound",
      "daemon-response",
      "daemon-error-info",
      "daemon-event-envelope",
      "daemon-event-meta",
      "daemon-resume-cursor",
      "agent-message",
      "client-inbound-frame",
    ].map(compileSchema),
  );
  const metadata = loadMetadata() as { outboundKinds: string[] };
  contract = {
    validateCommand,
    validateCommandEnvelope,
    validateCommandWire,
    validateOutboundFrame,
    validateResponse,
    validateErrorInfo,
    validateEventEnvelope,
    validateEventMeta,
    validateResumeCursor,
    validateAgentMessage,
    validateInboundFrame,
    knownOutboundKinds: metadata.outboundKinds as string[],
  };
  return contract;
}

export interface FrameVerdict {
  /** The envelope accepted the frame. */
  accepted: boolean;
  /** A known kind whose closed shape validated (known kinds must be strict). */
  knownKindValid: boolean;
  /** The frame's kind was not in this schema revision: pass through raw. */
  unknownKind: boolean;
}

/**
 * The receive-side frame check. Known kinds get strict shape validation;
 * unknown kinds pass through (forward raw). A known kind that fails its
 * closed shape is a conformance failure, not an unknown frame.
 */
export function checkInboundFrame(c: Contract, frame: unknown): FrameVerdict {
  if (!c.validateInboundFrame(frame)) return { accepted: false, knownKindValid: false, unknownKind: false };
  const type = (frame as { type?: unknown }).type;
  if (typeof type !== "string") return { accepted: false, knownKindValid: false, unknownKind: false };
  if (c.knownOutboundKinds.includes(type)) {
    return { accepted: true, knownKindValid: c.validateOutboundFrame(frame), unknownKind: false };
  }
  return { accepted: true, knownKindValid: false, unknownKind: true };
}
