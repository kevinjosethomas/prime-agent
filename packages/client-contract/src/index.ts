/**
 * The Prime Agent daemon client contract (generated surface).
 *
 * Types come from generated/ts (derived from the pa-types wire graph);
 * validators are Ajv compiled from the same canonical schemas; metadata
 * carries the derived command/error/capability catalogs.
 *
 * Non-goals (see README): transport, lifecycle, reconnect policy, platform
 * packaging. This package is unused until client adoption.
 */
export * from "../generated/ts/index.js";
export {
  loadContract,
  checkInboundFrame,
  type Contract,
  type SchemaValidator,
  type FrameVerdict,
} from "./validators.js";
import metadataJson from "../generated/metadata.json" with { type: "json" };
/** Derived wire metadata: protocol, commands, kinds, errors, capabilities. */
export const metadata = metadataJson;
