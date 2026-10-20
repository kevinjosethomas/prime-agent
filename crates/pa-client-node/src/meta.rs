//! Build-boundary metadata, all derived — no hand-maintained tables.

use napi::bindgen_prelude::*;
use napi_derive::napi;
use pa_types::daemon::{
    DAEMON_PROTOCOL_NAME, DAEMON_PROTOCOL_VERSION, DAEMON_SCHEMA_ID, DAEMON_SCHEMA_REVISION,
};

#[napi(object)]
#[allow(dead_code)]
pub struct ClientMeta {
    /// The binding crate version (workspace version at build time).
    pub version: String,
    /// The harness source commit the addon was built from (build script).
    pub harness_commit: String,
    /// The daemon wire schema id, from `pa-types`.
    pub schema_id: String,
    /// The daemon wire schema revision, from `pa-types`.
    pub schema_revision: u32,
    /// The daemon protocol name, from `pa-types`.
    pub protocol_name: String,
    /// The daemon protocol version, from `pa-types`.
    pub protocol_version: u32,
    /// The Rust target triple this addon was compiled for.
    pub target_triple: String,
    /// `std::env::consts::OS` at compile time.
    pub os: String,
    /// `std::env::consts::ARCH` at compile time.
    pub arch: String,
}

#[napi]
/// The addon's derived build metadata. The runtime Node-API version is
/// `process.versions.napi` on the JS side; nothing here is hand-written.
pub fn client_metadata() -> ClientMeta {
    ClientMeta {
        version: env!("CARGO_PKG_VERSION").to_string(),
        harness_commit: env!("PA_CLIENT_NODE_HARNESS_COMMIT").to_string(),
        schema_id: DAEMON_SCHEMA_ID.to_string(),
        schema_revision: u32::try_from(DAEMON_SCHEMA_REVISION)
            .expect("the schema revision fits a u32"),
        protocol_name: DAEMON_PROTOCOL_NAME.to_string(),
        protocol_version: u32::try_from(DAEMON_PROTOCOL_VERSION)
            .expect("the protocol version fits a u32"),
        target_triple: env!("PA_CLIENT_NODE_TARGET_TRIPLE").to_string(),
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
    }
}
