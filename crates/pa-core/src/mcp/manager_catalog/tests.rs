//! The manager-catalog unit battery: the resolution order, the endpoint
//! pinning, the paste install, and the demand-driven verification.
use super::*;
use crate::mcp::catalog_schema::{parse_plugins_catalog, McpServiceEntry};
use crate::mcp::catalog_views::{fresh_mcp_login_allowed, is_pasteable_token_service};
use crate::mcp::connection_store::McpConnectionStatus as RecordStatus;
use crate::mcp::probe::McpEndpointProbeImpl;
use crate::mcp::probe::ProbeOutcome;
use crate::mcp::McpManagerOptions;
use crate::mcp::McpServerConfig;
use std::collections::HashMap;
use std::path::PathBuf;

/// The REAL shipped catalog payload projected to the v2 client contract.
const REAL_PAYLOAD: &str = include_str!("../../../tests/fixtures/mcp/plugins-catalog.v2.json");

fn real_catalog() -> crate::mcp::catalog_schema::PluginsCatalog {
    parse_plugins_catalog(REAL_PAYLOAD.as_bytes()).expect("fixture parses")
}

fn entry_json(server: &str, url: &str) -> serde_json::Value {
    serde_json::json!({
        "server": server, "service": server, "label": server, "url": url,
        "aliases": [],
        "transport": { "type": "http", "url": url },
        "auth": { "strategy": "oauth", "clientRegistration": "dynamic" },
        "setup": { "status": "ready" },
        "verification": { "status": "unverified" },
        "legacyBuiltin": false, "provenance": [{ "source": "prime" }],
        "oauth": { "kind": "oauth" }
    })
}

fn pasteable_entry_json(server: &str, url: &str) -> serde_json::Value {
    serde_json::json!({
        "server": server, "service": server, "label": server, "url": url,
        "aliases": [],
        "transport": { "type": "http", "url": url },
        "auth": { "strategy": "api_key", "clientRegistration": "unknown" },
        "setup": {
            "status": "requires-setup",
            "reason": "paste a token",
            "fields": [
                { "id": "SERVICE_PAT_TOKEN", "label": "SERVICE_PAT_TOKEN",
                  "required": true, "kind": "bearer-token", "credentialSet": "pat" }
            ]
        },
        "verification": { "status": "unverified" },
        "legacyBuiltin": false, "provenance": [{ "source": "prime" }]
    })
}

fn catalog_from_entries(entries: &[serde_json::Value]) -> Vec<McpServiceEntry> {
    let doc = serde_json::json!({ "version": 2, "counts": {}, "entries": entries });
    parse_plugins_catalog(doc.to_string().as_bytes())
        .expect("test catalog parses")
        .entries
}

fn no_user_servers() -> Box<dyn Fn() -> Option<HashMap<String, McpServerConfig>> + Send + Sync> {
    Box::new(|| None)
}

fn manager_with_remote(agent_dir: PathBuf, remote: Vec<McpServiceEntry>) -> McpManager {
    McpManager::new(McpManagerOptions {
        // File-backed store in the agent dir: credentials persist across
        // manager instances like the product paths.
        auth_storage: crate::auth::AuthStorage::create(&agent_dir),
        get_user_servers: no_user_servers(),
        begin_login: None,
        agent_dir: Some(agent_dir),
        get_catalog_sources: None,
        remote_source: Some(Box::new(move || {
            let entries = remote.clone();
            Some(crate::mcp::catalog_schema::PluginsCatalog {
                version: 2,
                counts: crate::mcp::catalog_schema::CatalogCounts::default(),
                entries,
            })
        })),
        probe_override: None,
    })
}

/// Resolution order, first wins per id: builtins > local > remote.
#[test]
fn resolution_order_builtins_local_remote() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let local = serde_json::json!({
        "version": 1,
        "entries": [
            {
                "server": "my-local", "service": "my-local", "label": "My Local",
                "url": "https://my-local.example/mcp", "aliases": [],
                "transport": { "type": "http", "url": "https://my-local.example/mcp" },
                "auth": { "strategy": "oauth", "clientRegistration": "dynamic" },
                "setup": { "status": "ready" },
                "verification": { "status": "unverified" },
                "legacyBuiltin": false,
                "provenance": [{ "source": "user" }]
            }
        ]
    });
    std::fs::write(
        agent_dir.path().join("mcp-services.json"),
        local.to_string(),
    )
    .expect("write local source");
    let remote = catalog_from_entries(&[
        entry_json("linear", "https://evil.example/mcp"),
        entry_json("my-local", "https://remote-wins.example/mcp"),
        entry_json("remote-only", "https://remote-only.example/mcp"),
    ]);
    let manager = manager_with_remote(agent_dir.path().to_path_buf(), remote);
    let descriptors = manager.service_descriptors();
    let linear = descriptors
        .iter()
        .find(|service| service.service_id == "linear")
        .expect("linear resolved");
    assert!(linear.legacy_builtin);
    assert_eq!(
        linear.transport.endpoint(),
        Some("https://mcp.linear.app/mcp"),
        "the compiled builtin wins over the remote entry"
    );
    let my_local = descriptors
        .iter()
        .find(|service| service.service_id == "my-local")
        .expect("local entry resolved");
    assert!(
        my_local.local_source,
        "diagnostics: {:?}",
        manager.service_catalog_diagnostics()
    );
    assert_eq!(
        my_local.transport.endpoint(),
        Some("https://my-local.example/mcp")
    );
    assert!(
        manager
            .service_catalog_diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.contains("my-local")),
        "the id collision is visible: {:?}",
        manager.service_catalog_diagnostics()
    );
    assert!(descriptors
        .iter()
        .any(|service| service.service_id == "remote-only"));
    // The remote-only entry is never connectable through a user server
    // shadow until the user owns the id.
    let remote_only = descriptors
        .iter()
        .find(|service| service.service_id == "remote-only")
        .expect("remote-only");
    assert!(fresh_mcp_login_allowed(remote_only));
}

/// Local sources cannot shadow a bundled id: a local `linear` entry is
/// refused at load time with a visible diagnostic (TS loader parity).
#[test]
fn local_sources_cannot_shadow_builtins() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let local = serde_json::json!({
        "version": 1,
        "entries": [
            {
                "server": "linear", "service": "linear", "label": "Fake Linear",
                "url": "https://evil.example/mcp", "aliases": [],
                "transport": { "type": "http", "url": "https://evil.example/mcp" },
                "auth": { "strategy": "oauth", "clientRegistration": "dynamic" },
                "setup": { "status": "ready" },
                "verification": { "status": "unverified" },
                "legacyBuiltin": false,
                "provenance": [{ "source": "user" }]
            }
        ]
    });
    std::fs::write(
        agent_dir.path().join("mcp-services.json"),
        local.to_string(),
    )
    .expect("write local source");
    let manager = manager_with_remote(agent_dir.path().to_path_buf(), Vec::new());
    let linear = manager
        .service_descriptor("linear")
        .expect("linear still resolves");
    assert_eq!(
        linear.transport.endpoint(),
        Some("https://mcp.linear.app/mcp"),
        "the compiled builtin is never shadowed"
    );
    assert!(
        manager
            .service_catalog_diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.contains("collides with the bundled catalog entry")),
        "the refusal is visible: {:?}",
        manager.service_catalog_diagnostics()
    );
}

/// ENDPOINT PINNING: an installed connection keeps its approved record endpoint
/// even when the catalog URL changes afterwards.
#[test]
fn installed_connections_are_endpoint_pinned_across_catalog_url_changes() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let endpoint_a = "https://service-a.example/mcp";
    let endpoint_b = "https://service-b.example/mcp";
    let mut manager = manager_with_remote(
        agent_dir.path().to_path_buf(),
        catalog_from_entries(&[entry_json("pinned-service", endpoint_a)]),
    );
    let mut store = manager.connection_store.lock().unwrap();
    store
        .upsert(&new_pending_record(
            "pinned-service",
            "pinned-service",
            "pinned-service",
            endpoint_a,
        ))
        .map_err(|_| ())
        .unwrap();
    // A verified past handshake: store the OAuth grant bound to endpoint A.
    {
        let mut record = store.get("pinned-service").cloned().unwrap();
        record.status = RecordStatus::Connected;
        record.verified_at = Some(now_ms());
        store.upsert(&record).map_err(|_| ()).unwrap();
    }
    drop(store);
    {
        let storage = manager.auth_storage_handle();
        let mut auth = futures::executor::block_on(storage.lock());
        auth.set(
            "mcp:pinned-service",
            AuthCredential::Oauth {
                access: "pinned-access".to_string(),
                refresh: Some("pinned-refresh".to_string()),
                expires: (now_ms() as i64) + 3_600_000,
                account_id: None,
                endpoint: Some(endpoint_a.to_string()),
                token_endpoint: None,
                client_id: None,
                resource: None,
                issuer: None,
                audience_mode: None,
                enterprise_url: None,
            },
        );
    }
    manager.refresh();
    assert_eq!(
        manager.integration_endpoint("pinned-service").as_deref(),
        Some(endpoint_a),
        "dispatch config serves the installed endpoint"
    );
    let manager = {
        std::mem::drop(manager);
        manager_with_remote(
            agent_dir.path().to_path_buf(),
            catalog_from_entries(&[entry_json("pinned-service", endpoint_b)]),
        )
    };
    assert_eq!(
        manager.integration_endpoint("pinned-service").as_deref(),
        Some(endpoint_a),
        "the pin survives the catalog URL change"
    );
    let roster = manager.connection_roster();
    let pinned = roster
        .iter()
        .find(|entry| entry.server == "pinned-service")
        .expect("the pinned connection stays manageable");
    assert!(pinned.connected);
}

/// A vanished source keeps a durable pin built from the record: the
/// connection stays manageable and is NEVER one-click connectable.
#[test]
fn vanished_sources_pin_from_the_record_and_stay_manageable() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let endpoint = "https://vanished.example/mcp";
    let manager = manager_with_remote(
        agent_dir.path().to_path_buf(),
        catalog_from_entries(&[entry_json("vanished-service", endpoint)]),
    );
    let mut store = manager.connection_store.lock().unwrap();
    store
        .upsert(&new_pending_record(
            "vanished-service",
            "vanished-service",
            "Vanished",
            endpoint,
        ))
        .map_err(|_| ())
        .unwrap();
    drop(store);
    let manager = manager_with_remote(agent_dir.path().to_path_buf(), Vec::new());
    let pinned = manager
        .service_descriptor("vanished-service")
        .expect("pinned from the record");
    assert!(pinned.pinned_from_record);
    assert_eq!(pinned.transport.endpoint(), Some(endpoint));
    assert!(!fresh_mcp_login_allowed(pinned));
    let roster = manager.connection_roster();
    assert!(
        roster
            .iter()
            .any(|entry| entry.server == "vanished-service"),
        "the pinned connection stays manageable (verify/disconnect)"
    );
    assert!(!is_pasteable_token_service(pinned));
}

/// The pinned-definition hint needs PROOF: a record whose service is missing from a VALIDATED
/// remote snapshot is genuinely gone,.
#[test]
fn pinned_hint_shows_when_a_snapshot_proves_the_service_gone() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let endpoint = "https://vanished.example/mcp";
    let mut manager = manager_with_remote(
        agent_dir.path().to_path_buf(),
        catalog_from_entries(&[entry_json(
            "unrelated-service",
            "https://unrelated.example/mcp",
        )]),
    );
    {
        let mut store = manager.connection_store.lock().unwrap();
        store
            .upsert(&new_pending_record(
                "vanished-service",
                "vanished-service",
                "Vanished",
                endpoint,
            ))
            .map_err(|_| ())
            .unwrap();
    }
    manager.refresh();
    let view = manager
        .service_catalog_views()
        .into_iter()
        .find(|view| view.service_id == "vanished-service")
        .expect("pinned row");
    assert_eq!(
        view.setup_hint.as_deref(),
        Some(crate::mcp::catalog_plugin_views::PINNED_FROM_RECORD_HINT),
    );
}

/// Without a validated snapshot a pinned record CANNOT prove its source is unavailable — TS never
/// claims absence it cannot prove — so the card stays silent.
#[test]
fn pinned_hint_stays_silent_without_a_snapshot() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let endpoint = "https://vanished.example/mcp";
    let mut manager = McpManager::new(McpManagerOptions {
        auth_storage: crate::auth::AuthStorage::create(agent_dir.path()),
        get_user_servers: no_user_servers(),
        begin_login: None,
        agent_dir: Some(agent_dir.path().to_path_buf()),
        get_catalog_sources: None,
        remote_source: Some(Box::new(|| None)),
        probe_override: None,
    });
    {
        let mut store = manager.connection_store.lock().unwrap();
        store
            .upsert(&new_pending_record(
                "vanished-service",
                "vanished-service",
                "Vanished",
                endpoint,
            ))
            .map_err(|_| ())
            .unwrap();
    }
    manager.refresh();
    let view = manager
        .service_catalog_views()
        .into_iter()
        .find(|view| view.service_id == "vanished-service")
        .expect("the pinned row stays manageable");
    assert_ne!(
        view.setup_hint.as_deref(),
        Some(crate::mcp::catalog_plugin_views::PINNED_FROM_RECORD_HINT),
        "no snapshot in hand means no source-unavailable claim"
    );
    assert!(!view.connectable, "a pin is never one-click connectable");
}

/// A snapshot that still defines the service is not a pin at all: the
/// record resolves against the catalog and no hint renders.
#[test]
fn pinned_hint_stays_hidden_when_the_snapshot_defines_the_service() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let endpoint = "https://lives-on.example/mcp";
    let mut manager = manager_with_remote(
        agent_dir.path().to_path_buf(),
        catalog_from_entries(&[entry_json("lives-on", endpoint)]),
    );
    {
        let mut store = manager.connection_store.lock().unwrap();
        store
            .upsert(&new_pending_record(
                "lives-on", "lives-on", "Lives On", endpoint,
            ))
            .map_err(|_| ())
            .unwrap();
    }
    manager.refresh();
    let resolved = manager
        .service_descriptor("lives-on")
        .expect("resolved from the catalog");
    assert!(!resolved.pinned_from_record);
    let view = manager
        .service_catalog_views()
        .into_iter()
        .find(|view| view.service_id == "lives-on")
        .expect("catalog row");
    assert_ne!(
        view.setup_hint.as_deref(),
        Some(crate::mcp::catalog_plugin_views::PINNED_FROM_RECORD_HINT),
        "a service the snapshot defines never renders the pin hint"
    );
}

/// A fake probe: records the (url, token) pairs it verified in a shared
/// log the test asserts over.
struct FakeProbe {
    log: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    tool_count: usize,
}

impl McpEndpointProbeImpl for FakeProbe {
    fn probe_dyn(
        &self,
        url: &str,
        token: &str,
    ) -> futures::future::BoxFuture<'static, ProbeOutcome> {
        self.log
            .lock()
            .unwrap()
            .push((url.to_string(), token.to_string()));
        let tool_count = self.tool_count;
        Box::pin(async move { Ok(tool_count) })
    }
}

#[tokio::test]
async fn paste_flow_installs_a_bearer_token_service_end_to_end() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let endpoint = "https://paste-service.example/mcp";
    let remote = catalog_from_entries(&[pasteable_entry_json("paste-service", endpoint)]);
    let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let probe = McpEndpointProbe::new(std::sync::Arc::new(FakeProbe {
        log: std::sync::Arc::clone(&log),
        tool_count: 3,
    }));
    let probe_for_inputs = probe.clone();
    let manager = std::sync::Arc::new(std::sync::Mutex::new(McpManager::new(McpManagerOptions {
        auth_storage: crate::auth::AuthStorage::in_memory(
            &crate::auth::types::AuthStorageData::default(),
            std::sync::Arc::new(crate::auth::manager::NoOAuth),
        ),
        get_user_servers: no_user_servers(),
        begin_login: None,
        agent_dir: Some(agent_dir.path().to_path_buf()),
        get_catalog_sources: None,
        remote_source: Some(Box::new(move || {
            let entries = remote.clone();
            Some(crate::mcp::catalog_schema::PluginsCatalog {
                version: 2,
                counts: crate::mcp::catalog_schema::CatalogCounts::default(),
                entries,
            })
        })),
        probe_override: Some(probe_for_inputs),
    })));
    let views = {
        let manager = std::sync::Arc::clone(&manager);
        tokio::task::spawn_blocking(move || manager.lock().unwrap().service_catalog_views())
            .await
            .expect("blocking view read")
    };
    let before = views
        .iter()
        .find(|view| view.service_id == "paste-service")
        .expect("discovery row");
    assert_eq!(
        before.connection_status.as_str(),
        crate::mcp::catalog_status_views::McpConnectionStatus::SetupRequired.as_str()
    );
    assert_eq!(before.paste_token, Some(true));
    assert!(before
        .setup_hint
        .as_deref()
        .is_some_and(|hint| hint.contains("paste")));
    let inputs = manager
        .lock()
        .unwrap()
        .paste_install_inputs("paste-service")
        .expect("the service is pasteable");
    assert_eq!(inputs.endpoint, endpoint);
    let install = install_static_token(inputs, "ghp_pasted-token-value")
        .await
        .expect("install completes");
    assert!(install.verified, "verified: {:?}", install.error);
    assert_eq!(
        install.endpoint, endpoint,
        "the token is pinned to the endpoint"
    );
    assert_eq!(install.tool_count, Some(3));
    let probed = log.lock().unwrap().clone();
    assert_eq!(
        probed,
        vec![(endpoint.to_string(), "ghp_pasted-token-value".to_string())]
    );
    // The post-install view reads the auth store (blocking): the daemon
    // wraps the read in spawn_blocking; the test does the same.
    let (views, roster, credential) = {
        let manager = std::sync::Arc::clone(&manager);
        tokio::task::spawn_blocking(move || {
            manager.lock().unwrap().refresh();
            let views = manager.lock().unwrap().service_catalog_views();
            let roster = manager.lock().unwrap().connection_roster();
            let credential = manager
                .lock()
                .unwrap()
                .credential_snapshot()
                .get("mcp:paste-service")
                .cloned();
            (views, roster, credential)
        })
        .await
        .expect("blocking view read")
    };
    let after = views
        .iter()
        .find(|view| view.service_id == "paste-service")
        .expect("row after install");
    assert_eq!(after.connection_status.as_str(), "connected");
    assert_eq!(after.connection_ids, vec!["paste-service".to_string()]);
    assert_eq!(after.tool_count, Some(3));
    let entry = roster
        .iter()
        .find(|entry| entry.server == "paste-service")
        .expect("roster row");
    assert!(entry.connected);
    assert!(
        entry.generic,
        "the kernel dispatches it through the generic API"
    );
    let credential = credential.expect("credential stored");
    match credential {
        AuthCredential::McpStaticToken {
            bearer,
            endpoint: bound,
        } => {
            assert_eq!(bearer, "ghp_pasted-token-value");
            assert_eq!(bound.as_deref(), Some(endpoint));
        }
        other => panic!("wrong credential type: {other:?}"),
    }
    let two_cred = serde_json::json!({
        "server": "two-cred", "service": "two-cred", "label": "Two",
        "url": "https://two.example/mcp", "aliases": [],
        "transport": { "type": "http", "url": "https://two.example/mcp" },
        "auth": { "strategy": "api_key", "clientRegistration": "unknown" },
        "setup": {
            "status": "requires-setup", "reason": "two secrets",
            "fields": [
                { "id": "A_KEY", "label": "A", "required": true,
                  "kind": "bearer-token", "credentialSet": "cred-a" },
                { "id": "B_KEY", "label": "B", "required": true,
                  "kind": "api-key", "credentialSet": "cred-b" }
            ]
        },
        "verification": { "status": "unverified" },
        "legacyBuiltin": false, "provenance": [{ "source": "prime" }]
    });
    let entries = catalog_from_entries(&[two_cred]);
    for entry in &entries {
        let descriptor =
            crate::mcp::service_catalog::McpServiceDescriptor::from_entry(entry, false);
        assert!(
            !is_pasteable_token_service(&descriptor),
            "fail closed on two distinct credentials"
        );
    }
}

/// The GitHub alias pair from the REAL payload resolves to exactly ONE credential, and the derived
/// prompt label reads "GitHub personal access token".
#[test]
fn real_github_entry_is_pasteable_with_one_aliased_credential() {
    let catalog = real_catalog();
    let github = catalog
        .entries
        .iter()
        .find(|entry| entry.server == "github")
        .expect("github");
    let descriptor = crate::mcp::service_catalog::McpServiceDescriptor::from_entry(github, false);
    assert!(is_pasteable_token_service(&descriptor));
    let pasted =
        crate::mcp::catalog_views::mcp_paste_credential(&descriptor).expect("one credential");
    assert_eq!(pasted.field.id, "GITHUB_PAT_TOKEN");
    assert_eq!(
        pasted.field_ids,
        vec![
            "GITHUB_PAT_TOKEN".to_string(),
            "GITHUB_PERSONAL_ACCESS_TOKEN".to_string()
        ]
    );
    assert_eq!(
        crate::mcp::catalog_views::mcp_credential_field_prompt_label(&descriptor, &pasted.field),
        "GitHub personal access token"
    );
}

/// The full real catalog flows through the manager: 68 descriptors
/// resolve (builtins first, discovery rows connected-first).
#[test]
fn the_real_catalog_resolves_through_the_manager() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let manager = manager_with_remote(agent_dir.path().to_path_buf(), real_catalog().entries);
    assert_eq!(manager.service_descriptors().len(), 69);
    assert!(manager.is_reserved_server_name("linear"));
    assert!(manager.is_reserved_server_name("notion"));
    assert!(!manager.is_reserved_server_name("github"));
    assert_eq!(
        manager
            .connection_roster()
            .iter()
            .filter(|entry| !entry.user_declared)
            .count(),
        69
    );
}

/// The `/mcp` view's api-key credential rows: the catalog entry renders
/// with its honest configured state from the shared auth store (the
/// `serper` slot the websearch runtime reads), and only a stored API key
/// marks it configured.
#[test]
fn api_key_credential_views_read_the_shared_store() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let mut manager = McpManager::new(McpManagerOptions {
        auth_storage: crate::auth::AuthStorage::create(agent_dir.path()),
        get_user_servers: no_user_servers(),
        begin_login: None,
        agent_dir: Some(agent_dir.path().to_path_buf()),
        get_catalog_sources: None,
        remote_source: None,
        probe_override: None,
    });
    manager.refresh();
    // The web-search credential's row renders, honestly unconfigured.
    let views = manager.api_key_credential_views();
    assert_eq!(
        views,
        vec![McpCredentialView {
            id: "serper".to_string(),
            label: "Serper (web search)".to_string(),
            configured: false,
        }],
        "the api-key credential catalog serves the web-search entry"
    );
    // A stored API key at the slot marks the row configured.
    {
        let storage = manager.auth_storage_handle();
        let mut auth = futures::executor::block_on(storage.lock());
        auth.set(
            "serper",
            AuthCredential::ApiKey {
                key: "serper-key".to_string(),
                prime_team: None,
            },
        );
    }
    manager.refresh();
    assert!(
        manager
            .api_key_credential_views()
            .first()
            .expect("the serper row renders")
            .configured,
        "a stored API key marks the credential configured"
    );
    // A non-API-key value at the slot never reads configured.
    {
        let storage = manager.auth_storage_handle();
        let mut auth = futures::executor::block_on(storage.lock());
        auth.set(
            "serper",
            AuthCredential::Oauth {
                access: "access".to_string(),
                refresh: None,
                expires: 0,
                account_id: None,
                endpoint: None,
                token_endpoint: None,
                client_id: None,
                resource: None,
                issuer: None,
                audience_mode: None,
                enterprise_url: None,
            },
        );
    }
    manager.refresh();
    assert!(
        !manager
            .api_key_credential_views()
            .first()
            .expect("the serper row renders")
            .configured,
        "only an API-key credential marks the row configured"
    );
}

#[test]
fn api_key_credential_views_see_a_cross_instance_write_after_the_reload() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let mut manager = McpManager::new(McpManagerOptions {
        auth_storage: crate::auth::AuthStorage::create(agent_dir.path()),
        get_user_servers: no_user_servers(),
        begin_login: None,
        agent_dir: Some(agent_dir.path().to_path_buf()),
        get_catalog_sources: None,
        remote_source: None,
        probe_override: None,
    });
    manager.refresh();
    assert!(
        !manager
            .api_key_credential_views()
            .first()
            .expect("the serper row renders")
            .configured,
        "the fresh store holds no key"
    );
    // The interactive client's `/mcp` key flow stores through its OWN
    // storage instance (a different process writing the same auth.json).
    {
        let mut client_side = crate::auth::AuthStorage::create(agent_dir.path());
        client_side.set(
            "serper",
            AuthCredential::ApiKey {
                key: "serper-key".to_string(),
                prime_team: None,
            },
        );
    }
    // Without the reload the manager's cached copy stays stale.
    assert!(
        !manager
            .api_key_credential_views()
            .first()
            .expect("the serper row renders")
            .configured,
        "the cached copy does not see the other instance's write"
    );
    // The view open's reload picks the stored key up.
    manager.reload_auth_storage().expect("the reload applies");
    assert!(
        manager
            .api_key_credential_views()
            .first()
            .expect("the serper row renders")
            .configured,
        "the reload sees the cross-instance write"
    );
}

// ---------------------------------------------------------------------------
// Multi-account: add-account minting, identity labels, rename, disconnect.
// ---------------------------------------------------------------------------

/// A login UI whose manual paste derives the callback redirect from the
/// authorization URL (the `oauth` suite's `TestUi` shape, local to these tests).
struct ManualPasteUi {
    auth_url: std::sync::Mutex<String>,
}

impl crate::mcp::McpLoginUi for ManualPasteUi {
    fn on_progress(&self, _message: &str) {}
    fn on_auth(&self, url: &str, _instructions: &str) {
        *self.auth_url.lock().unwrap() = url.to_string();
    }
    fn on_prompt(
        &self,
        _message: &str,
        _placeholder: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send>> {
        Box::pin(async { Err(anyhow::anyhow!("prompt surface unavailable in tests")) })
    }
    fn on_manual_code_input(
        &self,
    ) -> Option<std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send>>> {
        let url = self.auth_url.lock().unwrap().clone();
        Some(Box::pin(async move {
            let value = |name: &str| -> Option<String> {
                url.split('?')
                    .nth(1)?
                    .split('&')
                    .filter_map(|pair| pair.split_once('='))
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| value.replace("%3A", ":").replace("%2F", "/"))
            };
            let redirect = value("redirect_uri")?;
            let state = value("state")?;
            Some(format!("{redirect}?code=the-code&state={state}"))
        }))
    }
}

/// A scripted OAuth transport: url -> (status, body), unknown urls fail.
struct LoginHttp {
    responses: std::collections::HashMap<String, crate::mcp::OAuthHttpResponse>,
}

impl LoginHttp {
    fn new(responses: Vec<(&str, u16, &str)>) -> Self {
        LoginHttp {
            responses: responses
                .into_iter()
                .map(|(url, status, body)| {
                    (
                        url.to_string(),
                        crate::mcp::OAuthHttpResponse {
                            status,
                            headers: vec![(
                                "content-type".to_string(),
                                "application/json".to_string(),
                            )],
                            body: body.to_string(),
                        },
                    )
                })
                .collect(),
        }
    }
}

impl crate::mcp::OAuthHttp for LoginHttp {
    fn request(
        &self,
        request: crate::mcp::OAuthHttpRequest,
    ) -> futures::future::BoxFuture<'_, anyhow::Result<crate::mcp::OAuthHttpResponse>> {
        let response = self.responses.get(&request.url).cloned();
        Box::pin(async move {
            response.ok_or_else(|| anyhow::anyhow!("unexpected request: {}", request.url))
        })
    }
}

fn oauth_service_json(server: &str, url: &str) -> serde_json::Value {
    let mut entry = entry_json(server, url);
    entry["label"] = serde_json::json!("Linear");
    entry
}

#[tokio::test]
async fn add_account_login_mints_the_next_id_and_persists_identity() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let endpoint = "https://srv.test/mcp";
    let remote = catalog_from_entries(&[oauth_service_json("acme", endpoint)]);
    let probe = McpEndpointProbe::new(std::sync::Arc::new(FakeProbe {
        log: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        tool_count: 7,
    }));
    let manager = std::sync::Arc::new(std::sync::Mutex::new(McpManager::new(McpManagerOptions {
        auth_storage: crate::auth::AuthStorage::in_memory(
            &crate::auth::types::AuthStorageData::default(),
            std::sync::Arc::new(crate::auth::manager::NoOAuth),
        ),
        get_user_servers: no_user_servers(),
        begin_login: None,
        agent_dir: Some(agent_dir.path().to_path_buf()),
        get_catalog_sources: None,
        remote_source: Some(Box::new(move || {
            let entries = remote.clone();
            Some(crate::mcp::catalog_schema::PluginsCatalog {
                version: 2,
                counts: crate::mcp::catalog_schema::CatalogCounts::default(),
                entries,
            })
        })),
        probe_override: Some(probe),
    })));
    manager.lock().unwrap().refresh();
    let http = LoginHttp::new(vec![
        (endpoint, 404, ""),
        (
            "https://srv.test/.well-known/oauth-protected-resource/mcp",
            404,
            "",
        ),
        (
            "https://srv.test/.well-known/oauth-authorization-server",
            200,
            r#"{"issuer":"https://srv.test/tenant","authorization_endpoint":"https://srv.test/authorize","token_endpoint":"https://srv.test/token","registration_endpoint":"https://srv.test/register","scopes_supported":["read"],"userinfo_endpoint":"https://srv.test/userinfo"}"#,
        ),
        (
            "https://srv.test/register",
            200,
            r#"{"client_id":"registered-client","client_id_issued_at":0,"client_secret_expires_at":0}"#,
        ),
        (
            "https://srv.test/token",
            200,
            r#"{"access_token":"second-account-access","refresh_token":"second-refresh"}"#,
        ),
        (
            "https://srv.test/userinfo",
            200,
            r#"{"name":"Acme workspace","email":"dev@acme.io"}"#,
        ),
    ]);
    // Unknown urls must fail; the scripted map above misses 404s, so give the
    // discovery candidates empty-200 fallbacks by scripting every candidate.
    let ui = ManualPasteUi {
        auth_url: std::sync::Mutex::new(String::new()),
    };
    let context = manager
        .lock()
        .unwrap()
        .add_account_context("acme")
        .expect("the OAuth service supports an add-account login");
    assert_eq!(context.server(), "acme-2", "the first alias id");
    context.run(&ui, &http).await.expect("login completes");

    let manager2 = std::sync::Arc::clone(&manager);
    let (records, roster, views) = {
        let manager = std::sync::Arc::clone(&manager2);
        tokio::task::spawn_blocking(move || {
            manager.lock().unwrap().refresh();
            let records = manager.lock().unwrap().records_by_id();
            let roster = manager.lock().unwrap().connection_roster();
            let views = manager
                .lock()
                .unwrap()
                .service_catalog_connection_views(&[]);
            (records, roster, views)
        })
        .await
        .expect("blocking read")
    };
    let record = records.get("acme-2").expect("the alias record exists");
    assert_eq!(
        record.service_id, "acme",
        "the alias keeps the parent service"
    );
    assert_eq!(record.endpoint, endpoint);
    assert_eq!(record.status, RecordStatus::Connected);
    assert_eq!(record.tool_count, Some(7));
    assert_eq!(record.account_label.as_deref(), Some("Acme workspace"));
    assert_eq!(record.account_email.as_deref(), Some("dev@acme.io"));
    let alias = roster
        .iter()
        .find(|entry| entry.server == "acme-2")
        .expect("roster row for the alias");
    assert_eq!(alias.service_id.as_deref(), Some("acme"));
    assert_eq!(alias.account_label.as_deref(), Some("Acme workspace"));
    assert_eq!(alias.account_email.as_deref(), Some("dev@acme.io"));
    assert_eq!(
        alias.label, "Acme workspace",
        "the roster label is the identity, never the internal id"
    );
    let inventory = views
        .iter()
        .find(|view| view.connection_id == "acme-2")
        .expect("kernel inventory row");
    assert_eq!(inventory.service_id.as_deref(), Some("acme"));
    assert_eq!(
        inventory.label, "Acme workspace",
        "mcp.list_connections shows the label next to the id"
    );

    // The next add-account login mints the NEXT free id.
    let next = manager.lock().unwrap().next_account_connection_id("acme");
    assert_eq!(next.as_deref(), Some("acme-3"));

    // The next add-account login mints the NEXT free id while acme-2 lives.
    let context = manager
        .lock()
        .unwrap()
        .add_account_context("acme")
        .expect("minting skips the live alias");
    assert_eq!(context.server(), "acme-3");

    // A rename survives a re-login's identity read.
    manager
        .lock()
        .unwrap()
        .connection_handles()
        .renamed("acme-2", "Second workspace")
        .expect("rename writes");
    let http_relogin = LoginHttp::new(vec![
        (endpoint, 404, ""),
        (
            "https://srv.test/.well-known/oauth-protected-resource/mcp",
            404,
            "",
        ),
        (
            "https://srv.test/.well-known/oauth-authorization-server",
            200,
            r#"{"issuer":"https://srv.test/tenant","authorization_endpoint":"https://srv.test/authorize","token_endpoint":"https://srv.test/token","registration_endpoint":"https://srv.test/register","scopes_supported":["read"],"userinfo_endpoint":"https://srv.test/userinfo"}"#,
        ),
        (
            "https://srv.test/register",
            200,
            r#"{"client_id":"registered-client","client_id_issued_at":0,"client_secret_expires_at":0}"#,
        ),
        (
            "https://srv.test/token",
            200,
            r#"{"access_token":"relogin-access","refresh_token":"relogin-refresh"}"#,
        ),
        (
            "https://srv.test/userinfo",
            200,
            r#"{"name":"A different workspace","email":"other@acme.io"}"#,
        ),
    ]);
    let ui = ManualPasteUi {
        auth_url: std::sync::Mutex::new(String::new()),
    };
    let relogin_context = manager
        .lock()
        .unwrap()
        .login_context("acme-2")
        .expect("the alias re-logins on its own id");
    relogin_context
        .run(&ui, &http_relogin)
        .await
        .expect("re-login completes");
    manager.lock().unwrap().refresh();
    let record = manager
        .lock()
        .unwrap()
        .records_by_id()
        .get("acme-2")
        .cloned()
        .expect("record after re-login");
    assert_eq!(
        record.account_label.as_deref(),
        Some("Second workspace"),
        "the user's rename survives the re-login's identity read"
    );
    assert_eq!(
        record.account_email.as_deref(),
        Some("dev@acme.io"),
        "the identity email from the first connect stays after the rename"
    );

    // Disconnect the alias account; nothing else moves.
    let handles = manager.lock().unwrap().connection_handles();
    crate::mcp::remove_mcp_connection(&handles, "acme-2")
        .await
        .expect("disconnect");
    let records = manager.lock().unwrap().records_by_id();
    assert!(!records.contains_key("acme-2"), "the record left");
    let snapshot = {
        let manager = std::sync::Arc::clone(&manager);
        tokio::task::spawn_blocking(move || manager.lock().unwrap().credential_snapshot())
            .await
            .expect("blocking credential read")
    };
    assert!(snapshot.get("mcp:acme-2").is_none(), "the credential left");
    // The id frees for the next add-account login.
    assert_eq!(
        manager
            .lock()
            .unwrap()
            .next_account_connection_id("acme")
            .as_deref(),
        Some("acme-2")
    );
}

#[tokio::test]
async fn add_paste_account_mints_the_next_id_under_the_parent_service() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let endpoint = "https://paste-service.example/mcp";
    let remote = catalog_from_entries(&[pasteable_entry_json("paste-service", endpoint)]);
    let probe = McpEndpointProbe::new(std::sync::Arc::new(FakeProbe {
        log: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        tool_count: 2,
    }));
    let manager = std::sync::Arc::new(std::sync::Mutex::new(McpManager::new(McpManagerOptions {
        auth_storage: crate::auth::AuthStorage::in_memory(
            &crate::auth::types::AuthStorageData::default(),
            std::sync::Arc::new(crate::auth::manager::NoOAuth),
        ),
        get_user_servers: no_user_servers(),
        begin_login: None,
        agent_dir: Some(agent_dir.path().to_path_buf()),
        get_catalog_sources: None,
        remote_source: Some(Box::new(move || {
            let entries = remote.clone();
            Some(crate::mcp::catalog_schema::PluginsCatalog {
                version: 2,
                counts: crate::mcp::catalog_schema::CatalogCounts::default(),
                entries,
            })
        })),
        probe_override: Some(probe),
    })));
    manager.lock().unwrap().refresh();
    // The primary account installs first.
    let primary = manager
        .lock()
        .unwrap()
        .paste_install_inputs("paste-service")
        .expect("pasteable");
    install_static_token(primary, "primary-token")
        .await
        .expect("primary install");
    // The add-account install: the connection id is minted, the service id stays the parent's.
    let inputs = manager
        .lock()
        .unwrap()
        .add_paste_account_inputs("paste-service")
        .expect("add-account paste");
    assert_eq!(inputs.server, "paste-service-2");
    assert_eq!(inputs.service_id, "paste-service");
    install_static_token(inputs, "second-token")
        .await
        .expect("second install");
    manager.lock().unwrap().refresh();
    let records = manager.lock().unwrap().records_by_id();
    let second = records.get("paste-service-2").expect("alias record");
    assert_eq!(second.service_id, "paste-service");
    assert_eq!(second.endpoint, endpoint);
    // Both credentials live side by side (the snapshot read blocks; the
    // daemon wraps it in spawn_blocking, the test does the same).
    let credentials = {
        let manager = std::sync::Arc::clone(&manager);
        tokio::task::spawn_blocking(move || manager.lock().unwrap().credential_snapshot())
            .await
            .expect("blocking credential read")
    };
    assert!(credentials.get("mcp:paste-service").is_some());
    assert!(credentials.get("mcp:paste-service-2").is_some());
    // Disconnecting the second account keeps the first.
    let handles = manager.lock().unwrap().connection_handles();
    crate::mcp::remove_mcp_connection(&handles, "paste-service-2")
        .await
        .expect("disconnect");
    let records = manager.lock().unwrap().records_by_id();
    assert!(records.contains_key("paste-service"));
    assert!(!records.contains_key("paste-service-2"));
    let credentials = {
        let manager = std::sync::Arc::clone(&manager);
        tokio::task::spawn_blocking(move || manager.lock().unwrap().credential_snapshot())
            .await
            .expect("blocking credential read")
    };
    assert!(
        credentials.get("mcp:paste-service").is_some(),
        "the first account stays"
    );
    assert!(credentials.get("mcp:paste-service-2").is_none());
}

#[test]
fn alias_labels_never_show_the_internal_id() {
    let agent_dir = tempfile::tempdir().expect("tempdir");
    let endpoint = "https://alpha.example/mcp";
    let remote = catalog_from_entries(&[oauth_service_json("alpha", endpoint)]);
    let manager = std::sync::Arc::new(std::sync::Mutex::new(McpManager::new(McpManagerOptions {
        auth_storage: crate::auth::AuthStorage::in_memory(
            &crate::auth::types::AuthStorageData::default(),
            std::sync::Arc::new(crate::auth::manager::NoOAuth),
        ),
        get_user_servers: no_user_servers(),
        begin_login: None,
        agent_dir: Some(agent_dir.path().to_path_buf()),
        get_catalog_sources: None,
        remote_source: Some(Box::new(move || {
            let entries = remote.clone();
            Some(crate::mcp::catalog_schema::PluginsCatalog {
                version: 2,
                counts: crate::mcp::catalog_schema::CatalogCounts::default(),
                entries,
            })
        })),
        probe_override: None,
    })));
    manager.lock().unwrap().refresh();
    // Two alias records without identity labels: the fallback is the service
    // label plus an ordinal, never the connection id.
    let mut first =
        crate::mcp::connection_store::new_pending_record("alpha-2", "alpha", "Linear", endpoint);
    first.status = RecordStatus::Connected;
    let mut second =
        crate::mcp::connection_store::new_pending_record("alpha-3", "alpha", "Linear", endpoint);
    second.status = RecordStatus::Connected;
    {
        let manager = manager.lock().unwrap();
        let store = manager.connection_store_handle();
        store.lock().unwrap().upsert(&first).unwrap();
        store.lock().unwrap().upsert(&second).unwrap();
    }
    manager.lock().unwrap().refresh();
    let roster = manager.lock().unwrap().connection_roster();
    let labels: Vec<(String, String)> = roster
        .iter()
        .filter(|entry| entry.server.starts_with("alpha-"))
        .map(|entry| (entry.server.clone(), entry.label.clone()))
        .collect();
    assert_eq!(
        labels,
        vec![
            ("alpha-2".to_string(), "Linear account 2".to_string()),
            ("alpha-3".to_string(), "Linear account 3".to_string()),
        ]
    );
    // A rename relabels the roster row.
    {
        let manager = manager.lock().unwrap();
        manager
            .connection_handles()
            .renamed("alpha-3", "Work workspace")
            .expect("rename");
    }
    manager.lock().unwrap().refresh();
    let roster = manager.lock().unwrap().connection_roster();
    let entry = roster
        .iter()
        .find(|entry| entry.server == "alpha-3")
        .expect("row");
    assert_eq!(entry.label, "Work workspace");
    assert_eq!(entry.account_label.as_deref(), Some("Work workspace"));
    let views = manager
        .lock()
        .unwrap()
        .service_catalog_connection_views(&[]);
    let view = views
        .iter()
        .find(|view| view.connection_id == "alpha-3")
        .expect("inventory row");
    assert_eq!(view.label, "Work workspace");
}
