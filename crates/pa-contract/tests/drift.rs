//! The drift gate: regeneration must byte-match the committed generated tree.

use std::path::Path;

fn generated_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/client-contract/generated")
        .canonicalize()
        .expect("committed generated dir exists")
        .leak()
}

#[test]
fn regeneration_matches_committed_output() {
    let committed = generated_dir();
    let fresh = pa_contract::export_map().expect("regenerate export");
    let mut checked = 0usize;
    for (relative, content) in &fresh {
        let path = committed.join(relative);
        let committed_content = std::fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("generated artifact not committed: {relative}"));
        assert_eq!(
            committed_content,
            *content,
            "generated output drifted: {relative} (run `cargo run -p pa-contract` and commit)"
        );
        checked += 1;
    }
    // Every committed file in the RUST-OWNED outputs must be known to the
    // exporter (no stale artifacts). `generated/ts` and
    // `generated/typecheck` are Node-owned (produced by `npm run gen:ts`);
    // the node-side drift gate owns them.
    for path in walk(&committed.join("schema"))
        .into_iter()
        .chain(walk(&committed.join("corpus")))
        .chain(vec![committed.join("metadata.json")].into_iter().filter(|p| p.exists()))
    {
        let relative = path.strip_prefix(committed).unwrap().to_string_lossy();
        assert!(
            fresh.contains_key(relative.as_ref()),
            "stale committed artifact: {relative}"
        );
    }
    assert_eq!(
        checked,
        fresh.len(),
        "every exported artifact must be committed and byte-identical"
    );
}

fn walk(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).expect("committed dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}
