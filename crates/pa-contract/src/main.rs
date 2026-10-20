//! `pa-contract export [-- DIR]`: write the canonical generated artifacts
//! (default: `packages/client-contract/generated/`).

use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let dir = std::env::args()
        .nth(1)
        .map_or_else(pa_contract::generated_dir, PathBuf::from);
    let written = pa_contract::export(&dir)?;
    for path in written {
        println!("wrote {}", path.display());
    }
    Ok(())
}
