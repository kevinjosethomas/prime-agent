//! napi build setup plus the derived metadata env vars: the harness commit
//! (git, falling back to a build-provided override) and the target triple.

fn main() {
    napi_build::setup();
    let commit = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|out| {
            (out.status.success()).then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
        })
        .or_else(|| std::env::var("PA_CLIENT_NODE_HARNESS_COMMIT").ok())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=PA_CLIENT_NODE_HARNESS_COMMIT={commit}");
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=PA_CLIENT_NODE_TARGET_TRIPLE={target}");
}
