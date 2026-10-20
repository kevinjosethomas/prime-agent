"""Require the current installed source to fail the identical readiness-barrier regression."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile


BASELINE = "0c8ecf086835e7688dd9f259dcca265a81a02c11"
FIXTURE = Path("crates/pa-daemon/src/supervisor/clients/read_tests.rs")
CLIENT = Path("crates/pa-daemon/src/supervisor/clients.rs")


def main() -> None:
    root = Path.cwd()
    fixture = (root / FIXTURE).read_bytes()
    evidence = root / "framing-proof"
    evidence.mkdir(exist_ok=True)
    baseline = Path(tempfile.mkdtemp(prefix="primer-framing-baseline-")) / "source"
    subprocess.run(["git", "worktree", "add", "--detach", str(baseline), BASELINE], check=True)
    try:
        (baseline / FIXTURE).parent.mkdir(parents=True, exist_ok=True)
        (baseline / FIXTURE).write_bytes(fixture)
        client = baseline / CLIENT
        source = client.read_text()
        marker = "#[cfg(all(test, unix))]\nmod tests {"
        assert source.count(marker) == 1, "Baseline test registration changed"
        assert "mod read_tests;" not in source, "Baseline already contains the fix's fixture"
        client.write_text(source.replace(marker, "#[cfg(all(test, unix))]\nmod read_tests;\n\n" + marker))
        result = subprocess.run(
            ["cargo", "test", "--locked", "-p", "pa-daemon", "--lib", "fragmented_client_frame"],
            cwd=baseline,
            env={**os.environ, "CARGO_TARGET_DIR": str(root / "target")},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=1200,
            check=False,
        )
        (evidence / "installed-baseline.log").write_text(result.stdout)
        expected_failure = (
            result.returncode != 0
            and "client frame correlation was lost" in result.stdout
            and "fragmented_client_frame_survives_interleaved_broadcast ... FAILED" in result.stdout
        )
        receipt = {
            "baseline": BASELINE,
            "fixtureSha256": hashlib.sha256(fixture).hexdigest(),
            "exitCode": result.returncode,
            "expectedFrameCorrelationFailure": expected_failure,
        }
        (evidence / "baseline.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps(receipt))
        assert expected_failure, "The baseline must fail the intended assertion; inspect installed-baseline.log"
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(baseline)], check=True)


if __name__ == "__main__":
    main()
