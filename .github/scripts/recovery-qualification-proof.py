"""Hosted-only diagnostics. Preserve all fixed sample results and original guards."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

root = Path.cwd()
tmp = Path(os.environ['RUNNER_TEMP'])
out = tmp / 'qualification-evidence'
out.mkdir(exist_ok=True)
source = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
(out / 'source.txt').write_text(source + '\n')
canonical = (tmp / 'import-canonical.rs').read_text()
(out / 'import-canonical.sha256').write_text(hashlib.sha256(canonical.encode()).hexdigest() + '\n')
path = root / 'crates/pa-daemon/tests/import_compaction_e2e.rs'
old = (tmp / 'import-legacy.rs').read_text()
def helper(text):
    start = text.index('fn session_rows(')
    end = text.index('\n#[test]\nfn imported_session_compacts()', start)
    return text[start:end]
legacy = helper(old)
needle = '        .expect("the imported session\'s copy in the session dir");'
assert legacy.count(needle) == 1
legacy = legacy.replace(needle, needle + '\n    eprintln!("LEGACY_SELECTED_PATH={}", file.display());')
legacy_fixture = canonical.replace(helper(canonical), legacy)
legacy_fixture = legacy_fixture.replace('session_rows(&mut harness,', 'session_rows(&harness,')
def run(command, logfile, env=None):
    with (out / logfile).open('w') as log:
        try:
            return subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                                  env=env, timeout=360, check=False).returncode
        except subprocess.TimeoutExpired:
            log.write('DIAGNOSTIC_SUBPROCESS_TIMEOUT\n')
            return 124
path.write_text(legacy_fixture)
legacy_status = run(['cargo', 'test', '--locked', '-p', 'pa-daemon', '--test', 'import_compaction_e2e', '--', '--nocapture'], 'import-legacy.log')
legacy_log = (out / 'import-legacy.log').read_text()
legacy_selected = [line for line in legacy_log.splitlines() if 'LEGACY_SELECTED_PATH=' in line]
# The old selector is diagnosed, not assumed to fail on unspecified directory order.
# Its unmodified original assertions stay in place; every outcome is retained.
path.write_text(canonical)
canonical_status = run(['cargo', 'test', '--locked', '-p', 'pa-daemon', '--test', 'import_compaction_e2e', '--', '--nocapture'], 'import-canonical.log')
canonical_log = (out / 'import-canonical.log').read_text()
production_diff = subprocess.check_output(['git', 'diff', '--name-only', '--', 'crates/*/src'], text=True)
assert not production_diff, production_diff
(out / 'fixture-only.patch').write_bytes(subprocess.check_output(['git', 'diff', '--binary']))
factory = tmp / 'qualification-scripts/review_factory_resumption_parity.py'
text = factory.read_text()
needle = "            assert len(server.child_requests) == 3, 'not two producer attempts plus one guarded verifier'"
assert text.count(needle) == 1
text = text.replace(needle, "            observed['provider_counts_at_assertion'] = {'children': len(server.child_requests), 'parents': len(server.parent_requests), 'all': len(server.requests)}\n" + needle)
factory.write_text(text)
(out / 'factory-diagnostic-source.py').write_text(text)
env = dict(os.environ)
env.update(PA_TS_ARCHIVE=str(tmp / 'ts-reference/prime-agent-0.9.8-linux-x64.tar.gz'),
           PA_TS_BINARY=str(tmp / 'ts-reference/package/prime-agent'),
           PA_RUST_BINARY=str(root / 'target/debug/prime-agent'),
           PI_PACKAGE_DIR=str(tmp / 'ts-reference/package'),
           PRIME_AGENT_KERNEL_PYTHON=str(Path.home() / '.prime/agent/kernel-venv/bin/python'),
           PA_E2E_KERNEL_PYTHON=str(Path.home() / '.prime/agent/kernel-venv/bin/python'),
           PYTHONPATH=str(tmp / 'ts-reference/package/skills/goal/src'))
samples = []
# Three predetermined samples per source, never retries that replace a failure.
for sample in range(1, 4):
    status = run(['python3', str(factory), '--receipt', str(out / f'factory-{sample}.json')], f'factory-{sample}.log', env)
    samples.append({'sample': sample, 'status': status})
summary = {'source': source, 'legacy_status': legacy_status, 'legacy_selected': legacy_selected,
           'legacy_selected_sidecar': any('info-cache' in line for line in legacy_selected),
           'legacy_zero_rows_failure': 'the imported rows persist: []' in legacy_log,
           'canonical_status': canonical_status,
           'canonical_original_assertions_passed': canonical_status == 0 and 'test result: ok. 1 passed; 0 failed' in canonical_log,
           'factory_samples': samples}
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
raise SystemExit(0 if summary['canonical_original_assertions_passed'] and all(s['status'] == 0 for s in samples) else 1)
