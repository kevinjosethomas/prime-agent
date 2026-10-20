"""Replace one exact, unique string in an existing file."""

from __future__ import annotations

from pathlib import Path

# Keep in sync with DIFF_DISPLAY_MIME in src/core/kernel/index.ts.
_DIFF_DISPLAY_MIME = "application/vnd.prime-agent.diff+json"


async def edit(path: str, old_str: str, new_str: str) -> str:
    """Replace one unique occurrence of ``old_str`` with ``new_str`` in ``path``."""
    filepath = Path(path).expanduser()
    if not filepath.exists():
        raise FileNotFoundError(f"{path} not found")
    content = filepath.read_text(encoding="utf-8")
    count = content.count(old_str)
    if count == 0:
        raise ValueError(f"string not found in {path}")
    if count > 1:
        raise ValueError(
            f"found {count} occurrences in {path}, need exactly 1 — "
            "widen the snippet to make it unique"
        )
    match_index = content.index(old_str)
    start_line = content.count("\n", 0, match_index) + 1
    filepath.write_text(content.replace(old_str, new_str, 1), encoding="utf-8")
    resolved_path = str(filepath.resolve())
    _emit_diff(resolved_path, old_str, new_str, start_line)
    return f"Edited {resolved_path}"


def _emit_diff(path: str, old_str: str, new_str: str, start_line: int) -> None:
    try:
        from . import emit

        emit(
            {
                _DIFF_DISPLAY_MIME: {
                    "path": path,
                    "old_str": old_str,
                    "new_str": new_str,
                    "start_line": start_line,
                },
                "text/plain": f"Edited {path}",
            }
        )
    except Exception:
        pass
