"""Private read-only loader for saved continual-harness entries.

The Rust host owns the shared harness store (``harness_state.json``);
refinement runs write it. Factory reads the saved ``factory`` and
``subagent`` entries it needs through this module: a narrow surface with
exactly the two lookups the executor uses, ``get(kind, id)`` and
``list(kind)``. Reads never write, lock, or cache — every call re-reads
the file, so host edits are always current and a missing or corrupt file
reads as empty.

Lookup keeps the store's public lookup semantics: an unprefixed id reads
the local store only, ``local:``/``global:`` prefixes route the way the
overview displayed them, and ``list`` returns the local store's entries
in the (path, title, id) order.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_DEFAULT_FILE_NAME = "harness_state.json"
_DEFAULT_HARNESS_DIR_NAME = "harness"


def _agent_dir() -> Path:
    raw = (
        os.environ.get("PRIME_AGENT_CODING_AGENT_DIR")
        or os.environ.get("PI_CODING_AGENT_DIR")
        or str(Path.home() / ".prime" / "agent")
    )
    return Path(raw).expanduser().resolve()


def _env_dir(name: str) -> str | None:
    # Set-but-empty env values behave as unset; a bare "" would skip the
    # session-dir fallback and land local reads in the global default.
    value = (os.environ.get(name) or "").strip()
    return value or None


def _strip_scope_prefix(id: str) -> tuple[str, str | None]:
    """Accept the [local:id]/[global:id] forms the overview displayed."""
    if isinstance(id, str):
        scope, sep, rest = id.partition(":")
        if sep and rest and scope in ("local", "global"):
            return rest, scope
    return id, None


@dataclass
class SavedHarnessEntry:
    """The saved-entry fields factory reads; the rest of the stored
    document is ignored."""

    id: str
    title: str
    content: str
    path: str = "general"
    arguments: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


def _read_kind(path: Path, kind: str) -> list[SavedHarnessEntry]:
    """Read one store file's entries for one kind; unreadable reads empty."""
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError):
        return []
    if not isinstance(data, dict):
        return []
    raw_entries = data.get("entries")
    if not isinstance(raw_entries, dict):
        return []
    raw_kind = raw_entries.get(kind)
    if not isinstance(raw_kind, dict):
        return []
    entries: list[SavedHarnessEntry] = []
    for entry_id, raw_entry in raw_kind.items():
        if not isinstance(raw_entry, dict):
            continue
        title = raw_entry.get("title")
        content = raw_entry.get("content")
        if not isinstance(title, str) or not isinstance(content, str):
            continue
        path = raw_entry.get("path")
        arguments = raw_entry.get("arguments")
        metadata = raw_entry.get("metadata")
        entries.append(
            SavedHarnessEntry(
                id=str(entry_id),
                title=title,
                content=content,
                path=path if isinstance(path, str) else "general",
                arguments=arguments if isinstance(arguments, dict) else {},
                metadata=metadata if isinstance(metadata, dict) else {},
            )
        )
    return entries


class SavedHarnessEntries:
    """Read-only view over the saved harness store.

    ``state_dir`` and ``global_state_dir`` override the env resolution for
    tests and seeded fixtures; by default local resolves from
    ``RLM_HARNESS_STATE_DIR`` then ``RLM_SESSION_DIR/harness`` (an
    unconfigured session reads as empty, never raises), and global from
    ``RLM_GLOBAL_HARNESS_STATE_DIR`` then ``<agent dir>/harness``.
    """

    def __init__(
        self,
        state_dir: "str | Path | None" = None,
        global_state_dir: "str | Path | None" = None,
    ) -> None:
        self._state_dir = state_dir
        self._global_state_dir = global_state_dir

    def _local_file(self) -> Path | None:
        if self._state_dir is not None:
            return Path(self._state_dir).expanduser().resolve() / _DEFAULT_FILE_NAME
        root = _env_dir("RLM_HARNESS_STATE_DIR")
        if root is None and (session_dir := _env_dir("RLM_SESSION_DIR")):
            root = str(Path(session_dir) / _DEFAULT_HARNESS_DIR_NAME)
        if root is None:
            return None
        return Path(root).expanduser().resolve() / _DEFAULT_FILE_NAME

    def _global_file(self) -> Path:
        if self._global_state_dir is not None:
            return Path(self._global_state_dir).expanduser().resolve() / _DEFAULT_FILE_NAME
        root = _env_dir("RLM_GLOBAL_HARNESS_STATE_DIR")
        if root is None:
            return _agent_dir() / _DEFAULT_HARNESS_DIR_NAME / _DEFAULT_FILE_NAME
        return Path(root).expanduser().resolve() / _DEFAULT_FILE_NAME

    def get(self, kind: str, id: str) -> SavedHarnessEntry | None:
        """Return the saved entry for ``id``: the local store, or the
        global store for a ``global:``-prefixed id."""
        id, scope = _strip_scope_prefix(id)
        if not isinstance(id, str):
            return None
        file = self._global_file() if scope == "global" else self._local_file()
        if file is None:
            return None
        for entry in _read_kind(file, kind):
            if entry.id == id:
                return entry
        return None

    def list(self, kind: str) -> list[SavedHarnessEntry]:
        """Return every saved entry of ``kind`` from the local store, in
        the (path, title, id) order the old ``list`` sorted to."""
        file = self._local_file()
        if file is None:
            return []
        return sorted(_read_kind(file, kind), key=lambda entry: (entry.path, entry.title, entry.id))
