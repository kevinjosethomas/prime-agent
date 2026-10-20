"""Desktop metadata contract validation before transport execution."""

from dataclasses import asdict
from typing import Any
from urllib.parse import urlsplit

from ._models import LINK_KINDS, Link, STATUSES


def text(value: Any, name: str, max_length: int = 512, *, empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > max_length or (not empty and not value.strip()):
        raise ValueError(f"Invalid {name}")
    return value


# The full record set the host accepts on a create.
WRITABLE_TASK_FIELDS = frozenset(
    {"id", "title", "status", "summary", "description", "owner", "parentId", "links"}
)

# The partial change set a patch event may carry.
PATCH_TASK_FIELDS = frozenset(
    {"title", "status", "summary", "description", "parentId", "links"}
)


def validate_links(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (tuple, list)) or len(value) > 20:
        raise ValueError("At most 20 links are supported")
    links: list[dict[str, Any]] = []
    for link in value:
        item = asdict(link) if isinstance(link, Link) else dict(link)
        if item.get("title") is None:
            item.pop("title", None)
        if item.keys() - {"kind", "url", "title"} or item.get("kind") not in LINK_KINDS:
            raise ValueError("Invalid link kind or fields")
        url = urlsplit(text(item.get("url"), "link URL", 4000))
        if url.scheme not in ("http", "https") or not url.hostname or url.username is not None or url.password is not None:
            raise ValueError("Links require http/https URLs without credentials")
        if "title" in item:
            text(item["title"], "link title", 160)
        links.append(item)
    return links


def validate_task(value: dict[str, Any]) -> dict[str, Any]:
    if value.keys() - WRITABLE_TASK_FIELDS:
        raise TypeError(f"Unknown task fields: {', '.join(sorted(value.keys() - WRITABLE_TASK_FIELDS))}")
    value = dict(value)
    text(value["id"], "task ID")
    text(value["title"], "title", 160)
    if value["status"] not in STATUSES:
        raise ValueError("Invalid task status")
    text(value["summary"], "summary", 2000, empty=True)
    text(value["owner"], "owner", 160)
    if "description" in value:
        text(value["description"], "description", 6000, empty=True)
    if value["status"] == "needs_input" and not value["summary"].strip():
        raise ValueError("Needs input requires the question in summary")
    if "parentId" in value:
        text(value["parentId"], "parent task ID")
        if value["parentId"] == value["id"]:
            raise ValueError("A task cannot parent itself")
    if "links" in value:
        value["links"] = validate_links(value["links"])
    return value


def validate_changes(changes: dict[str, Any]) -> dict[str, Any]:
    """Validate a partial patch; the host applies it in one transaction."""
    if changes.keys() - PATCH_TASK_FIELDS:
        raise TypeError(f"Unknown task fields: {', '.join(sorted(changes.keys() - PATCH_TASK_FIELDS))}")
    changes = dict(changes)
    if "title" in changes:
        text(changes["title"], "title", 160)
    if "status" in changes:
        if changes["status"] not in STATUSES:
            raise ValueError("Invalid task status")
    if "summary" in changes:
        text(changes["summary"], "summary", 2000, empty=True)
    if "description" in changes:
        text(changes["description"], "description", 6000, empty=True)
    if "parentId" in changes:
        text(changes["parentId"], "parent task ID")
    if "links" in changes:
        changes["links"] = validate_links(changes["links"])
    return changes
