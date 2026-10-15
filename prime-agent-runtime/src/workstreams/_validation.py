"""Desktop metadata contract validation before transport execution."""

from dataclasses import asdict
from typing import Any
from urllib.parse import urlsplit

from ._models import Link, STATUSES, Step


def text(value: Any, name: str, max_length: int = 512, *, empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > max_length or (not empty and not value.strip()):
        raise ValueError(f"Invalid {name}")
    return value


def validate_stream(value: dict[str, Any]) -> dict[str, Any]:
    allowed = {"id", "title", "status", "summary", "description", "question", "owner", "steps", "parentId", "links"}
    if value.keys() - allowed:
        raise TypeError(f"Unknown workstream fields: {', '.join(sorted(value.keys() - allowed))}")
    value = dict(value)
    text(value["id"], "workstream ID")
    text(value["title"], "title", 160)
    if value["status"] not in STATUSES:
        raise ValueError("Invalid workstream status")
    text(value["summary"], "summary", 2000, empty=True)
    text(value["question"], "question", 2000, empty=True)
    text(value["owner"], "owner", 160)
    if "description" in value:
        text(value["description"], "description", 6000, empty=True)
    steps = value["steps"]
    if not isinstance(steps, (tuple, list)) or len(steps) > 100:
        raise ValueError("At most 100 milestones are supported")
    output = []
    for step in steps:
        item = asdict(step) if isinstance(step, Step) else dict(step)
        if set(item) != {"id", "title", "done"} or type(item["done"]) is not bool:
            raise ValueError("Milestones require id, title, and boolean done")
        text(item["id"], "milestone ID")
        text(item["title"], "milestone title")
        output.append(item)
    if len({step["id"] for step in output}) != len(output):
        raise ValueError("Duplicate milestone IDs")
    if value["status"] == "needs_input" and not value["question"].strip():
        raise ValueError("Needs input requires the actual question")
    if value["status"] == "done" and any(not step["done"] for step in output):
        raise ValueError("Unfinished milestones prevent completion")
    value["steps"] = output
    if "parentId" in value:
        text(value["parentId"], "parent workstream ID")
        if value["parentId"] == value["id"]:
            raise ValueError("A workstream cannot parent itself")
    if "links" in value:
        if not isinstance(value["links"], (tuple, list)) or len(value["links"]) > 20:
            raise ValueError("At most 20 links are supported")
        links = []
        for link in value["links"]:
            item = asdict(link) if isinstance(link, Link) else dict(link)
            if item.get("title") is None:
                item.pop("title", None)
            if item.keys() - {"kind", "url", "title"} or item.get("kind") not in ("chat", "pr", "source"):
                raise ValueError("Invalid link kind or fields")
            url = urlsplit(text(item.get("url"), "link URL", 4000))
            if url.scheme not in ("http", "https") or not url.hostname or url.username is not None or url.password is not None:
                raise ValueError("Links require http/https URLs without credentials")
            if "title" in item:
                text(item["title"], "link title", 160)
            links.append(item)
        value["links"] = links
    return value
