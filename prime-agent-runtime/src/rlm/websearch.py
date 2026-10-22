"""Search the web via the Serper Google Search API."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

_MAX_OUTPUT = 8192
_DEFAULT_TIMEOUT = 45
_DEFAULT_NUM_RESULTS = 5


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ[name])
    except (KeyError, ValueError):
        return default


def _agent_dir() -> Path:
    raw = (
        os.environ.get("PRIME_AGENT_CODING_AGENT_DIR")
        or os.environ.get("PI_CODING_AGENT_DIR")
        or str(Path.home() / ".prime" / "agent")
    )
    return Path(raw).expanduser()


def _resolve_api_key() -> str:
    # Read auth.json on each call (not just the injected env var) so a key added
    # via /login after the kernel started is still picked up. Env var wins.
    env_key = os.environ.get("SERPER_API_KEY", "").strip()
    if env_key:
        return env_key

    try:
        auth = json.loads((_agent_dir() / "auth.json").read_text())
        cred = auth.get("serper") if isinstance(auth, dict) else None
        if isinstance(cred, dict) and cred.get("type") == "api_key":
            return _resolve_config_value(str(cred.get("key") or ""))
    except (OSError, ValueError):
        pass
    return ""


def _resolve_config_value(value: str) -> str:
    # Stored keys may be a literal or an env-var name; "!command" refs can't be run
    # safely here, so skip them (the agent injects those resolved at build time).
    value = value.strip()
    if not value or value.startswith("!"):
        return ""
    return (os.environ.get(value) or value).strip()


def _format_serper_results(data: dict, query: str, num_results: int) -> str:
    sections: list[str] = []

    kg = data.get("knowledgeGraph")
    if kg:
        kg_lines: list[str] = []
        title = (kg.get("title") or "").strip()
        if title:
            kg_lines.append(f"Knowledge Graph: {title}")
        description = (kg.get("description") or "").strip()
        if description:
            kg_lines.append(description)
        for key, value in (kg.get("attributes") or {}).items():
            text = str(value).strip()
            if text:
                kg_lines.append(f"{key}: {text}")
        if kg_lines:
            sections.append("\n".join(kg_lines))

    for i, result in enumerate((data.get("organic") or [])[:num_results]):
        title = (result.get("title") or "").strip() or "Untitled"
        lines = [f"Result {i}: {title}"]
        link = (result.get("link") or "").strip()
        if link:
            lines.append(f"URL: {link}")
        snippet = (result.get("snippet") or "").strip()
        if snippet:
            lines.append(snippet)
        sections.append("\n".join(lines))

    people_also_ask = data.get("peopleAlsoAsk") or []
    if people_also_ask:
        max_q = max(1, min(3, len(people_also_ask)))
        questions: list[str] = []
        for item in people_also_ask[:max_q]:
            question = (item.get("question") or "").strip()
            if not question:
                continue
            entry = f"Q: {question}"
            answer = (item.get("snippet") or "").strip()
            if answer:
                entry += f"\nA: {answer}"
            questions.append(entry)
        if questions:
            sections.append("People Also Ask:\n" + "\n".join(questions))

    if not sections:
        return f"No results returned for query: {query}"

    return "\n\n---\n\n".join(sections)


async def _fetch_serper(query: str, api_key: str, timeout: int, num_results: int) -> str:
    if importlib.util.find_spec("httpx") is None:
        raise ModuleNotFoundError("No module named 'httpx'")
    import httpx

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                "https://google.serper.dev/search",
                json={"q": query},
                headers={
                    "X-API-KEY": api_key,
                    "Content-Type": "application/json",
                },
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        body = e.response.text if e.response is not None else ""
        raise RuntimeError(f"Serper search error ({e.response.status_code}): {body}") from e

    return _format_serper_results(data, query, num_results=num_results)


async def websearch(query: str, *, num_results: int | None = None) -> str:
    """Run one Google search via Serper and return formatted results."""
    if not isinstance(query, str):
        raise TypeError(f"query must be str, got {type(query).__name__}")
    api_key = _resolve_api_key()
    if not api_key:
        return (
            "Web search is not set up yet: no Serper API key is configured.\n"
            "Tell the user how to enable it:\n"
            "  1. Get a free API key at https://serper.dev (sign up, copy the key).\n"
            "  2. In Prime Agent, run /mcp, choose \"Serper (web search)\", and paste the key.\n"
            "Do not ask the user to set environment variables. Once the key is saved, web search works automatically."
        )

    timeout = _env_int("PRIME_AGENT_WEBSEARCH_TIMEOUT", _DEFAULT_TIMEOUT)
    if num_results is None:
        num_results = _env_int("PRIME_AGENT_WEBSEARCH_NUM_RESULTS", _DEFAULT_NUM_RESULTS)

    try:
        result = await _fetch_serper(query, api_key, timeout=timeout, num_results=num_results)
    except Exception as e:
        result = f"Error searching for '{query}': {e}"
    output = f'Results for query "{query}":\n\n{result}'

    if len(output) > _MAX_OUTPUT:
        total = len(output)
        marker = f"\n... [output truncated, {total} chars total] ...\n"
        # Reserve room for the marker so the result stays within the cap.
        half = max(0, (_MAX_OUTPUT - len(marker)) // 2)
        output = output[:half] + marker + output[len(output) - half:]
        if len(output) > _MAX_OUTPUT:  # marker alone exceeds the budget
            output = output[:_MAX_OUTPUT]

    return output
