"""Local desktop reporter transport; no shell, network listener, or second store."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import math
import os
from typing import Any, Mapping, Sequence
from uuid import uuid4


@dataclass(frozen=True)
class Report:
    """An immutable delivery, reusable after an uncertain transport failure."""

    json: str

    @classmethod
    def create(cls, operation: str, **payload: Any) -> Report:
        return cls(json.dumps({"eventId": str(uuid4()), "operation": operation, **payload}))


class DeliveryError(RuntimeError):
    """Delivery was not confirmed; retry ``report`` to preserve deduplication."""

    def __init__(self, message: str, report: Report | None = None):
        super().__init__(message)
        self.report = report


class AssignmentError(DeliveryError):
    """A child was admitted, but metadata delivery failed. Never respawn to retry."""

    def __init__(self, handle: Any, error: Exception):
        super().__init__("Child spawned; workstream assignment was not confirmed. Retain handle and retry assignment, not spawn.",
                         error.report if isinstance(error, DeliveryError) else None)
        self.handle = handle


class Reporter:
    def __init__(self, command: Sequence[str], env: Mapping[str, str] | None, timeout: float):
        if isinstance(command, (str, bytes)) or not command or any(
            not isinstance(arg, str) or not arg or "\0" in arg for arg in command
        ):
            raise ValueError("command must be a nonempty argv sequence, never a shell string")
        if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be finite and positive")
        if env is not None and any(
            not isinstance(key, str) or not key or "=" in key or "\0" in key
            or not isinstance(value, str) or "\0" in value for key, value in env.items()
        ):
            raise ValueError("env must map valid environment names to strings")
        self.command = tuple(command)
        self.env = dict(env or {})
        self.timeout = timeout

    async def call(self, action: str, report: Report | None = None) -> Any:
        body = report.json.encode("utf-8") if report else None
        if body is not None and len(body) > 256 * 1024:
            raise ValueError("Report exceeds the desktop's 256 KiB limit")
        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                *self.command, action, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                env={**os.environ, **self.env},
            )
            stdout, _stderr = await asyncio.wait_for(process.communicate(body), self.timeout)
            if process.returncode != 0:
                raise DeliveryError(f"Workstream reporter exited with code {process.returncode}", report)
            try:
                return json.loads(stdout)
            except (ValueError, UnicodeError) as error:
                raise DeliveryError("Workstream reporter returned invalid JSON", report) from error
        except (OSError, asyncio.TimeoutError) as error:
            raise DeliveryError("Workstream reporter unavailable or timed out", report) from error
        finally:
            if process is not None and process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()
