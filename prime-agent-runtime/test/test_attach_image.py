from __future__ import annotations

import asyncio
import importlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


rlm_module = importlib.import_module("rlm")
attach_image_module = importlib.import_module("rlm.attach_image")
from rlm.attach_image import attach_image

PNG_HEADER = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


class AttachImageTest(unittest.TestCase):
    def _vision_host(self, input=("text", "image")):
        return AsyncMock(return_value={"id": "model-x", "provider": "p", "input": list(input)})

    def test_requires_at_least_one_path(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least one image path"):
            asyncio.run(attach_image())

    def test_refuses_a_non_vision_model(self) -> None:
        with patch.object(rlm_module, "host_request", self._vision_host(input=("text",))):
            with self.assertRaisesRegex(RuntimeError, "does not support vision"):
                asyncio.run(attach_image("whatever.png"))

    def test_rejects_missing_and_unsupported_files(self) -> None:
        with patch.object(rlm_module, "host_request", self._vision_host()):
            with self.assertRaisesRegex(FileNotFoundError, "missing.png"):
                asyncio.run(attach_image("missing.png"))

            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "notes.txt"
                target.write_bytes(b"plain text, not an image")
                with self.assertRaisesRegex(ValueError, "not a supported image"):
                    asyncio.run(attach_image(str(target)))

    def test_rejects_oversized_sources_before_pil(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "big.png"
            target.write_bytes(PNG_HEADER + b"\x00" * 4096)
            with patch.object(rlm_module, "host_request", self._vision_host()), patch.object(
                attach_image_module, "_MAX_SOURCE_IMAGE_BYTES", 10
            ):
                with self.assertRaisesRegex(ValueError, "must be under"):
                    asyncio.run(attach_image(str(target)))


if __name__ == "__main__":
    unittest.main()
