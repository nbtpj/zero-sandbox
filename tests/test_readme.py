"""The README is the first thing a stranger runs: its example must be a request the service accepts."""
from __future__ import annotations

import json
import pathlib
import re

README = pathlib.Path(__file__).resolve().parent.parent / "README.md"


def _json_blocks() -> list[dict]:
    return [json.loads(block) for block in re.findall(r"```json\n(.*?)\n```", README.read_text(), re.S)]


def test_the_readme_request_example_is_accepted_by_the_service():
    from sandbox.service import _request

    examples = [body for body in _json_blocks() if "runtime" in body and "session_id" in body]
    assert examples, "the README must keep a complete POST /execute example"
    for body in examples:
        sub, request = _request(body)
        assert len(sub.digest) == 64 and request["session_id"] == body["session_id"]


def test_the_quickstart_names_only_files_and_commands_that_exist():
    text = README.read_text()
    for name in re.findall(r"`?(?:bash )(build\.sh|launch-server\.sh)", text):
        assert (README.parent / name).is_file(), name
    assert "ZERO_SANDBOX_TOKEN" in text, "the quickstart must show the token it now requires"
