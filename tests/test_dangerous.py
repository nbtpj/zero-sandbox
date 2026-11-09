"""The corpus in `sample_request_test.json`, replayed against a live sandbox.

Each case is a complete `POST /execute` body describing something a submitted runtime could do to
the SERVICE rather than to itself: fill the node, exhaust its process table, burn its memory,
firehose its reply, or make the builder do any of that on its behalf. The assertion that matters
is not the individual expectation -- it is that `/health` still answers afterwards, which the
`clean_slate` fixture checks before and after every case.
"""
from __future__ import annotations

import json
import os
import pathlib
import secrets

import pytest

from conftest import api, live

CORPUS = json.loads((pathlib.Path(__file__).resolve().parent
                     / "sample_request_test.json").read_text(encoding="utf-8"))



def _auth() -> dict:
    """The shared secret, when the served sandbox enforces one (`ZERO_SANDBOX_TOKEN`)."""
    token = (os.environ.get("ZERO_SANDBOX_TOKEN") or "").strip()
    return {"Authorization": "Bearer " + token} if token else {}

def expand(value):
    """Directives keep the file readable: an oversize payload is a size, not 256 KiB of 'x'."""
    if isinstance(value, dict) and set(value) == {"repeat"}:
        text, count = value["repeat"]
        return text * count
    if isinstance(value, dict) and set(value) == {"concat"}:
        return "".join(expand(part) for part in value["concat"])
    if isinstance(value, dict):
        return {key: expand(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand(item) for item in value]
    return value


def satisfied(case: dict) -> str:
    """A case may need the server configured at or below a bound; say so instead of hanging.

    `build-runs-for-ever` against a default server holds a build slot for ZERO_BUILD_TIMEOUT_S,
    which is 7200 seconds -- that IS the danger, and it is not something a test can sit through.
    """
    import os

    for name, bound in case.get("requires_env", {}).items():
        actual = os.environ.get(name)
        if actual is None or float(actual) > float(bound):
            return f"needs {name} <= {bound}; server has {actual or 'the default'}"
    return ""


def post(url: str, body: dict) -> tuple[int, dict]:
    import urllib.error
    import urllib.request

    request = urllib.request.Request(
        url.rstrip("/") + "/execute", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **_auth()})
    try:
        with urllib.request.urlopen(request, timeout=body["timeout_s"] + 900) as reply:
            return 200, json.load(reply)
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def check(name: str, expect: dict, status: int, reply: dict) -> None:
    if (allowed := expect.get("status_in")) is not None:
        assert status in allowed, (name, status, str(reply)[:300])
    else:
        assert status == expect["status"], (name, status, str(reply)[:300])
    if prefix := expect.get("error_prefix"):
        assert reply.get("error", "").startswith(prefix), (name, reply)
        return
    if expect.get("returncode_nonzero"):
        assert reply["returncode"] != 0, (name, reply["returncode"], reply["stdout"][:200])
    if (signal := expect.get("signal")) is not None:
        assert reply["signal"] == signal, (
            name, f"killed by signal {reply['signal']}, expected {signal}")
    if expect.get("timed_out"):
        assert reply["timed_out"], (name, reply)
    if expect.get("not_timed_out"):
        assert not reply["timed_out"], (name, reply)
    if expect.get("not_alive"):
        assert not reply["alive"], (name, reply)
    if expect.get("over_quota"):
        assert reply["files"].get("over_quota"), (name, reply["files"])
    if (cap := expect.get("max_workspace_mb")) is not None:
        written = reply["files"].get("bytes_used", reply["files"].get("bytes_delta", 0)) / 2 ** 20
        assert written <= cap, (name, f"{written:.0f} MB held against a {cap} MB bound")
    if (cap := expect.get("max_retained_kb")) is not None:
        assert reply["stdout_bytes"] <= cap * 1024, (name, reply["stdout_bytes"])
        assert reply["stderr_bytes"] <= cap * 1024, (name, reply["stderr_bytes"])
    if (floor := expect.get("min_total_mb")) is not None:
        produced = reply["stdout_total_bytes"] / 2 ** 20
        assert produced >= floor, (name, f"only {produced:.0f} MB reached the service")
    if (cap := expect.get("stdout_int_at_most")) is not None:
        assert int(reply["stdout"].strip().splitlines()[-1]) <= cap, (name, reply["stdout"][:200])


FAST = [c for c in CORPUS["cases"] if not c["slow"]]
SLOW = [c for c in CORPUS["cases"] if c["slow"]]


@live
@pytest.mark.parametrize("case", FAST, ids=[c["name"] for c in FAST])
def test_a_dangerous_request_is_contained(url, case):
    if reason := satisfied(case):
        pytest.skip(reason)
    body = expand(case["request"])
    body["session_id"] = "danger" + secrets.token_hex(8)
    status, reply = post(url, body)
    check(case["name"], case["expect"], status, reply)


@live
@pytest.mark.slow
@pytest.mark.parametrize("case", SLOW, ids=[c["name"] for c in SLOW])
def test_a_slow_dangerous_request_is_contained(url, case):
    """The ones that cost real minutes: builds that never end, and gigabyte streams."""
    if reason := satisfied(case):
        pytest.skip(reason)
    body = expand(case["request"])
    body["session_id"] = "danger" + secrets.token_hex(8)
    status, reply = post(url, body)
    check(case["name"], case["expect"], status, reply)


@live
def test_the_corpus_covers_the_service_and_not_only_the_container(url):
    """A guard on the file itself: every case must say what it endangers, and the set must still
    reach the builder, the request validator and the node's own resources."""
    names = {case["name"] for case in CORPUS["cases"]}
    assert all(case["danger"].strip() for case in CORPUS["cases"])
    assert all(case["request"]["runtime"]["dockerfile"] for case in CORPUS["cases"])
    for required in ("workspace-fills-the-node", "fork-bomb", "memory-bomb",
                     "build-runs-for-ever", "oversized-dockerfile", "impossible-cpu-grant"):
        assert required in names, required
    assert api(url, "/health")["ok"]
