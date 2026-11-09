"""The suite's launcher and its shared vocabulary.

Everything here is submitted, never catalogued: a case builds a throwaway Dockerfile, the server
builds whatever arrives, and the suite depends on no runtime registry. `tests/runtimes/` holds
fixtures for hand runs, not a catalog.

Cases marked `@live` need a served sandbox in `ZERO_SANDBOX_URL`; the rest run anywhere.
"""
from __future__ import annotations

import importlib.util
import json
import os
import pathlib
import secrets
import sys
import time
import urllib.error
import urllib.request

import pytest

# `pytest` MUST work from a bare checkout. The repo root is itself the `sandbox` package
# (`package-dir = {sandbox = "."}`, so the submodule imports as `sandbox` in a consumer), which
# means no parent directory on `sys.path` ever spells that name -- an uninstalled checkout fails
# with `ModuleNotFoundError: No module named 'sandbox'` before a single case is collected. Bind
# the name to this checkout instead of asking every contributor to `pip install -e .` first.
_ROOT = pathlib.Path(__file__).resolve().parent.parent
try:  # an installed copy, or a consumer that already mounted us, always wins
    import sandbox  # noqa: F401
except ModuleNotFoundError:
    _spec = importlib.util.spec_from_file_location(
        "sandbox", _ROOT / "__init__.py", submodule_search_locations=[str(_ROOT)])
    _module = importlib.util.module_from_spec(_spec)
    sys.modules["sandbox"] = _module          # before exec: `__init__` imports sandbox.runtime
    _spec.loader.exec_module(_module)

# The unit cases construct gateways and sessions directly. Local execution stays OFF unless a
# case asks for it: `sandbox.route` must refuse to run containers in a caller's process by
# default, and a suite-wide opt-in would hide that.
os.environ.setdefault("ZERO_SANDBOX_CLIENT_THREADS", "32")


@pytest.fixture(autouse=True)
def _private_state_dir(tmp_path, monkeypatch):
    """A probe note left in the shared temp dir by an earlier run (or a live service on this host)
    must not decide a case's answer: every case gets its own state directory."""
    monkeypatch.setenv("ZERO_SANDBOX_STATE", str(tmp_path))

RESPONSE = {"runtime", "session_id", "code", "cwd", "timeout_s", "wait_s",
            "output", "returncode", "signal", "stdout", "err", "stdout_bytes",
            "stderr_bytes", "stdout_total_bytes", "stderr_total_bytes",
            "stdout_truncated", "stderr_truncated", "timed_out", "truncated", "alive",
            "elapsed_ms", "start_ms", "build_ms", "files", "network", "resources"}

RESOURCES = {"cpus": 1, "memory_gb": 1, "workdir_gb": 0.25,
             "workdir_medium": "temp", "gpus": 0, "pids": 64}

# The RUN line makes every name a distinct image: two cases must never share a build.
DYNAMIC = """FROM docker.io/library/alpine:latest
{extra}RUN head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \\n' > /stamp
ENV TEST_RUNTIME_NAME={name} ZERO_RUNTIME_SHELL=/bin/sh{egress}
"""



def _auth() -> dict:
    """The shared secret, when the served sandbox enforces one (`ZERO_SANDBOX_TOKEN`)."""
    token = (os.environ.get("ZERO_SANDBOX_TOKEN") or "").strip()
    return {"Authorization": "Bearer " + token} if token else {}

def dynamic(name: str, *, extra: str = "", egress: bool = False) -> str:
    return DYNAMIC.format(name=name, extra=extra,
                          egress=" ZERO_RUNTIME_EGRESS=endpoint" if egress else "")


def api(url: str, path: str, body: dict | None = None, timeout: float = 300) -> dict:
    request = urllib.request.Request(
        url.rstrip("/") + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **_auth()})
    with urllib.request.urlopen(request, timeout=timeout) as reply:
        return json.load(reply)


def submit(url: str, dockerfile: str, code: str, *, name: str, documentation: str = "",
           session_id: str | None = None, expect: int = 200, **request) -> dict:
    body = {"runtime": {"dockerfile": dockerfile, "documentation": documentation},
            "session_id": session_id or "t" + secrets.token_hex(8), "code": code,
            "cwd": "/work", "timeout_s": request.pop("timeout_s", 40),
            "wait_s": request.pop("wait_s", 0),
            "io_cap_kb": request.pop("io_cap_kb", 200),
            "resources": request.pop("resources", RESOURCES), **request}
    try:
        reply = api(url, "/execute", body, body["timeout_s"] + 900)
    except urllib.error.HTTPError as error:
        reply = {"http": error.code, **json.loads(error.read() or b"{}")}
    assert reply.get("http", 200) == expect, reply
    return reply


# A real marker, not a bare skipif, so `-m live` and `-m "not live"` select properly. The skip
# is applied at collection instead, where it can say why.
live = pytest.mark.live


def pytest_collection_modifyitems(config, items):
    if os.environ.get("ZERO_SANDBOX_URL"):
        return
    skip = pytest.mark.skip(reason="set ZERO_SANDBOX_URL to a served sandbox")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="module")
def url() -> str:
    return os.environ["ZERO_SANDBOX_URL"]


def drain(url: str, when: str, seconds: float = 120.0) -> dict:
    """Wait until the server holds no session, and prove it is still answering.

    A case that leaks a session leaves its container, its workspace and its CPU grant behind, and
    the next case then measures the leak instead of itself. Doing this BEFORE and after each case
    names the culprit rather than the victim -- and since it calls /health, a case that took the
    service down is reported against itself.
    """
    deadline = time.monotonic() + seconds
    while True:
        health = api(url, "/health", timeout=30)
        assert health.get("ok"), f"the service is not healthy {when}: {health}"
        if not health["sessions"]:
            return health
        if time.monotonic() > deadline:
            raise AssertionError(
                f"{health['sessions']} session(s) still live {when} after {seconds:g}s")
        time.sleep(0.5)


@pytest.fixture(autouse=True)
def clean_slate(request):
    """Every live case starts from a drained, answering server, and must leave one."""
    if "live" not in request.keywords or not os.environ.get("ZERO_SANDBOX_URL"):
        yield
        return
    target = os.environ["ZERO_SANDBOX_URL"]
    drain(target, f"before {request.node.name}")
    yield
    drain(target, f"after {request.node.name}")
