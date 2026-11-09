"""`sandbox.route`: the async client callers actually reach the service through.

It is the only place a caller can be made to execute containers in its OWN process, so the
refusal is a property worth pinning, not an implementation detail.
"""
from __future__ import annotations

import asyncio
import io
import json

import pytest

from conftest import RESOURCES, dynamic


def _reply(body: dict):
    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.close()
            return False

    return Response(json.dumps(body).encode())


def test_the_service_url_is_normalized_or_absent(monkeypatch):
    from sandbox import route

    monkeypatch.delenv("ZERO_SANDBOX_URL", raising=False)
    assert route.service_url() is None
    monkeypatch.setenv("ZERO_SANDBOX_URL", "")
    assert route.service_url() is None
    monkeypatch.setenv("ZERO_SANDBOX_URL", "http://host:8070/")
    assert route.service_url() == "http://host:8070"


def test_a_configured_token_is_sent_as_a_bearer_header_and_absent_otherwise(monkeypatch):
    """The 0.0.0.0-bound service is guarded by a shared secret. The client attaches it as
    `Authorization: Bearer <token>` when set, and sends no auth header when unset (dev)."""
    from sandbox import route

    seen = {}

    def fake(request, timeout=None):
        seen["auth"] = request.get_header("Authorization")
        return _reply({"returncode": 0})

    monkeypatch.setenv("ZERO_SANDBOX_URL", "http://host:8070")
    # Point the file fallback at a path that does not exist, so this test exercises only the env var.
    monkeypatch.setenv("ZERO_SANDBOX_TOKEN_FILE", str(tmp_path := __import__("pathlib").Path(
        "/nonexistent/zero-token-xyz")))
    monkeypatch.setattr(route.urllib.request, "urlopen", fake)
    text = dynamic("client")

    monkeypatch.setattr(route, "_TOKEN_CACHE", None)
    monkeypatch.setenv("ZERO_SANDBOX_TOKEN", "s3cr3t")
    asyncio.run(route.aexecute({"dockerfile": text}, "echo PASS", session_id="s1",
                               timeout_s=30, resources=RESOURCES))
    assert seen["auth"] == "Bearer s3cr3t"

    monkeypatch.setattr(route, "_TOKEN_CACHE", None)
    monkeypatch.delenv("ZERO_SANDBOX_TOKEN", raising=False)
    asyncio.run(route.aexecute({"dockerfile": text}, "echo PASS", session_id="s2",
                               timeout_s=30, resources=RESOURCES))
    assert seen["auth"] is None


def test_a_request_carries_exactly_the_documented_payload(monkeypatch):
    """Optional fields are OMITTED, not sent as null: the service rejects unsupported fields and
    a null `image` is not the same request as no image at all."""
    from sandbox import route

    sent = {}

    def fake(request, timeout=None):
        sent["url"] = request.full_url
        sent["body"] = json.loads(request.data)
        sent["type"] = request.headers.get("Content-type")
        return _reply({"returncode": 0})

    monkeypatch.setenv("ZERO_SANDBOX_URL", "http://host:8070")
    monkeypatch.setattr(route.urllib.request, "urlopen", fake)
    text = dynamic("client")
    asyncio.run(route.aexecute({"dockerfile": text}, "echo PASS", session_id="s1",
                               timeout_s=30, resources=RESOURCES))
    assert sent["url"] == "http://host:8070/execute" and sent["type"] == "application/json"
    assert set(sent["body"]) == {"runtime", "session_id", "code", "cwd", "timeout_s",
                                 "wait_s", "io_cap_kb", "resources"}
    # A descriptor passes through verbatim; only the path form composes documentation.
    assert sent["body"]["runtime"] == {"dockerfile": text}
    # Normalized by `Resources.from_wire`, so compare by value, not by type.
    assert {k: float(v) if k != "workdir_medium" else v
            for k, v in sent["body"]["resources"].items()} == {
        k: float(v) if k != "workdir_medium" else v for k, v in RESOURCES.items()}

    asyncio.run(route.aexecute({"dockerfile": text}, "echo PASS", session_id="s1", timeout_s=30,
                               resources=RESOURCES, image="swebench/x:1",
                               network="https://api.example.com/v1", env={"K": "v"}))
    assert sent["body"]["image"] == "swebench/x:1"
    assert sent["body"]["network"] == "https://api.example.com/v1"
    assert sent["body"]["env"] == {"K": "v"}


def test_a_dockerfile_path_is_read_with_its_documentation(tmp_path, monkeypatch):
    """The HTTP API takes CONTENTS, never a client path: the client is what resolves one."""
    from sandbox import route

    (tmp_path / "Dockerfile").write_text(dynamic("onpath"), encoding="utf-8")
    (tmp_path / "README.md").write_text("what it is for", encoding="utf-8")
    sent = {}
    monkeypatch.setenv("ZERO_SANDBOX_URL", "http://host:8070")
    monkeypatch.setattr(route.urllib.request, "urlopen",
                        lambda request, timeout=None: sent.update(
                            body=json.loads(request.data)) or _reply({"returncode": 0}))
    asyncio.run(route.aexecute(tmp_path / "Dockerfile", "true", session_id="s",
                               timeout_s=5, resources=RESOURCES))
    assert sent["body"]["runtime"] == {"dockerfile": dynamic("onpath"),
                                       "documentation": "what it is for"}
    with pytest.raises(ValueError, match="does not exist"):
        asyncio.run(route.aexecute(tmp_path / "absent", "true", session_id="s",
                                   timeout_s=5, resources=RESOURCES))


def test_the_service_error_reaches_the_caller_not_the_status_code(monkeypatch):
    """A caller acts on `bad_request:` or `unavailable:`; `HTTP 400` tells it nothing."""
    from sandbox import route

    def fake(request, timeout=None):
        raise route.urllib.error.HTTPError(
            request.full_url, 400, "Bad Request", {},
            io.BytesIO(json.dumps({"error": "bad_request:cwd must be /work or below it"}).encode()))

    monkeypatch.setenv("ZERO_SANDBOX_URL", "http://host:8070")
    monkeypatch.setattr(route.urllib.request, "urlopen", fake)
    with pytest.raises(RuntimeError, match="bad_request:cwd must be"):
        asyncio.run(route.aexecute({"dockerfile": dynamic("bad")}, "true", session_id="s",
                                   timeout_s=5, resources=RESOURCES))


def test_local_execution_is_refused_unless_it_was_asked_for(monkeypatch):
    """Without a URL the client would run containers in the CALLER's process. Production must go
    through the service, so the opt-in is explicit and is read once, at import."""
    from sandbox import route

    monkeypatch.delenv("ZERO_SANDBOX_URL", raising=False)
    monkeypatch.setattr(route, "_ALLOW_LOCAL", False)
    with pytest.raises(RuntimeError, match="refusing local execution"):
        asyncio.run(route.aexecute({"dockerfile": dynamic("local")}, "true", session_id="s",
                                   timeout_s=5, resources=RESOURCES))

    called = {}

    def local(payload):
        called["payload"] = payload
        return {"ok": True}

    monkeypatch.setattr(route, "_ALLOW_LOCAL", True)
    monkeypatch.setattr("sandbox.service.execute_local", local)
    assert asyncio.run(route.aexecute({"dockerfile": dynamic("local")}, "true", session_id="s",
                                      timeout_s=5, resources=RESOURCES)) == {"ok": True}
    assert called["payload"]["session_id"] == "s"


def test_a_grant_is_validated_before_anything_is_sent(monkeypatch):
    """A malformed grant must fail in the caller, not as a 400 after a round trip."""
    from sandbox import route

    monkeypatch.setenv("ZERO_SANDBOX_URL", "http://host:8070")
    monkeypatch.setattr(route.urllib.request, "urlopen",
                        lambda *a, **k: pytest.fail("a bad grant reached the wire"))
    with pytest.raises(ValueError, match="must declare cpus"):
        asyncio.run(route.aexecute({"dockerfile": dynamic("grant")}, "true", session_id="s",
                                   timeout_s=5, resources={"cpus": 1}))


def test_run_sync_works_inside_and_outside_a_running_loop():
    from sandbox import route

    async def answer():
        return 42

    assert route.run_sync(answer()) == 42

    async def nested():
        return route.run_sync(answer())

    assert asyncio.run(nested()) == 42
