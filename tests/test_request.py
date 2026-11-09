"""What the service accepts, what it refuses, and what it answers with.

Every rejection here is a documented status in `README.md`, and the error PREFIX is the contract:
`_status` maps it, and a caller acts on `forbidden:` or `conflict:`, never on the prose.
"""
from __future__ import annotations

import os

import pytest

from conftest import RESOURCES, dynamic


def request_for(**overrides) -> dict:
    body = {"runtime": {"dockerfile": dynamic("wire")}, "resources": RESOURCES,
            "session_id": "s1", "code": "true", "cwd": "/work", "timeout_s": 5,
            "wait_s": 0, "io_cap_kb": 200}
    body.update(overrides)
    return {key: value for key, value in body.items() if value is not ...}


def test_a_valid_request_normalizes_to_what_a_session_needs():
    from sandbox.service import _request

    sub, request = _request(request_for(cwd="/work/./sub/../nested"))
    assert request["cwd"] == "/work/nested"
    assert request["runtime_hash"] == sub.digest and len(sub.digest) == 64
    assert request["resources"].cpus == 1 and request["resources"].pids == 64
    assert len(request["config_hash"]) == 64
    assert request["network"] == {"mode": "none", "outbound": False}


@pytest.mark.parametrize("overrides,message", [
    ({"unexpected": 1}, "unsupported request fields: unexpected"),
    ({"session_id": "bad id"}, "session_id must use"),
    ({"session_id": ""}, "session_id must use"),
    ({"session_id": "x" * 129}, "session_id must use"),
    ({"code": "a\x00b"}, "code must be a string without NUL"),
    ({"code": 7}, "code must be a string without NUL"),
    ({"cwd": "/etc"}, "cwd must be /work or below it"),
    ({"cwd": "/work/../etc"}, "cwd must be /work or below it"),
    ({"timeout_s": 0}, "timeout_s must be in"),
    ({"timeout_s": ...}, "timeout_s and wait_s must be declared"),
    ({"wait_s": -1}, "wait_s must be in"),
    ({"io_cap_kb": 0}, "io_cap_kb must be in"),
    ({"io_cap_kb": 99999}, "io_cap_kb must be in"),
    ({"resources": {"cpus": 1}}, "must declare cpus"),
    ({"resources": {**RESOURCES, "cpus": 0}}, "must be positive"),
    ({"resources": {**RESOURCES, "gpus": -1}}, "must be positive"),
    ({"resources": {**RESOURCES, "workdir_medium": "disk"}}, "workdir_medium must be temp or ram"),
    ({"runtime": {"dockerfile": "FROM x", "extra": 1}}, "takes dockerfile"),
    ({"runtime": {"dockerfile": 7}}, "runtime fields must be strings"),
])
def test_a_malformed_request_is_refused_with_the_reason(overrides, message):
    from sandbox.service import _request

    with pytest.raises(ValueError, match=message):
        _request(request_for(**overrides))


def test_the_declared_limits_are_the_server_s_own(monkeypatch):
    """`timeout_s` and `wait_s` are bounded by the SERVER, not by the caller's optimism."""
    from sandbox import service

    with pytest.raises(ValueError, match=r"timeout_s must be in \(0,"):
        service._request(request_for(timeout_s=service.MAX_COMMAND_SECONDS + 1))
    with pytest.raises(ValueError, match=r"wait_s must be in \[0,"):
        service._request(request_for(wait_s=service.MAX_IDLE_SECONDS + 1))


@pytest.mark.parametrize("text,status", [
    ("forbidden:runtime does not allow endpoint egress", 403),
    ("not_found:route", 404),
    ("conflict:session_id already belongs to another execution config", 409),
    ("unavailable:runtime is not available", 503),
    ("bad_request:code must be a string without NUL", 400),
    ("anything else at all", 400),
])
def test_the_error_prefix_is_the_status(text, status):
    from sandbox.service import _status

    assert _status(ValueError(text)) == (status, {"error": text})


def test_request_environment_is_bounded_and_never_echoed():
    """Values are secrets: they are bounded, validated, and folded into the config hash so a live
    session cannot silently change them."""
    from sandbox.runtime import Resources, resolve
    from sandbox.service import _execution_config

    grant = Resources.from_wire(RESOURCES)
    spec = resolve({"dockerfile": dynamic("env")}).spec
    _policy, environment, first = _execution_config({"env": {"A": "1"}}, spec, grant)
    assert environment == {"A": "1"}
    _policy, _env, second = _execution_config({"env": {"A": "2"}}, spec, grant)
    assert first != second, "a changed secret must change the execution config"

    for env, message in (
            ({f"K{i}": "v" for i in range(65)}, "at most 64 entries"),
            ({"1BAD": "v"}, "invalid environment name"),
            ({"OK": "a\x00b"}, "NUL-free"),
            ({"OK": 1}, "NUL-free"),
            ({"BIG": "x" * (128 * 1024 + 1)}, "exceeds 128 KiB"),
            ([("A", "1")], "must be an object")):
        with pytest.raises(ValueError, match=message):
            _execution_config({"env": env}, spec, grant)


@pytest.mark.parametrize("network,message", [
    ("ftp://host/x", "must be one HTTP.S. endpoint URL"),
    ("https://", "must be one HTTP.S. endpoint URL"),
    ("https://user:pw@host", "must be one HTTP.S. endpoint URL"),
    ("https://ho_st", "requires a DNS hostname"),
    ("https://host:0", "invalid port"),
    ("https://host:99999", "invalid port"),
    ("https://host:https", "invalid port"),
])
def test_an_endpoint_grant_is_one_origin_or_nothing(network, message):
    """A URL path never narrows the grant, and a written-down port is honoured or refused --
    `parsed.port or default` conflated `:0` with "absent" and silently granted 443 instead."""
    from sandbox.runtime import Resources, resolve
    from sandbox.service import _execution_config

    grant = Resources.from_wire(RESOURCES)
    spec = resolve({"dockerfile": dynamic("net", egress=True)}).spec
    with pytest.raises(ValueError, match=message):
        _execution_config({"network": network}, spec, grant)
    # The path is discarded: the grant is the origin.
    policy, _env, _digest = _execution_config({"network": "http://host/a/b?c=d"}, spec, grant)
    assert policy["origin"] == "http://host:80" and policy["port"] == 80
    # An address literal is a name the relay can filter, and is allowed.
    literal, _env, _digest = _execution_config({"network": "https://1.2.3.4"}, spec, grant)
    assert literal["origin"] == "https://1.2.3.4:443"


def test_the_work_delta_is_a_bounded_metadata_view(tmp_path):
    """`/work` is reported by DELTA, never by content, and the scan is capped."""
    from sandbox.service import _file_delta, _files

    before, cut = _files(str(tmp_path))
    assert before == {} and cut is False
    (tmp_path / "kept").write_text("alpha", encoding="utf-8")
    (tmp_path / "gone").write_text("bravo", encoding="utf-8")
    (tmp_path / "dir").mkdir()
    (tmp_path / "dir" / "deep").write_text("x", encoding="utf-8")
    first, _cut = _files(str(tmp_path))
    assert set(first) == {"kept", "gone", "dir", "dir/deep"}

    (tmp_path / "gone").unlink()
    (tmp_path / "kept").write_text("alpha-and-more", encoding="utf-8")
    (tmp_path / "fresh").write_text("y", encoding="utf-8")
    after, cut = _files(str(tmp_path))
    delta = _file_delta(first, after, cut)
    assert (delta["created"], delta["deleted"], delta["modified"]) == (1, 1, 1)
    assert delta["bytes_delta"] == len("alpha-and-more") + 1 - len("alpha") - len("bravo")
    assert delta["scope"] == "/work" and delta["observed"] is True and delta["scanned"] == 4

    for index in range(30):
        (tmp_path / f"f{index}").write_text("z", encoding="utf-8")
    capped, cut = _files(str(tmp_path), limit=10)
    assert len(capped) <= 10 and cut is True
    assert _file_delta(first, capped, cut)["truncated"] is True
    # A memory-backed workspace has no host path to scan at all.
    assert _files(None) == ({}, True)


def test_a_symlink_is_never_followed_out_of_the_workspace(tmp_path):
    """The delta is metadata about `/work`; following a link would report the host."""
    from sandbox.service import _files

    (tmp_path / "escape").symlink_to(os.path.dirname(os.path.abspath(__file__)))
    found, _cut = _files(str(tmp_path))
    assert set(found) == {"escape"}


def test_an_egress_host_is_vetted_and_pinned():
    """A DNS name that resolves to a non-global address is the SSRF-to-metadata vector; the vetter
    refuses it and pins a global one so a later DNS answer cannot move the target (rebinding)."""
    import ipaddress

    from sandbox.service import _vet_endpoint

    for private in ("localhost", "localtest.me"):
        with pytest.raises(ValueError, match="non-global"):
            _vet_endpoint(private, 80)
    with pytest.raises(ValueError, match="does not resolve"):
        _vet_endpoint("no-such-host.invalid", 80)
    pinned = _vet_endpoint("example.com", 443)
    assert ipaddress.ip_address(pinned).is_global


def test_a_worker_reports_how_an_action_ended_as_data_never_as_a_traceback():
    """A raise inside a Ray actor reached the gateway as a RayTaskError whose text was the whole
    remote traceback, and the caller got `unavailable:session actor failed: ray::_SessionActor...`
    with the real cause (E2BIG, 2026-10-15) cut off. The worker now names each ending: a result,
    a REQUEST error (kept verbatim, with its status), or an infrastructure failure carrying the
    real exception -- which the gateway retries and logs, and the caller never sees."""
    from sandbox.service import InfraFailure, _outcome, _public, _status

    assert _outcome(lambda: {"ok": 1}, "execute") == {"result": {"ok": 1}}

    def forbidden():
        raise ValueError("forbidden:endpoint host localtest.me resolves to a non-global address")
    reply = _outcome(forbidden, "open")
    assert reply == {"error": "forbidden:endpoint host localtest.me resolves to a non-global address"}
    assert _status(ValueError(reply["error"]))[0] == 403

    closed = []

    def e2big():
        raise OSError(7, "Argument list too long", "podman")
    reply = _outcome(e2big, "execute", on_infra=lambda: closed.append(1))
    assert set(reply) == {"infra"} and closed == [1]
    assert "Argument list too long" in reply["infra"]["detail"]
    assert "Traceback" in reply["infra"]["detail"], "the whole cause travels, for the incident log"
    assert reply["infra"]["started"] is None          # a crash mid-action: unknown

    def unavailable():
        raise ValueError("unavailable:container:no working Podman/Docker runtime")
    assert _outcome(unavailable, "open")["infra"]["started"] is False

    def launch():
        raise InfraFailure("launch", "container launch failed (rc=125): Error: boom\nmore")
    reply = _outcome(launch, "execute")
    assert reply == {"infra": {"stage": "launch", "started": False, "final": False,
                               "detail": "container launch failed (rc=125): Error: boom\nmore"}}
    # What a caller would be told: one line, no traceback, pid or actor id.
    failure = InfraFailure(**reply["infra"])
    assert _public(failure) == "container launch failed (rc=125): Error: boom"

def test_a_control_mount_failure_names_the_shared_root_assumption(monkeypatch, tmp_path):
    """The engine resolves bind mounts on ITS filesystem. When that is not the service's -- a
    podman machine, a remote daemon -- the control mount fails per request with a cryptic statfs;
    the message must name ZERO_LOCAL_ROOT so a consumer reads misconfiguration, not a fault."""
    from sandbox import service

    monkeypatch.setattr(service, "_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setattr(service, "root", lambda: tmp_path)
    cryptic = ("Error: statfs /tmp/zero-sandbox-501/work/zero-control-9jp4xvux: "
               "no such file or directory")
    hint = service._root_hint(cryptic)
    assert "ZERO_LOCAL_ROOT" in hint and "engine-visible" in hint
    # An unrelated failure gets no hint.
    assert service._root_hint("Error: image not known") == ""

