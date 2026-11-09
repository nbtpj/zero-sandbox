"""Declarations, identity and grants: what a submitted Dockerfile is allowed to say.

The declaration supplies IMAGE CAPABILITIES only. Hardware is a per-session grant and is never
part of the image identity, which is the Dockerfile's SHA-256 and nothing else.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from conftest import RESOURCES, dynamic


def test_a_declaration_carries_image_capabilities_and_never_hardware():
    """Resources are granted per session. A Dockerfile that tries to name them is ignored."""
    from sandbox.runtime import declarations, spec_from

    hardware = {"ZERO_RUNTIME_CPUS", "ZERO_RUNTIME_MEM_GB", "ZERO_RUNTIME_WORKDIR_GB",
                "ZERO_RUNTIME_WORKDIR_MEDIUM", "ZERO_RUNTIME_GPUS", "ZERO_RUNTIME_PIDS",
                "ZERO_RUNTIME_IO_CAP_KB", "ZERO_RUNTIME_MAX_COMMAND_SECONDS",
                "ZERO_RUNTIME_MAX_IDLE_SECONDS"}
    declared = declarations(dynamic("plain") + "ENV ZERO_RUNTIME_CPUS=64 ZERO_RUNTIME_PIDS=9\n")
    assert hardware & set(declared), "the fixture must actually try to claim hardware"
    spec = spec_from(declared, "fixture")
    assert not hasattr(spec, "cpus") and not hasattr(spec, "pids")
    assert (spec.shell, spec.egress) == ("/bin/sh", "none")
    for path in sorted(Path(__file__).resolve().parent.glob("runtimes/*.Dockerfile")):
        assert hardware.isdisjoint(declarations(path.read_text()))

def test_a_submitted_declaration_is_the_authority_and_keys_the_image():
    from sandbox.runtime import as_descriptor, resolve

    first = resolve({"dockerfile": dynamic("made-up"), "documentation": "docs"})
    assert (first.spec.shell, first.documentation) == ("/bin/sh", "docs")
    assert first.digest == resolve({"dockerfile": dynamic("made-up")}).digest
    assert first.digest != resolve(
        {"dockerfile": dynamic("made-up", extra="ENV E=1\n")}).digest
    with pytest.raises(ValueError, match="takes dockerfile"):
        resolve({"name": "other", "dockerfile": dynamic("made-up")})
    path = Path(__file__).resolve().parent / "runtimes/shell.Dockerfile"
    assert as_descriptor(path)["dockerfile"] == path.read_text()

def test_resources_are_required_and_are_not_image_identity():
    from sandbox.runtime import Resources, resolve
    from sandbox.service import _request

    sub = resolve({"dockerfile": dynamic("grant")})
    assert sub.digest == resolve({"dockerfile": dynamic("grant")}).digest
    body = {"runtime": {"dockerfile": dynamic("grant")},
            "session_id": "s", "code": "true", "timeout_s": 1, "wait_s": 0}
    with pytest.raises(ValueError, match="resources must declare"):
        _request(body)
    with pytest.raises(ValueError, match="unsupported request fields"):
        _request({**body, "cache": "ram"})
    body["resources"] = RESOURCES
    _, request = _request(body)
    assert request["resources"] == Resources.from_wire(RESOURCES)
    fractional = Resources.from_wire({**RESOURCES, "cpus": 0.5, "gpus": 0.5})
    assert fractional.cpus == fractional.gpus == 0.5

def test_fractional_gpu_uses_rays_assigned_device_not_the_fraction_as_a_selector():
    import inspect
    from sandbox.service import LiveSession, _SessionActor

    opening = inspect.getsource(_SessionActor.open)
    launch = inspect.getsource(LiveSession.__post_init__)
    assert "get_accelerator_ids" in opening
    assert 'nvidia.com/gpu={gpu_id}' in launch
    assert '"--gpus", str(self.resources.gpus)' not in launch

def test_endpoint_egress_is_runtime_bounded_and_request_selected():
    from sandbox.runtime import Resources, resolve
    from sandbox.service import _execution_config

    grant = Resources.from_wire(RESOURCES)
    permitted = resolve({"dockerfile": dynamic("net", egress=True)}).spec
    policy, env, digest = _execution_config(
        {"network": "https://openrouter.ai/api/v1", "env": {"OPENAI_API_KEY": "secret"}},
        permitted, grant)
    assert policy == {"mode": "endpoint", "outbound": True,
                      "origin": "https://openrouter.ai:443",
                      "host": "openrouter.ai", "port": 443}
    assert env == {"OPENAI_API_KEY": "secret"} and len(digest) == 64
    with pytest.raises(ValueError, match="does not allow"):
        _execution_config({"network": "https://openrouter.ai"},
                          resolve({"dockerfile": dynamic("plain")}).spec,
                          grant)

def test_a_declaration_survives_a_restart(tmp_path, monkeypatch):
    """`/health` must still report what the node caches after the server is restarted."""
    from sandbox import service

    monkeypatch.setattr(service, "_LOCAL_ROOT", str(tmp_path))
    sub = service.resolve({"dockerfile": dynamic("kept"),
                           "documentation": "v1"})
    service.remember(sub)
    service.remember(sub)
    assert [x.documentation for x in service.recall()] == ["v1"]
    service.remember(service.resolve({"dockerfile": dynamic("kept"),
                                      "documentation": "v2"}))
    restored = service.recall()
    assert len(restored) == 1 and restored[0].documentation == "v2"
    assert restored[0].key == sub.key
    (tmp_path / "known" / "junk.json").write_text("not json", encoding="utf-8")
    assert len(service.recall()) == 1
