"""The service executes whatever it is sent, so it must not start open on a reachable address."""
from __future__ import annotations

import pytest


@pytest.mark.parametrize("host", ["0.0.0.0", "10.1.2.3", "example.org"])
def test_a_reachable_bind_without_a_token_is_refused(host):
    from sandbox import service

    reason = service._auth_refusal(host, env={})
    assert reason is not None and "ZERO_SANDBOX_TOKEN" in reason and host in reason


@pytest.mark.parametrize("host", ["127.0.0.1", "::1", "localhost"])
def test_loopback_may_start_without_a_token(host):
    from sandbox import service

    assert service._auth_refusal(host, env={}) is None


def test_a_token_allows_any_bind():
    from sandbox import service

    assert service._auth_refusal("0.0.0.0", env={"ZERO_SANDBOX_TOKEN": "s3cr3t"}) is None


def test_a_blank_token_is_not_a_token():
    from sandbox import service

    assert service._auth_refusal("0.0.0.0", env={"ZERO_SANDBOX_TOKEN": "   "}) is not None


def test_the_explicit_opt_out_allows_an_open_bind_and_nothing_else_does():
    from sandbox import service

    assert service._auth_refusal("0.0.0.0", env={"ZERO_SANDBOX_ALLOW_NO_AUTH": "1"}) is None
    assert service._auth_refusal("0.0.0.0", env={"ZERO_SANDBOX_ALLOW_NO_AUTH": "true"}) is not None


def test_main_refuses_before_it_touches_a_single_container(monkeypatch, capsys):
    """`_sweep_containers` kills live sessions: a refused start must not have run it."""
    from sandbox import service

    monkeypatch.delenv("ZERO_SANDBOX_TOKEN", raising=False)
    monkeypatch.delenv("ZERO_SANDBOX_ALLOW_NO_AUTH", raising=False)
    monkeypatch.setattr("sys.argv", ["sandbox-service", "--host", "0.0.0.0"])
    touched = []
    monkeypatch.setattr(service, "install", lambda: touched.append("install"))
    monkeypatch.setattr(service, "_sweep_containers", lambda: touched.append("sweep"))

    assert service.main() == 2
    assert touched == []
    assert "refusing to serve code execution" in capsys.readouterr().err
