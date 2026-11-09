"""Confinement invariants that hold before a container is ever started."""
from __future__ import annotations

import inspect


def test_a_submitted_runtime_never_reaches_a_container_engine_socket():
    """A task environment is built and driven over the API; nothing mounts the host engine."""
    import inspect

    from sandbox import service

    source = inspect.getsource(service)
    assert "docker.sock" not in source and "podman.sock" not in source
    assert "/var/run" not in source
    # Every bind mount is one of: the read-only control dir, the /work volume, or the startup
    # shared-root probe (a throwaway marker dir under root(), read-only). None is a host path a
    # submitted runtime chose, and none is an engine socket.
    volumes = [line for line in source.splitlines() if '"--volume"' in line]
    allowed = ("zero-control", "/work", "/zprobe")
    assert volumes and all(any(token in line for token in allowed) for line in volumes)

def test_startup_is_separated_from_the_command_budget():
    """`timeout_s` must bound the COMMAND, never container startup. A stateless action launches
    the container (bounded by its own startup timeout) and then execs the command under
    `timeout_s`, exactly as a live session does -- otherwise image and container startup counted
    against the candidate's budget, and identical deterministic code timed out or not depending
    on launch contention, which made verifier scores stochastic."""
    from sandbox.service import LiveSession

    source = inspect.getsource(LiveSession.execute)
    # Always launch, then exec: no branch carries the command into the run.
    assert "self._launch()" in source and "self.launch_argv + [" not in source
    assert '"exec", "--workdir"' in source
    # proc.wait uses timeout_s, and it wraps the EXEC -- start_ms is measured before it.
    assert "start_ms = int(" in source and "proc.wait(timeout=timeout_s)" in source
    # startup and build are reported apart from the command time.
    assert '"start_ms": start_ms' in source and '"build_ms"' in source

def test_liveness_is_known_without_asking_the_engine():
    """`container inspect` cost 13 ms per action to learn what the return already implies. The
    engine is asked only when the return CANNOT imply it: an exit of 125 or more is also what the
    engine client reports when the container died under the command (measured: rc 137 with the
    partial output), and only the engine can tell that from the command's own exit."""
    from sandbox import service

    assert not hasattr(service.LiveSession, "_running")
    source = inspect.getsource(service.LiveSession.execute)
    assert "not self.closed" in source
    assert source.count("self._survived()") == 1
    guard = [line for line in source.splitlines() if "returncode >= 125" in line]
    assert len(guard) == 1 and "began" in guard[0] and "not launcher_timeout" in guard[0]

def test_containers_are_removed_with_flags_the_engine_accepts(monkeypatch):
    """`docker rm` rejects `--time`; the call is captured, so it removed nothing and said nothing."""
    from sandbox import service

    calls = []
    monkeypatch.setattr(service.subprocess, "run",
                        lambda argv, **kw: calls.append(argv) or None)
    service._rm("podman", "a", "b")
    service._rm("docker", "a", "b")
    service._rm("docker")
    assert calls[0][-5:] == ["rm", "-f", "--time", "0", "a"][-5:] or "--time" in calls[0]
    assert "--time" not in calls[1] and calls[1][-3:] == ["-f", "a", "b"]
    assert len(calls) == 2

def test_an_egress_relay_cannot_outlive_its_session(monkeypatch):
    """The session container self-destructs on its deadline; the relay must watch the same one."""
    import inspect

    from sandbox import service

    source = inspect.getsource(service.LiveSession._open_network)
    assert "/zero-control/deadline" in source and "zero.egress=1" in source
    assert '"--volume", f"{self.control}:/zero-control:ro"' in source
    calls = []
    monkeypatch.setattr(service, "runtime", lambda: "podman")
    monkeypatch.setattr(service, "subprocess", type("S", (), {
        "run": staticmethod(lambda argv, **kw: calls.append(argv) or type(
            "R", (), {"stdout": "netid\n", "returncode": 0})())}))
    service._sweep_networks()
    assert calls[-1][-2:] == ["rm", "netid"] or calls[-1][-1] == "netid"
    assert "-f" not in calls[-1]
