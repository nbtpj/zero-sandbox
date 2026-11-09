"""The service never answers a valid request with an infrastructure failure: it retries them on
fresh resources, and if it cannot recover it goes DOWN. And it never re-runs a command that ran.

Each case pins one half of that, offline. The live half -- a flood across real runtimes with
chaos injected -- is `tests/chaos.py`, run by hand against a served sandbox.
"""
from __future__ import annotations

import asyncio
import io
import json
import os
import shlex
import socket
import sys
import tarfile
import threading
import time

import pytest

from conftest import RESOURCES, dynamic
from sandbox.runtime import resolve


# ---------------------------------------------------------------------------- the start marker

def _feed(chunks, cap, marker=b""):
    from sandbox import service
    read, write = os.pipe()

    def writer():
        with os.fdopen(write, "wb") as stream:
            for chunk in chunks:
                stream.write(chunk)
                stream.flush()
    thread = threading.Thread(target=writer)
    thread.start()
    out: list = []
    with os.fdopen(read, "rb") as stream:
        service._tail(stream, cap, out, marker)
    thread.join()
    return out[0]


@pytest.mark.parametrize("cap", [3, 10, 1 << 20])
def test_the_marker_is_invisible_in_what_is_counted_and_kept(cap):
    """The candidate's stderr must read EXACTLY as if no marker had been written: same bytes,
    same totals, same truncation flag -- whatever the cap, wherever the reads split."""
    mark = b"ZERO-START-" + b"ab" * 16
    body = b"warning: x\n" + b"e" * 50
    plain = _feed([body], cap)
    for chunks in ([mark + body], [mark[:5], mark[5:] + body[:7], body[7:]],
                   [mark, b"", body]):
        data, cut, total, seen = _feed(chunks, cap, mark)
        assert seen and (data, cut, total) == plain[:3]


def test_engine_chatter_before_the_marker_is_kept_and_the_start_still_seen():
    """The engine CLIENT prints onto the exec's stderr before the process starts (measured:
    `Failed to get rootless runtime dir`). That is not the submitted code, and it is not proof the
    code did not start."""
    mark = b"ZERO-START-" + b"cd" * 16
    data, cut, total, seen = _feed([b"level=warning msg=x\n", mark, b"boom"], 1 << 20, mark)
    assert seen and data == b"level=warning msg=x\nboom" and total == len(data) and not cut


def test_a_stream_without_the_marker_says_the_command_never_started():
    mark = b"ZERO-START-" + b"ef" * 16
    data, _cut, _total, seen = _feed([b"Error: no such container"], 1 << 20, mark)
    assert not seen and data == b"Error: no such container"
    # Past the window, a late marker proves nothing: only the engine writes before the code runs.
    from sandbox import service
    late = b"x" * (service._MARK_WINDOW + 100) + mark
    assert _feed([late], 1 << 30, mark)[3] is False


# ---------------------------------------------------------------- LiveSession.execute, for real

SHIM = r'''
import os, sys, tarfile, io
args = sys.argv[1:]
mode = os.environ.get("SHIM_MODE", "")
log = os.environ["SHIM_LOG"]
with open(log, "a") as stream:
    stream.write(repr(args[:1] + [a[:40] for a in args[1:]]) + "\n")
if args[0] == "inspect":
    state = os.environ.get("SHIM_STATE", "running")
    if state == "gone":
        sys.stderr.write("Error: no such container"); sys.exit(125)
    execs = '["7751a888"]' if state == "client-lost" else "[]"
    print(("true" if state in ("running", "client-lost") else "false") + " " + execs); sys.exit(0)
if args[0] == "cp":
    with tarfile.open(fileobj=io.BytesIO(sys.stdin.buffer.read())) as archive:
        archive.extractall(os.environ["SHIM_TMP"])
    sys.exit(0)
if args[0] == "rm":
    sys.exit(0)
if args[0] == "exec":
    rest = args[1:]
    if rest[0] == "--workdir":
        rest = rest[2:]
    rest = rest[1:]                                    # the container name
    if mode == "never-start":
        sys.stderr.write("Error: can only create exec sessions on running containers")
        sys.exit(125)
    sys.stderr.write("time=x level=warning msg=\"engine chatter\"\n"); sys.stderr.flush()
    if mode == "die-mid":
        # Let the wrapper start the command, then the "container" disappears under it.
        pid = os.fork()
        if pid == 0:
            os.execvp(rest[0], rest)
        import time; time.sleep(1.0)
        os.kill(pid, 9); os.waitpid(pid, 0)
        sys.exit(137)
    rest = [a.replace("/tmp/.zero-code", os.environ["SHIM_TMP"] + "/.zero-code") for a in rest]
    os.execvp(rest[0], rest)
sys.exit(0)
'''


@pytest.fixture
def shim(tmp_path, monkeypatch):
    from sandbox import service
    script = tmp_path / "engine.py"
    script.write_text(SHIM)
    log, scratch = tmp_path / "calls.log", tmp_path / "ctmp"
    scratch.mkdir()
    monkeypatch.setenv("SHIM_LOG", str(log))
    monkeypatch.setenv("SHIM_TMP", str(scratch))
    monkeypatch.setattr(service, "_engine_argv", lambda engine, stores=(): [sys.executable,
                                                                             str(script)])
    monkeypatch.setattr(service, "_is_remote", lambda: True)
    monkeypatch.setattr(service, "_LOCAL_ROOT", str(tmp_path))

    def session():
        live = service.LiveSession.__new__(service.LiveSession)
        live.sub = resolve({"dockerfile": dynamic("shim")})
        live.resources = service.Resources.from_wire(RESOURCES)
        live.session_id, live.engine, live.name = "s", "podman", "zero-session-shim"
        live.launched = live.spawned = True
        live.closed, live.work, live.file_state = False, None, {}
        live.environment, live.control = {}, str(tmp_path)
        live.network_policy = {"mode": "none", "outbound": False}
        live.network_name = live.proxy_name = None
        return live
    session.log = log
    return session


def _run(live, code, **request):
    body = {"timeout_s": 10, "wait_s": 0, "code": code, "cwd": "/work", "io_cap_kb": 200,
            **request}
    return live.execute(body)


def test_a_command_runs_unchanged_and_its_streams_carry_no_marker(shim):
    live = shim()
    result = _run(live, 'echo "out $0"; echo err >&2; exit 3')
    assert result["returncode"] == 3 and result["stdout"] == f"out {live.spec.shell}\n"
    # The engine chatter is still there, as it always was; the marker is not.
    assert result["err"].endswith("err\n") and "ZERO-START" not in result["err"]
    assert result["stderr_total_bytes"] == len(result["err"].encode())


def test_a_command_that_never_started_is_infrastructure_not_its_result(shim, monkeypatch):
    from sandbox import service
    monkeypatch.setenv("SHIM_MODE", "never-start")
    with pytest.raises(service.InfraFailure) as failure:
        _run(shim(), "echo hi")
    assert failure.value.stage == "exec-start" and failure.value.started is False
    assert "running containers" in failure.value.detail


def test_a_container_that_died_under_the_command_is_infrastructure(shim, monkeypatch):
    from sandbox import service
    monkeypatch.setenv("SHIM_MODE", "die-mid")
    monkeypatch.setenv("SHIM_STATE", "gone")
    with pytest.raises(service.InfraFailure) as failure:
        _run(shim(), "echo started; sleep 5; echo finished")
    assert failure.value.stage == "exec-lost" and failure.value.started is True


def test_a_client_that_lost_the_engine_is_infrastructure_even_with_the_container_alive(
        shim, monkeypatch):
    """Measured: the socket cut under a running command; the client exited 125 while the engine's
    exec record -- left behind, because only a client that completes deletes it -- said 0."""
    from sandbox import service
    monkeypatch.setenv("SHIM_STATE", "client-lost")
    with pytest.raises(service.InfraFailure) as failure:
        _run(shim(), "echo all; exit 125")
    assert failure.value.stage == "exec-lost" and "exec record" in failure.value.detail


@pytest.mark.parametrize("code,rc", [("exit 137", 137), ("exit 125", 125),
                                     ("sh -c 'kill -9 $$'; exit $?", 137),
                                     ("sh -c 'kill -11 $$'; exit $?", 139)])
def test_the_commands_own_high_exit_is_its_result(shim, monkeypatch, code, rc):
    """An exit of 125+ costs one engine probe, and with the container alive it is the RESULT --
    the candidate's own OOM kill, crash or exit code is never retried."""
    monkeypatch.setenv("SHIM_STATE", "running")
    result = _run(shim(), code)
    assert result["returncode"] == rc and not result["timed_out"]


def test_an_engine_client_killed_on_the_host_is_infrastructure(shim, monkeypatch):
    """A negative status is the engine CLIENT dying by signal on the host -- out of reach of any
    submitted program -- and is not the command's exit status."""
    from sandbox import service
    real = service.subprocess.Popen

    class Killed(real):
        def wait(self, timeout=None):
            super().wait(timeout)
            return -9
    monkeypatch.setattr(service.subprocess, "Popen", Killed)
    with pytest.raises(service.InfraFailure) as failure:
        _run(shim(), "echo hi")
    assert failure.value.stage == "exec-client"


def test_a_timeout_is_a_result(shim):
    result = _run(shim(), "sleep 20", timeout_s=1)
    assert result["timed_out"] and result["returncode"] == 124


def test_code_too_big_for_an_argument_is_staged_and_runs_the_same(shim):
    """E2BIG, 2026-10-15 16:07 and 20:26: a 128 KiB+ command as one argv string. It now runs."""
    code = "x=" + "a" * 300_000 + '; echo "${#x} $0"'
    live = shim()
    result = _run(live, code)
    assert result["returncode"] == 0 and result["stdout"] == f"300000 {live.spec.shell}\n"
    assert result["code"] == code, "the reply echoes what was submitted, not the staging"
    calls = shim.log.read_text().splitlines()
    assert any(line.startswith("['cp'") for line in calls), calls
    # Below the limit nothing is staged: the argv path is unchanged.
    shim.log.write_text("")
    assert _run(live, "echo small")["stdout"] == "small\n"
    assert not any(line.startswith("['cp'") for line in shim.log.read_text().splitlines())


def test_a_deadline_that_cannot_move_does_not_cost_the_result(shim, monkeypatch):
    from sandbox import service
    real = service.subprocess.run

    def run(argv, *args, **kwargs):
        if "printf '%s\\n'" in " ".join(map(str, argv)):
            raise service.subprocess.TimeoutExpired(argv, 30)
        return real(argv, *args, **kwargs)
    monkeypatch.setattr(service.subprocess, "run", run)
    result = _run(shim(), "echo kept", wait_s=30)
    assert result["stdout"] == "kept\n" and result["alive"] is True


# ------------------------------------------------------------------- the gateway's retry loop

def _gateway(monkeypatch, tmp_path):
    from sandbox import service
    monkeypatch.delenv("ZERO_SANDBOX_TOKEN", raising=False)   # these speak to the class directly
    monkeypatch.setattr(service, "_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setattr(service, "runtime", lambda: "podman")
    monkeypatch.setenv("ZERO_WORKER_CACHE_S", "3600")
    monkeypatch.setenv("ZERO_INFRA_BACKOFF_S", "0.01")
    monkeypatch.setenv("ZERO_INCIDENT_LOG", str(tmp_path / "incidents.jsonl"))
    monkeypatch.setattr(service, "_kill", lambda actor: None)      # no Ray behind these
    return service._gateway_class({"CPU": 4})()


def _drain(gateway):
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()


class _Actor:
    """A session worker that answers each action with the next scripted outcome."""

    def __init__(self, outcome, calls):
        self.outcome, self.calls = outcome, calls
        outer = self

        class Execute:
            def remote(self, request):
                async def run():
                    outer.calls.append(request)
                    return outer.outcome
                return run()

        class Close:
            def remote(self):
                async def run():
                    return None
                return run()
        self.execute, self.close = Execute(), Close()


def _scripted(gateway, service, outcomes):
    """Replace `_new` with one that opens a fresh scripted worker per attempt."""
    opened, calls = [], []

    async def fake_new(sub, request, lock, canary=False):
        token = None if canary else await gateway._acquire(service._grant(request["resources"]))
        actor = _Actor(outcomes[min(len(opened), len(outcomes) - 1)], calls)
        opened.append(actor)
        entry = service._RemoteEntry(sub.digest, None, actor, lock, ("w", sub.digest),
                                     service._grant(request["resources"]), request["config_hash"],
                                     (sub.digest, ""), grant=token)
        gateway.sessions[request["session_id"]] = entry
        return entry
    gateway._new = fake_new
    return opened, calls


def _action(name, **request):
    return {"runtime": {"dockerfile": dynamic(name)}, "session_id": "s-" + name, "code": "true",
            "cwd": "/work", "timeout_s": 5, "wait_s": 0, "io_cap_kb": 200,
            "resources": RESOURCES, **request}


RESULT = {"result": {"alive": False, "returncode": 0, "stdout": "ok"}}
NEVER_STARTED = {"infra": {"stage": "exec-start", "detail": "Error: no such container",
                           "started": False, "final": False}}


def test_infrastructure_failures_are_retried_on_fresh_workers_until_a_result(tmp_path,
                                                                            monkeypatch):
    from sandbox import service
    killed = []

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        monkeypatch.setattr(service, "_kill", killed.append)
        try:
            opened, calls = _scripted(gateway, service, [NEVER_STARTED, NEVER_STARTED, RESULT])
            status, body = await gateway._execute(_action("retry"))
            assert (status, body["stdout"]) == (200, "ok")
            assert len(opened) == 3 and len(set(map(id, opened))) == 3, "fresh worker each time"
            assert killed[:2] == opened[:2], "a worker that failed is killed, never pooled"
            assert gateway.infra["retried"] == 2 and gateway.infra["recovered"] == 1
            assert gateway.capacity.free == gateway.capacity.total and not gateway.grants
        finally:
            _drain(gateway)
    asyncio.run(check())
    lines = [json.loads(x) for x in (tmp_path / "incidents.jsonl").read_text().splitlines()]
    assert [x["kind"] for x in lines].count("infra-failure") == 2
    assert all("ts" in x and x.get("detail") for x in lines if x["kind"] == "infra-failure")


@pytest.mark.parametrize("result", [
    {"alive": False, "returncode": 125, "err": "Error: something", "timed_out": False},
    {"alive": False, "returncode": 124, "timed_out": True},
    {"alive": False, "returncode": 139, "timed_out": False},
    {"alive": False, "returncode": 0, "stdout": "", "timed_out": False},
])
def test_a_result_is_never_re_run_whatever_it_says(tmp_path, monkeypatch, result):
    """No retry-until-lucky: once the worker reports a RESULT, that is the answer."""
    from sandbox import service

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            opened, calls = _scripted(gateway, service, [{"result": result}, RESULT])
            status, body = await gateway._execute(_action("once"))
            assert status == 200 and body == result
            assert len(opened) == 1 and len(calls) == 1
            assert gateway.infra["failures"] == 0
        finally:
            _drain(gateway)
    asyncio.run(check())


def test_a_request_error_is_never_retried(tmp_path, monkeypatch):
    from sandbox import service

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            opened, _ = _scripted(gateway, service, [{"error": "forbidden:nope"}, RESULT])
            with pytest.raises(ValueError, match="forbidden:nope"):
                await gateway._execute(_action("reqerr"))
            assert len(opened) == 1
        finally:
            _drain(gateway)
    asyncio.run(check())


def test_a_session_that_held_state_is_reported_lost_not_rebuilt(tmp_path, monkeypatch):
    from sandbox import service

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            opened, _ = _scripted(gateway, service, [
                {"result": {"alive": True, "returncode": 0}}])
            assert (await gateway._execute(_action("state", wait_s=30)))[0] == 200
            opened[0].outcome = NEVER_STARTED
            with pytest.raises(ValueError, match="^not_found:session s-state was lost"):
                await gateway._execute(_action("state", wait_s=30))
            assert len(opened) == 1, "its state cannot be recreated on another worker"
            assert gateway.capacity.free == gateway.capacity.total
        finally:
            _drain(gateway)
    asyncio.run(check())


def test_past_the_bound_a_broken_service_goes_down_instead_of_answering(tmp_path, monkeypatch):
    from sandbox import service
    monkeypatch.setenv("ZERO_INFRA_ATTEMPTS", "2")
    monkeypatch.setenv("ZERO_SERVE_DRIVER_PID", "")

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            _scripted(gateway, service, [NEVER_STARTED])
            action = asyncio.create_task(gateway._execute(_action("doom")))
            await asyncio.sleep(1.0)
            # The canary failed too: the service is broken, so it does not answer AT ALL.
            assert not action.done(), "a broken service must not answer with an error"
            assert gateway.doomed and "self-check failed" in gateway.doomed
            assert (tmp_path / "serve-fatal").read_text().strip() == gateway.doomed
            # And from now on nothing is answered, /health included.
            health = asyncio.create_task(gateway(type("R", (), {
                "url": type("U", (), {"path": "/health"})(), "method": "GET"})()))
            await asyncio.sleep(0.2)
            assert not health.done()
        finally:
            _drain(gateway)
    asyncio.run(check())


def test_past_the_bound_a_request_specific_failure_is_reported(tmp_path, monkeypatch):
    """The self-check (the canary, through the same path) passes: the SERVICE runs, so the failure
    belongs to this request's runtime and is reported with its cause -- one runtime that cannot run
    must not take the service down for everyone."""
    from sandbox import service
    monkeypatch.setenv("ZERO_INFRA_ATTEMPTS", "2")

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            canary = service._canary_request()
            canary_digest = resolve(canary["runtime"]).digest

            async def fake_new(sub, request, lock, canary=False):
                outcome = ({"result": {"alive": False, "returncode": 0,
                                       "stdout": request["code"].split()[-1] + "\n"}}
                           if sub.digest == canary_digest else NEVER_STARTED)
                token = None if canary else await gateway._acquire(
                    service._grant(request["resources"]))
                entry = service._RemoteEntry(sub.digest, None, _Actor(outcome, []), lock,
                                             ("w",), service._grant(request["resources"]),
                                             request["config_hash"], (sub.digest, ""),
                                             grant=token)
                gateway.sessions[request["session_id"]] = entry
                return entry
            gateway._new = fake_new
            with pytest.raises(ValueError, match="^unavailable:Error: no such container"):
                await gateway._execute(_action("specific"))
            assert gateway.doomed is None and gateway.selfcheck_result[0] is True
        finally:
            _drain(gateway)
    asyncio.run(check())


def test_infrastructure_that_recovers_during_the_self_check_still_serves_the_request(
        tmp_path, monkeypatch):
    """The bound is spent during an outage; by the time the self-check runs, the engine is back
    and the canary passes. That is not this request's fault: it gets one more attempt."""
    from sandbox import service
    monkeypatch.setenv("ZERO_INFRA_ATTEMPTS", "2")

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            canary_digest = resolve(service._canary_request()["runtime"]).digest
            tries = []

            async def fake_new(sub, request, lock, canary=False):
                if sub.digest == canary_digest:
                    outcome = {"result": {"alive": False, "returncode": 0,
                                          "stdout": request["code"].split()[-1] + "\n"}}
                else:
                    tries.append(1)
                    outcome = NEVER_STARTED if len(tries) <= 2 else RESULT
                entry = service._RemoteEntry(sub.digest, None, _Actor(outcome, []), lock, ("w",),
                                             service._grant(request["resources"]),
                                             request["config_hash"], (sub.digest, ""))
                gateway.sessions[request["session_id"]] = entry
                return entry
            gateway._new = fake_new
            status, body = await gateway._execute(_action("recovers"))
            assert (status, body["stdout"], len(tries)) == (200, "ok", 3)
            assert gateway.doomed is None
        finally:
            _drain(gateway)
    asyncio.run(check())


def test_a_client_leaving_cannot_cancel_the_shared_load_for_everyone_else(tmp_path, monkeypatch):
    """Incident 2, 2026-10-15: one disconnect during a cold load cancelled the SHARED load task, it
    stayed in `self.caches` cancelled, and every later request for that runtime got a bare 500 in
    ~2 ms while /health said 200 -- until a restart."""
    from sandbox import service

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        loads, release = [], asyncio.Event()

        async def slow_load(sub, image, key):
            loads.append(key)
            await release.wait()
            info = {"image": "zero-runtime:x", "ram_bytes": 0, "bytes": 1, "node_id": "n",
                    "actor": None, "lease": None, "active": 0, "last_used": 0.0}
            gateway.cache_info[key] = info
            return info
        gateway._load = slow_load
        monkeypatch.setattr(gateway, "ram_budget", 0)
        try:
            sub = resolve({"dockerfile": dynamic("shared")})
            leaver = asyncio.create_task(gateway._cache(sub, None))
            await asyncio.sleep(0.05)
            leaver.cancel()                         # the client goes away mid-load
            with pytest.raises(asyncio.CancelledError):
                await leaver
            stayer = asyncio.create_task(gateway._cache(sub, None))
            await asyncio.sleep(0.05)
            release.set()
            assert (await stayer)["image"] == "zero-runtime:x"
            assert len(loads) == 1, "the load was neither cancelled nor repeated"
            # A load that WAS cancelled (by anything) is never re-awaited: it is replaced.
            key = (sub.digest, "")
            dead = asyncio.get_running_loop().create_future()
            dead.cancel()
            gateway.caches[key] = dead
            assert (await gateway._cache(sub, None))["image"] == "zero-runtime:x"
            assert len(loads) == 2
        finally:
            _drain(gateway)
    asyncio.run(check())


def test_a_leaked_grant_is_returned_to_admission(tmp_path, monkeypatch):
    from sandbox import service
    monkeypatch.setenv("ZERO_AUDIT_S", "0.05")

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            token = await gateway._acquire((1.0, 1.0, 1.0))   # an owner that vanished
            assert gateway.capacity.free != gateway.capacity.total
            await asyncio.sleep(0.4)
            assert token not in gateway.grants and gateway.infra["leaks_repaired"] == 1
            assert gateway.capacity.free == gateway.capacity.total
            # A held grant with a live owner is never touched.
            opened, _ = _scripted(gateway, service, [{"result": {"alive": True}}])
            await gateway._execute(_action("held", wait_s=60))
            await asyncio.sleep(0.3)
            assert len(gateway.grants) == 1 and gateway.infra["leaks_repaired"] == 1
        finally:
            _drain(gateway)
    asyncio.run(check())


def test_the_canary_skips_admission_and_is_not_counted(tmp_path, monkeypatch):
    from sandbox import service

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            opened, _ = _scripted(gateway, service, [RESULT])
            # Admission completely full: an ordinary request would queue behind it.
            await gateway._acquire(gateway.capacity.total)
            status, _ = await asyncio.wait_for(
                gateway._execute(_action("canary"), canary=True), 2)
            assert status == 200 and gateway.served == 0
        finally:
            _drain(gateway)
    asyncio.run(check())


# --------------------------------------------------------------- the client waits while it is down

def test_the_client_waits_while_the_service_is_down_and_never_while_it_answers(monkeypatch):
    """Down (refused, cut, or Serve's bare 5xx) is a pause: the request is sent again once the
    service is back. An answer -- even an error -- is returned at once."""
    import http.server
    from sandbox import route

    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    replies = iter([(500, b"Internal Server Error"), (200, b'{"ok": 1}'),
                    (503, b'{"error": "unavailable:runtime is not available: x"}')])

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            code, body = next(replies)
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(code)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    def later():
        time.sleep(1.5)                       # refused until then: the service is "restarting"
        server = http.server.HTTPServer(("127.0.0.1", port), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Thread(target=later, daemon=True).start()
    monkeypatch.setenv("ZERO_SANDBOX_URL", f"http://127.0.0.1:{port}")
    monkeypatch.setattr(route, "_token", lambda: "")
    started = time.monotonic()
    assert asyncio.run(route._request("/execute", {"x": 1})) == {"ok": 1}
    assert time.monotonic() - started >= 1.5
    with pytest.raises(RuntimeError, match="^unavailable:runtime is not available"):
        asyncio.run(route._request("/execute", {"x": 1}))
    monkeypatch.setenv("ZERO_SANDBOX_DOWN_WAIT_S", "1")
    monkeypatch.setenv("ZERO_SANDBOX_URL", "http://127.0.0.1:1")
    with pytest.raises(RuntimeError, match="^sandbox down for over 1s"):
        asyncio.run(route._request("/execute", {"x": 1}))
