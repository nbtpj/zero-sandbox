"""What happens to a container that never started, and what /health may certify.

Measured on the production host 2026-08-30, slurm job 241011, a rootful podman-machine VM: every runtime
launch came back `unavailable:container launch failed: ... no space left on device`, the VM held
1377 containers of which 1375 were in `Created` state, and `/health` reported every runtime
`available: true` throughout. Three separate defects, one incident; a case for each.
"""
from __future__ import annotations

import subprocess
import time

import pytest

from conftest import RESOURCES, dynamic
from sandbox.runtime import resolve


class _Engine:
    """A scriptable stand-in for `podman`/`docker`, recording the argv it was asked to run."""

    def __init__(self, created: list[str] | None = None):
        self.created = list(created or [])
        self.calls: list[list[str]] = []
        self.removed: list[str] = []

    def __call__(self, argv, *args, **kwargs):
        self.calls.append(list(argv))
        out, code = "", 0
        if "ps" in argv:
            out = "\n".join(self.created)
        elif "rm" in argv:
            names = [a for a in argv[argv.index("rm") + 1:] if not a.startswith("-")]
            names = [a for a in names if a != "0"]         # the podman `--time 0` value
            self.removed += names
            self.created = [c for c in self.created if c not in names]
        return subprocess.CompletedProcess(argv, code, out, "")


def test_a_container_that_never_started_does_not_survive_the_sweep(monkeypatch):
    """`--rm` fires on EXIT and the watchdog runs INSIDE the container, so a launch that fails
    after create leaks a full read-write image layer that nothing else ever removes."""
    from sandbox import service

    engine = _Engine(["cafe1", "cafe2"])
    monkeypatch.setattr(service.subprocess, "run", engine)
    monkeypatch.setattr(service, "runtime", lambda: "podman")
    monkeypatch.setattr(service, "_CREATED_SEEN", {})
    monkeypatch.setenv("ZERO_CREATED_GRACE_S", "180")

    # ONE observation reaps nothing: a container is `Created` for a moment during every
    # successful launch, and a sweep that raced one would kill a live session's own container.
    assert service._sweep_created(now=1000.0) == 0
    assert engine.removed == []
    assert service._sweep_created(now=1100.0) == 0, "still inside the grace"
    # Created and not started for longer than a launch is allowed to take (120s) => a leak.
    assert service._sweep_created(now=1200.0) == 2
    assert sorted(engine.removed) == ["cafe1", "cafe2"]
    # And the sweep asks for `status=created` only, which is why it is safe to run while serving:
    # a container a live session owns is RUNNING.
    listings = [c for c in engine.calls if "ps" in c]
    assert listings and all(any(f"status={s}" in c for s in ("created", "exited"))
                            for c in listings), "only states no live session can hold"
    assert any("status=created" in c for c in listings)
    assert any("status=exited" in c for c in listings)
    assert any("label=zero.session=1" in c for c in listings)
    assert any("name=zero-session-" in c for c in listings)
    # `running` must never be swept: that IS a live session's container.
    assert not any("status=running" in c for c in listings)


def test_an_exited_container_is_reaped_too(monkeypatch):
    """Sweeping only `created` left the other half of the leak in place.

    Measured on the production host 2026-09-09: 3538 container records -- 1156 `created` but also 1801
    `exited`. `db.sql` reached 73 MB, one podman process burned 894% CPU while the VM sat 95.3%
    idle, container create+destroy serialised the service to ~2.3 completions/s, and a read-only
    `podman ps -a` could not finish in 480 s. `--rm` collects an exited container in the normal
    case; what piles up is every case where it did not fire.
    """
    from sandbox import service

    engine = _Engine(["dead1", "dead2", "dead3"])
    monkeypatch.setattr(service.subprocess, "run", engine)
    monkeypatch.setattr(service, "runtime", lambda: "podman")
    monkeypatch.setattr(service, "_CREATED_SEEN", {})
    monkeypatch.setenv("ZERO_CREATED_GRACE_S", "180")

    # The same two-observation grace applies: an exited container is briefly visible before `--rm`
    # collects it, and sweeping inside that window would only race the engine's own teardown.
    assert service._sweep_created(now=1000.0) == 0
    assert engine.removed == []
    assert service._sweep_created(now=1200.0) == 3
    assert sorted(engine.removed) == ["dead1", "dead2", "dead3"]


def test_a_container_that_started_is_forgotten_rather_than_reaped(monkeypatch):
    """A container that leaves `Created` between two sweeps is a launch that SUCCEEDED. Its clock
    must reset, or a long-lived session whose id is later reused would be reaped on age alone."""
    from sandbox import service

    engine = _Engine(["cafe1"])
    monkeypatch.setattr(service.subprocess, "run", engine)
    monkeypatch.setattr(service, "runtime", lambda: "podman")
    monkeypatch.setattr(service, "_CREATED_SEEN", {})

    assert service._sweep_created(now=1000.0) == 0
    engine.created = []                                   # it started
    assert service._sweep_created(now=1010.0) == 0
    assert service._CREATED_SEEN == {}
    engine.created = ["cafe1"]                            # a NEW container, same id reused
    assert service._sweep_created(now=1020.0) == 0
    assert service._sweep_created(now=1100.0) == 0, "the clock restarted, so 1020 is the origin"
    assert engine.removed == []


def test_a_failed_launch_removes_its_own_container_and_reports_the_whole_error(monkeypatch):
    """The failing path must clean up after itself: what it leaks is what caused the failure, so
    a leak here is self-amplifying. And the error must survive intact -- the errno was cut off."""
    from sandbox import service

    enospc = (
        'time="2026-08-30T13:40:33Z" level=warning msg="container not cleaned up"\n'
        "Error: mounting storage for container 9f1c: creating overlay mount to "
        "/var/lib/containers/storage/vfs/dir/" + "a" * 64 + "/merged, mount_data=lowerdir=...: "
        "no space left on device")
    removed: list[str] = []

    def engine(argv, *args, **kwargs):
        if "rm" in argv:
            removed.extend(a for a in argv[argv.index("rm") + 1:] if a.startswith("zero-"))
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.CompletedProcess(argv, 125, "", enospc)

    session = service.LiveSession.__new__(service.LiveSession)
    session.engine, session.name = "podman", "zero-session-deadbeef"
    session.launched = session.spawned = False
    session.launch_argv = ["podman", "run", "--name", session.name]
    session.watchdog, session.image_ref = "sleep 1", "zero-runtime:abc"
    session.sub = resolve({"dockerfile": dynamic("launchfail")})   # `spec` is derived from it
    monkeypatch.setattr(service.subprocess, "run", engine)

    # Nothing submitted has run, so this is INFRASTRUCTURE: the gateway retries it on fresh
    # resources and the caller never sees it.
    with pytest.raises(service.InfraFailure) as failure:
        session._launch()
    assert failure.value.stage == "launch" and failure.value.started is False
    text = failure.value.detail
    assert text.startswith("container launch failed (rc=125): ")
    # The DECISIVE part. `detail[0][:240]` took the warning line and cut the path mid-hash, so
    # five failures on the production host read as a mystery path and the cause was found by hand instead.
    assert "no space left on device" in text
    assert removed == [session.name], "a launch that failed must remove its own container"
    # And it is owned even if the removal above is what fails: close() must still name it.
    assert session.spawned is True and session.launched is False


def test_close_reaps_a_container_whose_launch_failed(monkeypatch):
    from sandbox import service

    removed: list[str] = []

    def engine(argv, *args, **kwargs):
        if "rm" in argv:
            removed.extend(a for a in argv[argv.index("rm") + 1:] if a.startswith("zero-"))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(service.subprocess, "run", engine)
    session = service.LiveSession.__new__(service.LiveSession)
    session.engine, session.name = "podman", "zero-session-feed"
    session.closed, session.spawned, session.network_name = False, True, None
    session.work = session.control = None
    session.close()
    assert removed == ["zero-session-feed"]


def test_the_engine_error_is_reported_whole():
    from sandbox.service import _engine_error

    raw = 'level=warning msg="first"\nError: creating overlay mount: no space left on device'
    assert _engine_error(raw).endswith("no space left on device")
    assert "first" in _engine_error(raw)
    assert _engine_error("") == ""
    assert len(_engine_error("x" * 5000)) == 2000


def test_available_means_the_store_can_take_a_container(monkeypatch):
    """/health reported every runtime available while the engine behind it could not launch one.
    Liveness that does not exercise what it certifies is a local check standing in for a remote
    fact -- the exact failure this project already recorded on 2026-08-10."""
    from sandbox.service import _gateway_class

    monkeypatch.setenv("ZERO_STORE_MIN_FREE_GB", "5")
    gateway = _gateway_class({"CPU": 2.0}).__new__(_gateway_class({"CPU": 2.0}))
    gateway.known, gateway.caches, gateway.disk_facts, gateway.failures = {}, {}, {}, {}
    sub = resolve({"dockerfile": dynamic("store")})
    gateway.known[sub.digest] = sub
    monkeypatch.setattr("sandbox.service.runtime", lambda: "podman")

    gateway.store = {"known": True, "free_bytes": 200 * 2**30, "total_bytes": 400 * 2**30,
                     "root": "/var/lib/containers/storage", "reason": ""}
    assert gateway._facts(sub.digest, {})["available"] is True

    gateway.store = {"known": True, "free_bytes": 12 * 2**20, "total_bytes": 100 * 2**30,
                     "root": "/var/lib/containers/storage", "reason": ""}
    facts = gateway._facts(sub.digest, {})
    assert facts["available"] is False
    assert "store is full" in facts["why"] and "/var/lib/containers/storage" in facts["why"]

    # An engine that cannot report free space must not be reported as full either: unknown is
    # unknown, and it is named in the preflight rather than guessed at here.
    gateway.store = {"known": False, "free_bytes": 0, "total_bytes": 0, "root": "",
                     "reason": "engine does not report store free space"}
    assert gateway._facts(sub.digest, {})["available"] is True


def test_store_space_asks_the_engine_not_the_host(monkeypatch):
    """In remote mode the store is inside the VM: a host statvfs measures the wrong filesystem."""
    from sandbox import service

    seen: list[list[str]] = []

    def engine(argv, *args, **kwargs):
        seen.append(list(argv))
        return subprocess.CompletedProcess(
            argv, 0, "/var/lib/containers/storage\t107374182400\t107000000000", "")

    monkeypatch.setattr(service.subprocess, "run", engine)
    monkeypatch.setenv("ZERO_PODMAN_REMOTE", "1")
    facts = service.store_space("podman")
    assert facts["known"] and facts["total_bytes"] == 107374182400
    assert facts["free_bytes"] == 374182400
    assert "--remote" in seen[0] and "info" in seen[0]


def test_a_host_ram_image_store_is_refused_on_a_remote_engine(monkeypatch):
    """`_store_argv` returns the SAME remote argv for every store, so staging would save the image
    out of the VM and load it back into the same VM -- and then report a `ram` tier that is on the
    VM's disk. A phantom cache tier is a silent substitution; refuse the configuration."""
    from sandbox import service

    monkeypatch.setenv("ZERO_PODMAN_REMOTE", "1")
    monkeypatch.setenv("ZERO_RAM_IMAGE_STORE", "/dev/shm/zero-images-1970")
    with pytest.raises(RuntimeError, match="remote engine owns its storage"):
        service._preflight_store("podman")


def test_an_engine_too_busy_to_answer_is_not_an_engine_without_the_field(monkeypatch):
    """Measured under a 1500-request flood: `info` intermittently failed against the busy remote
    engine and the gate went quiet for three refresh cycles. It degrades safely -- unknown is
    never read as full -- but the two causes must not report as the same one."""
    from sandbox import service

    def busy(argv, *args, **kwargs):
        return subprocess.CompletedProcess(argv, 125, "", "Error: unable to connect to Podman "
                                           "socket: context deadline exceeded")

    monkeypatch.setattr(service.subprocess, "run", busy)
    facts = service.store_space("podman")
    assert facts["known"] is False
    assert "context deadline exceeded" in facts["reason"]

    def silent(argv, *args, **kwargs):
        return subprocess.CompletedProcess(argv, 0, "\t\t", "")

    monkeypatch.setattr(service.subprocess, "run", silent)
    assert service.store_space("podman")["reason"] == "engine does not report store free space"


def test_the_reaper_is_counted_where_an_operator_can_see_it():
    """The reap's only evidence was a per-worker file on node-local /tmp that dies with the job.

    Measured on the live service: the Serve ACCESS logger reaches the driver's log and four
    driver-side `[sandbox]` lines do, but zero prints from inside the replica ever did -- so the
    one mechanism standing between this service and the ENOSPC it was written for was invisible
    everywhere an operator actually looks. `/health` is driver-independent; the count goes there.
    """
    import inspect

    from sandbox import service

    source = inspect.getsource(service._gateway_class)
    assert "self.reaped += await asyncio.to_thread(_sweep_created)" in source, \
        "the sweep's return value must be accumulated, not discarded"
    assert '"reaped_created": self.reaped' in source, "/health must report it"


def test_a_transient_engine_failure_does_not_permanently_disable_a_worker(monkeypatch):
    """`@lru_cache` cached the FAILURE as eagerly as the answer, so one refused handshake under
    load convinced a worker process there was no engine -- for the rest of the run, with nothing
    to re-probe it. Measured on a cold 1500-request flood at concurrency 48: 438 of 1500 replies
    were `no working Podman/Docker runtime` from a VM that was healthy throughout."""
    from sandbox import service

    service.runtime.cache_clear()
    calls = {"n": 0}

    def flaky(argv, *args, **kwargs):
        calls["n"] += 1
        # The first probe is refused the way sshd refuses past MaxStartups; the next succeeds.
        if calls["n"] == 1:
            return subprocess.CompletedProcess(argv, 255, "", "ssh: handshake failed")
        return subprocess.CompletedProcess(argv, 0, "5.8.4\n", "")

    monkeypatch.setenv("ZERO_CONTAINER_RUNTIME", "podman")
    monkeypatch.setattr(service.shutil, "which", lambda name: "/usr/bin/" + name)
    monkeypatch.setattr(service.subprocess, "run", flaky)
    # No negative TTL here (that has its own test): the case is that a failure is not cached.
    monkeypatch.setattr(service, "_REPROBE_AFTER_S", 0.0)

    assert service.runtime() is None, "the refused probe must report no engine"
    assert service.runtime() == "podman", "and the NEXT call must re-probe, not serve a cached None"
    assert service.runtime() == "podman", "a success is still memoised"
    assert calls["n"] == 2, "the memo must stop the third probe"
    service.runtime.cache_clear()


def test_engine_cpus_also_re_probes_after_a_failure(monkeypatch):
    from sandbox import service

    service.engine_cpus.cache_clear()
    monkeypatch.setattr(service, "runtime", lambda: "podman")
    calls = {"n": 0}

    def flaky(argv, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] <= 2:                      # both template forms fail on the first probe
            return subprocess.CompletedProcess(argv, 255, "", "ssh: handshake failed")
        return subprocess.CompletedProcess(argv, 0, "224\n", "")

    monkeypatch.setattr(service.subprocess, "run", flaky)
    monkeypatch.setattr(service, "_REPROBE_AFTER_S", 0.0)
    assert service.engine_cpus() == 0
    assert service.engine_cpus() == 224, "a zero must not be cached as the answer"
    service.engine_cpus.cache_clear()


def test_the_remote_watchdog_writes_a_deadline_its_own_read_can_consume():
    """`read` returns NON-ZERO at EOF when the line has no delimiter, so `printf %s` paired with
    `read d ... || exit` exited on the FIRST iteration and every remote session self-destructed
    about a second after launch, whatever its deadline. Only requests whose `exec` landed inside
    that window succeeded; the rest returned `cannot exec in a container that is not running`,
    which reads as a transport or capacity fault -- and was chased as one.

    Measured on the live VM with a deadline an hour ahead: `printf %s` dead by 2s,
    `printf '%s\\n'` alive at 2s, 4s and 8s. The LOCAL path always wrote `str(ts) + "\\n"`, so this
    arrived with the remote port and nothing compared the two.
    """
    import inspect
    import re

    from sandbox import service

    source = inspect.getsource(service.LiveSession)
    seeds = [line for line in source.splitlines() if "> /tmp/zero-deadline" in line]
    assert seeds, "the remote watchdog must still seed its deadline file"
    for line in seeds:
        assert re.search(r"printf\s+'%s\\+n'", line), (
            "every write to the deadline file needs a trailing newline or `read` reports EOF "
            f"and the watchdog exits: {line.strip()}")
    # And the local path, which was always correct, must stay that way.
    assert 'str(ts) + "\\n"' in inspect.getsource(service.LiveSession._deadline)


def test_the_disk_tier_is_trimmed_by_BYTES_not_only_by_image_count(monkeypatch):
    """`ZERO_DISK_IMAGE_CACHE` keeps N images whatever they weigh -- 64 at 8 GB is 512 GB. On a
    remote engine the store is a FIXED VM disk, so that overruns into a DEAD VM, not a full
    directory: measured 2026-08-30, a 24-Dockerfile flood grew the qcow2 to 140 of its 144 GiB and
    the VM died mid-run, surfacing as `session actor failed` plus a store probe that could not
    connect."""
    import inspect

    from sandbox import service

    source = inspect.getsource(service._gateway_class)
    assert "store_target_free" in source, "the trim must consult measured free space"
    assert "len(images) // 2" in source, "and trim harder than the count when the store is low"

    total = 144 * 2**30
    monkeypatch.setenv("ZERO_STORE_TARGET_FREE_FRAC", "0.30")
    assert service.store_target_free(total) == int(total * 0.30)
    # The target can never fall below the floor a single launch needs.
    monkeypatch.setenv("ZERO_STORE_TARGET_FREE_FRAC", "0.0")
    assert service.store_target_free(total) == service.store_min_free()


def test_a_launch_that_times_out_is_attributed_and_does_not_kill_the_actor(monkeypatch):
    """A slow `podman run` must be an `unavailable:` string, not a Python traceback.

    Measured 2026-09-09 against a downstream workload: `podman run` serialises on container creation, so
    at ~94 live sessions it exceeded the hard-coded 120s and `subprocess.TimeoutExpired`
    propagated out of `_SessionActor.execute`. The client saw
    `unavailable:session actor failed: ray::_SessionActor.execute() ... TimeoutExpired` -- a
    traceback rather than one of the error-string prefixes that ARE the HTTP contract -- so a slow
    launch was indistinguishable from a broken service and could not be attributed. 24 of 64
    graded rows were lost that way.
    """
    from sandbox import service

    removed: list[str] = []

    def engine(argv, *args, **kwargs):
        if "rm" in argv:
            removed.extend(a for a in argv[argv.index("rm") + 1:] if a.startswith("zero-"))
            return subprocess.CompletedProcess(argv, 0, "", "")
        raise subprocess.TimeoutExpired(argv, kwargs.get("timeout", 0))

    session = service.LiveSession.__new__(service.LiveSession)
    session.engine, session.name = "podman", "zero-session-slowstart"
    session.launched = session.spawned = False
    session.launch_argv = ["podman", "run", "--name", session.name]
    session.watchdog, session.image_ref = "sleep 1", "zero-runtime:abc"
    session.sub = resolve({"dockerfile": dynamic("launchslow")})
    monkeypatch.setenv("ZERO_LAUNCH_TIMEOUT_S", "7")
    monkeypatch.setattr(service.subprocess, "run", engine)

    with pytest.raises(service.InfraFailure) as failure:
        session._launch()
    assert failure.value.stage == "launch" and failure.value.started is False
    text = failure.value.detail
    assert "7s" in text, "say the budget that was exceeded, so it can be raised"
    # Same reason as the failed-launch case: `run` CREATES then starts, so a timed-out launch has
    # already allocated a read-write layer that nothing else reaps.
    assert removed == [session.name], "a launch that timed out must remove its own container"
    assert session.spawned is True and session.launched is False


def test_the_launch_budget_is_configurable(monkeypatch):
    """It was hard-coded at 120s, which is the cliff the arm kept hitting."""
    from sandbox import service

    seen: dict = {}

    def engine(argv, *args, **kwargs):
        seen["timeout"] = kwargs.get("timeout")
        return subprocess.CompletedProcess(argv, 0, "", "")

    session = service.LiveSession.__new__(service.LiveSession)
    session.engine, session.name = "podman", "zero-session-budget"
    session.launched = session.spawned = False
    session.launch_argv = ["podman", "run", "--name", session.name]
    session.watchdog, session.image_ref = "sleep 1", "zero-runtime:abc"
    session.sub = resolve({"dockerfile": dynamic("launchbudget")})
    monkeypatch.setenv("ZERO_LAUNCH_TIMEOUT_S", "600")
    monkeypatch.setattr(service.subprocess, "run", engine)

    session._launch()
    assert seen["timeout"] == 600
    assert session.launched is True
