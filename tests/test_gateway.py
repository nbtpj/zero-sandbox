"""The gateway's own bookkeeping, driven through the real class.

`build_app` is what binds this class to Ray Serve; the class itself is constructible, so these
cases exercise admission, discovery and the lifetime bounds with no cluster behind them.
"""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import threading
import time

import pytest

from conftest import RESOURCES, dynamic


def test_cpu_admission_is_weighted_and_fifo():
    import asyncio
    from sandbox.service import _Capacity, _LocalCapacity

    inf = float("inf")

    async def check():
        capacity = _Capacity((3, inf, inf))
        await capacity.acquire((2, 0, 1))
        large = asyncio.create_task(capacity.acquire((2, 0, 1)))
        await asyncio.sleep(0)
        small = asyncio.create_task(capacity.acquire((1, 0, 1)))
        await asyncio.sleep(0)
        assert not large.done() and not small.done()
        capacity.release((2, 0, 1))
        await asyncio.gather(large, small)
        assert capacity.free[0] == 0
        capacity.release((3, 0, 3))
        assert capacity.free == (3, inf, inf)

    asyncio.run(check())
    with pytest.raises(ValueError, match="requested 3 CPUs; server grants at most 2"):
        _LocalCapacity((2, inf, inf)).acquire((3, 0, 1))

    async def impossible():
        with pytest.raises(ValueError, match="requested 9 GB of memory; server grants at most 8"):
            await _Capacity((64, 8, inf)).acquire((1, 9, 1))

    asyncio.run(impossible())

def test_each_admission_axis_binds_in_its_own_unit():
    """The the production host wedge of 2026-09-12: memory gave 19.2 SESSION slots, and admission spent them in
    CPUs, so five 4-CPU lean/cpp checks filled a 240-vCPU VM whose containers used 2-20 MB each.
    A 4-CPU, 2.5 GB session must cost four CPUs and 2.5 GB -- not four sessions' memory."""
    from types import SimpleNamespace
    from sandbox import service
    from sandbox.service import _Capacity, _grant

    lean = SimpleNamespace(cpus=4, memory_gb=2, workdir_gb=0.5, workdir_medium="temp", gpus=0)
    remote = service._is_remote
    try:
        service._is_remote = lambda: True
        assert _grant(lean) == (4.0, 2.5, 1.0)    # /work is charged to --memory on a VM
        service._is_remote = lambda: False
        assert _grant(lean) == (4.0, 2.0, 1.0)    # ...and only when RAM-backed locally
    finally:
        service._is_remote = remote

    async def one_large_host():
        # 240 vCPUs, 96 GB x 0.8 of engine memory, a host that holds ~132 Ray workers.
        capacity = _Capacity((240, 76.8, 132))
        held = [asyncio.create_task(capacity.acquire((4, 2.5, 1))) for _ in range(30)]
        await asyncio.sleep(0)
        assert all(task.done() for task in held)         # 30, not the 4 a CPU count of 19.2 allowed
        # Memory is the axis that binds next: 30 x 2.5 = 75 of 76.8 GB, so a 3 GB session waits
        # although 120 CPUs are free, and a release that frees memory is what admits it.
        waiter = asyncio.create_task(capacity.acquire((1, 3, 1)))
        await asyncio.sleep(0)
        assert not waiter.done() and capacity.free[0] == 120
        capacity.release((4, 2.5, 1))
        await asyncio.sleep(0)
        assert waiter.done()

    asyncio.run(one_large_host())

def test_the_declaration_registry_is_bounded(monkeypatch):
    """Each entry is a whole Dockerfile; a long-lived gateway must not keep every one."""
    source = inspect.getsource(__import__("sandbox.service", fromlist=["x"]))
    assert "ZERO_KNOWN_LIMIT" in source and "self.known_limit" in source

def _get(path: str, headers: dict | None = None):
    """The attributes the gateway reads off a Starlette request (headers only under a token)."""
    return type("Request", (), {"url": type("U", (), {"path": path})(), "method": "GET",
                                "headers": headers or {}})()

def _remote(result):
    class Remote:
        def remote(self, *args, **kwargs):
            async def done():
                return result
            return done()
    return Remote()

def _stub_new(gateway, service, *, alive=False, active=1):
    """Replace actor creation: these cases are about the gateway's own bookkeeping."""

    async def fake_new(sub, request, lock, canary=False):
        key = (sub.digest, "")
        done = asyncio.get_running_loop().create_future()
        done.set_result(None)
        gateway.caches[key] = asyncio.ensure_future(done)
        gateway.cache_info[key] = {"image": service._tag(sub.digest), "active": active,
                                   "ram_bytes": 0, "bytes": 1, "node_id": "n",
                                   "actor": sub.digest[:8], "lease": None,
                                   "last_used": time.monotonic()}
        entry = service._RemoteEntry(
            sub.digest, None, type("A", (), {
                "execute": _remote({"result": {"alive": alive or bool(request["wait_s"])}}),
                "close": _remote(None)})(), lock, ("w", sub.digest),
            service._grant(request["resources"]), request["config_hash"], key)
        gateway.sessions[request["session_id"]] = entry
        return entry

    gateway._new = fake_new

def _action(name: str, index: int, **request) -> dict:
    return {"runtime": {"dockerfile": dynamic(f"{name}{index}")}, "session_id": f"s{index}",
            "code": "true", "cwd": "/work", "timeout_s": 5, "wait_s": 0, "io_cap_kb": 200,
            "resources": RESOURCES, **request}

def _gateway(monkeypatch, tmp_path, **cluster):
    """A gateway with no Serve, no Ray actors and no container engine behind it."""
    from sandbox import service

    monkeypatch.delenv("ZERO_SANDBOX_TOKEN", raising=False)   # these speak to the class directly
    monkeypatch.setattr(service, "_LOCAL_ROOT", str(tmp_path))
    monkeypatch.setattr(service, "runtime", lambda: "podman")
    monkeypatch.setenv("ZERO_WORKER_CACHE_S", "3600")
    return service._gateway_class(cluster or {"CPU": 4})()

def _drain(gateway):
    for task in getattr(gateway, "tasks", []):
        task.cancel()
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()

def test_an_unauthenticated_health_says_only_that_the_service_is_up(tmp_path, monkeypatch):
    """/health is open for liveness probes, but its full report is /runtimes (which is gated)
    plus build-failure text and the store's host path. Without the token it must say only `ok`."""
    from sandbox import service

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            _drain(gateway)
            gateway.auth_token = "s3cret"
            sub = service.resolve({"dockerfile": dynamic("gated"), "documentation": "private"})
            gateway.known[sub.key] = sub
            gateway.failures[sub.key] = "build failed: private detail"

            async def body(path, headers=None):
                reply = await gateway(_get(path, headers))
                return reply.status_code, json.loads(bytes(reply.body))

            assert await body("/health") == (200, {"ok": True})
            assert await body("/health", {"authorization": "Bearer wrong"}) == (200, {"ok": True})
            assert (await body("/runtimes"))[0] == 403
            status, full = await body("/health", {"authorization": "Bearer s3cret"})
            assert status == 200 and full["runtimes"][sub.key]["documentation"] == "private"
            assert full["failures"][sub.key] == "build failed: private detail"
        finally:
            _drain(gateway)

    asyncio.run(check())


def test_discovery_is_served_from_gateway_facts_not_the_engine(tmp_path, monkeypatch):
    """One replica runs every execution on one event loop: a synchronous `images` call inside
    /health stalls all of them. Measured 4-8 s idle on the four-CPU acceptance host, and past a
    10 s client deadline under an execution flood."""
    from sandbox import service

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            _drain(gateway)
            sub = service.resolve({"dockerfile": dynamic("cold"), "documentation": "doc"})
            gateway.known[sub.key] = sub
            service._forget()  # an expired image table: the case that has to shell out
            monkeypatch.setattr(service.subprocess, "run", lambda *a, **k: pytest.fail(
                "discovery reached the container engine on the request path"))
            for route in ("/health", "/runtimes"):
                body = json.loads(bytes((await gateway(_get(route))).body))
                assert body["runtimes"][sub.key]["build"] == "cold", body
                assert body["runtimes"][sub.key]["tier"] == "none"
                assert body["runtimes"][sub.key]["documentation"] == "doc"
            # What the background pass finds is what discovery then reports.
            monkeypatch.setattr(service, "_cache_state", lambda digest, image=None: {
                "image": "zero-runtime:staged", "tier": "ram", "bytes": 7})
            await gateway._refresh()
            named = json.loads(bytes((await gateway(_get("/health"))).body))["runtimes"][sub.key]
            assert (named["tier"], named["bytes"], named["build"]) == ("ram", 7, "cached")
            # A declaration the gateway forgot leaves no fact behind for the next one to inherit.
            gateway.known.clear()
            await gateway._refresh()
            assert gateway.disk_facts == {}
        finally:
            _drain(gateway)

    asyncio.run(check())

def test_gateway_bounds_hold_over_its_lifetime(tmp_path, monkeypatch):
    """Both bounds used to be defeated by the gateway's own bookkeeping: a cached declaration
    stopped the `known` trim outright, and a cache record protected its image for ever. Releasing
    a record is driven by the tier being full, never by elapsed time."""
    from sandbox import service

    monkeypatch.setenv("ZERO_KNOWN_LIMIT", "3")
    removed, killed = [], []
    monkeypatch.setattr(service, "_remove_images",
                        lambda engine, images: removed.extend(images))

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        monkeypatch.setattr(service, "_kill", killed.append)
        try:
            _drain(gateway)
            _stub_new(gateway, service)
            digests = []
            for index in range(7):
                body = _action("bound", index, wait_s=60 if index == 6 else 0)
                digests.append(hashlib.sha256(
                    body["runtime"]["dockerfile"].encode()).hexdigest())
                status, _reply = await gateway._execute(body)
                assert status == 200
                # The invariant is per request, not eventual.
                assert len(gateway.known) <= 3, len(gateway.known)

            # Six stateless actions released their sessions; the seventh is still live.
            assert list(gateway.sessions) == ["s6"]
            assert gateway.cache_info[(digests[6], "")]["active"] == 1
            assert all(gateway.cache_info[(d, "")]["active"] == 0 for d in digests[:6])

            images = [(service._tag(d), float(i)) for i, d in enumerate(digests)]
            monkeypatch.setattr(service, "_owned_images", lambda engine: images)

            # NOTHING is dropped while the tier has room, however long a record has sat idle:
            # serving is time-sensitive and a rebuild is the only thing an early drop buys.
            monkeypatch.setenv("ZERO_DISK_IMAGE_CACHE", "64")
            assert await gateway._trim_disk() is False
            assert len(gateway.caches) == 7 and not removed and not killed

            monkeypatch.setenv("ZERO_DISK_IMAGE_CACHE", "2")
            assert await gateway._trim_disk() is True
            # keep=2 of seven owned images, and the live one survives whatever its age.
            assert removed == [service._tag(d) for d in digests[:5]]
            assert service._tag(digests[6]) not in removed
            # A record may never outlive its image: it would hand the next session a
            # `cached_image` the engine no longer has.
            assert set(gateway.caches) == {(digests[5], ""), (digests[6], "")}
            assert killed == [d[:8] for d in digests[:5]]
            assert not {info["image"] for info in gateway.cache_info.values()} & set(removed)
        finally:
            _drain(gateway)

    asyncio.run(check())

def test_a_client_that_leaves_during_the_close_does_not_keep_its_grant(tmp_path, monkeypatch):
    """A stateless action ends in `_drop`, which awaits the actor's `close` for up to 15 s. A
    client timing out inside that window cancels the drop with CancelledError, which `except
    Exception` does not catch; the session was already popped, so its CPU grant was never
    released. Measured on the production host 2026-09-12: admission is 19.2 slots, each leak lost one for good,
    and once the head of the queue could not fit every request waited out its 900 s client
    deadline with zero sessions, zero containers and an idle Ray cluster."""
    from sandbox import service

    killed = []

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        monkeypatch.setattr(service, "_kill", killed.append)
        closing = asyncio.Event()

        class Close:
            def remote(self):
                async def hang():
                    closing.set()
                    await asyncio.Event().wait()
                return hang()

        async def fake_new(sub, request, lock, canary=False):
            units = service._grant(request["resources"])
            token = await gateway._acquire(units)
            actor = type("A", (), {"execute": _remote({"result": {"alive": False}}),
                                   "close": Close()})()
            entry = service._RemoteEntry(
                sub.digest, None, actor, lock, ("w", sub.digest), units,
                request["config_hash"], (sub.digest, ""), grant=token)
            gateway.sessions[request["session_id"]] = entry
            return entry

        try:
            _drain(gateway)
            gateway._new = fake_new
            action = asyncio.create_task(gateway._execute(_action("leave", 0)))
            await asyncio.wait_for(closing.wait(), 5)
            assert gateway.capacity.free < gateway.capacity.total
            action.cancel()
            with pytest.raises(asyncio.CancelledError):
                await action
            assert gateway.capacity.free == gateway.capacity.total
            assert gateway.sessions == {} and len(killed) == 1
        finally:
            _drain(gateway)

    asyncio.run(check())

def test_a_declaration_is_persisted_off_the_request_path(tmp_path, monkeypatch):
    """A 256 KiB declaration written through the event loop stalls every execution the single
    replica is serving, and no reply depends on that write."""
    from sandbox import service

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            _drain(gateway)
            _stub_new(gateway, service, active=0)
            wrote, release, started = [], threading.Event(), threading.Event()

            def slow(sub):
                wrote.append(threading.current_thread())
                started.set()
                release.wait(10)

            monkeypatch.setattr(service, "remember", slow)
            await gateway._execute(_action("persist", 0))
            # The reply is back while the write is still in flight.
            assert gateway.writes and not any(task.done() for task in gateway.writes)
            # A created task has not necessarily started, and `to_thread` needs a further hop, so
            # WAIT for the write to actually begin instead of assuming it already has. Awaiting
            # here yields the loop, which is exactly what lets the write task dispatch.
            await asyncio.wait_for(asyncio.to_thread(started.wait, 5), timeout=6)
            assert wrote and wrote[0] is not threading.current_thread()
            release.set()
            await asyncio.gather(*gateway.writes)
            # The same declaration is not rewritten; a changed documentation string is.
            await gateway._execute(_action("persist", 0))
            await asyncio.gather(*gateway.writes)
            assert len(wrote) == 1
            body = _action("persist", 0)
            body["runtime"]["documentation"] = "changed"
            await gateway._execute(body)
            await asyncio.gather(*gateway.writes)
            assert len(wrote) == 2
        finally:
            _drain(gateway)

    asyncio.run(check())

def test_the_declaration_map_is_bounded_at_every_limit(tmp_path, monkeypatch):
    """`ZERO_KNOWN_LIMIT` of 0 and 1 are the cases the old trim could not reach at all."""
    from sandbox import service

    async def check(limit: int):
        monkeypatch.setenv("ZERO_KNOWN_LIMIT", str(limit))
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            _drain(gateway)
            _stub_new(gateway, service, active=0)
            for index in range(4):
                gateway.failures[f"stale{index}"] = "x"
                await gateway._execute(_action("limit", index))
                assert len(gateway.known) <= limit
            # Failure text is keyed by declaration, so it is bounded by the same trim.
            assert set(gateway.failures) <= set(gateway.known)
        finally:
            _drain(gateway)

    for limit in (0, 1, 2):
        asyncio.run(check(limit))


def _done(value=None):
    future = asyncio.get_running_loop().create_future()
    future.set_result(value)
    return asyncio.ensure_future(future)


def test_a_demanded_image_is_never_trimmed_from_under_its_launch(tmp_path, monkeypatch):
    """A cache record is published with `active=0`, and only `_new` makes it active. A trim
    landing in that gap evicted the image a launch was about to use; the client saw
    `unavailable:container launch failed: Unable to find image ... locally`, intermittently."""
    from sandbox import service

    removed = []
    monkeypatch.setattr(service, "_remove_images", lambda engine, images: removed.extend(images))
    monkeypatch.setattr(service, "_kill", lambda actor: None)
    monkeypatch.setenv("ZERO_DISK_IMAGE_CACHE", "1")

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            _drain(gateway)
            sub = service.resolve({"dockerfile": dynamic("gap")})
            key, tag = (sub.digest, ""), service._tag(sub.digest)
            published, hold = asyncio.Event(), asyncio.Event()

            async def cached(_sub, _image, cache_key):
                # exactly what `_cache` publishes once the build is done and before it returns
                gateway.caches[cache_key] = _done()
                gateway.cache_info[cache_key] = {"image": tag, "active": 0, "ram_bytes": 0,
                                                 "bytes": 1, "node_id": "n", "actor": "a",
                                                 "lease": None, "last_used": time.monotonic()}
                published.set()
                await hold.wait()
                return gateway.cache_info[cache_key]

            monkeypatch.setattr(gateway, "_cached", cached)
            demand = asyncio.create_task(gateway._cache(sub, None))
            await published.wait()

            older = [("zero-runtime:" + "0" * 16, 1.0), ("zero-runtime:" + "1" * 16, 2.0)]
            monkeypatch.setattr(service, "_owned_images", lambda engine: older + [(tag, 3.0)])
            assert gateway.demand[key] == 1
            assert tag in gateway._pinned()
            assert await gateway._trim_disk() is True
            assert tag not in removed and removed == [n for n, _ in older]
            assert key in gateway.caches, "the record of a demanded image was released"

            hold.set()
            await demand
            assert key not in gateway.demand
        finally:
            _drain(gateway)

    asyncio.run(check())


def test_a_demand_waits_for_an_eviction_already_removing_its_image(tmp_path, monkeypatch):
    """Between releasing a record and the engine actually deleting the image, a fresh cache actor
    would list the doomed image and hand it back unbuilt. The demand waits instead."""
    from sandbox import service

    async def check():
        gateway = _gateway(monkeypatch, tmp_path)
        try:
            _drain(gateway)
            sub = service.resolve({"dockerfile": dynamic("racing")})
            tag = service._tag(sub.digest)
            built = []

            async def cached(_sub, _image, cache_key):
                built.append(cache_key)
                return {"image": tag, "active": 0, "ram_bytes": 0, "bytes": 1, "node_id": "n",
                        "actor": "a", "lease": None, "last_used": time.monotonic()}

            monkeypatch.setattr(gateway, "_cached", cached)
            done = asyncio.Event()
            gateway.evicting[tag] = done
            demand = asyncio.create_task(gateway._cache(sub, None))
            await asyncio.sleep(0)
            assert not demand.done() and not built, "the demand adopted an image being deleted"
            done.set()
            await demand
            assert built == [(sub.digest, "")]
        finally:
            _drain(gateway)

    asyncio.run(check())


def test_a_pooled_worker_is_not_bound_to_one_runtime():
    """`open` re-parameterizes the actor, so only the Ray options fixed at creation belong in the
    pool key. Keying it by digest made every new runtime pay for a fresh Python process: measured
    p50 1636 ms against 212 ms for a pooled one, on the same warm image."""
    from sandbox import service

    source = inspect.getsource(service._gateway_class)
    assignment = [line for line in source.splitlines() if "worker_key = (" in line]
    assert len(assignment) == 1, assignment
    assert "digest" not in assignment[0] and "image" not in assignment[0], assignment[0]
    assert "node_id" in assignment[0] and "resources" in assignment[0], assignment[0]
    # What the actor was created with, and therefore what a reused one must still satisfy.
    assert '"num_cpus": resources.cpus' in source and "NodeAffinitySchedulingStrategy" in source
    # A reused actor must be handed a whole new session, never left holding the previous one.
    assert "self.close()" in inspect.getsource(service._SessionActor.open)


def test_a_missing_image_is_never_a_reason_to_reach_a_registry():
    """`docker run` defaults to --pull=missing, which turns a local fault into a pull of a name
    the request supplied."""
    from sandbox import service

    assert '"--pull=never"' in inspect.getsource(service.LiveSession.__post_init__)


def test_concurrent_builds_are_bounded_by_what_a_build_may_consume(tmp_path, monkeypatch):
    """A cache actor costs Ray no CPU, so nothing stopped N cold runtimes from starting N
    `docker build`s at once, each allowed ZERO_BUILD_CPUS. On the four-CPU acceptance host four
    at once took /health from 35 ms to 3.0 s: the replica stopped being scheduled."""
    from sandbox import service

    monkeypatch.setenv("ZERO_BUILD_CPUS", "2")
    # The builder's own CPU count wins over Ray's: builds run on the engine's machine, and behind
    # a VM or a remote daemon that is not the machine Ray measured. An 8-CPU host driving a
    # 4-CPU VM ran four builds at once and took /health from 24 ms to 3.0 s.
    monkeypatch.setattr(service, "engine_cpus", lambda: 8)

    async def check():
        gateway = _gateway(monkeypatch, tmp_path, CPU=64)
        try:
            _drain(gateway)
            assert gateway.build_slots == 4, gateway.build_slots
            live, peak, release = 0, 0, asyncio.Event()

            class Actor:
                class ensure:
                    @staticmethod
                    def remote(sub, image):
                        async def build():
                            nonlocal live, peak
                            live += 1
                            peak = max(peak, live)
                            await release.wait()
                            live -= 1
                            return {"image": "i"}
                        return build()

            sub = service.resolve({"dockerfile": dynamic("slots")})
            builds = [asyncio.create_task(gateway._ensure(Actor(), sub, None)) for _ in range(10)]
            await asyncio.sleep(0)
            assert peak == 4 and live == 4, (peak, live)
            release.set()
            await asyncio.gather(*builds)
            assert peak == 4, "more builds ran at once than a slot allows"
        finally:
            _drain(gateway)

    asyncio.run(check())
    # An explicit slot count wins over the derived one.
    monkeypatch.setenv("ZERO_BUILD_SLOTS", "1")

    async def explicit():
        monkeypatch.setattr(service, "engine_cpus", lambda: 64)
        gateway = _gateway(monkeypatch, tmp_path, CPU=64)
        try:
            assert gateway.build_slots == 1
        finally:
            _drain(gateway)

    asyncio.run(explicit())


def test_engine_cpus_reads_both_docker_and_podman(monkeypatch):
    """Build-slot sizing must work on the production engine. docker prints `.NCPU`, podman prints
    `.Host.CPUs` (capitalised); querying the wrong field is a template error, so both are tried."""
    from sandbox import service

    service.engine_cpus.cache_clear()
    monkeypatch.setattr(service, "runtime", lambda: "podman")

    def podman_info(argv, **kw):
        fmt = argv[argv.index("--format") + 1]
        if fmt == "{{.NCPU}}":      # docker's field: podman errors
            return type("R", (), {"returncode": 125, "stdout": "", "stderr": "no field"})()
        if fmt == "{{.Host.CPUs}}":
            return type("R", (), {"returncode": 0, "stdout": "4\n"})()
        return type("R", (), {"returncode": 1, "stdout": ""})()

    monkeypatch.setattr(service.subprocess, "run", podman_info)
    assert service.engine_cpus() == 4
    service.engine_cpus.cache_clear()

    monkeypatch.setattr(service.subprocess, "run",
                        lambda argv, **kw: type("R", (), {"returncode": 0, "stdout": "8\n"})()
                        if "{{.NCPU}}" in argv else type("R", (), {"returncode": 1, "stdout": ""})())
    assert service.engine_cpus() == 8   # docker path
    service.engine_cpus.cache_clear()

