"""Raw, ordered command sessions in Docker-declared runtimes."""
from __future__ import annotations

import argparse
import asyncio
import functools
import hashlib
import ipaddress
import json
import os
import pathlib
import posixpath
import re
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from dataclasses import dataclass, field

from sandbox.runtime import Resources, RuntimeSpec, Submitted, resolve


_IMAGE_PREFIX = "zero-runtime"
_OWNED_PREFIXES = (_IMAGE_PREFIX, "zero-egress")
REQUIRE_GVISOR_ENV = "ZERO_REQUIRE_GVISOR"
_SESSION_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
_ENV_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
MAX_COMMAND_SECONDS = float(os.environ.get("ZERO_MAX_COMMAND_SECONDS", "28800"))
MAX_IDLE_SECONDS = float(os.environ.get("ZERO_MAX_IDLE_SECONDS", "3600"))
MAX_IO_CAP_KB = int(os.environ.get("ZERO_MAX_IO_CAP_KB", "1024"))
#: Request errors: the REQUEST is wrong, so running it again cannot help and is never tried.
_REQUEST_ERRORS = ("forbidden:", "not_found:", "conflict:", "bad_request:")


# THE SERVICE MUST NOT FAIL; IT CAN ONLY BE SHUT DOWN (user ruling 2026-10-15). For a valid request
# it returns the correct execution result, or it is not reachable at all. An infrastructure failure
# is therefore never an answer: it is retried inside the service on fresh resources, and a failure
# that outlives the retry bound -- with the self-check failing too -- takes the service DOWN.
#
# THE ONE INVARIANT THAT MAKES RETRYING SAFE: a retry happens only when NO RESULT EXISTS. A result
# exists exactly when the submitted command ran under its own lifecycle and its output was captured
# whole -- including when it timed out, crashed, was OOM-killed inside its own grant, or printed
# nothing: those are results and are returned once, never re-run (re-running them is
# retry-until-lucky). No result exists when the infrastructure failed: the container did not
# launch, the command never STARTED (proved by the start marker below, which the wrapper writes
# before the submitted code can run), the container died under a still-running command (measured
# 2026-10-15: no signal, OOM, fork bomb or tmpfs fill from inside kills the container's PID 1, so
# this is never the candidate's doing), the output could not be captured, or the session worker
# itself died. Every one of those is decided by something the submitted code cannot touch.
class InfraFailure(Exception):
    """The infrastructure failed before a result existed. ``started`` is True when the command had
    started (its partial effects are discarded with the container), False when it provably never
    ran, None when unknown (the worker died)."""

    def __init__(self, stage: str, detail: str, started: bool | None = False,
                 final: bool = False):
        super().__init__(stage, detail, started, final)
        self.stage, self.detail, self.started, self.final = stage, detail, started, final

    def __str__(self) -> str:
        return f"{self.stage}: {self.detail}"

    def wire(self) -> dict:
        return {"stage": self.stage, "detail": self.detail, "started": self.started,
                "final": self.final}


_INCIDENT_LOCK = threading.Lock()


def _incident_path() -> str:
    return os.environ.get("ZERO_INCIDENT_LOG") or os.path.join(
        os.environ.get("ZERO_LOCAL_ROOT") or os.path.join(
            os.environ.get("SLURM_TMPDIR", "/tmp"), f"zero-sandbox-{os.getuid()}"),
        "incidents.jsonl")


def incident(kind: str, **fields) -> None:
    """Append one timestamped JSON line to the persistent incident log, from ANY process.

    Today's `unavailable:session actor failed` (2026-10-15) was undiagnosable on the server: the
    real exception lived only in a Ray worker log that was recycled before anyone looked, and the
    gateway threw it away by formatting the remote traceback into one line. Every internal failure
    -- what failed, the real exception and traceback, which attempt, what was done about it -- is
    written here instead, by the process that saw it, with O_APPEND so lines from the gateway and
    every session worker interleave whole. It is also printed, which reaches the driver's log.
    Point ``ZERO_INCIDENT_LOG`` at durable storage to keep it past the node's /tmp.
    """
    record = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "epoch": round(time.time(), 3),
              "kind": kind, "pid": os.getpid()}
    for key, value in fields.items():
        text = value if isinstance(value, (int, float, bool, type(None))) else str(value)
        record[key] = text[:20000] if isinstance(text, str) else text
    line = json.dumps(record, default=str) + "\n"
    try:
        path = _incident_path()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        limit = int(float(os.environ.get("ZERO_INCIDENT_LOG_MAX_MB", "64")) * 2**20)
        with _INCIDENT_LOCK:
            try:
                if os.path.getsize(path) > limit:
                    os.replace(path, path + ".1")
            except OSError:
                pass
            fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
            try:
                os.write(fd, line.encode("utf-8", "replace"))
            finally:
                os.close(fd)
    except OSError:
        pass
    print(f"[sandbox:incident] {line[:600].rstrip()}", flush=True)


def root() -> pathlib.Path:
    local = (os.environ.get("ZERO_LOCAL_ROOT") or
             os.path.join(os.environ.get("SLURM_TMPDIR", "/tmp"),
                          f"zero-sandbox-{os.getuid()}"))
    path = pathlib.Path(os.environ.get("ZERO_SANDBOX_TMP") or os.path.join(local, "work"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def install() -> pathlib.Path:
    path = root()
    for name in ("TMPDIR", "TEMP", "TMP"):
        os.environ[name] = str(path)
    sweep()
    return path


def _tree_usage(path) -> tuple[int, int]:
    """Bytes AND inodes under ``path`` in one walk. A byte quota is blind to a million EMPTY
    files, and empty files still exhaust the filesystem's inode table -- which fails every write
    on the node, the service included -- and make the walk itself O(inodes)."""
    total = files = dirs = 0
    for base, subdirs, names in os.walk(path):
        dirs += len(subdirs)
        for name in names:
            files += 1
            try:
                total += os.lstat(os.path.join(base, name)).st_size
            except OSError:
                pass
    return total, files + dirs


def _tree_size(path: pathlib.Path) -> int:
    return _tree_usage(path)[0]


def sweep(older_than_s: float = 900.0) -> tuple[int, float]:
    now, count, freed = time.time(), 0, 0
    for child in root().iterdir():
        try:
            if now - child.stat().st_mtime < older_than_s:
                continue
            size = _tree_size(child) if child.is_dir() else child.stat().st_size
            shutil.rmtree(child, ignore_errors=True) if child.is_dir() else child.unlink()
            count, freed = count + 1, freed + size
        except OSError:
            pass
    return count, freed / 2**20


_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOLCHAIN = os.path.join(_HERE, ".toolchain", "bin")
os.environ.setdefault("CONTAINERS_POLICY", os.path.join(_HERE, "policy.json"))
if os.path.isdir(_TOOLCHAIN):
    os.environ["PATH"] = _TOOLCHAIN + os.pathsep + os.environ.get("PATH", "")
_LOCAL_ROOT = (os.environ.get("ZERO_LOCAL_ROOT") or
               os.path.join(os.environ.get("SLURM_TMPDIR", "/tmp"),
                            f"zero-sandbox-{os.getuid()}"))
_IMAGE_STORE = os.environ.get("ZERO_IMAGE_STORE", os.path.join(_LOCAL_ROOT, "images"))
if os.path.isdir(_IMAGE_STORE):
    for _name, _relative in (("CONTAINERS_STORAGE_CONF", "storage.conf"),
                             ("CONTAINERS_POLICY", "config/containers/policy.json"),
                             ("XDG_CONFIG_HOME", "config"), ("XDG_DATA_HOME", ""),
                             ("XDG_CACHE_HOME", "cache"), ("XDG_RUNTIME_DIR", "runroot"),
                             ("TMPDIR", "tmp")):
        _path = os.path.join(_IMAGE_STORE, _relative) if _relative else _IMAGE_STORE
        if os.path.exists(_path):
            os.environ.setdefault(_name, _path)


def _remote_argv(path: str) -> list[str] | None:
    """Remote engine mode: drive a rootful podman over the API (e.g. a podman-machine VM), so the
    sandbox runs on a node without a host subuid range. Local storage flags (--root/--storage-driver/
    --storage-opt) are invalid with --remote; the VM owns storage. Set ZERO_PODMAN_URL for an explicit
    connection, or ZERO_PODMAN_REMOTE=1 to use podman's default (already-configured) connection."""
    if os.path.basename(path) != "podman":
        return None
    url = os.environ.get("ZERO_PODMAN_URL")
    if url:
        argv = [path, "--log-level", "error", "--remote", "--url", url]
        if ident := os.environ.get("ZERO_PODMAN_IDENTITY"):
            argv += ["--identity", ident]
        return argv
    if os.environ.get("ZERO_PODMAN_REMOTE"):
        return [path, "--log-level", "error", "--remote"]
    return None


def _is_remote() -> bool:
    """True when the engine is a remote/VM podman that does NOT share the host filesystem. A session
    then runs with NO host bind mounts: env is passed with -e, the self-destruct deadline rides an
    env var, and /work is a tmpfs. This is what lets the sandbox serve from a subuid=0 CPU node,
    where the only way to get a usable engine is a rootful podman-machine VM (no 9p/virtiofs share)."""
    return bool(os.environ.get("ZERO_PODMAN_URL") or os.environ.get("ZERO_PODMAN_REMOTE"))


def _engine_argv(path: str, image_stores: tuple[str, ...] = ()) -> list[str]:
    if (remote := _remote_argv(path)) is not None:
        return remote
    graph, runroot = os.environ.get("ZERO_PODMAN_ROOT"), os.environ.get("ZERO_PODMAN_RUNROOT")
    if graph and os.path.basename(path) == "podman":
        if not runroot:
            raise RuntimeError("ZERO_PODMAN_ROOT requires ZERO_PODMAN_RUNROOT")
        out = [path, "--log-level", "error", "--root", graph, "--runroot", runroot,
               "--storage-driver", "overlay"]
        fuse = os.path.join(_TOOLCHAIN, "fuse-overlayfs")
        if os.path.isfile(fuse):
            out += ["--storage-opt", f"overlay.mount_program={fuse}"]
        stores = image_stores + tuple(
            x for x in os.environ.get("ZERO_PODMAN_IMAGE_STORES", "").split(os.pathsep) if x)
        for store in stores:
            if store:
                out += ["--storage-opt", f"additionalimagestore={store}"]
        return out
    return [path]


def _store_runroot(store: str) -> str:
    runroot = os.path.join(store, "runroot/containers")
    if len(runroot) > 48:
        runroot = os.path.join("/tmp", f"zr-{os.getuid()}-" +
                               hashlib.sha256(store.encode()).hexdigest()[:12])
    return runroot


def _store_argv(path: str, store: str) -> list[str]:
    if (remote := _remote_argv(path)) is not None:
        return remote
    argv = [path, "--log-level", "error", "--root", os.path.join(store, "containers/storage"),
            "--runroot", _store_runroot(store),
            "--storage-driver", "overlay"]
    fuse = os.path.join(_TOOLCHAIN, "fuse-overlayfs")
    if os.path.isfile(fuse):
        argv += ["--storage-opt", f"overlay.mount_program={fuse}"]
    return argv


def _ram_store(image: str) -> str:
    base = os.environ.get("ZERO_RAM_IMAGE_STORE", "")
    return os.path.join(base, hashlib.sha256(image.encode()).hexdigest()[:24]) if base else ""


def _engine_env() -> dict[str, str]:
    env = os.environ.copy()
    if home := os.environ.get("ZERO_ENGINE_HOME"):
        env["HOME"] = home
    return env


def _idle_io(argv: list[str]) -> list[str]:
    """Cache promotion yields CPU and disk priority to request-serving processes."""
    prefix = ([shutil.which("ionice"), "-c", "3"] if shutil.which("ionice") else [])
    prefix += ([shutil.which("nice"), "-n", "19"] if shutil.which("nice") else [])
    return prefix + argv


#: How long a FAILED engine probe is reused before anyone re-probes. Long enough that a flood
#: cannot turn into a probe storm, short enough that a genuine recovery is picked up promptly.
_REPROBE_AFTER_S = float(os.environ.get("ZERO_ENGINE_REPROBE_S", "3"))


def _memoize_success(func=None, *, coerce=None):
    """`coerce` opts a function INTO cross-process persistence and says how to read the note back.
    Without it the memo stays in-process: `engine_cpus()` returns an int and a blind string
    round-trip would silently hand every caller a string."""
    if func is None:
        return functools.partial(_memoize_success, coerce=coerce)
    persist = coerce is not None
    """Memoise only a SUCCESSFUL answer, and re-probe after a failure.

    `functools.lru_cache` caches a failure as eagerly as a success, and both functions below probe
    a REMOTE engine over a transport that can refuse under load. One refused handshake then
    convinced that worker process, PERMANENTLY, that this node has no container runtime -- there is
    no expiry and nothing re-probes, so the process never recovers. Measured on a cold
    1500-request flood at concurrency 48: 438 of 1500 replies came back
    `unavailable:container:no working Podman/Docker runtime` from a VM that was healthy the whole
    time, with 34 GiB free and zero containers leaked. The engine was never gone; a cached `None`
    was. Re-probing after a failure costs one subprocess on a path that is already failing, which
    is the cheap side of this trade.
    """
    cache: dict[str, object] = {}
    lock = threading.Lock()
    # WHICH ENGINE THIS HOST HAS IS A PROPERTY OF THE HOST, NOT OF A REQUEST, so it is shared
    # across PROCESSES and not just within one. Sessions run in their own Ray actors, so an
    # in-process memo dedupes nothing: admitting 240 sessions meant 240 fresh processes each
    # shelling out `podman --remote version` at once, and the VM's API answered none of them.
    # Measured 1500 @ 600 after the in-process fix: still 419 of 1500 (28%) came back
    # `unavailable:container`. A successful answer is written once and read by every later
    # process, so the probe leaves the hot path entirely.
    def note() -> pathlib.Path:
        return pathlib.Path(os.environ.get("ZERO_SANDBOX_STATE")
                            or tempfile.gettempdir()) / f"zero-engine-{func.__name__}"

    @functools.wraps(func)
    def wrapper():
        if cache.get("value"):
            return cache["value"]
        try:                                  # a previous process already answered this
            if persist and (seen := note().read_text(encoding="utf-8").strip()):
                cache["value"] = coerce(seen)
                return cache["value"]
        except (OSError, ValueError):
            pass
        # SINGLE-FLIGHT, WITH A SHORT NEGATIVE TTL. Re-probing after a failure is what stops a
        # refused handshake poisoning a worker permanently, and it must stay. But re-probing on
        # EVERY failing request turns the probe into the load: each call shells out
        # `podman --remote version` against the same transport that is already saturated, so a
        # flood of N requests becomes a flood of N engine probes on top of it, and the probe fails
        # BECAUSE it is being probed. Measured 2026-08-31: both arms died on
        # `unavailable:container:no working Podman/Docker runtime` while `/health` reported the
        # engine fine with 183 GiB free -- the engine was never gone, it was being stampeded.
        #
        # One prober at a time; everyone else reuses the recent answer for a couple of seconds.
        # Recovery is preserved (the next probe after the TTL still runs), the stampede is not.
        with lock:
            if cache.get("value"):
                return cache["value"]
            now = time.monotonic()
            if now - float(cache.get("failed_at") or 0.0) < _REPROBE_AFTER_S:
                return cache.get("last")
            value = func()
            if value:
                cache["value"] = value
                if persist:
                    try:
                        note().write_text(str(value), encoding="utf-8")
                    except OSError:
                        pass                  # a read-only state dir just costs the memo
            else:
                cache["failed_at"] = time.monotonic()
                cache["last"] = value
            return value

    wrapper.cache_clear = cache.clear
    return wrapper


@_memoize_success(coerce=str)
def runtime() -> str | None:
    names = ([os.environ["ZERO_CONTAINER_RUNTIME"]]
             if os.environ.get("ZERO_CONTAINER_RUNTIME") else ["podman", "docker"])
    for name in names:
        path = shutil.which(name)
        if not path:
            continue
        try:
            result = subprocess.run(
                _engine_argv(path) + ["version", "--format", "{{.Server.Version}}"],
                capture_output=True, timeout=60, env=_engine_env())
        except (OSError, subprocess.SubprocessError):
            continue
        if result.returncode == 0:
            return name
    return None


@_memoize_success
def engine_cpus() -> int:
    """How many CPUs the BUILDER has, which is not always how many Ray can see.

    Builds run wherever the engine's daemon runs. On a cluster node that is the same machine Ray
    measures; behind a VM or a remote daemon it is not, and sizing build concurrency off the
    wrong one oversubscribes it. Measured on the acceptance box -- an 8-CPU host driving a 4-CPU
    VM -- four concurrent builds each allowed two CPUs took /health from 24 ms to 3.0 s.
    """
    engine = runtime()
    if engine is None:
        return 0
    # docker prints .NCPU; podman prints .Host.CPUs (capitalised). Neither field exists on the
    # other engine and querying the wrong one is a template error, so try both.
    for argv in (_engine_argv(engine) + ["info", "--format", "{{.NCPU}}"],
                 _engine_argv(engine) + ["info", "--format", "{{.Host.CPUs}}"]):
        try:
            result = subprocess.run(argv, capture_output=True, text=True, timeout=30,
                                    env=_engine_env())
            if result.returncode == 0 and result.stdout.strip().isdigit():
                return int(result.stdout.strip())
        except (OSError, subprocess.SubprocessError):
            continue
    return 0


def store_space(engine: str | None = None) -> dict:
    """Free bytes in the store THE ENGINE writes to, asked OF THE ENGINE.

    `/health` used to certify every runtime `available: true` off `runtime()` alone, which only
    proves the engine binary answers `version`. On the production host 2026-08-30 it said available while the
    engine behind it could not launch a single container: the VM's /var was full. That is the
    "local check standing in for a remote fact" failure this project has already paid for twice,
    and the fix is to ask the party that knows -- in remote/VM mode the store lives INSIDE the VM,
    where a host statvfs measures a different filesystem entirely.

    Podman reports it in `info`. Docker has no such field, so a LOCAL docker is measured with
    statvfs on its root dir and a remote one is reported unknown, named, rather than passed.
    """
    engine = engine or runtime()
    if engine is None:
        return {"known": False, "free_bytes": 0, "total_bytes": 0, "root": "",
                "reason": runtime_unavailable_reason()}
    argv = _engine_argv(engine)
    try:
        # Podman: whole-filesystem numbers for the graph root, reported by the server, so this is
        # the VM's disk under --remote and the node's under a local engine.
        result = subprocess.run(
            argv + ["info", "--format",
                    "{{.Store.GraphRoot}}\t{{.Store.GraphRootAllocated}}\t{{.Store.GraphRootUsed}}"],
            capture_output=True, text=True, timeout=60, env=_engine_env())
        parts = result.stdout.strip().split("\t")
        if result.returncode == 0 and len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
            total, used = int(parts[1]), int(parts[2])
            if total:
                return {"known": True, "free_bytes": max(0, total - used), "total_bytes": total,
                        "root": parts[0], "reason": ""}
        podman_said = _engine_error(result.stderr or "", 160) if result.returncode else ""
        result = subprocess.run(argv + ["info", "--format", "{{.DockerRootDir}}"],
                                capture_output=True, text=True, timeout=60, env=_engine_env())
        path = result.stdout.strip()
        if result.returncode == 0 and path and not _is_remote() and os.path.isdir(path):
            stat = os.statvfs(path)
            return {"known": True, "free_bytes": stat.f_bavail * stat.f_frsize,
                    "total_bytes": stat.f_blocks * stat.f_frsize, "root": path, "reason": ""}
    except (OSError, subprocess.SubprocessError) as error:
        return {"known": False, "free_bytes": 0, "total_bytes": 0, "root": "",
                "reason": f"engine did not answer info: {type(error).__name__}"}
    # SAY WHICH IT WAS. Measured under a 1500-request flood: `info` intermittently failed against a
    # busy remote engine and the gate went quiet for three refresh cycles. It degrades SAFELY --
    # unknown is not treated as full -- but "the engine does not report this field" and "the engine
    # was too busy to answer" are different operational facts and were reported as the same one.
    return {"known": False, "free_bytes": 0, "total_bytes": 0, "root": "",
            "reason": ("engine did not answer info: " + podman_said) if podman_said
            else "engine does not report store free space"}


def store_target_free(total_bytes: int) -> int:
    """Free bytes the disk tier tries to KEEP, above the hard floor a launch needs.

    A fraction, not a constant: the same service runs against a 144 GiB VM disk and a multi-TB
    node scratch, and a fixed number is either meaningless on one or crippling on the other.
    """
    share = float(os.environ.get("ZERO_STORE_TARGET_FREE_FRAC", "0.30"))
    return max(store_min_free(), int(total_bytes * share))


def store_min_free() -> int:
    """Below this, a launch is expected to fail with ENOSPC, so the runtime is NOT available.

    Sized from what a launch actually costs: a container's read-write layer plus whatever the
    session writes to it. Deliberately not zero -- reporting available at 100 MB free means
    certifying a store that the next build fills."""
    return int(float(os.environ.get("ZERO_STORE_MIN_FREE_GB", "5")) * 2**30)


def runtime_unavailable_reason() -> str:
    return "" if runtime() else "no working Podman/Docker runtime"


def _sweep_containers() -> None:
    """Startup only: this kills LIVE sessions, so nothing may call it while the server serves."""
    engine = runtime()
    if not engine:
        return
    base = _engine_argv(engine)
    found = subprocess.run(base + ["ps", "-aq", "--filter", "label=zero.session=1"],
                           capture_output=True, text=True, env=_engine_env())
    _rm(engine, *found.stdout.split())
    _sweep_networks(force=True)


# id -> when it was FIRST seen in a reapable state. A container is reaped only when it has sat in
# that state for longer than a launch is allowed to take, so this can never race a launch that is
# still in flight in another process (sessions launch in Ray actors, this sweeps in the gateway, so
# an in-process registry of in-flight names would not see them).
_CREATED_SEEN: dict[str, float] = {}
_CREATED_LOCK = threading.Lock()
_SESSION_PREFIXES = ("zero-session-", "zero-proxy-")

# `created` -- launched but never started. `exited` -- ran and finished. NEITHER can belong to a
# live session: a session that owns a container holds it RUNNING, and one action per session runs
# at a time under an asyncio.Lock. `running`, `paused`, `stopping` and `removing` are deliberately
# absent: the first three are or may be live, and `removing` is already being torn down by the
# engine, so sweeping it only races that.
_REAPABLE_STATES = ("created", "exited")


def _reapable_containers(engine: str) -> set[str]:
    """Our containers in a state no live session can be holding.

    `exited` was added 2026-09-09. Sweeping only `created` left the OTHER half of the leak
    unreaped: on the production host the store reached 3538 records -- 1156 `created` but ALSO 1801 `exited` --
    which put `db.sql` at 73 MB and one `podman` process at 894% CPU while the VM sat 95.3% idle.
    Container create+destroy then serialised the whole service to ~2.3 completions/s, and a
    read-only `podman ps -a` could not finish in 480 s. `--rm` is supposed to remove these on exit
    and mostly does; what accumulates is every case where it did not fire -- a killed conmon, an
    engine restart mid-teardown, a removal that itself timed out.
    """
    base = _engine_argv(engine)
    found: set[str] = set()
    for state in _REAPABLE_STATES:
        for scope in (["--filter", "label=zero.session=1"],
                      *(["--filter", f"name={prefix}"] for prefix in _SESSION_PREFIXES)):
            # The label is belt; the name prefix is braces. Both are asked separately because
            # repeating one filter KEY is an OR on docker and an AND on podman, and a sweep that
            # silently matches nothing is how this leak survived in the first place.
            result = subprocess.run(base + ["ps", "-aq", "--filter", f"status={state}"] + scope,
                                    capture_output=True, text=True, timeout=60, env=_engine_env())
            if result.returncode == 0:
                found.update(result.stdout.split())
    return found


def _sweep_created(engine: str | None = None, now: float | None = None) -> int:
    """Reap containers no live session can hold -- `created` or `exited`. Safe while serving.

    `--rm` fires on container EXIT and the deadline watchdog runs INSIDE the container, so a
    launch that fails after create -- ENOSPC, a bad mount, a missing network -- leaves a `Created`
    container holding a full read-write image layer that NOTHING removes. On the production host 2026-08-30
    that was 1375 of 1377 containers in the VM and it is what filled its 100 GiB /var.
    `_sweep_containers` drains these too but is startup-only, because it kills live sessions; this
    one is state-scoped, so it may run periodically: a container a live session owns is RUNNING,
    never `Created` and never `Exited`.

    SWEEPING ONLY `created` WAS HALF A FIX, and the other half is what recurred on 2026-09-09:
    1801 `exited` records alongside 1156 `created`. See `_reapable_containers` for the measured
    cost -- it degraded the service to ~2.3 completions/s and cost a training run 8 dead steps.

    TWO OBSERVATIONS, ``ZERO_CREATED_GRACE_S`` apart, before anything is removed. A container is
    `Created` for a moment during every successful launch, and `Exited` for a moment before `--rm`
    collects it; the launcher's own timeout is 120s, so the grace defaults above it. Neither a
    launch in flight nor the engine's own teardown can be swept out from under itself.
    """
    engine = engine or runtime()
    if engine is None:
        return 0
    now = time.monotonic() if now is None else now
    grace = float(os.environ.get("ZERO_CREATED_GRACE_S", "180"))
    current = _reapable_containers(engine)
    with _CREATED_LOCK:
        for gone in [key for key in _CREATED_SEEN if key not in current]:
            del _CREATED_SEEN[gone]
        for key in current:
            _CREATED_SEEN.setdefault(key, now)
        stale = sorted(key for key, first in _CREATED_SEEN.items() if now - first >= grace)
        for key in stale:
            del _CREATED_SEEN[key]
    if stale:
        _rm(engine, *stale)
        print(f"[sandbox] reaped {len(stale)} leaked container(s) (created or exited)", flush=True)
    return len(stale)


def _sweep_networks(force: bool = False) -> None:
    """Reap egress networks. Without ``force`` the engine refuses one that still has an endpoint,
    which is exactly the test for "the session that owned it is gone"."""
    engine = runtime()
    if not engine:
        return
    base = _engine_argv(engine)
    found = subprocess.run(base + ["network", "ls", "-q", "--filter", "label=zero.egress=1"],
                           capture_output=True, text=True, env=_engine_env())
    if networks := found.stdout.split():
        subprocess.run(base + ["network", "rm"] + (["-f"] if force else []) + networks,
                       capture_output=True, env=_engine_env())


_TAGS: dict[str, tuple[float, set[str]]] = {}
_SIZES: dict[tuple[str, str], int] = {}
_IMAGE_LOCK = threading.Lock()
_TAGS_TTL = float(os.environ.get("ZERO_IMAGE_TABLE_TTL_S", "10"))


def _forget(image: str = "") -> None:
    with _IMAGE_LOCK:
        _TAGS.clear()
        for key in [k for k in _SIZES if not image or k[1] == image]:
            del _SIZES[key]


def _tags(argv: list[str]) -> set[str]:
    """One listing per store, not one probe per image: /health asks about every runtime.

    SCOPED TO WHAT THIS SERVICE BUILT. An unfiltered listing costs time linear in every image on
    the host — measured 446 ms against 69 ms for the same call restricted to our two prefixes —
    and on a shared node most of those images are somebody else's. Instance images supplied by a
    request carry arbitrary names and are probed one at a time instead.
    """
    key = "\0".join(argv)
    with _IMAGE_LOCK:
        stamp, tags = _TAGS.get(key, (0.0, None))
        if tags is not None and time.monotonic() - stamp < _TAGS_TTL:
            return tags
    scope = [x for prefix in _OWNED_PREFIXES for x in ("--filter", f"reference={prefix}")]
    result = subprocess.run(argv + ["images", "--format", "{{.Repository}}:{{.Tag}}"] + scope,
                            capture_output=True, text=True, timeout=120, env=_engine_env())
    tags = set(result.stdout.split()) if result.returncode == 0 else set()
    # Podman renders locally tagged images as ``localhost/name:tag`` in some storage
    # configurations and as ``name:tag`` in others. Image identity must not depend on that display
    # convention; both references address the same local image.
    tags |= {tag.removeprefix("localhost/") for tag in tags}
    with _IMAGE_LOCK:
        _TAGS[key] = (time.monotonic(), tags)
    return tags


def _engine_error(raw: str, limit: int = 2000) -> str:
    """The engine's failure text, WHOLE. Two truncations hid the decisive part of a real one.

    Measured on the production host 2026-08-30: five launch failures reported
    `open /var/lib/containers/storage/vfs/dir/<id>` with the errno CUT OFF, and the cause -- the
    VM's /var was full -- was only found by running the container by hand. Both halves were ours:
    `detail[0]` took the FIRST stderr line, where podman prints warnings and puts `Error:` LAST,
    and `[:240]` then cut the tail off whichever line it picked. Every character after the colon
    was the diagnosis. Keep all of it: an engine error is bounded by the engine, and a wrong
    answer here costs hours of the wrong investigation."""
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    return " | ".join(lines)[:limit]


def _rm(engine: str, *names: str) -> None:
    """Remove containers now. ``--time`` is Podman's; Docker's ``rm`` rejects it outright, and the
    call is captured, so passing it to Docker silently removed nothing at all.

    BOUNDED, AND NEVER RAISES. It runs on paths that already hold a result or are already failing:
    an engine that hangs here used to hang the request forever after its command had finished, and
    a raise destroyed the result it was cleaning up after. Whatever this misses, the periodic sweep
    and the container's own deadline watchdog reap."""
    if not names:
        return
    grace = ["--time", "0"] if os.path.basename(engine) == "podman" else []
    try:
        subprocess.run(_engine_argv(engine) + ["rm", "-f", *grace, *names],
                       capture_output=True, env=_engine_env(),
                       timeout=float(os.environ.get("ZERO_RM_TIMEOUT_S", "120")))
    except (OSError, subprocess.SubprocessError) as error:
        incident("rm-failed", containers=" ".join(names), error=repr(error))


def _remove_images(engine: str, images: list[str]) -> None:
    """Untag images the service owns. An image still referenced by a container is refused, which
    is the check we want: the engine, not our bookkeeping, is the authority on what is in use."""
    if not images:
        return
    subprocess.run(_engine_argv(engine) + ["image", "rm", *images],
                   capture_output=True, env=_engine_env())
    _forget()


def _inspect(engine: str, image: str) -> bool:
    return _image_size(_engine_argv(engine), image) is not None


def _tag(digest: str) -> str:
    return f"{_IMAGE_PREFIX}:{digest[:16]}"


def _build_limits(engine: str) -> list[str]:
    """A submitted Dockerfile is executable input: bound what building it may consume."""
    cpus = float(os.environ.get("ZERO_BUILD_CPUS", "2"))
    memory = float(os.environ.get("ZERO_BUILD_MEM_GB", "8"))
    disk = int(float(os.environ.get("ZERO_BUILD_DISK_GB", "32")) * 2**30)
    pids = int(os.environ.get("ZERO_BUILD_PIDS", "2048"))
    argv = ["--memory", f"{memory:g}g", "--cpu-period", "100000",
            "--cpu-quota", str(int(cpus * 100000)), "--ulimit", f"fsize={disk}:{disk}",
            "--ulimit", f"nproc={pids}:{pids}"]
    return argv


def _build(sub: Submitted, engine: str, *, stage: bool = True) -> tuple[str, int]:
    image = _tag(sub.digest)
    build_store = os.environ.get("ZERO_BUILD_IMAGE_STORE")
    build = _store_argv(engine, build_store) if build_store else _engine_argv(engine)
    locks = os.path.join(_LOCAL_ROOT, "locks")
    os.makedirs(locks, exist_ok=True)
    with open(os.path.join(locks, sub.digest + ".lock"), "w") as lock:
        import fcntl
        fcntl.flock(lock, fcntl.LOCK_EX)
        ram = _ram_store(image)
        if ram:
            size = _image_size(_store_argv(engine, ram), image)
            if size is not None:
                return image, size
        if _image_size(build, image) is not None:
            return image, _stage(build, engine, image) if stage else 0
        if not build_store and _inspect(engine, image):
            return image, 0
        # An empty context: nothing on this host can be copied into a submitted image.
        with tempfile.TemporaryDirectory(prefix="zero-build-", dir=str(root())) as context:
            path = os.path.join(context, "Dockerfile")
            with open(path, "w", encoding="utf-8") as stream:
                stream.write(sub.dockerfile)
            command = ["build"]
            if os.path.basename(engine) == "podman" and os.environ.get("CONTAINERS_POLICY"):
                command += ["--signature-policy", os.environ["CONTAINERS_POLICY"]]
            # EVERY submitted Dockerfile can read a build secret (`RUN --mount=type=secret`) and
            # builds have network, so this hands the operator's HF_TOKEN to every caller. It used
            # to follow from HF_TOKEN merely being in the service's environment; now the operator
            # must say so.
            if os.environ.get("HF_TOKEN") and os.environ.get("ZERO_BUILD_SHARE_HF_TOKEN") == "1":
                command += ["--secret", "id=hf_token,env=HF_TOKEN"]
            argv = build + command + _build_limits(engine) + ["-t", image, "-f", path, context]
            result = subprocess.run(
                argv, capture_output=True, text=True,
                timeout=float(os.environ.get("ZERO_BUILD_TIMEOUT_S", "7200")),
                env=_engine_env())
        if result.returncode:
            detail = (result.stderr or result.stdout).strip().splitlines()
            raise ValueError("unavailable:runtime build failed: " +
                             (detail[-1][:500] if detail else "unknown"))
        _forget(image)
        staged = _stage(build, engine, image) if stage else 0
    return image, staged


def _egress_image(engine: str) -> str:
    context = os.path.join(_HERE, "egress")
    dockerfile = os.path.join(context, "Dockerfile")
    digest = hashlib.sha256(open(dockerfile, "rb").read()).hexdigest()
    image = f"zero-egress:{digest[:16]}"
    if _inspect(engine, image):
        return image
    locks = os.path.join(_LOCAL_ROOT, "locks")
    os.makedirs(locks, exist_ok=True)
    with open(os.path.join(locks, "egress-" + digest + ".lock"), "w") as lock:
        import fcntl
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not _inspect(engine, image):
            result = subprocess.run(
                _engine_argv(engine) + ["build", "--network=host", "-t", image,
                                        "-f", dockerfile, context],
                capture_output=True, text=True, timeout=600, env=_engine_env())
            if result.returncode:
                detail = (result.stderr or result.stdout).strip().splitlines()
                raise ValueError("unavailable:egress image build failed: " +
                                 (detail[-1][:500] if detail else "unknown"))
            _forget(image)
    return image


def _image_size(argv: list[str], image: str) -> int | None:
    """Exact bytes. Images we built are gated by the cheap scoped listing first; an instance image
    named by a request is outside that scope and is probed directly.

    A miss on an owned image sizes EVERY owned image in one call. `/health` asks about each
    remembered runtime in turn, and one inspect per image is what put a ~300 ms tail on it;
    measured, five images cost 60 ms batched against 105 ms one at a time.
    """
    store = "\0".join(argv)
    owned = image.startswith(_OWNED_PREFIXES)
    present = _tags(argv) if owned else None
    if owned and image not in present:
        return None
    with _IMAGE_LOCK:
        cached = _SIZES.get((store, image))
    if cached is not None:
        return cached
    wanted = sorted(present) if owned else [image]
    # Keyed by the tags the engine reports, never by argument order: one missing image makes the
    # engine skip a line, and a positional read would then size every later image wrongly.
    result = subprocess.run(
        argv + ["image", "inspect", "--format", "{{.RepoTags}}\t{{.Size}}"] + wanted,
        capture_output=True, text=True, timeout=120, env=_engine_env())
    found: dict[tuple[str, str], int] = {}
    for line in result.stdout.splitlines():
        tags, _, raw = line.rpartition("\t")
        try:
            size = int(raw.strip())
        except ValueError:
            continue
        for tag in tags.strip().strip("[]").split():
            found[(store, tag)] = size
            found[(store, tag.removeprefix("localhost/"))] = size
    if found:
        with _IMAGE_LOCK:
            _SIZES.update(found)
    return found.get((store, image))


def _stage(source: list[str], engine: str, image: str) -> int:
    base, ram = os.environ.get("ZERO_RAM_IMAGE_STORE"), _ram_store(image)
    if not ram:
        return 0
    os.makedirs(ram, exist_ok=True)
    with open(os.path.join(ram, "stage.lock"), "w") as lock:
        import fcntl
        fcntl.flock(lock, fcntl.LOCK_EX)
        return _stage_locked(source, engine, image, ram, base)


def _stage_locked(source: list[str], engine: str, image: str, ram: str, base: str) -> int:
    target = _store_argv(engine, ram)
    present = _image_size(target, image)
    if present is not None:
        return present
    size = _image_size(source, image)
    if size is None:
        return 0
    reserve = int(float(os.environ.get("ZERO_RAM_MIN_FREE_GB", "3")) * 2**30)
    mem = next((int(line.split()[1]) * 1024 for line in open("/proc/meminfo")
                if line.startswith("MemAvailable:")), 0)
    stat = os.statvfs(base)
    if int(size * 1.1) > min(stat.f_bavail * stat.f_frsize, mem - reserve):
        return 0
    saved = subprocess.Popen(_idle_io(source + ["save", image]), stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=_engine_env())
    loaded = subprocess.run(_idle_io(target + ["load"]), stdin=saved.stdout, capture_output=True,
                            env=_engine_env())
    saved.stdout.close()
    saved.wait()
    save_error = saved.stderr.read().decode(errors="replace") if saved.stderr else ""
    if saved.returncode or loaded.returncode:
        _remove_tree(engine, ram)
        _remove_tree(engine, _store_runroot(ram))
        detail = (loaded.stderr.decode(errors="replace") or save_error).strip().splitlines()
        raise ValueError("unavailable:RAM image staging failed: " +
                         (detail[-1][:300] if detail else "unknown"))
    _remove_tree(engine, _store_runroot(ram))
    _forget(image)
    return size


def _image(sub: Submitted, requested: object = None) -> tuple[str, str]:
    engine = runtime()
    if engine is None:
        raise ValueError(f"unavailable:container:{runtime_unavailable_reason()}")
    if requested is None or requested == "":
        if sub.spec.image_required:
            raise ValueError(f"bad_request:runtime {sub.digest} requires an instance image")
        image, _staged = _build(sub, engine, stage=False)
    elif not isinstance(requested, str) or "\x00" in requested:
        raise ValueError("bad_request:image must be a string")
    else:
        image = requested
        if not any(image.startswith(prefix) for prefix in sub.spec.image_prefixes):
            raise ValueError(f"forbidden:image is not allowed by runtime {sub.digest}")
    if not _inspect(engine, image):
        raise ValueError(f"unavailable:image is not present locally: {image}")
    return engine, image


def _known_dir() -> pathlib.Path:
    path = pathlib.Path(_LOCAL_ROOT) / "known"
    path.mkdir(parents=True, exist_ok=True)
    return path


def remember(sub: Submitted) -> None:
    """Keep the declaration on node-local disk so a restart still reports what it caches."""
    path = _known_dir() / f"{sub.digest}.json"
    body = json.dumps({"dockerfile": sub.dockerfile,
                       "documentation": sub.documentation})
    if path.exists() and path.read_text(encoding="utf-8") == body:
        return
    fresh = path.with_suffix(".new")
    fresh.write_text(body, encoding="utf-8")
    os.replace(fresh, path)


def recall() -> list[Submitted]:
    out = []
    for path in sorted(_known_dir().glob("*.json")):
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
            body.pop("name", None)  # migrate declarations written by the former named API
            out.append(resolve(body))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return out


def _ram_present(image: str) -> str:
    """The RAM store holding ``image``, or "" when nothing is staged there.

    Every RAM entry is its OWN private store, so asking the engine costs one `images` listing per
    runtime rather than one per store: `/health` on a gateway holding ZERO_KNOWN_LIMIT
    declarations ran hundreds of them, which is what made discovery unanswerable. The store path
    is derived from the image, so absence -- the common case -- is a stat, and only a runtime that
    really is staged pays for an engine call.
    """
    store = _ram_store(image)
    return store if store and os.path.isdir(os.path.join(store, "containers/storage")) else ""


def _cache_state(digest: str, image: str | None = None) -> dict:
    engine = runtime()
    image = image or _tag(digest)
    if engine is None:
        return {"image": image, "tier": "none", "bytes": 0}
    for tier, store in (("ram", _ram_present(image)),
                        ("local", os.environ.get("ZERO_BUILD_IMAGE_STORE"))):
        size = _image_size(_store_argv(engine, store), image) if store else None
        if size is not None:
            return {"image": image, "tier": tier, "bytes": size}
    size = _image_size(_engine_argv(engine), image)
    return {"image": image, "tier": "runtime" if size is not None else "none",
            "bytes": size or 0}


def _ram_budget(cluster: dict[str, float]) -> int:
    """Bytes the sandbox may keep in its automatic RAM image tier."""
    ram = os.environ.get("ZERO_RAM_IMAGE_STORE")
    if not ram:
        return 0
    configured = os.environ.get("ZERO_RAM_IMAGE_CACHE_GB")
    if configured is not None:
        return max(0, int(float(configured) * 2**30))
    try:
        stat = os.statvfs(ram)
        mounted = stat.f_blocks * stat.f_frsize
    except OSError:
        return 0
    ray_memory = int(cluster.get("memory", mounted))
    reserve = int(float(os.environ.get("ZERO_RAM_MIN_FREE_GB", "3")) * 2**30)
    return max(0, min(mounted, ray_memory) - reserve)


def _ram_victims(entries: dict[tuple, dict], key: tuple, needed: int,
                 budget: int, busy: set[tuple] | None = None) -> list[tuple] | None:
    """Oldest inactive RAM entries whose removal admits ``needed``; None means impossible."""
    if needed > budget:
        return None
    used = sum(info["ram_bytes"] for other, info in entries.items() if other != key)
    if used + needed <= budget:
        return []
    candidates = sorted(
        (info["last_used"], other, info["ram_bytes"]) for other, info in entries.items()
        if other != key and info["ram_bytes"] and not info["active"] and
        other not in (busy or set()))
    victims = []
    for _, other, size in candidates:
        victims.append(other)
        used -= size
        if used + needed <= budget:
            return victims
    return None


def _disk_victims(images: list[tuple[str, float]], keep: int,
                  protected: set[str]) -> list[str]:
    """Oldest unprotected images beyond ``keep``. Discovery cost is linear in what is kept.

    Measured on the acceptance host: one `images` listing costs ~2.2 ms per MATCHING image, so an
    unbounded disk tier is what makes `/health` slow — 155 accumulated images read 357 ms where a
    single one reads 23 ms. The RAM tier already has a budget and an LRU; this gives the disk tier
    the same, and nothing else keeps it from growing for the life of the node.
    """
    if keep <= 0:
        return []
    candidates = sorted((created, name) for name, created in images
                        if name not in protected)
    # Surplus is measured against the LISTING, not against the protected set: a protected name
    # that is not among the owned images -- an instance image, or one already evicted -- costs
    # nothing to keep, and counting it removed images that were not surplus at all.
    surplus = len(images) - keep
    return [name for _, name in candidates[:max(0, surplus)]]


def _owned_images(engine: str) -> list[tuple[str, float]]:
    """Every image this service built, with its creation time, in one engine call."""
    result = subprocess.run(
        _engine_argv(engine) + ["images", "--format", "{{.Repository}}:{{.Tag}}\t{{.CreatedAt}}",
                                "--filter", f"reference={_IMAGE_PREFIX}"],
        capture_output=True, text=True, timeout=120, env=_engine_env())
    out = []
    for line in result.stdout.splitlines():
        name, _, created = line.partition("\t")
        if not name or name.endswith(":<none>"):
            continue
        stamp = created.strip()[:19]
        try:
            out.append((name, time.mktime(time.strptime(stamp, "%Y-%m-%d %H:%M:%S"))))
        except ValueError:
            out.append((name, 0.0))
    return out


def _remove_tree(engine: str, path: str | None) -> None:
    if not path:
        return
    shutil.rmtree(path, ignore_errors=True)
    if os.path.exists(path):
        try:
            subprocess.run(_engine_argv(engine) + ["unshare", "rm", "-rf", "--", path],
                           capture_output=True, env=_engine_env(), timeout=300)
        except (OSError, subprocess.SubprocessError) as error:
            incident("remove-tree-failed", path=path, error=repr(error))


#: How far into stderr the start marker may sit. Only the ENGINE CLIENT writes before it (podman
#: prints its own warnings onto the exec's stderr -- measured: `Failed to get rootless runtime dir`),
#: and the submitted code cannot write anything until after it.
_MARK_WINDOW = 64 * 1024


def _tail(pipe, cap: int, target: list, marker: bytes = b"") -> None:
    """Keep the last ``cap`` bytes of a stream, and find ``marker`` near its head.

    The marker is removed from the stream before anything is counted, so ``data``/``cut``/``total``
    are exactly what they would be for the unmarked stream. ``seen`` says whether it was found;
    without a marker it is True."""
    data, cut, total = bytearray(), False, 0
    seen, looking, pending = not marker, bool(marker), bytearray()

    def feed(block) -> None:
        nonlocal cut, total
        total += len(block)
        data.extend(block)
        if len(data) > cap:
            cut = True
            del data[:-cap]

    while block := pipe.read(64 * 1024):
        if not looking:
            feed(block)
            continue
        pending += block
        at = pending.find(marker, 0, _MARK_WINDOW + len(marker))
        if at >= 0:
            seen, looking = True, False
            rest = bytes(pending[:at] + pending[at + len(marker):])
            pending.clear()
            if rest:
                feed(rest)
        elif len(pending) > _MARK_WINDOW + len(marker):
            looking = False          # not within the window: whatever follows did not start
            feed(bytes(pending))
            pending.clear()
    if pending:
        feed(bytes(pending))
    target.append((bytes(data), cut, total, seen))


def _scan(path: str | None, limit: int = 10_000) -> tuple[dict, bool, int, int]:
    """One walk over the work volume, never following links: the bounded delta-map that the file
    report needs AND the FULL byte and inode totals the quota needs.

    The map is capped at ``limit`` entries -- a submitted program can make millions of files and
    the delta must not hold them all -- but the totals keep accumulating past the cap, because the
    inode ceiling only means anything if it sees every inode. This replaces a second full
    traversal per action: the post-action path used to walk the tree once for the delta and again
    for the quota."""
    if not path:
        return {}, True, 0, 0
    found, stack, cut = {}, [(path, "")], False
    total_bytes = total_inodes = 0
    while stack:
        base, relative = stack.pop()
        try:
            entries = os.scandir(base)
        except OSError:
            continue
        with entries:
            for entry in entries:
                total_inodes += 1
                name = posixpath.join(relative, entry.name)
                try:
                    stat = entry.stat(follow_symlinks=False)
                    total_bytes += stat.st_size
                    if len(found) < limit:
                        found[name] = (stat.st_mode, stat.st_size, stat.st_mtime_ns)
                    else:
                        cut = True
                    if entry.is_dir(follow_symlinks=False):
                        stack.append((entry.path, name))
                except OSError:
                    pass
    return found, cut, total_bytes, total_inodes


def _files(path: str | None, limit: int = 10_000) -> tuple[dict, bool]:
    """A bounded metadata view of the persistent work volume; never follows links."""
    found, cut, _bytes, _inodes = _scan(path, limit)
    return found, cut


def _file_delta(before: dict, after: dict, cut: bool) -> dict:
    created, deleted = set(after) - set(before), set(before) - set(after)
    modified = {name for name in set(before) & set(after) if before[name] != after[name]}
    delta = sum(after[name][1] for name in created | modified)
    delta -= sum(before[name][1] for name in deleted | modified)
    return {"observed": True, "scope": "/work", "created": len(created), "modified": len(modified),
            "deleted": len(deleted), "bytes_delta": delta, "scanned": len(after),
            "truncated": cut}


def _execution_config(raw: dict, spec: RuntimeSpec,
                      resources: Resources) -> tuple[dict, dict[str, str], str]:
    network = raw.get("network")
    if network in (None, ""):
        policy = {"mode": "none", "outbound": False}
    else:
        if spec.egress != "endpoint" or not isinstance(network, str):
            raise ValueError("forbidden:runtime does not allow endpoint egress")
        parsed = urllib.parse.urlsplit(network)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname or
                parsed.username or parsed.password):
            raise ValueError("bad_request:network must be one HTTP(S) endpoint URL")
        try:
            port = parsed.port
        except ValueError:      # urlsplit refuses a non-numeric or out-of-range port
            raise ValueError("bad_request:network has an invalid port") from None
        # An ABSENT port takes the scheme's default; a port that was written down is honoured or
        # refused. `or` conflated them, so `:0` silently became 443 and the applied grant was not
        # the requested one.
        if port is None:
            port = 443 if parsed.scheme == "https" else 80
        if not 0 < port < 65536:
            raise ValueError("bad_request:network has an invalid port")
        if not re.fullmatch(r"[A-Za-z0-9.-]+", parsed.hostname):
            raise ValueError("bad_request:endpoint egress requires a DNS hostname")
        origin = f"{parsed.scheme}://{parsed.hostname}:{port}"
        policy = {"mode": "endpoint", "outbound": True, "origin": origin,
                  "host": parsed.hostname, "port": port}
    supplied = raw.get("env", {})
    if not isinstance(supplied, dict) or len(supplied) > 64:
        raise ValueError("bad_request:env must be an object with at most 64 entries")
    environment: dict[str, str] = {}
    for key, value in supplied.items():
        if not isinstance(key, str) or not _ENV_RE.fullmatch(key):
            raise ValueError("bad_request:invalid environment name")
        if not isinstance(value, str) or "\x00" in value:
            raise ValueError("bad_request:environment values must be NUL-free strings")
        environment[key] = value
    if sum(len(k) + len(v) for k, v in environment.items()) > 131072:
        raise ValueError("bad_request:environment exceeds 128 KiB")
    # ONE entry is ONE argv string on a remote engine (`-e KEY=VALUE`), and Linux refuses any
    # single argv string of MAX_ARG_STRLEN (128 KiB) or more with E2BIG -- which used to surface
    # as a dead session worker, not as a refusal. The 128 KiB total above admitted one entry of
    # exactly that size; this leaves room for `=` and the terminator.
    if any(len((k + "=" + v).encode()) > 131072 - 64 for k, v in environment.items()):
        raise ValueError("bad_request:one environment entry exceeds 128 KiB")
    material = repr((sorted(policy.items()), sorted(environment.items()),
                     sorted(resources.wire().items()))).encode()
    return policy, environment, hashlib.sha256(material).hexdigest()


def _vet_endpoint(host: str, port: int) -> str:
    """Resolve an egress host and refuse anything that is not a GLOBAL address; return the IP to
    pin it to.

    The grant rejects an IP literal and demands a DNS name, but a name resolves, and nothing
    stopped it resolving to the cloud metadata service (169.254.169.254), to loopback, or to the
    node's own private network. tinyproxy re-resolves at connect time, so vetting the name is not
    enough on its own -- the returned address is written into the relay's /etc/hosts, so the name
    can reach only the address that was vetted and a later DNS answer cannot move it (rebinding).

    Runs in the session actor, never on the gateway event loop: a slow resolver must not stall
    every other execution.
    """
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except OSError as error:
        raise ValueError(f"unavailable:endpoint host does not resolve: {host}: {error}") from None
    addresses = []
    for info in infos:
        raw = info[4][0].split("%", 1)[0]        # drop any IPv6 zone id
        try:
            addresses.append(ipaddress.ip_address(raw))
        except ValueError:
            continue
    if not addresses:
        raise ValueError(f"unavailable:endpoint host does not resolve: {host}")
    # EVERY answer must be global. One private answer among several is a rebinding attempt.
    bad = [str(a) for a in addresses if not a.is_global]
    if bad:
        raise ValueError(
            f"forbidden:endpoint host {host} resolves to a non-global address: {', '.join(bad)}")
    # Pin an IPv4 answer when there is one: the relay's bridge network is not guaranteed IPv6.
    ordered = sorted(addresses, key=lambda a: a.version)
    return str(ordered[0])


@dataclass
class LiveSession:
    sub: Submitted
    resources: Resources
    session_id: str
    image: str | None = None
    cached_image: str | None = None
    cached_store: str = ""
    network_policy: dict = field(default_factory=lambda: {"mode": "none", "outbound": False})
    environment: dict[str, str] = field(default_factory=dict)
    config_hash: str = ""
    gpu_ids: tuple[str, ...] = ()
    container: str = ""               # chosen by the gateway, so it can reap it if we die
    engine: str = field(init=False)
    image_ref: str = field(init=False)
    name: str = field(init=False)
    work: str | None = field(init=False, default=None)
    control: str = field(init=False)
    closed: bool = field(init=False, default=False)
    launched: bool = field(init=False, default=False)
    spawned: bool = field(init=False, default=False)
    launch_argv: list = field(init=False, default_factory=list)
    watchdog: str = field(init=False, default="")
    file_state: dict = field(init=False, default_factory=dict)
    network_name: str | None = field(init=False, default=None)
    proxy_name: str | None = field(init=False, default=None)
    proxy_ip: str | None = field(init=False, default=None)

    @property
    def spec(self) -> RuntimeSpec:
        return self.sub.spec

    @property
    def runtime_hash(self) -> str:
        return self.sub.digest

    def __post_init__(self):
        if self.cached_image:
            self.engine, self.image_ref = runtime(), self.cached_image
            if self.engine is None:
                raise ValueError(f"unavailable:container:{runtime_unavailable_reason()}")
        else:
            self.engine, self.image_ref = _image(self.sub, self.image)
        self.name = self.container or "zero-session-" + secrets.token_hex(12)
        self.control = tempfile.mkdtemp(prefix="zero-control-", dir=str(root()))
        os.chmod(self.control, 0o755)
        self.environment["HOME"] = "/tmp"
        if self.resources.gpus:
            if not self.gpu_ids:
                raise ValueError("unavailable:Ray assigned no GPU to the session")
            self.environment["CUDA_VISIBLE_DEVICES"] = ",".join(self.gpu_ids)
        if self.network_policy["mode"] == "endpoint":
            self._open_network()
            proxy = f"http://{self.proxy_ip}:8888"
            self.environment.update({"HTTP_PROXY": proxy, "HTTPS_PROXY": proxy,
                                     "http_proxy": proxy, "https_proxy": proxy,
                                     "NO_PROXY": "", "no_proxy": ""})
        if self.environment:
            with open(os.path.join(self.control, "environment"), "w", encoding="utf-8") as stream:
                for key, value in self.environment.items():
                    stream.write(f"export {key}={shlex.quote(value)}\n")
            os.chmod(os.path.join(self.control, "environment"), 0o444)
        self._deadline(MAX_COMMAND_SECONDS + MAX_IDLE_SECONDS + 30)
        # A remote/VM engine shares no host filesystem, so the session carries NO host bind mounts:
        # env is passed with -e, the self-destruct deadline rides an env var, and /work is a tmpfs
        # (charged to the container's memory like the ram medium). Confinement is unchanged and
        # arguably tighter -- not one host path is exposed into the container.
        remote = _is_remote()
        memory = self.resources.memory_gb + (
            self.resources.workdir_gb
            if remote or self.resources.workdir_medium == "ram" else 0)
        image_stores = ((os.path.join(self.cached_store, "containers/storage"),)
                        if self.cached_store else ())
        argv = _engine_argv(self.engine, image_stores) + [
            "run", "--rm", "--name", self.name, "--label", "zero.session=1",
            "--user", "65532:65532", "--read-only", "--cap-drop=ALL",
            "--security-opt", "no-new-privileges", "--pids-limit", str(self.resources.pids),
            "--cpus", str(self.resources.cpus), "--memory", f"{memory}g",
            "--memory-swap", f"{memory}g",
            "--ulimit", f"fsize={int(self.resources.workdir_gb * 2**30)}:{int(self.resources.workdir_gb * 2**30)}",
            "--ulimit", "core=0:0",
            "--tmpfs", "/tmp:rw,size=256m,mode=1777",
            "--tmpfs", "/dev/shm:rw,size=256m,mode=1777",
            # A missing image is a local fault, never a reason to reach a registry: the default
            # is `missing`, which turns one into a pull of a name the request supplied.
            "--pull=never",
        ]
        if remote:
            # self.remote_deadline was set by _deadline() above; the watchdog seeds its file from it
            # and _deadline() updates the file per action via exec, so idle sessions reap promptly.
            for key, value in self.environment.items():
                argv += ["-e", f"{key}={value}"]
            argv += ["-e", f"ZERO_DEADLINE={self.remote_deadline}"]
        else:
            argv += ["--volume", f"{self.control}:/zero-control:ro"]
        if self.network_policy["mode"] == "endpoint":
            argv += ["--network", self.network_name]
        else:
            argv += ["--network=none"]
        if remote or self.resources.workdir_medium == "ram":
            # Size in whole MB: a float GB renders as e.g. "1.0g", which crun rejects with
            # `mount tmpfs: Invalid argument`. A latent bug the ram medium never hit (zero uses
            # temp) until the remote path made /work a tmpfs unconditionally.
            argv += ["--tmpfs",
                     f"/work:rw,exec,size={int(self.resources.workdir_gb * 1024)}m,mode=1777"]
        else:
            self.work = tempfile.mkdtemp(prefix=f"zero-{self.runtime_hash[:16]}-", dir=str(root()))
            os.chmod(self.work, 0o777)
            argv += ["--volume", f"{self.work}:/work:rw"]
        for gpu_id in self.gpu_ids:
            argv += ["--device", f"nvidia.com/gpu={gpu_id}"]
        if os.environ.get(REQUIRE_GVISOR_ENV) == "1":
            argv += ["--runtime=runsc"]
        if remote:
            # Read the deadline from a tmpfs file the gateway updates via exec (env is fixed at
            # launch and cannot be refreshed). Seed it from the launch value.
            # THE TRAILING NEWLINE IS LOAD-BEARING. `read` returns NON-ZERO at EOF when the line
            # has no delimiter, so `printf %s` + `read d ... || exit` exited on the FIRST loop
            # iteration and every remote session self-destructed about one second after launch --
            # whatever its deadline. Only requests whose `exec` landed inside that window worked;
            # the rest came back `cannot exec in a container that is not running`, which reads as
            # a transport or capacity fault and was chased as one. Measured on the live VM with a
            # deadline an HOUR ahead: `printf %s` dead by 2s, `printf '%s\n'` alive at 2/4/8s.
            # The LOCAL path never had this -- it writes `str(ts) + "\n"` -- so the bug arrived
            # with the remote port and nothing compared the two.
            watchdog = ("printf '%s\\n' \"$ZERO_DEADLINE\" > /tmp/zero-deadline; "
                        'while sleep 1; do read d </tmp/zero-deadline 2>/dev/null || exit; '
                        '[ "$(date +%s)" -lt "$d" ] || exit; done')
        else:
            watchdog = ("while sleep 1; do read d </zero-control/deadline || exit; "
                        "[ \"$(date +%s)\" -lt \"$d\" ] || exit; done")
        self.watchdog, self.launch_argv = watchdog, argv
        self.file_state, _cut = _files(self.work)

    def _launch(self) -> None:
        """Start the resident container. A stateless request never gets here: it runs one
        container that carries its own command, which is one engine round trip instead of
        launch + exec + remove."""
        if self.launched:
            return
        # OWNED FROM THE MOMENT THE NAME IS USED, not from the moment the container RUNS.
        # `run` CREATES the container and then starts it, so a run that fails in between --
        # storage, mount, network -- leaves a `Created` container that has already allocated a
        # read-write image layer. Nothing else ever reaps one: `--rm` fires only on EXIT, and the
        # deadline watchdog is a process INSIDE the container that never started. Measured on
        # the production host 2026-08-30: 1375 of 1377 containers in the VM were `Created`, which is what
        # filled its 100 GiB /var -- and the leak is self-amplifying, since each failure costs a
        # full image copy and makes the next ENOSPC likelier. Setting `spawned` first is what
        # makes close() reap the name whatever happens next.
        self.spawned = True
        # THE LAUNCH BUDGET IS A CLIFF, AND AT 120s IT WAS BEING HIT (measured 2026-09-09).
        # `podman run` CREATES then STARTS, and creation serialises on the container store, so its
        # cost grows with how many launches are in flight -- not with anything the caller asked
        # for. A downstream workload fanned out 110 checks per item and had ~94
        # sessions live; 24 of 64 rows came back without a result, of which the
        # session-actor deaths were all THIS subprocess raising `TimeoutExpired`.
        #
        # AND AN UNCAUGHT TimeoutExpired KILLS THE ACTOR, WHICH IS THE WORSE HALF. The client then
        # sees `unavailable:session actor failed: ray::_SessionActor.execute()` with a Python
        # traceback instead of one of the error-string prefixes that ARE the HTTP contract, so a
        # slow launch is indistinguishable from a broken service and cannot be attributed. Catch
        # it, reap the container the failed launch already allocated (same reason as the
        # `returncode` branch below -- the thing it leaks is the thing that caused the failure),
        # and raise the `unavailable:` string the caller knows how to read.
        # DEFAULT 600, NOT 120. The old 120 was never a chosen number and it was a cliff: it is
        # the same order as a single container's startup on an idle host, which leaves nothing for
        # the queueing that concurrency guarantees. 600 matches ZERO_MAX_COMMAND_SECONDS -- a
        # launch may take as long as the work it is launching -- and a genuinely stuck launch is
        # still bounded, still reaped, and still attributed.
        # A LAUNCH FAILURE IS INFRASTRUCTURE BY CONSTRUCTION: nothing submitted has run yet, so it
        # is raised as `InfraFailure` and the gateway retries on fresh resources. It used to be the
        # caller's `unavailable:` answer -- a valid request answered with an infrastructure error.
        _budget = int(os.environ.get("ZERO_LAUNCH_TIMEOUT_S", "600"))
        try:
            result = subprocess.run(
                self.launch_argv + ["-d", "--entrypoint", self.spec.shell, self.image_ref,
                                    "-lc", self.watchdog],
                capture_output=True, text=True, timeout=_budget, env=_engine_env())
        except subprocess.TimeoutExpired:
            _rm(self.engine, self.name)
            raise InfraFailure(
                "launch", f"container launch exceeded {_budget}s. The engine serialises "
                f"container creation, so this is launch CONTENTION, not the caller's command -- "
                f"raise ZERO_LAUNCH_TIMEOUT_S or lower the concurrency reaching this node.") from None
        except OSError as error:
            _rm(self.engine, self.name)
            raise InfraFailure("launch", f"cannot run the engine client: {error!r}") from None
        if result.returncode:
            # HERE as well as at close(): the failing path must clean up after itself, because
            # the thing it leaks is the thing that caused the failure.
            _rm(self.engine, self.name)
            raw = (result.stderr or result.stdout).strip()
            raise InfraFailure("launch", f"container launch failed (rc={result.returncode}): " +
                               (_engine_error(raw) or "unknown") + _root_hint(raw))
        self.launched = True

    def _open_network(self) -> None:
        if _is_remote():
            # The egress relay still mounts the control dir; endpoint egress on a remote engine is
            # not wired yet. Fail LOUDLY rather than silently drop the network grant.
            # A capability this deployment does not have, the same answer every time: refused as
            # such, not reported as an outage to be retried.
            raise ValueError("forbidden:endpoint egress is not supported on a remote engine")
        host, port = self.network_policy["host"], self.network_policy["port"]
        pinned = _vet_endpoint(host, port)
        token = secrets.token_hex(10)
        self.network_name, self.proxy_name = "zero-net-" + token, "zero-proxy-" + token
        base = _engine_argv(self.engine)
        created = subprocess.run(base + ["network", "create", "--internal", "--label",
                                         "zero.egress=1", self.network_name],
                                 capture_output=True, text=True, env=_engine_env())
        if created.returncode:
            self.network_name = None
            raise ValueError("unavailable:cannot create endpoint network")
        try:
            image = _egress_image(self.engine)
            config = ("Port 8888\nListen 0.0.0.0\nTimeout 600\nLogLevel Error\n"
                      "Allow 0.0.0.0/0\n"
                      "Filter \"/tmp/filter\"\nFilterDefaultDeny Yes\nFilterURLs Off\n"
                      f"ConnectPort {port}\n")
            # The relay watches the SESSION's deadline. Without it, any path that loses the
            # gateway's cleanup leaves a proxy and its network resident forever: the session
            # container self-destructs on the same deadline, the relay had nothing to stop it.
            script = ("printf %s " + shlex.quote(config) + " > /tmp/tinyproxy.conf; "
                      "printf %s " + shlex.quote("^" + re.escape(host) + "$\n") +
                      " > /tmp/filter; tinyproxy -d -c /tmp/tinyproxy.conf & "
                      "while sleep 5; do read d </zero-control/deadline || break; "
                      "[ \"$(date +%s)\" -lt \"$d\" ] || break; done")
            launched = subprocess.run(
                base + ["run", "-d", "--rm", "--name", self.proxy_name,
                        "--add-host", f"{host}:{pinned}",
                        "--label", "zero.session=1", "--label", "zero.egress=1",
                        "--user", "65532:65532", "--read-only", "--cap-drop=ALL", "--security-opt",
                        "no-new-privileges", "--tmpfs", "/tmp:rw,size=1m,mode=1777",
                        "--volume", f"{self.control}:/zero-control:ro",
                        "--network", "bridge", "--entrypoint", "/bin/sh", image,
                        "-c", script],
                capture_output=True, text=True, timeout=120, env=_engine_env())
            if launched.returncode:
                raw = (launched.stderr or launched.stdout).strip()
                detail = raw.splitlines()
                raise ValueError("unavailable:cannot launch endpoint relay: " +
                                 (detail[-1][:500] if detail else "unknown") + _root_hint(raw))
            # No DNS alias for the endpoint: it would resolve inside the relay too, and the
            # relay must reach the real host. The submitted container has no route of its own.
            attached = subprocess.run(
                base + ["network", "connect", self.network_name, self.proxy_name],
                capture_output=True, text=True, timeout=30, env=_engine_env())
            if attached.returncode:
                detail = (attached.stderr or attached.stdout).strip().splitlines()
                raise ValueError("unavailable:cannot attach endpoint relay: " +
                                 (detail[-1][:500] if detail else "unknown"))
            inspected = subprocess.run(base + ["inspect", self.proxy_name],
                                       capture_output=True, text=True, timeout=30,
                                       env=_engine_env())
            try:
                settings = json.loads(inspected.stdout)[0]["NetworkSettings"]
                network = (settings.get("Networks") or {}).get(self.network_name, {})
                self.proxy_ip = network.get("IPAddress") or settings.get("IPAddress")
                ipaddress.ip_address(self.proxy_ip)
            except (AttributeError, IndexError, KeyError, TypeError, ValueError,
                    json.JSONDecodeError):
                detail = (inspected.stderr or inspected.stdout).strip().replace("\n", " ")
                raise ValueError("unavailable:cannot resolve endpoint relay address: " +
                                 (detail[:500] or "empty inspect result")) from None
            if not self.proxy_ip:
                raise ValueError("unavailable:endpoint relay has no internal address")
        except Exception:
            self.close()
            raise

    def _deadline(self, after_s: float) -> None:
        ts = int(time.time() + max(1, after_s))
        if _is_remote():
            # No control mount to update. Keep the value for the launch entrypoint, and once the
            # container exists push each new deadline into it with `exec` so an IDLE keep-alive
            # session self-destructs promptly -- exactly like the local watchdog reading its file.
            # Without this the deadline is frozen at its generous launch value and warm sessions
            # pile up until the multi-hour cap, saturating the engine (the freeze-stall leak).
            self.remote_deadline = ts
            if getattr(self, "launched", False):
                # Same trailing newline, same reason: a deadline the watchdog cannot `read`
                # kills the session it was meant to extend.
                # NEVER RAISES: this runs AFTER an action produced its result, and an uncaught
                # TimeoutExpired here destroyed that result and killed the worker. A deadline that
                # failed to move leaves the container on the earlier, longer one -- bounded.
                try:
                    subprocess.run(_engine_argv(self.engine) + ["exec", self.name, "sh", "-c",
                                   f"printf '%s\\n' {ts} > /tmp/zero-deadline"],
                                   capture_output=True, timeout=30, env=_engine_env())
                except (OSError, subprocess.SubprocessError) as error:
                    incident("deadline-not-moved", container=self.name, error=repr(error))
            return
        path = os.path.join(self.control, "deadline")
        fresh = path + ".new"
        with open(fresh, "w", encoding="ascii") as stream:
            stream.write(str(ts) + "\n")
        os.chmod(fresh, 0o644)
        os.replace(fresh, path)

    def _stage_code(self, code: str) -> str:
        """Put ``code`` where the shell can SOURCE it, and return the command that does.

        AN ARGV STRING CANNOT EXCEED 128 KiB (MAX_ARG_STRLEN), and `podman exec ... -lc <code>`
        made the submitted code one. Measured 2026-10-15 16:07 and 20:26: a stage-2 check whose
        code carried the stage-1 output base64'd crossed it, `Popen` raised
        `OSError: [Errno 7] Argument list too long: 'podman'` inside the session worker, and the
        caller got `unavailable:session actor failed` -- deterministic, so no retry could ever
        have helped, and each one stopped a training run. Code below the limit is still passed
        exactly as before; only code that CANNOT be an argument is staged, outside the timed
        exec, and sourced (`$0` is unchanged; a syntax error names the file instead of `-c`).
        The engine copies it in from a tar on stdin, so the image needs no `cat` and the host
        shares no filesystem with a remote engine."""
        name = f".zero-code-{secrets.token_hex(8)}"
        data = code.encode("utf-8")
        if not _is_remote():
            path = os.path.join(self.control, name)
            with open(path, "wb") as stream:
                stream.write(data)
            os.chmod(path, 0o444)
            return f". /zero-control/{name}"
        import io
        import tarfile
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            info = tarfile.TarInfo(name)
            info.size, info.mode, info.uid, info.gid = len(data), 0o444, 65532, 65532
            info.mtime = int(time.time())
            archive.addfile(info, io.BytesIO(data))
        try:
            staged = subprocess.run(
                _engine_argv(self.engine) + ["cp", "-", f"{self.name}:/tmp"],
                input=buffer.getvalue(), capture_output=True, env=_engine_env(),
                timeout=float(os.environ.get("ZERO_STAGE_TIMEOUT_S", "120")))
        except (OSError, subprocess.SubprocessError) as error:
            raise InfraFailure("stage", f"cannot copy the code into the container: {error!r}")
        if staged.returncode:
            raise InfraFailure("stage", "cannot copy the code into the container: " +
                               _engine_error(staged.stderr.decode(errors="replace")))
        return f". /tmp/{name}"

    def _survived(self) -> str | None:
        """None when the command's own exit status stands, else why it does not -- asked of the
        ENGINE, in one `inspect`, never of anything the command could write. Never raises.

        * the container is gone or stopped: it died under the command (the command cannot kill
          its container's PID 1 -- measured);
        * the container still holds an EXEC RECORD: a client that completes its exec deletes the
          record, so one left behind means our client lost the engine mid-protocol. Measured
          2026-10-15 with the engine socket cut for 4 s under a running command: the client
          exited 125 with `Error: ... dial unix ...`, while the engine's record said the command
          had exited 0 with all of its output -- the old code returned that 125 as the result;
        * the engine does not answer: nothing can be certified."""
        try:
            probe = subprocess.run(
                _engine_argv(self.engine) + ["inspect", "--format",
                                             "{{.State.Running}} {{json .ExecIDs}}", self.name],
                capture_output=True, text=True, timeout=60, env=_engine_env())
        except (OSError, subprocess.SubprocessError) as error:
            return f"the engine did not answer inspect: {error!r}"
        if probe.returncode:
            said = (probe.stderr or "").lower()
            if "no such" in said or "not found" in said:
                return "the container is gone"
            return "the engine did not answer inspect: " + _engine_error(probe.stderr or "", 300)
        running, _, execs = probe.stdout.strip().partition(" ")
        if running.lower() != "true":
            return "the container stopped"
        try:
            left = json.loads(execs or "null") or []
        except json.JSONDecodeError:
            return "unreadable exec records: " + execs[:200]
        if left:
            return f"the engine still holds {len(left)} exec record(s): the client lost its stream"
        return None

    def execute(self, request: dict) -> dict:
        timeout_s, wait_s = request["timeout_s"], request["wait_s"]
        self._deadline(timeout_s + wait_s + 15)
        code = request["code"]
        if self.environment and not _is_remote():
            # On a remote engine the env is already in the container (-e); there is no control mount.
            code = ". /zero-control/environment\n" + code
        # STARTUP IS SEPARATED FROM THE COMMAND BUDGET. The container is launched first, bounded
        # by its own startup timeout, and ONLY the exec is bounded by ``timeout_s``. A stateless
        # action still runs one command and is closed after -- it just no longer carries the
        # command into the run, because that counted image and container startup against the
        # candidate's execution budget: under concurrent admission the same deterministic code
        # would time out or not depending on launch contention, which made verifier scores
        # stochastic. A stateless launch is one extra engine call for a deterministic budget.
        # The deadline is the host's, not the image's: a submitted image need not ship GNU
        # coreutils, and a timeout destroys the whole session either way.
        launch_started = time.monotonic()
        self._launch()
        start_ms = int((time.monotonic() - launch_started) * 1000)
        if not _is_remote():
            # The local watchdog reads its file every second, and the deadline written before the
            # launch counted the launch against the command: a launch slower than 15 s under
            # contention killed the container mid-command. Re-arm it from the moment the command
            # can start. (A remote container carries the generous deadline set at open.)
            self._deadline(timeout_s + wait_s + 15)
        # Bytes, not characters: the limit is on the encoded argv string. 100 KiB leaves room for
        # the wrapper and the terminator under MAX_ARG_STRLEN.
        if len(code.encode("utf-8")) > int(os.environ.get("ZERO_ARGV_CODE_LIMIT", str(100 * 1024))):
            code = self._stage_code(code)
        # THE START MARKER. A wrapper shell writes a per-action nonce to stderr and only THEN
        # `exec`s the runtime shell exactly as before (`$0 -lc code`; `exec` keeps the pid, and
        # SHLVL is put back, so the command sees the same process, argv, environment, options and
        # limits -- verified by diffing `env`/`declare -p`/`set -o`/`shopt`/`ulimit -a`/`umask`/
        # `trap -p` both ways). `&&`: if the marker cannot be written the command does not run.
        # So "marker absent" PROVES the submitted code never ran, and "marker present" proves it
        # did -- the code cannot write before the marker exists, and cannot unwrite it after.
        marker = "ZERO-START-" + secrets.token_hex(16)
        wrapper = (f"printf %s {marker} >&2 && "
                   '{ [ -z "${BASH_VERSION:-}" ] || export SHLVL=$((SHLVL-1)); '
                   'exec "$0" -lc "$1"; }')
        argv = _engine_argv(self.engine) + [
            "exec", "--workdir", request["cwd"], self.name, self.spec.shell, "-c", wrapper,
            self.spec.shell, code]
        started = time.monotonic()
        try:
            proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    env=_engine_env())
        except OSError as error:
            raise InfraFailure("exec", f"cannot run the engine client: {error!r}") from None
        cap = request["io_cap_kb"] * 1024
        stdout, stderr = [], []
        readers = [threading.Thread(target=_tail, args=(proc.stdout, cap, stdout), daemon=True),
                   threading.Thread(target=_tail, args=(proc.stderr, cap, stderr,
                                                        marker.encode()), daemon=True)]
        for reader in readers:
            reader.start()
        # A bind-mounted workspace has no engine-level size: `--ulimit fsize` bounds ONE file, so
        # files that each stay under the grant fill the node -- measured, a 0.25 GB grant wrote
        # 800 MB. Sampling bounds the overshoot to one interval of write bandwidth instead of
        # leaving it unbounded. The preventive answer is a quota on the filesystem itself
        # (XFS project quota on the scratch directory); this is what works everywhere.
        finished, over_quota = threading.Event(), threading.Event()
        guard = None
        if self.work:
            allowed = int(self.resources.workdir_gb * 2**30)
            max_files = int(os.environ.get("ZERO_WORKDIR_MAX_FILES", "500000"))
            interval = float(os.environ.get("ZERO_WORKDIR_POLL_S", "0.5"))

            def watch():
                while not finished.wait(interval):
                    used, count = _tree_usage(self.work)
                    if used > allowed or count > max_files:
                        over_quota.set()
                        # The CONTAINER, before the client. Killing `docker run` leaves the
                        # container writing, and the workspace kept growing until `close()`:
                        # measured 710 MB against a 256 MB grant, where removing the container
                        # first stops it at the sample that caught it.
                        _rm(self.engine, self.name)
                        proc.kill()
                        return

            guard = threading.Thread(target=watch, daemon=True)
            guard.start()
        launcher_timeout = False
        try:
            returncode = proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            launcher_timeout = True
            proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            returncode = 124
        finished.set()
        if guard is not None:
            guard.join(timeout=5)
        # The readers end when the engine client's pipes close, which nothing inside the container
        # can hold open. One that has not ended means the capture failed, and a partial capture is
        # not a result.
        capture_s = float(os.environ.get("ZERO_CAPTURE_TIMEOUT_S", "120"))
        for reader in readers:
            reader.join(timeout=capture_s)
        if any(reader.is_alive() for reader in readers) or not stdout or not stderr:
            proc.kill()
            raise InfraFailure("capture", f"output capture did not complete within {capture_s:g}s "
                               f"after the engine client exited (rc={returncode})", started=None)
        out, out_cut, out_total, _ = stdout[0]
        err, err_cut, err_total, began = stderr[0]
        exceeded = over_quota.is_set()
        if not began and not exceeded:
            # Never started: the engine could not exec (container gone, API refused, `exec` hung
            # until the budget ran out). What the engine client printed is ITS error, not a
            # result -- this used to be returned as the candidate's own rc 125 and stderr.
            raise InfraFailure(
                "exec-start", f"the command never started (engine client rc={returncode}"
                f"{', hung past timeout_s' if launcher_timeout else ''}): " +
                (_engine_error(err.decode("utf-8", "replace"), 1000) or "no output"))
        if returncode < 0 and not launcher_timeout and not exceeded:
            # The ENGINE CLIENT was killed by a signal on this host (OOM killer, an operator):
            # nothing inside a container can do that, and its exit status is not the command's.
            raise InfraFailure("exec-client", f"the engine client was killed by signal "
                               f"{-returncode}", started=began)
        if began and not launcher_timeout and not exceeded and returncode >= 125:
            # rc 125+ is also what the engine client reports when the container dies under a
            # running command -- with the partial output, and for `stop` with no error text at
            # all (measured: rc 137, stdout `started`) -- and when the client itself loses the
            # engine (rc 125). Only the engine can tell either apart from the command's own
            # 125+/signal exit, and it costs a probe only on such exits.
            why = self._survived()
            if why is not None:
                raise InfraFailure(
                    "exec-lost", f"{why}, after the command started (engine client "
                    f"rc={returncode}): " +
                    (_engine_error(err.decode("utf-8", "replace")[-2000:], 1000) or "no output"),
                    started=True)
        # FROM HERE ON A RESULT EXISTS. Nothing below may raise: it is cleanup and bookkeeping,
        # and a raise would turn a result that exists into an error the caller must not see.
        timed_out = launcher_timeout or returncode == 124
        try:
            after, files_cut, used, count = _scan(self.work)
        except Exception as error:            # pragma: no cover -- `_scan` swallows OSError
            incident("scan-failed", error=repr(error))
            after, files_cut, used, count = self.file_state, True, 0, 0
        files = (_file_delta(self.file_state, after, files_cut) if self.work else
                 {"observed": False, "scope": "/work",
                  "reason": "work volume is memory-backed"})
        self.file_state = after
        # `workdir_gb` was only ever a per-file `fsize` ulimit, so files that each stayed under it
        # filled the node: a 0.25 GB grant wrote 800 MB, and the disk it fills is the one holding
        # the image store and every other workspace. A tmpfs workspace is already bounded by the
        # engine, because it is charged to the container's memory limit; a bind mount has no
        # engine-level size, so the grant is enforced here. It is a CEILING, not a preventer: one
        # action may overshoot by what it writes between two scans, and the session then ends.
        if self.work:
            max_files = int(os.environ.get("ZERO_WORKDIR_MAX_FILES", "500000"))
            exceeded = (exceeded or used > int(self.resources.workdir_gb * 2**30)
                        or count > max_files)
            files["bytes_used"] = used
            files["inodes_used"] = count
            files["over_quota"] = exceeded
        try:
            if timed_out or exceeded:
                self.close()
            elif wait_s:
                self._deadline(wait_s)
        except Exception as error:            # cleanup must not cost the result
            incident("post-result-cleanup-failed", container=self.name, error=repr(error))
        # No engine probe: the container is alive by construction if the action returned, did
        # not time out and was not closed. A container that dies in the gap is discovered by the
        # next request, which is the same guarantee `container inspect` gave.
        alive = bool(wait_s and not timed_out and not exceeded and not self.closed)
        return {
            "runtime": {"hash": self.runtime_hash},
            "resources": self.resources.wire(),
            "session_id": self.session_id,
            "code": request["code"], "cwd": request["cwd"],
            "timeout_s": timeout_s, "wait_s": wait_s,
            "output": str(returncode), "returncode": returncode,
            "signal": returncode - 128 if 128 < returncode <= 192 else None,
            "stdout": out.decode("utf-8", "replace"),
            "err": err.decode("utf-8", "replace"),
            "stdout_bytes": len(out), "stderr_bytes": len(err),
            "stdout_total_bytes": out_total, "stderr_total_bytes": err_total,
            "stdout_truncated": out_cut, "stderr_truncated": err_cut,
            "timed_out": timed_out, "truncated": out_cut or err_cut,
            "alive": alive, "files": files,
            "network": {key: value for key, value in self.network_policy.items()
                        if key not in ("host", "port")},
            # `elapsed_ms` is now the COMMAND ONLY; `start_ms` is container startup and `build_ms`
            # is image build (upstream, already outside the budget). `timeout_s` bounds elapsed_ms.
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "start_ms": start_ms, "build_ms": int(request.get("build_ms", 0)),
        }

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        engine = getattr(self, "engine", None)
        if engine:
            _rm(engine, *(name for name in (
                getattr(self, "name", None) if getattr(self, "spawned", False) else None,
                getattr(self, "proxy_name", None)) if name))
        if engine and getattr(self, "network_name", None):
            try:
                subprocess.run(_engine_argv(engine) + ["network", "rm", "-f",
                                                       self.network_name],
                               capture_output=True, env=_engine_env(), timeout=120)
            except (OSError, subprocess.SubprocessError) as error:
                incident("network-rm-failed", network=self.network_name, error=repr(error))
        if engine:
            _remove_tree(engine, getattr(self, "work", None))
            _remove_tree(engine, getattr(self, "control", None))


def _request(raw: dict) -> tuple[Submitted, dict]:
    allowed = {"runtime", "resources", "session_id", "code", "cwd", "timeout_s",
               "wait_s", "io_cap_kb", "image", "network", "env"}
    if not isinstance(raw, dict):
        raise ValueError("bad_request:request must be an object")
    if extra := set(raw) - allowed:
        raise ValueError("bad_request:unsupported request fields: " + ", ".join(sorted(extra)))
    sub = resolve(raw.get("runtime"))
    spec = sub.spec
    resources = Resources.from_wire(raw.get("resources"))
    network, environment, config_hash = _execution_config(raw, spec, resources)
    session_id = raw.get("session_id")
    if not isinstance(session_id, str) or not _SESSION_RE.fullmatch(session_id):
        raise ValueError("bad_request:session_id must use 1-128 letters, digits, . _ : or -")
    code = raw.get("code")
    if not isinstance(code, str) or "\x00" in code:
        raise ValueError("bad_request:code must be a string without NUL")
    try:
        timeout_s, wait_s = float(raw["timeout_s"]), float(raw["wait_s"])
        io_cap_kb = int(raw.get("io_cap_kb", min(200, MAX_IO_CAP_KB)))
    except (KeyError, TypeError, ValueError):
        raise ValueError("bad_request:timeout_s and wait_s must be declared numbers") from None
    if not 0 < timeout_s <= MAX_COMMAND_SECONDS:
        raise ValueError(f"bad_request:timeout_s must be in (0,{MAX_COMMAND_SECONDS:g}]")
    if not 0 <= wait_s <= MAX_IDLE_SECONDS:
        raise ValueError(f"bad_request:wait_s must be in [0,{MAX_IDLE_SECONDS:g}]")
    if not 1 <= io_cap_kb <= MAX_IO_CAP_KB:
        raise ValueError(f"bad_request:io_cap_kb must be in [1,{MAX_IO_CAP_KB}]")
    cwd = posixpath.normpath(raw.get("cwd", "/work"))
    if cwd != "/work" and not cwd.startswith("/work/"):
        raise ValueError("bad_request:cwd must be /work or below it")
    return sub, {"runtime_hash": sub.digest, "session_id": session_id,
                 "code": code, "timeout_s": timeout_s, "wait_s": wait_s,
                 "io_cap_kb": io_cap_kb, "cwd": cwd, "image": raw.get("image"),
                 "resources": resources,
                 "network": network, "environment": environment, "config_hash": config_hash}


@dataclass
class _LocalEntry:
    live: LiveSession
    units: tuple[float, float, float]
    lock: threading.Lock = field(default_factory=threading.Lock)
    timer: threading.Timer | None = None
    generation: int = 0


# ADMISSION IS A VECTOR: (CPUs, GB of memory, sessions). It used to be one number, the CPU grant,
# checked against a total that was the MIN of the CPU count and a SESSION count derived from
# memory -- so a 4-CPU session spent four "memory slots". Measured on the production host 2026-09-12: memory
# gave 19.2 slots (96 GB engine at 4 GB/session), lean and cpp ask 4 CPUs, so five of them filled
# the service while the 240-vCPU VM sat 94.8% idle and each container used 2-20 MB; requests
# queued past their 900 s client deadline and a quarter of the admitted proposals came back
# unmeasured. Each axis is now checked in its own unit: the CPU grant against the engine's CPUs,
# the MEMORY GRANT (what the container may actually use) against the engine's memory, and one
# session against the host that runs its Ray worker (~3.7 GB each, `ZERO_SESSION_MEM_GB`).
_AXES = ("CPUs", "GB of memory", "sessions")


def _grant(resources) -> tuple[float, float, float]:
    # The memory axis is exactly the container's `--memory` (see LiveSession): /work is charged to
    # it on a remote engine whatever the medium, and on a local one only when it is RAM-backed.
    work = resources.workdir_gb if _is_remote() or resources.workdir_medium == "ram" else 0.0
    return (float(resources.cpus), float(resources.memory_gb) + float(work), 1.0)


def _fits(need: tuple, free: tuple) -> bool:
    return all(n <= f for n, f in zip(need, free))


def _refuse_oversize(need: tuple, total: tuple, prefix: str = "unavailable:") -> None:
    for axis, n, t in zip(_AXES, need, total):
        if n > t:
            raise ValueError(f"{prefix}requested {n:g} {axis}; server grants at most {t:g}")


def _mem_fraction() -> float:
    return float(os.environ.get("ZERO_SANDBOX_MEM_FRACTION", "0.80"))


def _session_mem_bytes() -> float:
    return max(0.1, float(os.environ.get("ZERO_SESSION_MEM_GB", "4.0"))) * (1 << 30)


def _capacity_total() -> tuple[float, float, float]:
    cpus = min(max(1.0, float(os.cpu_count() or 1) - 1.0),
               float(os.environ.get("ZERO_SANDBOX_SLOTS", 1e9)))
    try:
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") * _mem_fraction()
    except (ValueError, OSError):
        memory = float("inf")
    # Local sessions are threads of this process, not Ray workers: no per-session host cost.
    return (cpus, memory / (1 << 30), float("inf"))


class _LocalCapacity:
    """The local development path observes the same vector admission as Ray."""

    def __init__(self, total: tuple):
        self.total = self.free = tuple(float(t) for t in total)
        self.condition = threading.Condition()
        self.waiting: list[tuple[object, tuple]] = []

    def acquire(self, units: tuple) -> None:
        _refuse_oversize(units, self.total)
        token = object()
        with self.condition:
            self.waiting.append((token, units))
            while self.waiting[0][0] is not token or not _fits(units, self.free):
                self.condition.wait()
            self.waiting.pop(0)
            self.free = tuple(f - u for f, u in zip(self.free, units))

    def release(self, units: tuple) -> None:
        with self.condition:
            self.free = tuple(min(t, f + u) for t, f, u in zip(self.total, self.free, units))
            self.condition.notify_all()


def _infra_attempts() -> int:
    """How many times one request is tried against infrastructure failures before the service
    decides whether it is the request or the service that is broken."""
    return max(1, int(os.environ.get("ZERO_INFRA_ATTEMPTS", "8")))


def _backoff(attempt: int) -> float:
    """1, 2, 4, ... capped at 30 s: eight attempts span ~90 s, so an engine hiccup of a minute is
    absorbed by retrying instead of by restarting the whole service under every client."""
    base = float(os.environ.get("ZERO_INFRA_BACKOFF_S", "1"))
    return min(30.0, base * (2 ** (attempt - 1)))


def _lost(session_id: str, failure: InfraFailure) -> str:
    """A session that ALREADY held state cannot be retried on fresh resources: its state is what
    was lost. Said as what it is -- the session no longer exists -- rather than as `unavailable:`."""
    return (f"not_found:session {session_id} was lost to an infrastructure failure "
            f"({failure.stage}) and its state is gone; start a new session")


class LocalSessions:
    def __init__(self):
        self.entries: dict[str, _LocalEntry] = {}
        self.lock = threading.Lock()
        self.capacity = _LocalCapacity(_capacity_total())

    def _expire(self, session_id: str, generation: int) -> None:
        with self.lock:
            entry = self.entries.get(session_id)
            if entry is None or entry.generation != generation:
                return
            self.entries.pop(session_id, None)
        with entry.lock:
            entry.live.close()
        self.capacity.release(entry.units)

    def execute(self, raw: dict) -> dict:
        """The Ray path's failure semantics, in-process: an infrastructure failure on a session
        this request opened is retried on a fresh session (nothing of the caller's exists yet);
        on one that already held state it is the loss of that state (`not_found:`)."""
        sub, request = _request(raw)
        attempts = _infra_attempts()
        for attempt in range(1, attempts + 1):
            with self.lock:
                existed = request["session_id"] in self.entries
            try:
                return self._execute_once(sub, request)
            except InfraFailure as failure:
                incident("infra-failure", path="local", attempt=attempt, of=attempts,
                         stage=failure.stage, started=failure.started, detail=failure.detail,
                         session_id=request["session_id"], runtime=sub.digest)
                if existed:
                    raise ValueError(_lost(request["session_id"], failure)) from None
                if attempt == attempts:
                    raise ValueError("unavailable:" + str(failure)[:2000]) from None
                time.sleep(_backoff(attempt))
        raise AssertionError("unreachable")

    def _execute_once(self, sub: Submitted, request: dict) -> dict:
        sid = request["session_id"]
        with self.lock:
            entry = self.entries.get(sid)
        if entry is None:
            units = _grant(request["resources"])
            self.capacity.acquire(units)
            try:
                candidate = _LocalEntry(LiveSession(sub, request["resources"], sid,
                                                    request.get("image"), None, "",
                                                    request["network"], request["environment"],
                                                    request["config_hash"]), units)
            except BaseException as error:
                self.capacity.release(units)
                if isinstance(error, ValueError) and not str(error).startswith(_REQUEST_ERRORS):
                    raise InfraFailure("open", str(error)) from None
                raise
            with self.lock:
                entry = self.entries.setdefault(sid, candidate)
            if entry is not candidate:
                candidate.live.close()
                self.capacity.release(candidate.units)
        if (entry.live.runtime_hash, entry.live.image, entry.live.config_hash) != (
                sub.digest, request.get("image"), request["config_hash"]):
            raise ValueError("conflict:session_id already belongs to another execution config")
        with entry.lock:
            if entry.timer:
                entry.timer.cancel()
            try:
                result = entry.live.execute(request)
            except BaseException:
                with self.lock:
                    if self.entries.get(sid) is entry:
                        self.entries.pop(sid, None)
                entry.live.close()
                self.capacity.release(entry.units)
                raise
            if not result["alive"]:
                with self.lock:
                    self.entries.pop(sid, None)
                entry.live.close()
                self.capacity.release(entry.units)
            else:
                entry.generation += 1
                entry.timer = threading.Timer(request["wait_s"], self._expire,
                                              args=(sid, entry.generation))
                entry.timer.daemon = True
                entry.timer.start()
            return result


_LOCAL_SESSIONS = LocalSessions()


def execute_local(request: dict) -> dict:
    return _LOCAL_SESSIONS.execute(request)


def _outcome(call, stage: str, on_infra=None) -> dict:
    """Run ``call`` and report how it ended as DATA, never as a raised exception.

    A raise inside a Ray actor reaches the gateway as a RayTaskError whose text is the whole remote
    traceback, and the gateway used to forward that text as `unavailable:session actor failed:
    ray::_SessionActor.execute() (pid=...)` -- the real cause (E2BIG, 2026-10-15) was in it, cut off
    by every reader. Here each ending is named: a result, a REQUEST error (returned as-is), or an
    infrastructure failure carrying the real exception and traceback, which the gateway logs whole
    and retries."""
    import traceback
    try:
        return {"result": call()}
    except InfraFailure as failure:
        if on_infra is not None:
            on_infra()
        return {"infra": failure.wire()}
    except ValueError as error:
        if str(error).startswith(_REQUEST_ERRORS):
            return {"error": str(error)}
        if on_infra is not None:
            on_infra()
        return {"infra": {"stage": stage, "started": None if stage == "execute" else False,
                          "detail": f"{error}\n{traceback.format_exc()}"}}
    except Exception as error:
        if on_infra is not None:
            on_infra()
        return {"infra": {"stage": stage, "started": None if stage == "execute" else False,
                          "detail": f"{type(error).__name__}: {error}\n{traceback.format_exc()}"}}


class _SessionActor:
    def __init__(self):
        self.live: LiveSession | None = None

    def open(self, sub: Submitted, resources: Resources, session_id: str,
             image: str | None, cached_image: str, cached_store: str,
             network: dict, environment: dict[str, str], config_hash: str,
             container: str = "") -> dict:
        import ray

        def run():
            self.close()
            gpu_ids = tuple(ray.get_runtime_context().get_accelerator_ids().get("GPU", ()))
            self.live = LiveSession(sub, resources, session_id, image, cached_image,
                                    cached_store, network, environment, config_hash, gpu_ids,
                                    container)
            return True
        return _outcome(run, "open", on_infra=self.close)

    def execute(self, request: dict) -> dict:
        def run():
            if self.live is None:
                raise InfraFailure("execute", "the session worker holds no open session")
            return self.live.execute(request)
        # An infrastructure failure leaves the container in an unknown state: it is removed here,
        # before the gateway retries on a fresh worker, so a failed attempt leaks nothing.
        return _outcome(run, "execute", on_infra=self.close)

    def close(self) -> None:
        live, self.live = self.live, None
        if live is not None:
            try:
                live.close()
            except Exception as error:
                incident("session-close-failed", error=repr(error))


class _CacheActor:
    def ensure(self, sub: Submitted, image: str | None) -> dict:
        import ray
        if image:
            engine, cached = _image(sub, image)
            ram_bytes = _image_size(
                _store_argv(engine, _ram_store(cached)), cached
            ) if os.environ.get("ZERO_RAM_IMAGE_STORE") else None
        else:
            engine = runtime()
            if engine is None:
                raise RuntimeError(runtime_unavailable_reason())
            cached, ram_bytes = _build(sub, engine, stage=False)
        return {"image": cached, "ram_bytes": ram_bytes or 0,
                "ram_store": _ram_store(cached) if ram_bytes else "", "bytes": _image_size(
            _engine_argv(engine), cached) or 0,
            "node_id": ray.get_runtime_context().get_node_id()}

    def stage(self, image: str) -> int:
        engine = runtime()
        if engine is None:
            return 0
        store = os.environ.get("ZERO_BUILD_IMAGE_STORE")
        source = (_store_argv(engine, store)
                  if store and _image_size(_store_argv(engine, store), image) is not None
                  else _engine_argv(engine))
        return _stage(source, engine, image)

    def evict(self, image: str) -> bool:
        engine, ram = runtime(), os.environ.get("ZERO_RAM_IMAGE_STORE")
        if engine is None or not ram:
            return True
        store = _ram_store(image)
        _remove_tree(engine, store)
        _remove_tree(engine, _store_runroot(store))
        _forget(image)
        return not os.path.exists(store)


class _RamLease:
    def ready(self) -> bool:
        return True


@dataclass
class _RemoteEntry:
    runtime_hash: str
    image: str | None
    actor: object
    lock: asyncio.Lock
    worker_key: tuple
    units: tuple[float, float, float]
    config_hash: str
    cache_key: tuple[str, str]
    generation: int = 0
    expiry: asyncio.Task | None = None
    build_ms: int = 0
    grant: int | None = None          # the admission ledger token this session holds
    container: str = ""               # its container's name, to reap if its worker dies


class _Capacity:
    """FIFO admission over (CPUs, memory, sessions): the head waits until it fits on EVERY axis,
    so a large grant is never starved by a stream of small ones."""

    def __init__(self, total: tuple):
        self.total = self.free = tuple(float(t) for t in total)
        self.waiting: list[tuple[tuple, asyncio.Future]] = []

    def _wake(self) -> None:
        while self.waiting and _fits(self.waiting[0][0], self.free):
            units, future = self.waiting.pop(0)
            self.free = tuple(f - u for f, u in zip(self.free, units))
            if not future.done():
                future.set_result(None)

    async def acquire(self, units: tuple) -> None:
        _refuse_oversize(units, self.total)
        future = asyncio.get_running_loop().create_future()
        self.waiting.append((units, future))
        self._wake()
        try:
            await future
        except BaseException:
            item = (units, future)
            if item in self.waiting:
                self.waiting.remove(item)
                self._wake()
            else:
                self.release(units)
            raise

    def release(self, units: tuple) -> None:
        self.free = tuple(f + u for f, u in zip(self.free, units))
        self._wake()




def _cancelled_by_caller() -> bool:
    """Whether a CancelledError being handled is THIS task's own cancellation -- the client left,
    or a timeout fired -- rather than one raised by an awaited object something else cancelled.

    Only the first is a reason to stop. The second is a bug of the shape incident 2 was, and
    letting it propagate is what turned it into a 500: Serve reads any CancelledError out of the
    replica as "client disconnected". `Task.cancelling()` exists from 3.11; before it, every
    CancelledError is taken as the caller's, which is the old behaviour."""
    task = asyncio.current_task()
    cancelling = getattr(task, "cancelling", None)
    return True if cancelling is None else bool(cancelling())


def _public(failure: InfraFailure) -> str:
    """What a caller is told about an infrastructure failure: the cause, one line, no traceback,
    pid or actor id. The whole of it is in the incident log."""
    lines = [line for line in (failure.detail or "").splitlines() if line.strip()]
    return (lines[0] if lines else failure.stage).removeprefix("unavailable:")[:1500]


def _fatal_path() -> str:
    """Where the gateway records that the service must go down; the driver polls it and exits."""
    return os.path.join(_LOCAL_ROOT, "serve-fatal")


def _signal(pid: int) -> None:
    import signal
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass


def _canary_dockerfile() -> str | None:
    """The canary runtime: ``ZERO_CANARY_DOCKERFILE``, else the minimal one shipped here. Point it
    at a runtime the deployment always has cached, so the self-check never depends on a pull."""
    path = os.environ.get("ZERO_CANARY_DOCKERFILE") or os.path.join(_HERE, "canary", "Dockerfile")
    try:
        with open(path, encoding="utf-8") as stream:
            return stream.read()
    except OSError:
        return None


def _canary_request() -> dict | None:
    """One tiny action whose correct answer is known: the service's own end-to-end probe."""
    text = _canary_dockerfile()
    if text is None:
        return None
    nonce = secrets.token_hex(8)
    return {"runtime": {"dockerfile": text, "documentation": "the sandbox's own liveness canary"},
            "resources": {"cpus": 0.25, "memory_gb": 0.25, "workdir_gb": 0.0625,
                          "workdir_medium": "temp", "gpus": 0, "pids": 64},
            "session_id": "zero-canary-" + nonce, "code": "echo zero-canary-" + nonce,
            "timeout_s": 60, "wait_s": 0, "io_cap_kb": 16}


def _kill(actor) -> None:
    """Best-effort teardown: retiring a cache record must not fail because Ray already went."""
    try:
        import ray
        ray.kill(actor, no_restart=True)
    except Exception:
        pass


def _status(error: Exception) -> tuple[int, dict]:
    text = str(error)
    code = (403 if text.startswith("forbidden:") else
            404 if text.startswith("not_found:") else
            409 if text.startswith("conflict:") else
            503 if text.startswith("unavailable:") else 400)
    return code, {"error": text}


def _gateway_class(cluster: dict[str, float]):
    """The gateway itself, undecorated: ``build_app`` binds it to Serve, tests construct it."""
    import ray
    from starlette.requests import Request
    from starlette.responses import JSONResponse

    SessionActor = ray.remote(max_restarts=0)(_SessionActor)
    CacheActor = ray.remote(num_cpus=0, max_restarts=0)(_CacheActor)
    RamLease = ray.remote(num_cpus=0, max_restarts=0)(_RamLease)

    class Gateway:
        def __init__(self):
            self.sessions: dict[str, _RemoteEntry] = {}
            self.session_locks: dict[str, asyncio.Lock] = {}
            self.session_users: dict[str, int] = {}
            self.caches: dict[tuple[str, str], asyncio.Task] = {}
            self.promotions: dict[tuple[str, str], asyncio.Task] = {}
            self.cache_info: dict[tuple[str, str], dict] = {}
            # A record is published with active=0 and the demand that caused it only counts as
            # active once `_new` has a session. Without this counter a trim landing in that gap
            # evicts an image a launch is about to use, which reads as "Unable to find image".
            self.demand: dict[tuple[str, str], int] = {}
            # image -> set when its removal finishes. A demand arriving mid-eviction waits, so a
            # fresh cache actor cannot list the doomed image and hand it back unbuilt.
            self.evicting: dict[str, asyncio.Event] = {}
            self.ram_lock = asyncio.Lock()
            self.ram_budget = _ram_budget(cluster)
            # Bounded, insertion-ordered: each entry holds a whole submitted Dockerfile (up to
            # MAX_DOCKERFILE_BYTES), and a long-lived gateway sees every runtime a curriculum ever
            # mints. The declarations stay on disk either way; this is only what /health can name.
            self.known: dict[str, Submitted] = {}
            self.known_limit = max(0, int(os.environ.get("ZERO_KNOWN_LIMIT", "512")))
            self.failures: dict[str, str] = {}
            self.workers: dict[tuple, list] = {}
            # Everything discovery answers from. Engine-derived state is refreshed by `_discover`
            # in a worker thread; nothing on the request path may reach the engine.
            self.disk_facts: dict[str, dict] = {}
            # What the ENGINE says about its own store. Refreshed by `_discover` off the request
            # path; `/health` reads it and never shells out. See `store_space`.
            self.store: dict = {"known": False, "free_bytes": 0, "total_bytes": 0, "root": "",
                                "reason": "not measured yet"}
            # Reported on /health, not only printed. A bare `print` from inside a Serve REPLICA
            # does not reach the driver's log in this deployment -- measured: the Serve access
            # logger does, four driver-side `[sandbox]` lines do, and zero replica prints ever did.
            # The reap line existed only in a per-worker file under the Ray session dir on
            # node-local /tmp, which dies with the job, so the one mechanism standing between this
            # service and the ENOSPC it was written for had no evidence anywhere an operator looks.
            self.reaped = 0
            self.writes: set = set()
            runtime()      # prime the lru_caches off the request path: both shell out once
            engine_cpus()
            # ADMISSION IS SIZED OFF THE ENGINE, NOT THE HOST. This is the same mistake the
            # build-slot comment below already records, on the other tier: sessions run wherever
            # the ENGINE runs, and behind a VM that is not the machine Ray measures. Sized off the
            # host, a 384-CPU node admits 383 concurrent sessions into a single VM's podman, which
            # cannot serve anywhere near that many container lifecycles -- it saturates, the engine
            # probe starts failing, and admitted work comes back
            # `unavailable:container:no working Podman/Docker runtime` instead of queueing.
            #
            # Measured on the production host 2026-08-31, 384-CPU host driving one VM:
            #     600 requests @ 600 concurrent   600 ok,   0 failed
            #    1500 requests @ 600 concurrent   902 ok, 598 failed (40%), sessions peaked 178
            # The flood is not too big; admitting all of it at once is. A queue is the correct
            # answer to a flood -- shedding 40% of it is not.
            _host = max(1.0, float(cluster.get("CPU", os.cpu_count() or 1)) - 1.0)
            _engine = float(engine_cpus() or 0.0)
            # AND MEMORY IS A SECOND CEILING, BECAUSE A SESSION IS A RAY WORKER PROCESS.
            # Admission bounded CPU and nothing else, so on a node with far more cores than
            # gigabytes it admits more sessions than the node can hold. Every admitted session is
            # a `_SessionActor`, i.e. a whole Python worker, and Ray's own OOM killer is what
            # reports the consequence -- measured on the production host 2026-09-09, 240-vCPU VM, downstream workload:
            #   Killing 167 worker(s): Memory on the node was 722.33GB / 754.97GB (0.956772)
            #   OOM kill reason: Memory usage 770570072064B exceeded threshold of 770110128128B
            #                    (95.0% of 810642264064B total)
            # twice inside six minutes. That is ~3.7 GB per live session at ~190 concurrent, and
            # 240 CPU-derived slots would need ~888 GB on a 755 GB node. The client saw it as
            # `unavailable:session actor failed`, which reads as a broken service rather than as
            # admitting more than the node can hold.
            #
            # CAPPING COSTS NOTHING HERE, WHICH IS WHY IT IS THE RIGHT LEVER. Measured completion
            # rate was flat at ~3.9/s from 16 to 1500 concurrent, so beyond a small number the
            # extra concurrency buys latency, not throughput -- the same reason the CPU bound
            # above exists. A queue is the correct answer to a flood.
            #
            # `ZERO_SESSION_MEM_GB` is the per-session estimate (3.7 measured; 4.0 for headroom)
            # and `ZERO_SANDBOX_MEM_FRACTION` how much of the node sessions may claim, kept under
            # Ray's own 95% kill threshold. Ray reports `memory` in bytes; if it reports nothing
            # this bound simply does not apply, exactly as before.
            #
            # AND IT MUST BE THE ENGINE'S MEMORY, NOT RAY'S. `cluster["memory"]` is the machine
            # the Ray gateway runs on. When the engine is a remote podman machine that is a
            # DIFFERENT machine, and the sessions live in the engine: measured on the production host
            # 2026-09-09, an 810 GB host admitted ~162 slots into a 16 GB VM, 45 arrived at once,
            # and podman wedged INSIDE the guest -- surfacing as `/execute` hanging indefinitely
            # while `/health`, which touches no engine, answered in 4 ms. `ZERO_ENGINE_MEM_MB` is
            # set by scripts/sandbox_vm.sh from the size it actually gave the machine; without it
            # the Ray figure is still used, which is correct for a local engine.
            # TWO MACHINES CONSTRAIN THIS AND BOTH MUST BIND, so it is a min, not a replacement.
            # A live session costs memory in two places: a `_SessionActor` Ray worker on the HOST
            # (~3.7 GB, the figure the OOM above measured) and a container inside the ENGINE. When
            # the engine is a remote podman machine those are different machines with wildly
            # different sizes, and bounding by either alone admits past the other.
            # EACH MACHINE IS BOUND IN ITS OWN UNIT (see `_AXES`): the HOST by one ~3.7 GB Ray
            # worker per session, the ENGINE by the sum of the containers' memory GRANTS. Both
            # used to be folded into one session count and then spent in CPUs.
            _per, _frac = _session_mem_bytes(), _mem_fraction()
            _ray_mem = float(cluster.get("memory", 0.0) or 0.0)
            _engine_mem_mb = float(os.environ.get("ZERO_ENGINE_MEM_MB", "0") or 0.0)
            _engine_bytes = _engine_mem_mb * (1 << 20) if _engine_mem_mb > 0 else _ray_mem
            _cpus = max(1.0, min(_host if _engine <= 0 else max(1.0, _engine),
                                 float(os.environ.get("ZERO_SANDBOX_SLOTS", 1e9))))
            _memory_gb = _engine_bytes * _frac / (1 << 30) if _engine_bytes > 0 else float("inf")
            _sessions = max(1.0, _ray_mem * _frac / _per) if _ray_mem > 0 else float("inf")
            self.capacity = _Capacity((_cpus, _memory_gb, _sessions))
            print(f"[sandbox] session admission: {_cpus:g} CPUs (host {_host:g}, engine "
                  f"{_engine:g}), {_memory_gb:.1f} GB of grants ("
                  f"{'engine ' + format(_engine_mem_mb / 1024, '.0f') + ' GB' if _engine_mem_mb > 0 else 'ray host'}"
                  f" x {_frac:g}), {_sessions:.0f} sessions (ray host at "
                  f"{_per / (1 << 30):g} GB/worker)", flush=True)
            # Builds were the one unbounded thing left. A cache actor costs Ray no CPU, so N cold
            # runtimes start N concurrent `docker build`s and each is allowed ZERO_BUILD_CPUS --
            # on the four-CPU acceptance host four at once took /health from 35 ms to 3.0 s,
            # because the replica simply stopped being scheduled. Every other tier here has a
            # budget; this gives builds one, sized from what a build is allowed to consume.
            builder = engine_cpus() or float(cluster.get("CPU", os.cpu_count() or 1))
            self.build_slots = max(1, int(os.environ.get("ZERO_BUILD_SLOTS", 0)) or int(
                max(1.0, float(builder))
                // max(1.0, float(os.environ.get("ZERO_BUILD_CPUS", "2")))))
            self.builds = asyncio.Semaphore(self.build_slots)
            self.started, self.served, self.trimmed = time.time(), 0, 0
            # Shared-secret gate. The service binds 0.0.0.0 on a shared cluster; without this any
            # user who reaches the port runs code on our grant or floods capacity until our own
            # checks come back `unavailable:`. Enforced only when set, so an unset token leaves the
            # dev/test behaviour unchanged; a set token is required on every route but /health.
            self.auth_token = (os.environ.get("ZERO_SANDBOX_TOKEN") or "").strip() or None
            for sub in recall()[-self.known_limit:]:
                self.known[sub.key] = sub
            # THE ADMISSION LEDGER. Every grant taken from `capacity` is recorded under a token
            # and released by token, exactly once. Grants used to be released by whichever path
            # noticed the session ending, and twice (2026-09-12) a path that missed one leaked it
            # until admission could fit nothing while `/health` said 200 against an idle engine.
            # `_audit` releases a token whose owner is gone, so a leak repairs itself.
            self.grants: dict[int, tuple] = {}
            self.grant_seq = 0
            self.opening: set[int] = set()
            # Failure accounting, reported on /health and written to the incident log.
            self.infra = {"failures": 0, "retried": 0, "recovered": 0, "exhausted": 0,
                          "by_stage": {}, "last": None, "lost_sessions": 0, "leaks_repaired": 0}
            # Set once the service has decided it cannot serve: from then on nothing is answered
            # and the driver exits, so a broken service is DOWN rather than answering with errors.
            self.doomed: str | None = None
            self.canary_secret = os.environ.get("ZERO_CANARY_SECRET") or None
            self.selfcheck: asyncio.Task | None = None
            self.selfcheck_at, self.selfcheck_result = 0.0, (False, "never run")
            self.tasks = [asyncio.create_task(self._reap()),
                          asyncio.create_task(self._discover()),
                          asyncio.create_task(self._adopt()),
                          asyncio.create_task(self._audit())]

        async def _reap(self) -> None:
            """Off the request path: reap dead relays and never-started containers, and hold the
            disk tier to its budget."""
            while True:
                await asyncio.sleep(float(os.environ.get("ZERO_SWEEP_S", "300")))
                await asyncio.to_thread(_sweep_networks)
                # Drains leaks that predate this process, which is the only way a store already
                # full of `Created` containers recovers without a restart.
                try:
                    self.reaped += await asyncio.to_thread(_sweep_created)
                except Exception as error:
                    print(f"[sandbox] created-container sweep skipped: {error}", flush=True)
                try:
                    if await self._trim_disk():
                        await self._refresh()
                except Exception as error:
                    print(f"[sandbox] disk trim skipped: {error}", flush=True)

        async def _discover(self) -> None:
            """Keep gateway-owned discovery facts fresh WITHOUT touching the request path.

            One replica serves every execution on one event loop, so a synchronous engine call
            inside `/health` stalls all of them: measured 4-8 s idle on the four-CPU acceptance
            host and past a 10 s client deadline under an execution flood.
            """
            period = float(os.environ.get("ZERO_DISCOVERY_REFRESH_S", "10"))
            while True:
                try:
                    # BEFORE `_refresh`, which returns early when nothing is known yet: a store
                    # that cannot take a container must be visible on a gateway that has never
                    # been asked for a runtime.
                    self.store = await asyncio.to_thread(store_space)
                    await self._refresh()
                except Exception as error:
                    print(f"[sandbox] discovery refresh skipped: {error}", flush=True)
                await asyncio.sleep(period)

        async def _refresh(self) -> None:
            """Resolve every remembered runtime's cache tier in ONE worker-thread pass."""
            digests = list(self.known)
            if not digests:
                self.disk_facts = {}
                return
            facts = await asyncio.to_thread(
                lambda: {digest: _cache_state(digest) for digest in digests})
            self.disk_facts = {digest: state for digest, state in facts.items()
                               if digest in self.known}

        async def _adopt(self) -> None:
            """Re-attach to images this node already staged, after the replica is answering."""
            await self._refresh()
            for digest, state in list(self.disk_facts.items()):
                sub = self.known.get(digest)
                if state["tier"] == "ram" and sub is not None and (digest, "") not in self.caches:
                    asyncio.create_task(self._cache(sub, None))

        def _pinned(self) -> set[str]:
            """Images something is using RIGHT NOW. A trim may never touch these."""
            pinned = {_tag(entry.runtime_hash) for entry in self.sessions.values()}
            for key, task in self.caches.items():
                info = self.cache_info.get(key)
                if info is None or not task.done():
                    pinned.add(_tag(key[0]))         # a build still in flight
                elif (info["active"] or info["ram_bytes"] or key in self.promotions
                      or self.demand.get(key)):
                    pinned.add(info["image"])        # a session, lease, promotion or a caller
            return pinned

        def _release(self, images: set[str]) -> None:
            """Drop the cache records naming images this trim is about to evict.

            A record protects its image, so a completed one used to pin it for the whole gateway
            lifetime and ZERO_DISK_IMAGE_CACHE could evict nothing at all. Releasing is driven by
            the TIER BEING FULL and never by elapsed time: serving is time-sensitive, an idle
            record on a large node costs nothing, and dropping it would only buy a rebuild for the
            next request that wants it. It is also not optional -- a record outliving its image
            hands the next session a `cached_image` the engine no longer has.
            """
            for key, info in list(self.cache_info.items()):
                if info["image"] not in images:
                    continue
                self.caches.pop(key, None)
                self.cache_info.pop(key, None)
                _kill(info["actor"])

        async def _trim_disk(self) -> bool:
            """Evict the oldest images beyond ``ZERO_DISK_IMAGE_CACHE``.

            An unbounded disk tier is what makes discovery slow: an `images` listing costs time
            linear in the images it matches. Anything a session or a cache entry is using is
            protected, so this only removes what nothing refers to any more.
            """
            keep = int(os.environ.get("ZERO_DISK_IMAGE_CACHE", "64"))
            engine = runtime()
            if engine is None or keep <= 0:
                return False
            images = await asyncio.to_thread(_owned_images, engine)
            # AN IMAGE COUNT IS NOT A DISK BUDGET. `ZERO_DISK_IMAGE_CACHE` keeps N images whatever
            # they weigh: 64 of them at 8 GB is 512 GB. On a remote engine the store is a FIXED VM
            # disk, so that overruns into a dead VM rather than a full directory -- measured
            # 2026-08-30, a 24-Dockerfile flood grew the qcow2 to 140 of its 144 GiB and the VM
            # died mid-run, which surfaced as `session actor failed` and a store probe that could
            # not connect. When the engine reports the store low, trim harder than the count says.
            facts = await asyncio.to_thread(store_space, engine)
            if facts["known"] and facts["free_bytes"] < store_target_free(facts["total_bytes"]):
                keep = max(1, min(keep, len(images) // 2))
                print(f"[sandbox] store low ({facts['free_bytes'] // 2**30} GiB free of "
                      f"{facts['total_bytes'] // 2**30}); trimming to {keep} image(s)", flush=True)
            victims = _disk_victims(images, keep, self._pinned())
            if not victims:
                return False
            self._release(set(victims))
            removed = asyncio.Event()
            self.evicting.update({name: removed for name in victims})
            try:
                await asyncio.to_thread(_remove_images, engine, victims)
            finally:
                for name in victims:
                    self.evicting.pop(name, None)
                removed.set()
            self.trimmed += len(victims)
            print(f"[sandbox] disk tier: removed {len(victims)} image(s) beyond keep={keep}",
                  flush=True)
            return True

        async def _cache(self, sub: Submitted, image: str | None) -> dict:
            key = sub.digest, image or ""
            if (removed := self.evicting.get(image or _tag(sub.digest))) is not None:
                await removed.wait()     # its image is being deleted; build again after
            self.demand[key] = self.demand.get(key, 0) + 1
            try:
                return await self._cached(sub, image, key)
            finally:
                # `_new` increments `active` with no await after this returns, so the image is
                # pinned continuously from the demand to the session that holds it.
                self.demand[key] -= 1
                if not self.demand[key]:
                    self.demand.pop(key, None)

        async def _ensure(self, actor, sub: Submitted, image: str | None) -> dict:
            """Build under a slot. A WARM lookup never reaches here, so the bound costs it
            nothing; only work that may actually shell out to a builder queues."""
            async with self.builds:
                return await actor.ensure.remote(sub, image)

        async def _cached(self, sub: Submitted, image: str | None, key: tuple) -> dict:
            """The cache record for ``key``, loading it once for every concurrent caller.

            THE LOAD IS SHARED, SO NO CALLER MAY CANCEL IT. It is one Task awaited by every
            request for the runtime, and an un-shielded `await task` propagates the AWAITER'S
            cancellation into the task itself. Measured 2026-10-15 (incident 2, reproduced on a
            test instance): one client disconnecting during a cold load cancelled the shared
            load, `except Exception` does not catch CancelledError, so the cancelled task stayed
            in `self.caches` -- and every later request for that runtime re-awaited it, raised
            CancelledError in ~2 ms, Serve logged it as a client disconnect (499) and uvicorn
            answered the still-connected caller `500 Internal Server Error`, while /health said
            200, until a restart. The downstream workload sent every check's second request to
            the same runtime, so every second request of every check failed.

            A load that FAILS is retried a bounded number of times: a build or a cache-worker
            death is usually transient. One that keeps failing is reported -- a runtime that
            cannot be built is a property of its Dockerfile, and taking the service down for it
            would let any one Dockerfile stop everyone."""
            tries = max(1, int(os.environ.get("ZERO_BUILD_ATTEMPTS", "3")))
            for attempt in range(1, tries + 1):
                task = self.caches.get(key)
                if task is None or (task.done() and (task.cancelled() or task.exception())):
                    task = self.caches[key] = asyncio.create_task(self._load(sub, image, key))
                try:
                    info = await asyncio.shield(task)
                except asyncio.CancelledError:
                    if _cancelled_by_caller():
                        raise                     # this caller left; the load carries on
                    if self.caches.get(key) is task:
                        self.caches.pop(key, None)
                    error: BaseException = InfraFailure("cache", "the shared load was cancelled")
                except Exception as failed:
                    error = failed
                    if self.caches.get(key) is task:
                        self.caches.pop(key, None)
                else:
                    self.failures.pop(sub.key, None)
                    info["last_used"] = time.monotonic()
                    if (not info["ram_bytes"] and self.ram_budget and
                            key not in self.promotions):
                        self.promotions[key] = asyncio.create_task(
                            self._promote(key, f"zero-cache-{key[0][:16]}-" +
                                          hashlib.sha256(key[1].encode()).hexdigest()[:12],
                                          info["actor"], info))
                    return info
                detail = str(error).strip().splitlines()
                self.failures[sub.key] = detail[-1][:500] if detail else type(error).__name__
                incident("cache-load-failed", runtime=sub.digest, image=image or "",
                         attempt=attempt, of=tries, error=f"{type(error).__name__}: {error}")
                if attempt < tries:
                    await asyncio.sleep(_backoff(attempt))
            # FINAL: the attempt loop does not rebuild again, it asks the self-check whether the
            # service or this runtime is broken (`_exhausted`).
            raise InfraFailure("cache", "runtime is not available: " + self.failures[sub.key],
                               final=True)

        async def _load(self, sub: Submitted, image: str | None, key: tuple) -> dict:
            suffix = hashlib.sha256(key[1].encode()).hexdigest()[:12]
            name = f"zero-cache-{key[0][:16]}-{suffix}"
            actor = CacheActor.options(name=name, lifetime="detached",
                                       get_if_exists=True).remote()
            info = await self._ensure(actor, sub, image)
            # The build happened in the cache actor's process; this one's image listing
            # still predates it, and /health would report a fresh image as cold.
            _forget()
            info.update(actor=actor, lease=None, active=0, last_used=time.monotonic())
            self.cache_info[key] = info
            if info["ram_bytes"]:
                if await self._make_room(key, info["ram_bytes"]):
                    info["lease"] = await self._lease(name, info)
                elif await actor.evict.remote(info["image"]):
                    info["ram_bytes"] = 0
            return info

        async def _evict_ram(self, key: tuple[str, str]) -> bool:
            info = self.cache_info.get(key)
            if not info or not info["ram_bytes"] or info["active"] or key in self.promotions:
                return False
            if not await info["actor"].evict.remote(info["image"]):
                return False
            if info.get("lease") is not None:
                ray.kill(info["lease"], no_restart=True)
            info.update(ram_bytes=0, ram_store="", lease=None)
            return True

        async def _make_room(self, key: tuple[str, str], needed: int) -> bool:
            async with self.ram_lock:
                while True:
                    victims = _ram_victims(self.cache_info, key, needed,
                                           self.ram_budget, set(self.promotions))
                    if victims == []:
                        return True
                    if victims is None or not await self._evict_ram(victims[0]):
                        return False

        async def _lease(self, name: str, info: dict):
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
            lease = RamLease.options(
                name=name + "-ram", lifetime="detached", get_if_exists=True,
                memory=info["ram_bytes"] or info["bytes"],
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    info["node_id"], soft=False)).remote()
            await lease.ready.remote()
            return lease

        async def _promote(self, key: tuple[str, str], name: str,
                           actor, info: dict) -> None:
            lease = None
            try:
                if not await self._make_room(key, info["bytes"]):
                    return
                lease = await self._lease(name, info)
                size = await actor.stage.remote(info["image"])
                if not size:
                    ray.kill(lease, no_restart=True)
                    return
                info.update(ram_bytes=size, ram_store=_ram_store(info["image"]), lease=lease)
            except Exception as error:
                if lease is not None:
                    ray.kill(lease, no_restart=True)
                print(f"[sandbox] RAM promotion skipped for {info['image']}: {error}",
                      flush=True)
            finally:
                self.promotions.pop(key, None)

        async def _acquire(self, units: tuple) -> int:
            """Take a grant from admission and record it in the ledger, under a fresh token."""
            await self.capacity.acquire(units)
            self.grant_seq += 1
            self.grants[self.grant_seq] = units
            return self.grant_seq

        def _release_grant(self, token: int | None) -> None:
            """Release a ledger token. Idempotent: a token is returned to admission exactly once,
            however many paths notice that its session ended."""
            units = self.grants.pop(token, None) if token is not None else None
            if units is not None:
                self.capacity.release(units)

        async def _drop(self, sid: str, entry: _RemoteEntry, healthy: bool = True) -> None:
            """End a session. ``healthy=False`` -- the worker just failed -- never returns it to
            the pool: a retry must land on fresh resources, not on the worker that failed."""
            if self.sessions.get(sid) is entry:
                self.sessions.pop(sid, None)
            elif not healthy and entry.grant in self.grants:
                pass                          # an attempt that failed before it was registered
            else:
                return
            if entry.expiry and entry.expiry is not asyncio.current_task():
                entry.expiry.cancel()
            # The session is popped above, so nothing else will ever release its grant: that
            # has to happen whatever the close does. A client timing out during it cancels this
            # with CancelledError, which `except Exception` missed -- one lost slot per timeout,
            # until admission (19.2 slots on the production host) could fit nothing and every request waited
            # out its client deadline against an idle engine (2026-09-12).
            closed = False
            try:
                if healthy:
                    await asyncio.wait_for(entry.actor.close.remote(), 15)
                    closed = True
            except Exception:
                pass
            finally:
                if closed:
                    pool = self.workers.setdefault(entry.worker_key, [])
                    token = object()
                    pool.append((entry.actor, token))
                    asyncio.create_task(self._retire(entry.worker_key, entry.actor, token))
                else:
                    _kill(entry.actor)
                if info := self.cache_info.get(entry.cache_key):
                    info["active"] = max(0, info["active"] - 1)
                self._release_grant(entry.grant)

        async def _retire(self, key: tuple, actor, token: object) -> None:
            await asyncio.sleep(float(os.environ.get("ZERO_WORKER_CACHE_S", "2")))
            pool = self.workers.get(key, [])
            item = (actor, token)
            if item not in pool:
                return
            pool.remove(item)
            if not pool:
                self.workers.pop(key, None)
            ray.kill(actor, no_restart=True)

        async def _expire(self, sid: str, entry: _RemoteEntry, generation: int,
                          seconds: float) -> None:
            await asyncio.sleep(seconds)
            async with entry.lock:
                if entry.generation == generation:
                    await self._drop(sid, entry)
            if not self.session_users.get(sid) and sid not in self.sessions:
                self.session_users.pop(sid, None)
                self.session_locks.pop(sid, None)

        async def _new(self, sub: Submitted, request: dict, lock: asyncio.Lock,
                       canary: bool = False) -> _RemoteEntry:
            """Admit, cache and open a session on a FRESH or pooled worker.

            Raises ValueError for a request error and `InfraFailure` for anything else, with the
            grant released and the worker killed -- never a worker that failed back into the
            pool. The canary (the service's own liveness probe) takes no admission grant: it must
            measure the execution path, not wait behind the queue it is checking."""
            resources = request["resources"]
            units = _grant(resources)
            _refuse_oversize(units, self.capacity.total, prefix="bad_request:")
            if resources.gpus > float(cluster.get("GPU", 0)):
                raise ValueError(f"bad_request:session requests {resources.gpus} GPUs; "
                                 f"cluster has {float(cluster.get('GPU', 0)):g}")
            token = None if canary else await self._acquire(units)
            if token is not None:
                self.opening.add(token)
            actor, cache, cache_key = None, None, (sub.digest, request.get("image") or "")
            try:
                build_started = time.monotonic()
                cache = await self._cache(sub, request.get("image"))
                build_ms = int((time.monotonic() - build_started) * 1000)
                cache["active"] += 1
                memory = resources.memory_gb + (
                    resources.workdir_gb if resources.workdir_medium == "ram" else 0)
                from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
                options = {"num_cpus": resources.cpus, "num_gpus": resources.gpus,
                           "memory": int(memory * 2**30),
                           "scheduling_strategy": NodeAffinitySchedulingStrategy(
                               cache["node_id"], soft=False)}
                # NOT keyed by runtime. `open` closes the actor's previous session and builds a
                # new one, so only what Ray fixed at creation -- node, CPUs, GPUs, memory -- has
                # to match. Keying by digest meant a second runtime never hit the pool and paid a
                # fresh Python process: measured p50 1636 ms against 212 ms for a pooled actor.
                worker_key = (cache["node_id"], tuple(sorted(resources.wire().items())))
                pool = self.workers.get(worker_key, [])
                actor = pool.pop()[0] if pool else SessionActor.options(**options).remote()
                if not pool:
                    self.workers.pop(worker_key, None)
                # BOUNDED: a worker Ray cannot place, or one wedged in its own startup, used to
                # hold the request (and its grant) forever.
                opened = float(os.environ.get("ZERO_OPEN_TIMEOUT_S", "600"))
                container = "zero-session-" + secrets.token_hex(12)
                try:
                    outcome = await asyncio.wait_for(actor.open.remote(
                        sub, resources, request["session_id"], request.get("image"),
                        cache["image"], cache["ram_store"], request["network"],
                        request["environment"], request["config_hash"], container), opened)
                except asyncio.TimeoutError:
                    raise InfraFailure("open", f"the session worker did not open within "
                                       f"{opened:g}s") from None
                except asyncio.CancelledError:
                    if _cancelled_by_caller():
                        raise
                    raise InfraFailure("open", "a stray cancellation reached the open") from None
                except InfraFailure:
                    raise
                except Exception as error:   # the worker died, or Ray could not reach it
                    raise InfraFailure("open", f"{type(error).__name__}: {error}") from None
                if "error" in outcome:
                    raise ValueError(outcome["error"])
                if "infra" in outcome:
                    raise InfraFailure(**outcome["infra"])
            except BaseException as error:
                # BaseException, not Exception: a client that drops mid-open cancels this with
                # CancelledError, and if that path does not release the CPU grant the FIFO admission
                # wedges (Gateway accepts but never dispatches another container).
                if actor is not None:
                    _kill(actor)
                if cache is not None:
                    cache["active"] = max(0, cache["active"] - 1)
                if token is not None:
                    self.opening.discard(token)
                    self._release_grant(token)
                if isinstance(error, asyncio.CancelledError) and not _cancelled_by_caller():
                    raise InfraFailure("open", "a stray cancellation reached the open") from None
                raise
            entry = _RemoteEntry(sub.digest, request.get("image"), actor, lock,
                                 worker_key, units, request["config_hash"],
                                 cache_key, build_ms=build_ms, grant=token, container=container)
            # Registered BEFORE the token leaves `opening`, with no await between: the ledger
            # audit must always find an owner for a held token.
            self.sessions[request["session_id"]] = entry
            if token is not None:
                self.opening.discard(token)
            return entry

        async def _remember(self, sub: Submitted) -> None:
            """Persist a declaration off the request path. Discovery, not execution, needs it."""
            try:
                await asyncio.to_thread(remember, sub)
            except Exception as error:
                print(f"[sandbox] declaration not persisted: {error}", flush=True)

        async def _execute(self, raw: dict, canary: bool = False) -> tuple[int, dict]:
            sub, request = _request(raw)
            # Reinsert either way, so the trim below sees a real least-recently-demanded order.
            # The digest already fixes the Dockerfile, so only the documentation can have moved.
            seen = self.known.pop(sub.key, None)
            self.known[sub.key] = sub
            if seen is None or seen.documentation != sub.documentation:
                # A 256 KiB declaration written through the event loop stalls every other
                # execution the replica is serving; the request does not depend on the write.
                write = asyncio.create_task(self._remember(sub))
                self.writes.add(write)
                write.add_done_callback(self.writes.discard)
            # Oldest first, and unconditionally: an entry that happened to be cached used to be
            # moved to the end and the loop broken, so the map stayed over the limit for good.
            # This is only what discovery can NAME -- every declaration is still on disk.
            while len(self.known) > self.known_limit:
                stale = next(iter(self.known))
                self.known.pop(stale)
                self.disk_facts.pop(stale, None)
            # Failure text is keyed by declaration and is reported beside it, so the same bound
            # has to reach it: a digest trimmed before its build failed would otherwise stay.
            for stale in [key for key in self.failures if key not in self.known]:
                self.failures.pop(stale, None)
            sid = request["session_id"]
            lock = self.session_locks.setdefault(sid, asyncio.Lock())
            self.session_users[sid] = self.session_users.get(sid, 0) + 1
            try:
                async with lock:
                    return await self._attempts(sub, request, sid, lock, canary)
            finally:
                self.session_users[sid] -= 1
                if not self.session_users[sid] and sid not in self.sessions:
                    self.session_users.pop(sid)
                    self.session_locks.pop(sid, None)

        async def _attempts(self, sub: Submitted, request: dict, sid: str, lock: asyncio.Lock,
                            canary: bool) -> tuple[int, dict]:
            """Run one action, retrying INFRASTRUCTURE failures on fresh resources.

            A session this request opens holds nothing of the caller's yet, so an attempt that
            failed before a result existed (see `InfraFailure`) is simply made again -- fresh
            worker, fresh container, through admission again -- with backoff, up to
            ZERO_INFRA_ATTEMPTS / ZERO_INFRA_RETRY_S. Past that bound `_exhausted` decides
            between "this request cannot run" and "the service cannot run", and in the second
            case the service goes down instead of answering. A RESULT is never retried, whatever
            it says; a request error is never retried. A session that already held state cannot
            be rebuilt elsewhere, so its loss is reported as such (`not_found:`)."""
            entry = self.sessions.get(sid)
            existed = entry is not None
            if existed and (entry.runtime_hash, entry.image, entry.config_hash) != (
                    sub.digest, request.get("image"), request["config_hash"]):
                raise ValueError(
                    "conflict:session_id already belongs to another execution config")
            attempts = _infra_attempts()
            give_up = time.monotonic() + float(os.environ.get("ZERO_INFRA_RETRY_S", "1800"))
            attempt, verified = 0, False
            while True:
                attempt += 1
                try:
                    try:
                        if entry is None:
                            entry = await self._new(sub, request, lock, canary=canary)
                        if entry.expiry:
                            entry.expiry.cancel()
                        action = {**request, "build_ms": entry.build_ms}
                        entry.build_ms = 0        # only the action that built it pays the build
                        result = await self._act(entry, action)
                    except (InfraFailure, ValueError, asyncio.CancelledError):
                        raise
                    except Exception as error:     # a gateway bug: no result exists either
                        import traceback
                        raise InfraFailure("gateway", f"{type(error).__name__}: {error}\n"
                                           f"{traceback.format_exc()}", started=None) from None
                except InfraFailure as failure:
                    self._failed(failure, attempt, attempts, sub, sid, existed, canary)
                    if entry is not None:
                        await asyncio.shield(self._drop(sid, entry, healthy=False))
                        if failure.started is not False and entry.container:
                            # The worker may be gone with its container still running the
                            # command: reap it now rather than at its deadline (~22 min).
                            self._reap_container(entry.container)
                        entry = None
                    if existed:
                        self.infra["lost_sessions"] += 1
                        raise ValueError(_lost(sid, failure)) from None
                    if failure.final or attempt >= attempts or time.monotonic() >= give_up \
                            or verified:
                        # Returns only when the self-check PASSED. The infrastructure may have
                        # recovered while it ran, so the request gets one more attempt; failing
                        # again right after a canary succeeded is what makes it request-specific.
                        await self._exhausted(failure, sub, sid, canary, verified)
                        verified = True
                    self.infra["retried"] += 1
                    await asyncio.sleep(_backoff(attempt))
                    continue
                except ValueError:
                    if entry is not None:
                        await asyncio.shield(self._drop(sid, entry))
                    raise
                except BaseException:
                    # Client dropped/timed out -> CancelledError. Drop the session so its grant
                    # is released; shield so the drop finishes despite the cancellation.
                    if entry is not None:
                        await asyncio.shield(self._drop(sid, entry))
                    raise
                if attempt > 1:
                    self.infra["recovered"] += 1
                    incident("recovered", attempt=attempt, runtime=sub.digest, session_id=sid,
                             canary=canary)
                if not canary:
                    self.served += 1
                if not result["alive"]:
                    await self._drop(sid, entry)
                else:
                    entry.generation += 1
                    entry.expiry = asyncio.create_task(self._expire(
                        sid, entry, entry.generation, request["wait_s"]))
                return 200, result

        def _reap_container(self, name: str) -> None:
            engine = runtime()
            if engine:
                task = asyncio.create_task(asyncio.to_thread(_rm, engine, name))
                self.writes.add(task)
                task.add_done_callback(self.writes.discard)

        async def _act(self, entry: _RemoteEntry, request: dict) -> dict:
            """One action on an open session. Every way it can end that is not a result or a
            request error is an `InfraFailure`; the worker's own report says whether the command
            had started."""
            limit = (request["timeout_s"] + float(os.environ.get("ZERO_LAUNCH_TIMEOUT_S", "600"))
                     + float(os.environ.get("ZERO_ACT_SLACK_S", "600")))
            try:
                outcome = await asyncio.wait_for(entry.actor.execute.remote(request), limit)
            except asyncio.TimeoutError:
                raise InfraFailure("execute", f"the session worker did not answer within "
                                   f"{limit:g}s", started=None) from None
            except asyncio.CancelledError:
                if _cancelled_by_caller():
                    raise
                raise InfraFailure("execute", "a stray cancellation reached the action",
                                   started=None) from None
            except Exception as error:      # the worker process died (OOM kill, crash, node)
                raise InfraFailure("worker", f"{type(error).__name__}: {error}",
                                   started=None) from None
            if "result" in outcome:
                return outcome["result"]
            if "error" in outcome:
                raise ValueError(outcome["error"])
            raise InfraFailure(**outcome["infra"])

        def _failed(self, failure: InfraFailure, attempt: int, attempts: int, sub: Submitted,
                    sid: str, existed: bool, canary: bool) -> None:
            self.infra["failures"] += 1
            stages = self.infra["by_stage"]
            stages[failure.stage] = stages.get(failure.stage, 0) + 1
            self.infra["last"] = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                                  "stage": failure.stage,
                                  "detail": failure.detail.splitlines()[0][:500]
                                  if failure.detail else ""}
            incident("infra-failure", stage=failure.stage, started=failure.started,
                     final=failure.final, attempt=attempt, of=attempts, runtime=sub.digest,
                     session_id=sid, existing_session=existed, canary=canary,
                     detail=failure.detail)

        async def _exhausted(self, failure: InfraFailure, sub: Submitted, sid: str,
                             canary: bool, verified: bool) -> None:
            """The retry bound is spent. Either the service is broken -- then it goes DOWN and
            this request is never answered -- or the self-check (the canary runtime, through the
            same path) passes, and the request is given one more attempt (returns). A request
            that fails again right after a passing self-check (``verified``) is the one that is
            broken -- its runtime cannot run -- and that is reported with its real cause."""
            if canary:
                self.infra["exhausted"] += 1
                raise ValueError("unavailable:canary failed: " + _public(failure))
            ok, why = await self._self_checked(fresh=verified)
            if not ok:
                self.infra["exhausted"] += 1
                await self._doom(f"request {sid} ({sub.digest[:16]}) failed every attempt "
                                 f"({failure.stage}: {_public(failure)}) and the self-check "
                                 f"failed too: {why}")
            if not verified:
                return
            self.infra["exhausted"] += 1
            incident("request-failure-isolated", runtime=sub.digest, session_id=sid,
                     stage=failure.stage, selfcheck=why, detail=failure.detail)
            raise ValueError("unavailable:" + _public(failure))

        async def _self_checked(self, fresh: bool = False) -> tuple[bool, str]:
            """The self-check, single-flight: an outage exhausts many requests at once, and they
            share one answer for ZERO_SELFCHECK_TTL_S. ``fresh`` refuses an answer older than the
            caller's last attempt -- a verdict on a request must postdate its failure."""
            ttl = 0.0 if fresh else float(os.environ.get("ZERO_SELFCHECK_TTL_S", "30"))
            if self.selfcheck is None or (self.selfcheck.done() and
                                          time.monotonic() - self.selfcheck_at > ttl):
                self.selfcheck_at = time.monotonic()
                self.selfcheck = asyncio.create_task(self._self_check())
            return await asyncio.shield(self.selfcheck)

        async def _self_check(self) -> tuple[bool, str]:
            """Run the canary through the real execution path (admission excepted)."""
            raw = _canary_request()
            if raw is None:
                return False, "no canary runtime is readable"
            budget = float(os.environ.get("ZERO_SELFCHECK_TIMEOUT_S", "900"))
            try:
                status, body = await asyncio.wait_for(self._execute(raw, canary=True), budget)
            except asyncio.TimeoutError:
                result = (False, f"the canary did not finish within {budget:g}s")
            except Exception as error:
                result = (False, f"{type(error).__name__}: {error}")
            else:
                expect = raw["code"].split()[-1]
                ok = status == 200 and body.get("returncode") == 0 and expect in body.get(
                    "stdout", "")
                result = (ok, "canary ran" if ok else f"canary returned {status}: "
                          f"rc={body.get('returncode')} stdout={body.get('stdout', '')[:200]!r}")
            self.selfcheck_result = result
            incident("self-check", ok=result[0], why=result[1])
            return result

        async def _doom(self, reason: str) -> None:
            """Take the service DOWN. Never returns: this request, and every request from now on,
            gets no answer, and the driver -- told through the fatal file, and by signal if it
            does not act -- exits, which closes the port. The supervisor starts it again."""
            if self.doomed is None:
                self.doomed = reason
                incident("shutdown-requested", reason=reason)
                try:
                    with open(_fatal_path(), "w", encoding="utf-8") as stream:
                        stream.write(reason + "\n")
                except OSError as error:
                    incident("fatal-file-not-written", error=repr(error))
                pid = os.environ.get("ZERO_SERVE_DRIVER_PID", "")
                if pid.isdigit():
                    asyncio.get_running_loop().call_later(
                        float(os.environ.get("ZERO_DOOM_SIGNAL_S", "30")),
                        lambda: _signal(int(pid)))
            await asyncio.Event().wait()

        async def _audit(self) -> None:
            """Release ledger tokens whose owner is gone -- a grant no session and no open holds,
            seen orphaned on two passes in a row. Leaked grants are what wedged admission."""
            period = float(os.environ.get("ZERO_AUDIT_S", "30"))
            suspects: dict[int, float] = {}
            while True:
                await asyncio.sleep(period)
                try:
                    live = {e.grant for e in self.sessions.values()} | self.opening
                    for token in list(suspects):
                        if token not in self.grants or token in live:
                            suspects.pop(token, None)
                    for token in list(self.grants):
                        if token in live:
                            continue
                        if token in suspects:
                            units = self.grants.get(token)
                            self._release_grant(token)
                            suspects.pop(token, None)
                            self.infra["leaks_repaired"] += 1
                            incident("grant-leak-repaired", token=token, units=units)
                        else:
                            suspects[token] = time.monotonic()
                except Exception as error:
                    incident("audit-failed", error=repr(error))

        def _facts(self, key: str, sessions: dict[str, int]) -> dict:
            """Gateway-owned facts ONLY. Engine state is refreshed by `_discover`, off the path."""
            sub = self.known[key]
            task = self.caches.get((key, ""))
            done = task is not None and task.done() and not task.cancelled()
            info = (task.result() if done and task.exception() is None else None)
            state = ({"image": info["image"],
                      "tier": "ram" if info["ram_bytes"] else "local",
                      "bytes": info["ram_bytes"] or info["bytes"]}
                     if info is not None else
                     self.disk_facts.get(key) or {"image": _tag(key), "tier": "none", "bytes": 0})
            # AVAILABLE MEANS THE ENGINE CAN ACTUALLY TAKE A CONTAINER. `runtime() is not None`
            # only proves the binary answers `version`: on the production host 2026-08-30 /health certified
            # every runtime available while the VM's store was full and not one launch succeeded.
            low = self.store["known"] and self.store["free_bytes"] < store_min_free()
            return {**sub.spec.wire_facts(
                        available=runtime() is not None and not low,
                        why=(f"container store is full: {self.store['free_bytes']} bytes free of "
                             f"{self.store['total_bytes']} at {self.store['root']}") if low
                        else state["tier"] if state["tier"] != "none"
                        else runtime_unavailable_reason() or "builds on first use"),
                    "hash": key, **state,
                    "build": ("failed" if key in self.failures else
                              "cached" if state["tier"] != "none" else
                              "building" if task is not None and not done else "cold"),
                    "cache_actor_node": info["node_id"] if info is not None else None,
                    "sessions": sessions.get(key, 0),
                    "failure": self.failures.get(key, ""),
                    "documentation": sub.documentation}

        async def __call__(self, request: Request):
            if self.doomed is not None:
                # Going down: nothing is answered, not even /health. The driver exits within
                # seconds and the dropped connection is the only reply -- "down", never an error.
                await asyncio.Event().wait()
            path = request.url.path.rstrip("/") or "/"
            # /health stays open for liveness probes; everything else requires the shared secret
            # when one is configured. compare_digest is constant-time.
            authorized = True
            if self.auth_token:
                sent = request.headers.get("authorization", "")
                authorized = (sent.startswith("Bearer ")
                              and secrets.compare_digest(sent[7:], self.auth_token))
            if not authorized and path != "/health":
                return JSONResponse({"error": "forbidden:missing or invalid sandbox token"},
                                    status_code=403)
            if path in ("/health", "/runtimes"):
                if request.method != "GET":
                    return JSONResponse({"error": "method_not_allowed:GET required"},
                                        status_code=405)
                # An open /health answered the full report: every runtime's digest, submitted
                # documentation and build-failure text, the store's host path -- i.e. /runtimes,
                # which IS gated, to anyone who could reach the port. Without the token it now
                # says only that the service is up.
                if not authorized:
                    return JSONResponse({"ok": True})
                # Counted once, not once per runtime: this was ZERO_KNOWN_LIMIT scans of the
                # whole session table per poll, and the flood polls discovery four times a second.
                counts: dict[str, int] = {}
                for entry in self.sessions.values():
                    counts[entry.runtime_hash] = counts.get(entry.runtime_hash, 0) + 1
                known = {digest: self._facts(digest, counts) for digest in sorted(self.known)}
                if path == "/runtimes":
                    return JSONResponse({"ok": True, "runtimes": known})
                ram = [info for info in self.cache_info.values() if info["ram_bytes"]]
                return JSONResponse({"ok": True, "runtimes": known,
                                     "sessions": len(self.sessions), "served": self.served,
                                     "cache": {"ram_budget_bytes": self.ram_budget,
                                               "ram_used_bytes": sum(
                                                   info["ram_bytes"] for info in ram),
                                               "ram_entries": len(ram)},
                                     "store": {**self.store, "min_free_bytes": store_min_free()},
                                     "reaped_created": self.reaped,
                                     "failures": dict(sorted(self.failures.items())),
                                     "infra": {**self.infra,
                                               "by_stage": dict(self.infra["by_stage"]),
                                               "grants_held": len(self.grants)},
                                     "selfcheck": {"ok": self.selfcheck_result[0],
                                                   "why": self.selfcheck_result[1],
                                                   "age_s": round(time.monotonic() -
                                                                  self.selfcheck_at, 1)
                                                   if self.selfcheck_at else None},
                                     "uptime_s": round(time.time() - self.started, 1)})
            if path != "/execute":
                return JSONResponse({"error": "not_found:route"}, status_code=404)
            if request.method != "POST":
                return JSONResponse({"error": "method_not_allowed:POST required"},
                                    status_code=405)
            try:
                try:
                    raw = await request.json()
                except Exception:
                    raise ValueError("bad_request:body must be JSON") from None
                # The driver's liveness probe proves itself with a per-process secret; it skips
                # admission (it measures the execution path, not the queue) and nothing else.
                sent = request.headers.get("x-zero-canary", "")
                canary = bool(self.canary_secret and sent and
                              secrets.compare_digest(sent, self.canary_secret))
                status, body = await self._execute(raw, canary=canary)
            except ValueError as error:
                status, body = _status(error)
            except asyncio.CancelledError:
                if _cancelled_by_caller():
                    raise
                # A cancellation nobody asked for reached the top: Serve would report it as a
                # client disconnect and the caller would get a bare 500 (incident 2). It is a
                # bug, and a service with that bug in it goes down rather than answering so.
                incident("stray-cancellation", path=path)
                await self._doom("a stray CancelledError reached the request handler")
            except Exception as error:
                import traceback
                incident("server-error", error=f"{type(error).__name__}: {error}",
                         traceback=traceback.format_exc())
                status, body = 500, {"error": f"server_error:{type(error).__name__}:{error}"}
            return JSONResponse(body, status_code=status)

    return Gateway


def _root_hint(detail: str) -> str:
    """When a launch fails because the engine cannot see the control mount, say why."""
    if "zero-control" in detail and ("no such file" in detail.lower() or "statfs" in detail.lower()):
        return (" -- the engine cannot see ZERO_LOCAL_ROOT (" + str(root()) + "); bind-mount "
                "sources resolve inside the engine (a podman machine shares $HOME, not /tmp), "
                "so set ZERO_LOCAL_ROOT to an engine-visible path")
    return ""


def _preflight_shared_root(engine: str) -> None:
    """Refuse to serve if the engine cannot see ``root()``.

    Bind-mount sources resolve inside the ENGINE, not the service. A local daemon on a cluster
    node shares the filesystem; a `podman machine`, a `DOCKER_HOST` over ssh, or a daemon in
    another mount namespace does not. When it does not, every session's control mount and every
    egress relay fails per request with a `statfs ...: no such file` from deep inside launch,
    which a consumer reads as a transient sandbox fault rather than a misconfiguration. This turns
    it into one clear message at startup.

    Best effort: it needs an image to mount into, and reuses the egress image the service builds
    anyway. If that image cannot be built or pulled -- an air-gapped node with no base image, say
    -- the probe is skipped with a warning rather than blocking a node that may only run
    non-egress work. It refuses ONLY when the mount provably does not appear.
    """
    probe = tempfile.mkdtemp(prefix="zero-preflight-", dir=str(root()))
    try:
        marker = secrets.token_hex(16)
        with open(os.path.join(probe, "marker"), "w", encoding="ascii") as stream:
            stream.write(marker)
        try:
            image = _egress_image(engine)
        except Exception as error:
            print(f"[sandbox] shared-root preflight skipped (no probe image): {error}", flush=True)
            return
        result = subprocess.run(
            _engine_argv(engine) + ["run", "--rm", "--network=none",
                                    "--volume", f"{probe}:/zprobe:ro",
                                    "--entrypoint", "/bin/sh", image, "-c", "cat /zprobe/marker"],
            capture_output=True, text=True, timeout=120, env=_engine_env())
        if result.stdout.strip() != marker:
            raise RuntimeError(
                "the container engine cannot see ZERO_LOCAL_ROOT (" + str(root()) + "): bind-mount "
                "sources resolve inside the engine, not the service, so every session control "
                "mount and egress relay would fail. Set ZERO_LOCAL_ROOT to a path the engine "
                "shares -- a podman machine shares $HOME, not /tmp -- or run the engine's daemon "
                "on the same filesystem as the service. Engine said: "
                + (result.stderr or result.stdout).strip().replace(chr(10), " ")[:300])
    finally:
        shutil.rmtree(probe, ignore_errors=True)


def _preflight_store(engine: str) -> None:
    """State what bounds the image/container store, and refuse a configuration that cannot.

    THE REMOTE-MODE LIMITATION, STATED OUT LOUD. `launch-server.sh` gives a LOCAL engine a
    compute-local build store plus a `/dev/shm` RAM tier that is a size-bounded LRU with a
    free-memory reserve. A remote engine owns storage outright -- `_remote_argv` refuses
    `--root`/`--storage-driver`, because they are invalid with `--remote` -- so none of that
    applies inside the VM: the store is the VM's disk, and the only thing bounding it is the
    image-count trim (`ZERO_DISK_IMAGE_CACHE`). Whoever provisions the VM owns its size.

    And a host RAM store under a remote engine is WORSE than none. `_store_argv` returns the same
    remote argv for every store, so `_stage` would `save` the image out of the VM and `load` it
    straight back INTO the same VM, then report a `ram` tier for an image that is on the VM's
    disk. A phantom cache tier is the silent-substitution shape this project has paid for four
    times; refuse it instead.
    """
    if _is_remote() and os.environ.get("ZERO_RAM_IMAGE_STORE"):
        raise RuntimeError(
            "ZERO_RAM_IMAGE_STORE is set with a remote engine (ZERO_PODMAN_URL/"
            "ZERO_PODMAN_REMOTE). A remote engine owns its storage, so the host tmpfs is "
            "invisible to it: staging would save the image out of the VM and load it back into "
            "the same VM, and every runtime would report a `ram` tier it does not have. Bound "
            "the VM's own storage where the VM is provisioned, and leave this unset.")
    facts = store_space(engine)
    if not facts["known"]:
        print(f"[sandbox] WARNING: the engine does not report store free space "
              f"({facts['reason']}); /health cannot tell a full store from a healthy one",
              flush=True)
        return
    print(f"[sandbox] store {facts['root']}: {facts['free_bytes'] / 2**30:.1f} GiB free of "
          f"{facts['total_bytes'] / 2**30:.1f} GiB"
          + (" (remote engine: the VM owns this store, and the /dev/shm RAM tier does NOT apply "
             "inside it -- size it where the VM is provisioned)" if _is_remote() else ""),
          flush=True)
    if facts["free_bytes"] < store_min_free():
        print(f"[sandbox] WARNING: store free space is below ZERO_STORE_MIN_FREE_GB; every "
              f"runtime will report available=false until it drains", flush=True)


def build_app():
    import ray
    if runtime() is None:
        raise RuntimeError(runtime_unavailable_reason())
    from ray import serve
    try:
        cluster = ray.cluster_resources()
    except Exception:
        cluster = {}
    # Above this Serve answers 503 BackPressure itself -- an infrastructure error for a valid
    # request. A waiting request is one coroutine in admission's FIFO, so the bound is memory,
    # not engine load; admission is what bounds the engine.
    ongoing = int(os.environ.get("ZERO_MAX_ONGOING_REQUESTS", "65536"))
    gateway = serve.deployment(num_replicas=1, max_ongoing_requests=ongoing,
                               ray_actor_options={"num_cpus": 1})(_gateway_class(cluster))
    return gateway.bind()


class _Liveness(threading.Thread):
    """The DEEP liveness check, from the driver, through the real front door.

    Incident 2 (2026-10-15) was a service whose /health answered 200 while every /execute for a
    runtime got a bare 500, for over ten minutes, until a human redeployed it. /health proves the
    replica's event loop answers; it says nothing about the path a request takes. This probe IS a
    request: an /execute of the canary runtime through Serve's proxy, the gateway, a session
    worker and the engine, checked for its known answer. Once it has succeeded once, failing
    ZERO_LIVENESS_FAILURES times in a row takes the service down, and the supervisor starts it
    again. Before its first success a failure is only logged: the canary may still be building."""

    def __init__(self, port: int, secret: str):
        super().__init__(name="zero-liveness", daemon=True)
        self.url, self.secret = f"http://127.0.0.1:{port}/execute", secret
        self.token = (os.environ.get("ZERO_SANDBOX_TOKEN") or "").strip()
        self.established, self.failures, self.doom = False, 0, None

    def probe(self) -> tuple[bool, str]:
        import urllib.error
        import urllib.request
        raw = _canary_request()
        if raw is None:
            return False, "no canary runtime is readable"
        headers = {"Content-Type": "application/json", "X-Zero-Canary": self.secret}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        timeout = float(os.environ.get("ZERO_LIVENESS_TIMEOUT_S", "300")) if self.established \
            else float(os.environ.get("ZERO_LIVENESS_FIRST_TIMEOUT_S", "1800"))
        request = urllib.request.Request(self.url, data=json.dumps(raw).encode(), headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as reply:
                body = json.loads(reply.read() or b"{}")
        except urllib.error.HTTPError as error:
            return False, f"HTTP {error.code}: {error.read()[:300]!r}"
        except Exception as error:
            return False, f"{type(error).__name__}: {error}"
        expect = raw["code"].split()[-1]
        if body.get("returncode") == 0 and expect in body.get("stdout", ""):
            return True, "ok"
        return False, f"wrong answer: rc={body.get('returncode')} stdout={body.get('stdout')!r:.200}"

    def run(self) -> None:
        period = float(os.environ.get("ZERO_LIVENESS_S", "30"))
        limit = max(1, int(os.environ.get("ZERO_LIVENESS_FAILURES", "3")))
        while self.doom is None:
            started = time.monotonic()
            ok, why = self.probe()
            if ok:
                if not self.established or self.failures:
                    incident("liveness-ok", established_now=not self.established,
                             after_failures=self.failures, seconds=round(time.monotonic() - started, 1))
                self.established, self.failures = True, 0
            else:
                self.failures += 1
                incident("liveness-failed", why=why, consecutive=self.failures,
                         established=self.established, seconds=round(time.monotonic() - started, 1))
                if self.established and self.failures >= limit:
                    self.doom = f"the deep liveness probe failed {self.failures} times in a row: {why}"
                    return
            time.sleep(period)


def _go_down(reason: str, code: int) -> None:
    """Exit the driver, which takes Serve's proxy and every worker of this Ray instance with it
    and closes the port. A service that cannot serve is DOWN, never answering with errors."""
    incident("shutdown", reason=reason, exit_code=code)
    try:
        with open(_fatal_path(), "w", encoding="utf-8") as stream:
            stream.write(reason + "\n")
    except OSError:
        pass
    print(f"[sandbox] GOING DOWN (exit {code}): {reason}", flush=True)
    import ray

    def front_first():
        # THE FRONT DIES FIRST. Shut down in Ray's own order and the gateway replica goes before
        # Serve's proxy, which then answers every request it still holds with a bare `500
        # Internal Server Error` (measured: 60 of them in one shutdown). Killing the controller
        # (so nothing restarts the proxy) and then the proxy closes those connections with no
        # reply at all, which is what "down" is.
        try:
            from ray.util import list_named_actors
            named = list_named_actors(all_namespaces=True)
            for prefix in ("SERVE_CONTROLLER_ACTOR", "SERVE_PROXY_ACTOR"):
                for entry in named:
                    if entry.get("name", "").startswith(prefix):
                        ray.kill(ray.get_actor(entry["name"], namespace=entry.get("namespace")),
                                 no_restart=True)
        except Exception as error:
            print(f"[sandbox] front not stopped first: {error!r}", flush=True)
        ray.shutdown()
    closer = threading.Thread(target=front_first, daemon=True)
    closer.start()
    closer.join(float(os.environ.get("ZERO_SHUTDOWN_GRACE_S", "20")))
    os._exit(code)


_LOOPBACK = ("127.0.0.1", "::1", "localhost")


def _auth_refusal(host: str, env=os.environ) -> str | None:
    """Why this bind may not start without authentication, or None when it may.

    The service executes whatever it is sent, so an open port is remote code execution for anyone
    who can reach it. It starts unauthenticated only on loopback, or when the operator says the
    network itself is the boundary."""
    if (env.get("ZERO_SANDBOX_TOKEN") or "").strip():
        return None
    if host in _LOOPBACK or env.get("ZERO_SANDBOX_ALLOW_NO_AUTH") == "1":
        return None
    return (f"refusing to serve code execution on {host} without authentication: set "
            "ZERO_SANDBOX_TOKEN (every route but /health then requires "
            "'Authorization: Bearer <token>'), bind --host 127.0.0.1 for local use, or set "
            "ZERO_SANDBOX_ALLOW_NO_AUTH=1 if the network itself is the boundary")


def main() -> int:
    parser = argparse.ArgumentParser("sandbox-service")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8070)
    args = parser.parse_args()
    if (refusal := _auth_refusal(args.host)) is not None:
        print(f"[sandbox] {refusal}", file=sys.stderr, flush=True)
        return 2
    install()
    _sweep_containers()
    if (engine := runtime()) is not None:
        _preflight_store(engine)
    if engine is not None and not _is_remote():
        # A remote engine shares no host fs by design; the session carries no host bind mount, so
        # there is nothing to preflight (and the probe's own bind would falsely fail).
        _preflight_shared_root(engine)
    import ray
    from ray import serve
    # A previous run that went down left its reason: keep it, under its time, and start clean.
    fatal = _fatal_path()
    if os.path.exists(fatal):
        try:
            with open(fatal, encoding="utf-8") as stream:
                incident("previous-shutdown", reason=stream.read().strip())
            os.replace(fatal, fatal + "." + time.strftime("%Y%m%dT%H%M%S"))
        except OSError:
            pass
    # Inherited by the gateway replica (a local Ray instance's workers inherit this process's
    # environment): the liveness probe's secret, and who to signal to go down.
    secret = secrets.token_hex(24)
    os.environ["ZERO_CANARY_SECRET"] = secret
    os.environ["ZERO_SERVE_DRIVER_PID"] = str(os.getpid())
    incident("startup", host=socket.gethostname(), port=args.port)
    if not ray.is_initialized():
        ray.init(address="auto" if os.environ.get("RAY_ADDRESS") else None,
                 namespace="zero-sandbox",
                 object_store_memory=None if os.environ.get("RAY_ADDRESS") else
                 int(float(os.environ.get("ZERO_RAY_OBJECT_STORE_GB", "0.5")) * 2**30),
                 ignore_reinit_error=True)
    serve.start(http_options={"host": args.host, "port": args.port})
    serve.run(build_app(), route_prefix="/")
    print(f"[sandbox] serving submitted runtimes on {args.host}:{args.port}", flush=True)
    liveness = _Liveness(args.port, secret)
    liveness.start()
    while True:
        time.sleep(1)
        if os.path.exists(fatal):
            try:
                with open(fatal, encoding="utf-8") as stream:
                    reason = stream.read().strip() or "the gateway requested a shutdown"
            except OSError:
                reason = "the gateway requested a shutdown"
            _go_down(reason, 70)
        if liveness.doom is not None:
            _go_down(liveness.doom, 71)


if __name__ == "__main__":
    # Import by name so Ray pickles this module's helpers by reference, not by value:
    # under ``python -m`` they would otherwise live in ``__main__`` and drag their locks along.
    from sandbox.service import main as _main

    raise SystemExit(_main())
