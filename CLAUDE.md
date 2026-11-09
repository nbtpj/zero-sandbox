# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this service is

A blind execution service. It accepts a Dockerfile and a terminal action over HTTP, runs them
inside the requested isolation and resource envelope, and returns raw observations. It never
inspects, parses, or interprets the caller's code or output. Anything that looks like task,
parser, checker, metric, reward, or runtime-catalog semantics does not belong here.

`README.md` is the API contract (routes, every request/response field, limits, error statuses).
Read it before changing wire behaviour.

## Commands

```bash
pip install -e ".[dev,server]"

bash build.sh                 # bootstrap pinned toolchain + preflight (builds no runtime image)
bash build.sh --bootstrap     # only fetch podman/crun/conmon/CNI into .toolchain/bin
bash build.sh --preflight     # only the checks: builder, subuid/userns, setuid helpers
bash build.sh --prune         # recover storage retained by an interrupted build

bash launch-server.sh serve --host 0.0.0.0 --port 8070
bash launch-server.sh status  # list images in the configured stores
```

Tests:

```bash
pytest                                              # unit cases only; no container engine needed
ZERO_SANDBOX_URL=http://127.0.0.1:8070 pytest       # also runs the live + measured cases
pytest tests/test_gateway.py::test_cpu_admission_is_weighted_and_fifo   # a single test
python tests/perf.py --url http://127.0.0.1:8070 \
  --n 120 --concurrency 8 --json /tmp/sandbox-perf.json                 # the report, by hand
```

| module | subject |
|---|---|
| `conftest.py` | env defaults, the `live` marker, the `url` fixture, and `dynamic`/`submit`/`api` |
| `test_runtime.py` | declarations, identity, grants, egress policy |
| `test_cache.py` | the three tiers, listings, sizes, build bounds, victim selection |
| `test_isolation.py` | confinement invariants that hold before a container starts |
| `test_gateway.py` | admission, discovery, lifetime bounds, build slots, the eviction races |
| `test_request.py` | what the wire accepts and refuses, the error-prefix contract, the /work delta |
| `test_client.py` | `sandbox.route`, including the refusal to execute in the caller's process |
| `test_contract.py` | live: the HTTP contract, sessions, resource limits, egress |
| `test_confinement.py` | live: what submitted code CANNOT do — the escape matrix |
| `test_dangerous.py` + `sample_request_test.json` | live: complete `/execute` bodies that attack the SERVICE, and the health check that is the real assertion |
| `test_flood.py` | live: concurrency as CORRECTNESS -- no reply from the wrong image, no session cross-talk, failed builds do not poison the gateway |
| `test_never_fail.py` | the start marker, the retry invariant (a result is never re-run), staging, the shared-load shield, the grant ledger, doom, the client's wait-while-down |
| `chaos.py` | live, by hand: a checked flood across runtimes with fault injection (worker kills, container rm/stop, engine socket outage, disconnect storms) |
| `perf.py` + `test_performance.py` | live: the measured report, and the claims it must satisfy |

Test modules import siblings directly (`from conftest import ...`, `import perf`) — `tests/` has
NO `__init__.py`, and must not get one: the repo root is itself the `sandbox` package, so pytest
would try to name the package after the checkout directory and fail to import anything.

`pytest` runs from a bare checkout: `tests/conftest.py` binds the checkout to the `sandbox` name,
because the root IS the package and no parent directory spells it. Do not remove that bootstrap
without adding `pip install -e .` to CI. `live` is a real marker (`-m live` / `-m "not live"`),
and `--strict-markers --strict-config` are on, so a typo'd mark fails instead of skipping nothing.
`.github/workflows/test.yml` runs the unit half on 3.10 and 3.12; the live half is run by hand
against a served sandbox.

Live cases skip unless `ZERO_SANDBOX_URL` is set. The suite submits its own throwaway Dockerfiles; `tests/runtimes/*.Dockerfile` are
fixtures for hand runs, not a catalog.

**Run one sandbox per container engine.** Two services against one daemon trim each other's
images, and the loser sees intermittent `unavailable:container launch failed: Unable to find
image ... locally`. `_owned_images` is scoped by image prefix, not by service instance.

On Slurm, set `ZERO_LOCAL_ROOT=${SLURM_TMPDIR:?}/zero-sandbox-$UID` so image layers never land on
NAS-backed `$HOME`.

## Layout and packaging

Four Python files, no subpackages: `runtime.py`, `route.py`, `service.py`, `__init__.py`.

**The checkout root is the package.** `pyproject.toml` sets `package-dir = {sandbox = "."}`
because this repo can be mounted as the `sandbox/` Git submodule of a consuming project. Consequences that break
things when forgotten:

- Always import absolutely: `from sandbox.service import ...`. Never relative imports.
- `policy.json` and `egress/Dockerfile` are read at runtime from the package directory
  (`service.py` `_HERE`). Any new runtime-read data file must be added to
  `[tool.setuptools.package-data]` or a wheel install breaks with `FileNotFoundError`.
- `launch-server.sh serve` symlinks the checkout to `$ZERO_LOCAL_ROOT/python/sandbox` so a
  standalone clone (named `zero-sandbox`) still imports as `sandbox`.
- `service.py` `__main__` re-imports `main` by name so Ray pickles module helpers by reference,
  not by value. Keep that indirection.

## Architecture

### Identity

A runtime's identity is the SHA-256 of the submitted Dockerfile text and nothing else
(`runtime.resolve`). Capabilities come only from `ENV ZERO_RUNTIME_*` lines parsed out of that
text (`runtime.declarations` / `spec_from`): `SHELL`, `EGRESS` (`none`|`endpoint`),
`IMAGE_PREFIXES`, `IMAGE_REQUIRED`, `VERSION`. Resources are a per-session hardware grant and are
deliberately **not** part of image identity — the same image serves sessions with different
grants without rebuilding.

Note the collision: `ZERO_RUNTIME_*` is both the Dockerfile declaration prefix *and* the prefix of
several server env vars (`ZERO_RUNTIME_STORE`). Declarations are read from Dockerfile text only.

### Request path

`_request` (`service.py:951`) validates and normalizes into a dict plus a `Submitted`, then one of
two admission paths runs it. Both must stay behaviourally identical:

**Admission is strict FIFO over a grant vector** (`_Capacity`, `_grant`): CPUs against the engine's
CPUs, the container's memory grant against `ZERO_SANDBOX_MEM_FRACTION` of the engine's memory, and
one session against the host's Ray workers (`ZERO_SESSION_MEM_GB` each). Each axis in its own
unit -- a single CPU number checked against a memory-derived session count let five 4-CPU checks
fill a 240-vCPU VM (2026-09-12). A request at the head of the queue holds its place until its whole
grant fits on every axis, so a big job is never starved by a stream of small
ones — but the reverse cost is real: a 1-CPU action queued behind a 3-CPU one waits for 3 CPUs to
free even while 1 is idle (head-of-line blocking). `test_lean_flood.py` measures it — a light
action's tail latency is the admission cost of queueing behind a Lean proof. This is deliberate;
a backfill scheduler would trade it for the starvation the FIFO exists to prevent.

- **Ray path** — `Gateway` (`_gateway_class`, one Serve replica, `max_ongoing_requests=4096`;
  `build_app` applies the `serve.deployment` decorator so the class itself stays constructible in
  tests) owns the session table, `_Capacity` (async FIFO vector admission), the cache
  tables, and the background tasks.
- **Local dev path** — `LocalSessions` / `_LocalCapacity` (`execute_local`), reached by
  `route.aexecute` when `ZERO_SANDBOX_URL` is unset *and* `ZERO_ALLOW_LOCAL_EXEC=1`. It exists so
  the CPU-admission contract is testable without Ray. Changing admission means changing both.

Three actor kinds: `_SessionActor` (holds one `LiveSession`, scheduled with the session's
`num_cpus`/`num_gpus`/`memory`), `_CacheActor` (detached, `num_cpus=0`, one per
`(digest, image)`, builds and stages), `_RamLease` (detached, `num_cpus=0`, exists purely so Ray
accounts for RAM-tier bytes on a pinned node).

Session actors are pinned by `NodeAffinitySchedulingStrategy(cache["node_id"], soft=False)` to the
node holding the image, and are pooled for `ZERO_WORKER_CACHE_S` after close, keyed by
`(digest, image, node_id, resources)`.

### Failure semantics — a result, or down (2026-10-15)

README "Failure semantics" is the contract. In code: `LiveSession.execute` raises `InfraFailure`
only where NO result exists (launch failed, start marker absent, container gone under a command
that exited 125+, engine client killed on the host, capture incomplete); everything after the
"A RESULT EXISTS" comment must not raise. `_SessionActor` reports `{"result"|"error"|"infra"}` as
data via `_outcome` -- never a raised traceback. `Gateway._attempts` retries `InfraFailure` on
fresh workers (`_drop(healthy=False)` kills, never pools), and `_exhausted` runs the single-flight
`_self_check` (the canary through the same path): fail -> `_doom` (no further answers, fatal file,
driver exits), pass -> `unavailable:` with the cause. Shared Tasks awaited by requests are always
`asyncio.shield`ed, and a CancelledError is the caller's only if `Task.cancelling()` says so
(`_cancelled_by_caller`) -- incident 2 was a shared load cancelled by one client and re-awaited by
every later one. Admission grants live in a token ledger (`_acquire`/`_release_grant`), audited by
`_audit`. The driver runs `_Liveness`, a canary `/execute` through the front door. Every internal
failure goes to `incident()` (`ZERO_INCIDENT_LOG`). `tests/test_never_fail.py` pins the invariant
offline; `tests/chaos.py` is the live flood with fault injection.

### Discovery — nothing blocking on the request path

One replica serves every execution on one event loop, so a synchronous engine call, disk write, or
O(known × sessions) scan inside `__call__` stalls all of them. `_facts` reads gateway-owned state
only (a completed cache record, else `self.disk_facts`); `_discover` refreshes it in
`asyncio.to_thread`; `remember` goes through `_remember` → thread; session counts are tallied once
per poll; `runtime()`'s `lru_cache` is primed in `__init__`.
`test_discovery_is_served_from_gateway_facts_not_the_engine` fails if any subprocess starts while
discovery is served — keep it that way when adding `/health` fields.

### Session semantics

- One `session_id` executes strictly in order (an `asyncio.Lock` per id).
- A live session may not change runtime hash, instance image, resources, env, or network policy:
  those are folded into `config_hash` (`_execution_config`) and a mismatch is HTTP `409`.
- Every action launches the container, then execs the command: `timeout_s` bounds ONLY the exec
  (`elapsed_ms`), never container startup (`start_ms`) or image build (`build_ms`, upstream in the
  cache actor). Counting startup against the budget made deterministic code time out under
  concurrent launch contention — stochastic verifier scores. A stateless `wait_s=0`
  action closes the container after; `wait_s>0` keeps it resident with a shell watchdog polling
  `/zero-control/deadline` so it self-destructs even if the gateway loses it.
- `ZERO_LOCAL_ROOT` must be visible to the ENGINE, not just the service — bind-mount sources
  resolve inside the engine. `_preflight_shared_root` refuses to serve at startup otherwise; a
  per-request failure adds a hint via `_root_hint`.
- A timeout destroys the whole session.

### Isolation

Two different questions, and both need live cases. `test_contract.py` asks whether the resource
envelope BINDS (memory, PIDs, file size, wall time). `test_confinement.py` asks what a hostile
submitted program can reach: the read-only control mount, an engine socket, `mount`, `chown`,
sysctls, cgroups, host device nodes, the tmpfs caps, the process table, DNS and routing without a
grant, another session's workspace, and a host other than the one an endpoint grant named.

Assert capability state by READING it (`/proc/self/status`: `CapEff`/`CapPrm`/`CapBnd` zero,
`NoNewPrivs: 1`, `Seccomp: 2`), never by inferring it from a behavioural probe. busybox `ping`
succeeds against loopback with every capability dropped — it falls back to an ICMP datagram
socket, which needs no `CAP_NET_RAW`.

`LiveSession.__post_init__` builds the engine argv: UID/GID 65532, `--read-only`, `--cap-drop=ALL`,
`no-new-privileges`, `--pids-limit`, `--cpus`, `--memory`/`--memory-swap` equal (no swap),
`fsize` and `core` ulimits, tmpfs `/tmp` and `/dev/shm`, `--network=none` by default,
`--runtime=runsc` when `ZERO_REQUIRE_GVISOR=1`. `/work` is a compute-local bind mount, or a tmpfs
when `workdir_medium=ram`.

Endpoint egress (`_open_network`): an `--internal` network plus a tinyproxy relay built from
`egress/Dockerfile`, whose filter is an anchored regex of the single requested host and whose
`ConnectPort` is the single requested port. `_vet_endpoint` resolves the host (in the actor, off
the gateway loop), refuses any non-global answer — loopback, private, link-local, **cloud
metadata at 169.254.169.254** — and pins the vetted IP into the relay's `/etc/hosts` so the relay's
own re-resolution cannot move the target (DNS rebinding). The relay watches the same deadline file
as the session, so it cannot outlive it. `HTTP(S)_PROXY` are injected; the session container has
no route of its own. URL paths never narrow the grant.

Request `env` values are written to a `0444` file in the read-only control mount and are never
echoed in a response.

### Image cache — three tiers

1. **RAM** — a private per-image store under `ZERO_RAM_IMAGE_STORE` (`/dev/shm`). Size-bounded
   LRU (`_ram_budget`, `_ram_victims`); live sessions pin their image; promotion (`_promote`)
   happens off the request path and only when both a node-pinned Ray memory lease and the measured
   free-memory reserve (`ZERO_RAM_MIN_FREE_GB`) permit. Attached read-only only after staging
   completes, so its metadata locks are never shared.
2. **Compute-local disk** — `ZERO_BUILD_IMAGE_STORE`, bounded by `ZERO_DISK_IMAGE_CACHE` (64) and
   trimmed by the gateway's background reaper (`_trim_disk`), which never evicts an image a
   session or cache entry uses.
3. **Engine store** — fallback.

The disk bound is a *latency* mechanism, not just a disk one: an `images` listing costs time linear
in the images it matches, and `test_discovery_stays_responsive_under_a_flood` holds `/health` to
`ZERO_HEALTH_DEADLINE_S` (1.0s) while builds run. Keep listings scoped to `_OWNED_PREFIXES` and
keep size resolution batched — one call for all owned images, never one call per image. The RAM
tier is one private store *per image*, so probe it with `_ram_present` (a stat), never an engine
call per runtime. `_trim_disk` releases the cache records naming the images it evicts
(`_release`), so a record pins an image only while a live session, RAM lease, promotion or
unfinished build needs it (`_pinned`). Release is driven by the tier being FULL, never by elapsed
time — an idle record costs nothing and dropping it only buys a rebuild. It is also mandatory: a
record outliving its image hands the next session a `cached_image` the engine no longer has.

Submitted declarations are persisted to `$ZERO_LOCAL_ROOT/known/<digest>.json` (`remember` /
`recall`) so a restart still reports what it caches; the in-memory table is bounded by
`ZERO_KNOWN_LIMIT` (512).

## Conventions

**Error strings are the HTTP contract.** Raise `ValueError` with one of these prefixes; `_status`
(`service.py:1215`) maps them: `forbidden:` → 403, `not_found:` → 404, `conflict:` → 409,
`unavailable:` → 503, anything else → 400. Use `bad_request:` for clarity. Inside the execution
path an infrastructure failure is `InfraFailure`, never an `unavailable:` string: it is retried,
and becomes `unavailable:` only past the bound with the self-check passing. Never let a raw Ray
remote traceback (pids, actor ids) reach the client — `_cache` deliberately reduces one to its
last line.

**All engine state stays under `ZERO_LOCAL_ROOT`.** Podman otherwise scatters four state
directories across `$HOME`, which is CephFS on the target cluster and the direct cause of past
metadata storms. `build.sh` and `launch-server.sh` must resolve to the same image store, or the
service reports "image not present" with the images on disk.

**Podman is pinned to v4.9.4, not `latest`.** 5.x resolves `netavark`, which needs glibc ≥ 2.39;
the target cluster is on 2.35. Override with `ZERO_PODMAN_URL` on a newer node. The full reasoning
is in the `build.sh` header comment — read it before touching the toolchain.

Comments in this codebase explain *why*, usually recording a failure that motivated the code. Match
that: dense, reason-giving, no restating of what the line does.

## Environment variables

`README.md` covers the client-facing ones. The rest, by group:

- **Client** — `ZERO_SANDBOX_URL`, `ZERO_ALLOW_LOCAL_EXEC`, `ZERO_SANDBOX_TIMEOUT_S`,
  `ZERO_SANDBOX_CLIENT_THREADS`.
- **Auth** — `ZERO_SANDBOX_TOKEN`: a shared secret read by BOTH the service and the client. When
  it is set the service refuses every route but `/health` without a matching
  `Authorization: Bearer <token>` (403), and the client attaches it. `/health` without the token
  answers only `{"ok": true}`: its full report is `/runtimes` plus failure text and the store's
  host path, so leaving it open made the `/runtimes` gate meaningless. The service FAILS CLOSED:
  `main()` exits 2 before touching any container when the bind is not loopback and no token is
  set, unless `ZERO_SANDBOX_ALLOW_NO_AUTH=1` says the network itself is the boundary
  (`service._auth_refusal`; the `Gateway` class itself stays token-agnostic so unit cases can
  build it directly). Both sides `.strip()`, so a launcher's `$(cat file)` trailing newline is
  harmless, and a blank token is no token.
- **Limits** — `ZERO_MAX_COMMAND_SECONDS` (28800), `ZERO_MAX_IDLE_SECONDS` (3600),
  `ZERO_MAX_IO_CAP_KB` (1024), `ZERO_SANDBOX_SLOTS`, `ZERO_SESSION_MEM_GB` (4.0) and
  `ZERO_SANDBOX_MEM_FRACTION` (0.80) — the MEMORY bound on session admission, which exists
  because a session is a Ray worker process and CPU-only admission let a 240-vCPU VM admit ~190
  sessions into a 755 GB node until Ray OOM-killed 167 workers at once. `ZERO_LAUNCH_TIMEOUT_S` (600) — how long
  `podman run` itself may take. It is NOT the caller's budget: the engine serialises container
  creation, so this bounds launch CONTENTION. Exceeding it used to raise `TimeoutExpired` out of
  `_SessionActor.execute` and kill the actor, which reached the client as a Python traceback
  instead of an error-string prefix; it is now caught and reported as `unavailable:`.
- **Storage** — `ZERO_LOCAL_ROOT`, `ZERO_IMAGE_STORE`, `ZERO_BUILD_IMAGE_STORE`,
  `ZERO_RAM_IMAGE_STORE`, `ZERO_RUNTIME_STORE`, `ZERO_SANDBOX_TMP`, `ZERO_PODMAN_ROOT`,
  `ZERO_PODMAN_RUNROOT`, `ZERO_PODMAN_IMAGE_STORES`, `ZERO_ENGINE_HOME`.
- **Cache** — `ZERO_RAM_IMAGE_CACHE_GB`, `ZERO_RAM_MIN_FREE_GB` (3), `ZERO_DISK_IMAGE_CACHE` (64),
  `ZERO_KNOWN_LIMIT` (512), `ZERO_IMAGE_TABLE_TTL_S` (10),
  `ZERO_DISCOVERY_REFRESH_S` (10), `ZERO_WORKER_CACHE_S` (2), `ZERO_SWEEP_S` (300).
- **Workspace** — `ZERO_WORKDIR_POLL_S` (0.5) — how often a compute-local `/work` is measured
  against its grant; `ZERO_WORKDIR_MAX_FILES` (500000) — inode ceiling, since a byte quota is
  blind to empty files. A bind mount takes no size from the engine, so these are the ceiling.
- **Build** — `ZERO_BUILD_CPUS`/`MEM_GB`/`DISK_GB`/`PIDS`/`TIMEOUT_S`, `ZERO_BUILD_BUILDER`,
  `ZERO_BUILD_SHARE_HF_TOKEN` (`1` passes the service's `HF_TOKEN` to builds as secret `hf_token`;
  every submitted Dockerfile can then read it, so it is off by default),
  `ZERO_BUILD_SLOTS` (concurrent builds; derived from cluster CPU // `ZERO_BUILD_CPUS`).
- **Engine** — `ZERO_CONTAINER_RUNTIME`, `ZERO_OCI_RUNTIME`, `ZERO_CGROUP_MANAGER`,
  `ZERO_REQUIRE_GVISOR`, `ZERO_IMAGE_DRIVER`, `ZERO_PODMAN_VERSION`/`URL`, `ZERO_CONMON_URL`,
  `ZERO_CNI_VERSION`/`URL`.
- **Ray/test** — `ZERO_RAY_OBJECT_STORE_GB`, `ZERO_HEALTH_DEADLINE_S`.
- **Failure handling** — `ZERO_INFRA_ATTEMPTS` (8), `ZERO_INFRA_BACKOFF_S` (1, doubling, capped
  30), `ZERO_INFRA_RETRY_S` (1800), `ZERO_BUILD_ATTEMPTS` (3), `ZERO_OPEN_TIMEOUT_S` (600),
  `ZERO_ACT_SLACK_S` (600), `ZERO_CAPTURE_TIMEOUT_S` (120), `ZERO_STAGE_TIMEOUT_S` (120),
  `ZERO_RM_TIMEOUT_S` (120), `ZERO_ARGV_CODE_LIMIT` (100 KiB), `ZERO_SELFCHECK_TTL_S` (30),
  `ZERO_SELFCHECK_TIMEOUT_S` (900), `ZERO_CANARY_DOCKERFILE` (default `canary/Dockerfile`),
  `ZERO_LIVENESS_S` (30), `ZERO_LIVENESS_TIMEOUT_S` (300), `ZERO_LIVENESS_FIRST_TIMEOUT_S`
  (1800), `ZERO_LIVENESS_FAILURES` (3), `ZERO_DOOM_SIGNAL_S` (30), `ZERO_SHUTDOWN_GRACE_S` (20),
  `ZERO_AUDIT_S` (30), `ZERO_INCIDENT_LOG`, `ZERO_INCIDENT_LOG_MAX_MB` (64),
  `ZERO_MAX_ONGOING_REQUESTS` (65536); client `ZERO_SANDBOX_DOWN_WAIT_S` (1800).
