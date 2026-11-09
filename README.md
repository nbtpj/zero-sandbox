# Sandbox

The sandbox is a general execution service. It blindly accepts a runtime setup and a terminal
action, executes them inside the requested isolation and resource envelope, and returns the raw
result. It does not inspect or interpret the caller's purpose, code, or output. Ray Serve admits,
places, and scales execution across the available cluster; the service automatically builds and
caches runtime images to serve as many requests as quickly as the allocation permits.

This repository is the canonical HTTP sandbox implementation. It can also be vendored as a
`sandbox/` Git submodule of a consuming project, which is why the checkout root is the package.

## Quickstart

```bash
git clone https://github.com/nbtpj/zero-sandbox.git && cd zero-sandbox
pip install -e ".[server]"
bash build.sh                        # bootstraps and validates the pinned rootless container toolchain
export ZERO_SANDBOX_TOKEN=$(python -c 'import secrets; print(secrets.token_hex(24))')
bash launch-server.sh serve --host 127.0.0.1 --port 8070
```

From another shell with the same `ZERO_SANDBOX_TOKEN` exported, `GET /health` needs no token (it
then answers only `{"ok": true}`) and every other route needs
`Authorization: Bearer $ZERO_SANDBOX_TOKEN`:

```bash
curl -s http://127.0.0.1:8070/health
curl -s http://127.0.0.1:8070/execute -H "Authorization: Bearer $ZERO_SANDBOX_TOKEN" \
  -H 'Content-Type: application/json' -d @request.json     # a body like the example under `/execute`
```

The first request for a new Dockerfile builds its image; later ones reuse it.

## Security

The service executes the code it is sent, so read this before exposing it.

- **Authentication.** Set `ZERO_SANDBOX_TOKEN`. Every route except `/health` then requires
  `Authorization: Bearer <token>`; without it `/health` answers only `{"ok": true}`, and the full
  report below needs the token. The service refuses to start on a non-loopback address without
  a token; `ZERO_SANDBOX_ALLOW_NO_AUTH=1` overrides that for a network that is itself the
  boundary. The token is one shared secret, not a per-user credential: treat it as an admin key.
- **Isolation.** Sessions run in rootless containers as an unprivileged user with a read-only
  root, all capabilities dropped, no new privileges, no swap and enforced resource limits (see
  [Isolation and cache](#isolation-and-cache)). That is container isolation, not a virtual-machine
  boundary: a kernel or container-runtime vulnerability would reach the host. Run the service on a
  dedicated machine or VM that holds no credentials you would mind losing.
- **Network.** Sessions have no network unless the Dockerfile declares endpoint egress, which is
  limited to one vetted, pinned, publicly routable origin.

Report a vulnerability as described in [SECURITY.md](SECURITY.md).

## API

| Method | Route | Result |
|---|---|---|
| `POST` | `/execute` | Execute one ordered terminal action. |
| `GET` | `/runtimes` | Return submitted runtime hashes, declarations, documentation, and build/cache state. |
| `GET` | `/health` | Return `/runtimes` data plus service, session, failure, and RAM-cache statistics. |

### `POST /execute`

| Request field | Type | Required | Meaning and limit |
|---|---:|:---:|---|
| `runtime` | object | yes | Submitted runtime descriptor. |
| `runtime.dockerfile` | string | yes | Complete Dockerfile, at most 256 KiB. Its SHA-256 is the sole runtime identity. |
| `runtime.documentation` | string | no | Served metadata, at most 256 KiB; not part of identity. |
| `resources` | object | yes | Exact per-session grant described below. |
| `session_id` | string | yes | 1–128 letters, digits, `.`, `_`, `:`, or `-`. Requests with one ID execute in order. |
| `code` | string | yes | NUL-free terminal input executed by the runtime shell. |
| `cwd` | string | no | `/work` or a path below it; default `/work`. |
| `timeout_s` | number | yes | COMMAND wall-time limit in `(0, server maximum]`; bounds `elapsed_ms` only, never container startup or image build. A timeout destroys the session. |
| `wait_s` | number | yes | Idle session lifetime in `[0, server maximum]`; `0` destroys it after this action. |
| `io_cap_kb` | integer | no | Retained tail of each output stream, 1–1024 KiB; default 200 KiB. |
| `image` | string | no | Existing instance image, only when allowed or required by Dockerfile declarations. |
| `network` | string | no | One HTTP(S) origin, only when the Dockerfile declares endpoint egress; URL paths do not narrow the grant. The host is resolved and refused if it maps to a non-global address (loopback, private, link-local, cloud metadata), and the vetted IP is pinned into the relay so a later DNS answer cannot move it. |
| `env` | object | no | Up to 64 request-scoped NUL-free string values, 128 KiB total; never echoed. |

| `resources` field | Type | Constraint |
|---|---:|---|
| `cpus` | number | `> 0`; fractional grants are supported. |
| `memory_gb` | number | `> 0`. |
| `workdir_gb` | number | `> 0`. |
| `workdir_medium` | string | `temp` or `ram`; `temp` is compute-local storage. |
| `gpus` | number | `>= 0`; fractional Ray scheduling is supported. |
| `pids` | integer | `> 0`. |

Example:

```json
{
  "runtime": {
    "dockerfile": "FROM docker.io/library/alpine:latest\nENV ZERO_RUNTIME_SHELL=/bin/sh",
    "documentation": "Alpine terminal runtime."
  },
  "resources": {
    "cpus": 1,
    "memory_gb": 2,
    "workdir_gb": 0.5,
    "workdir_medium": "temp",
    "gpus": 0,
    "pids": 256
  },
  "session_id": "client-chosen-id",
  "code": "python3 -I -B -c 'print(42)'",
  "cwd": "/work",
  "timeout_s": 30,
  "wait_s": 0,
  "io_cap_kb": 200
}
```

The HTTP API receives Dockerfile contents, never a client path. `sandbox.route.aexecute` also
accepts a local Dockerfile path and sends its contents. A live `session_id` cannot change its
runtime hash, instance image, resources, environment, or network policy. CPU and fractional GPU
grants are Ray scheduling quantities. A fractional GPU grant shares Ray's assigned physical
NVIDIA device; it is not a hard VRAM fraction. Use MIG devices for GPU-memory isolation.

| Successful response field | Type | Meaning |
|---|---:|---|
| `runtime` | object | `{"hash": "<Dockerfile SHA-256>"}`. |
| `resources` | object | Normalized grant applied to the session. |
| `session_id` | string | Submitted session ID. |
| `code` | string | Submitted terminal input. |
| `cwd` | string | Applied working directory. |
| `timeout_s`, `wait_s` | number | Applied command and idle limits. |
| `output` | string | Decimal string form of `returncode`; retained for wire compatibility. |
| `returncode` | integer | Raw process return code. |
| `signal` | integer or null | Signal number inferred from a `128+signal` return code. |
| `stdout`, `err` | string | Independent UTF-8 replacement-decoded stream tails. |
| `stdout_bytes`, `stderr_bytes` | integer | Bytes retained in each tail. |
| `stdout_total_bytes`, `stderr_total_bytes` | integer | Total bytes produced by each stream. |
| `stdout_truncated`, `stderr_truncated`, `truncated` | boolean | Per-stream and combined tail truncation. |
| `timed_out` | boolean | Whether the command exceeded `timeout_s`. |
| `alive` | boolean | Whether the session remains available for the next ordered request. |
| `elapsed_ms` | integer | Command execution time, and the ONLY thing `timeout_s` bounds. |
| `start_ms` | integer | Container startup time, excluded from `timeout_s`. |
| `build_ms` | integer | Image build time (0 when cached), excluded from `timeout_s`. |
| `files` | object | `/work` metadata delta: `observed`, `scope`, and, when observed, `created`, `modified`, `deleted`, `bytes_delta`, `bytes_used`, `over_quota`, `scanned`, `truncated`; otherwise `reason`. |
| `network` | object | Applied `mode`, `outbound`, and endpoint `origin` when enabled. |

The service returns raw execution observations and never parses output.

| Error status | Body | Condition |
|---:|---|---|
| `400` | `{"error":"bad_request:..."}` | Invalid or unsupported request. |
| `403` | `{"error":"forbidden:..."}` | Runtime capability does not permit the request. |
| `404` | `{"error":"not_found:..."}` | Requested object is absent. |
| `405` | `{"error":"method_not_allowed:..."}` | Route called with the wrong HTTP method. |
| `409` | `{"error":"conflict:..."}` | A live session ID has a different execution configuration. |
| `503` | `{"error":"unavailable:..."}` | This request's runtime cannot be built or run while the service itself runs (see below). |
| `500` | `{"error":"server_error:..."}` | Unexpected server failure. |

### Failure semantics: a result, or the service is down

For a valid request the service answers with **the correct execution result**, or it is **not
reachable at all**. It never answers a valid request with an infrastructure failure while it is
up, and never with a partial result.

* **A result is never re-run.** A result exists once the command has run under its own lifecycle
  and its output was captured whole. Its own exit code, signal, timeout, OOM kill inside its grant,
  truncated or empty output are results, returned once.
* **An infrastructure failure is retried inside the service, on fresh resources** (a new session
  worker and container), with backoff, up to `ZERO_INFRA_ATTEMPTS` (8, ~90 s of backoff) or
  `ZERO_INFRA_RETRY_S` (1800). It is an infrastructure failure only when no result can exist, and
  only when the submitted code could not have caused it: the container did not launch, the command
  provably never started (a wrapper writes a per-action start marker to stderr before it `exec`s
  the runtime shell exactly as before; the marker is removed from the reply), the container died
  under the running command (the command cannot kill its container's PID 1), the engine client
  died on the host, the output could not be captured, or the session worker died.
* **Past that bound the service decides who is broken.** It runs its self-check -- the canary
  runtime through the same path. If the self-check fails, the SERVICE is broken: it stops
  answering everything and exits (its supervisor restarts it), so the caller sees it go away, not
  an error. If the self-check passes, only this request's runtime is broken (a Dockerfile that
  does not build, an image whose shell does not exist); that is answered `503 unavailable:` with
  the cause, because one runtime must not take the service down for everyone.
* **A session that already held state** (an earlier `wait_s > 0` action) cannot be rebuilt
  elsewhere; if its infrastructure fails its loss is reported as `404 not_found:session ... was
  lost`, never as a retry on an empty session.
* **Deep liveness.** The service probes itself with a canary `/execute` through its own front door
  every `ZERO_LIVENESS_S` (30) and goes down after `ZERO_LIVENESS_FAILURES` (3) failures in a row,
  once the probe has succeeded once. `/health` alone cannot see a front that stops forwarding.
* **Down, to a client**, is: connection refused or cut, or a bare non-JSON 5xx from Serve's own
  front. `sandbox.route` waits through it (up to `ZERO_SANDBOX_DOWN_WAIT_S`, 1800) and re-sends;
  every answer of the service itself is JSON.
* Every internal failure, retry, recovery, self-check and shutdown is appended, with its real
  exception and traceback, to the incident log (`ZERO_INCIDENT_LOG`, default
  `$ZERO_LOCAL_ROOT/incidents.jsonl`) and printed to the service log.

Code longer than 100 KiB (`ZERO_ARGV_CODE_LIMIT`) cannot be one argv string (Linux refuses 128
KiB and more with E2BIG); it is copied into the container's `/tmp` and sourced, outside the timed
exec. One `env` entry must stay below 128 KiB for the same reason.

### Discovery responses

`runtimes` is an object keyed by the full Dockerfile SHA-256.

| Runtime value field | Meaning |
|---|---|
| `hash` | Full Dockerfile SHA-256. |
| `version`, `shell`, `egress`, `image_required`, `image_prefixes` | Capabilities declared by the Dockerfile. |
| `available`, `why` | Current availability summary. |
| `build` | `cold`, `building`, `cached`, or `failed`. |
| `image`, `tier`, `bytes` | Internal image reference, active cache tier, and measured size. |
| `cache_actor_node`, `sessions` | Cache placement and live-session count. |
| `failure`, `documentation` | Latest failure text and submitted documentation. |

| `/health` field | Meaning |
|---|---|
| `ok` | Service health boolean. |
| `runtimes` | Runtime map above. |
| `sessions`, `served`, `uptime_s` | Live sessions, completed actions, and process uptime. |
| `failures` | Failure text keyed by full runtime hash. |
| `store.known`, `store.root` | Whether the engine reports its store's free space, and where that store is. |
| `store.free_bytes`, `store.total_bytes`, `store.min_free_bytes` | Measured free/total bytes of the store the engine writes to, and the floor below which every runtime reports `available: false`. |
| `reaped_created` | Containers reaped by the periodic sweep because they were created and never started. Reported here because a `print` from inside a Serve replica does not reach the driver's log. |
| `cache.ram_budget_bytes` | Automatically derived or deployment-configured RAM image budget. |
| `cache.ram_used_bytes`, `cache.ram_entries` | Current RAM image occupancy. |
| `infra` | Infrastructure failures seen (`failures`, `by_stage`, `last`), how many were `retried`, `recovered` and `exhausted`, `lost_sessions`, `leaks_repaired` (admission grants returned by the ledger audit) and `grants_held`. |
| `selfcheck` | The last self-check: `ok`, `why`, `age_s`. |

`/runtimes` returns only `ok` and `runtimes`.

## Isolation and cache

Commands run as UID/GID 65532 with a read-only root, all capabilities dropped, no new privileges,
no swap, and enforced CPU, memory, PID, file, `/tmp`, `/dev/shm`, and workspace limits. Mutable
state stays in compute-local temporary storage; a RAM workspace is used only when requested.

A RAM workspace is bounded by the engine, because `workdir_gb` is added to the container's memory
limit. A compute-local one is a bind mount, which takes no size from the engine: `--ulimit fsize`
bounds one FILE, so `workdir_gb` is enforced as a ceiling instead. The workspace is sampled every
`ZERO_WORKDIR_POLL_S` for BOTH its bytes and its inode count (`ZERO_WORKDIR_MAX_FILES`, default
500000 — a byte quota is blind to a million empty files, which still exhaust the filesystem's
inode table), and the container is removed once either is passed, which bounds the overshoot to
one interval of write bandwidth. The reply reports `files.bytes_used`, `files.inodes_used` and
`files.over_quota`, and the session is destroyed. A true barrier needs a quota on the filesystem
itself — an XFS project quota on the scratch directory, with an inode limit.

The sandbox automatically assigns one detached Ray cache actor to each content-addressed image;
clients neither select a cache tier nor request an image mount. The actor builds in compute-local
storage. In the background, the sandbox asks Ray for a node-pinned, memory-accounted lease and
stages immutable image material into a private per-image store under `/dev/shm` only when both
that lease and the measured
free-memory reserve permit it. Otherwise requests continue from compute-local cache. Ray schedules
each session from its request grant. The RAM tier is a size-bounded LRU: a newly demanded image may
evict the least-recently-demanded inactive image, while live sessions pin their image. The durable
compute-local copy remains reloadable. A store is attached read-only only after staging completes,
so its metadata locks are never shared with builds, other images, or in-flight local-cache launches.
Heavy immutable files are shared; workspaces are not.

`ZERO_RAM_IMAGE_CACHE_GB` sets the RAM-tier budget. By default the sandbox derives it from the
smaller of the RAM mount and Ray's memory resource after `ZERO_RAM_MIN_FREE_GB` reserve.

### Store space is what `available` means

A runtime is `available` only when the engine can actually take a container, which means the
store it writes to has more than `ZERO_STORE_MIN_FREE_GB` (default 5) free. That number is asked
of the ENGINE — `podman info`'s graph-root figures, or `statvfs` on a LOCAL Docker's root dir —
never of the host, because under a remote engine the store lives inside the VM and a host
`statvfs` measures a different filesystem entirely. An engine that reports no such figure is
`store.known: false` and is not guessed at: the service says so at startup and `available` falls
back to "the engine answers `version`".

Measured on 2026-08-30: `/health` reported every runtime `available: true` while the engine behind
it could not launch a single container, because its store was full. Liveness that does not
exercise what it certifies is worse than none.

### Containers that never started are reaped

`--rm` fires when a container EXITS and the session deadline watchdog is a process INSIDE the
container, so a launch that fails between create and start — no space, a bad mount, a missing
network — leaves a `Created` container that has already allocated a full read-write image layer
and that nothing else ever removes. The leak is self-amplifying: each failure costs a copy of the
image and makes the next one likelier. Measured on 2026-08-30, a VM held 1377 containers of which
1375 were `Created`, and that is what filled its 100 GiB store.

Two reapers, because one failure mode is the launch and the other is the backlog:

* a launch that fails removes its own container before it raises, and the session owns the name
  from the moment `run` is called, so `close()` reaps it even if the removal itself fails;
* the periodic sweep (`ZERO_SWEEP_S`) removes containers that have been `Created` and not started
  for `ZERO_CREATED_GRACE_S` (default 180 s, above the 120 s launch timeout). Two observations,
  never one: a container is `Created` for a moment during every successful launch. This is why
  the sweep is safe while serving, unlike the startup sweep, which kills live sessions.

### Remote/VM engines: the RAM tier does not apply inside the VM

`launch-server.sh` gives a LOCAL engine a compute-local build store plus a `/dev/shm` RAM tier
that is a size-bounded LRU with a free-memory reserve. A remote engine (`ZERO_PODMAN_URL` /
`ZERO_PODMAN_REMOTE`) owns storage outright — `--root`, `--storage-driver` and `--storage-opt` are
invalid with `--remote` — so **none of that reaches inside the VM**. There, the store is the VM's
own disk and the only thing the service bounds is the number of images (`ZERO_DISK_IMAGE_CACHE`);
bytes are the VM provisioner's responsibility, and the VM must be sized and its storage backed
where it is created.

Setting `ZERO_RAM_IMAGE_STORE` on a remote engine is REFUSED at startup rather than honoured:
every store resolves to the same remote argv, so staging would `save` the image out of the VM and
`load` it straight back into the same VM, then report a `ram` tier for an image that is on the
VM's disk. A phantom cache tier is worse than no tier.

`/health` and `/runtimes` answer from gateway-owned state only; the container engine is never
called on the request path. Cache tiers are resolved in a worker thread every
`ZERO_DISCOVERY_REFRESH_S`. A cache record is released only when the disk tier is over
`ZERO_DISK_IMAGE_CACHE` and its image is the one being evicted, never because it has been idle.

## Deploy

```bash
cd sandbox  # omit when this is a standalone checkout
bash build.sh
bash launch-server.sh serve --host 0.0.0.0 --port 8070
```

The first command bootstraps and validates the pinned rootless toolchain; it builds no runtime.
Set `ZERO_LOCAL_ROOT=${SLURM_TMPDIR:?}/zero-sandbox-$UID` on Slurm so image layers and small-file
trees never use NAS-backed home. Nested execution still requires a writable delegated cgroup-v2
hierarchy; being UID 0 inside Enroot alone does not supply it.

**`ZERO_LOCAL_ROOT` must be visible to the container ENGINE, not only to the service.** Bind-mount
sources resolve inside the engine: a local daemon on a cluster node shares the filesystem, but a
`podman machine`, a `DOCKER_HOST` over ssh, or a daemon in another mount namespace does not — a
podman machine shares `$HOME`, not `/tmp`. If the engine cannot see `ZERO_LOCAL_ROOT`, the service
refuses to start with a message naming it (a startup probe binds a directory under it into a
throwaway container). Point it at a path the engine shares.

The suite lives here and needs nothing outside this repo: it submits its own throwaway
Dockerfiles and depends on no runtime catalog.

```bash
pip install -e ".[dev,server]"
pytest                                              # unit cases, no engine needed
ZERO_SANDBOX_URL=http://127.0.0.1:8070 pytest       # adds the live contract and measured cases
python tests/perf.py --url http://127.0.0.1:8070 \
  --n 120 --concurrency 8 --json /tmp/sandbox-perf.json
```

`tests/perf.py` measures one claim per phase -- cold build, warm cache, stateless action, session
action, flood, confinement, discovery under load -- and prints the report `test_performance.py`
asserts against. Run ONE sandbox per container engine: two services sharing a daemon trim each
other's images, and the second one sees `unavailable:container launch failed: Unable to find
image`.

`ZERO_DISK_IMAGE_CACHE` (64) bounds how many built images the disk tier keeps; the background
reaper evicts the oldest beyond it and never one a live session, RAM lease, promotion or
in-flight build is using. A listing costs time linear in the images it matches, so this is what
keeps discovery answerable. `ZERO_BUILD_SLOTS` bounds how many submitted Dockerfiles build at once; it defaults to cluster
CPU divided by `ZERO_BUILD_CPUS`, because a cache actor costs Ray no CPU and an unbounded number
of builds starves the gateway itself. `ZERO_KNOWN_LIMIT` (512) bounds the declarations the gateway holds in
memory and is enforced on every request, `ZERO_DISCOVERY_REFRESH_S` (10) is the discovery
refresh period, and
`ZERO_HEALTH_DEADLINE_S` (1.0) is the ceiling the flood test asserts.


## License

Apache License 2.0; see [LICENSE](LICENSE).
