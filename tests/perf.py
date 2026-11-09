#!/usr/bin/env python3
"""What a served sandbox COSTS, not just whether it works.

    python tests/perf.py --url http://127.0.0.1:8070 --json report.json

Every phase measures one claim the service makes, and the report is the evidence for it:

===========  =====================================================================
phase        claim under measurement
===========  =====================================================================
cold         a submitted Dockerfile is built on demand, and the build is the cost
warm         the second request for the same hash pays no build at all
stateless    ``wait_s=0`` is ONE container carrying its command
session      a live session pays launch once and an exec per ordered action
flood        throughput and tail latency with cold builds in flight
hostile      the confinement envelope holds under each attack, and what it costs
discovery    /health and /runtimes answer from gateway facts, under a deadline
===========  =====================================================================

Nothing here is catalogued: each phase submits its own Dockerfile and the server builds whatever
arrives.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import secrets
import statistics
import threading
import time
import urllib.error
import urllib.request

RESOURCES = {"cpus": 1, "memory_gb": 1, "workdir_gb": 0.25,
             "workdir_medium": "temp", "gpus": 0, "pids": 64}

DYNAMIC = """FROM docker.io/library/alpine:latest
{extra}RUN head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \\n' > /stamp
ENV TEST_RUNTIME_NAME={name} ZERO_RUNTIME_SHELL=/bin/sh{egress}
"""

# The cases are the ones the live contract already pins, so a number here is a number for
# behaviour that is separately asserted -- a fast sandbox that stopped confining is not faster.
HOSTILE = [
    ("unprivileged-user", 'test "$(id -u)" -ne 0 && echo PASS',
     lambda r: r["returncode"] == 0 and "PASS" in r["stdout"]),
    ("read-only-root", "touch /MUST_NOT_EXIST", lambda r: r["returncode"] != 0),
    ("workspace-flood", "set -e; dd if=/dev/zero of=fill bs=1M count=512 status=none; echo BAD",
     lambda r: (r["returncode"] != 0 and "BAD" not in r["stdout"]
                and r["files"]["bytes_delta"] <= 256 * 2**20)),
    ("pid-flood", "for i in $(seq 1 400); do sleep 30 & done; wait",
     lambda r: "Resource temporarily unavailable" in r["err"]),
    ("memory-flood", "dd if=/dev/zero of=/dev/null bs=1500M count=1",
     lambda r: r["returncode"] != 0 and not r["timed_out"]),
    ("no-network", "wget -T2 -q -O- http://1.1.1.1/ && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED" and not r["network"]["outbound"]),
    ("stdio-tail", "dd if=/dev/zero bs=1k count=400 2>/dev/null | tr '\\0' x; printf OUT_END",
     lambda r: (r["stdout"].endswith("OUT_END") and r["stdout_truncated"]
                and r["stdout_total_bytes"] > r["stdout_bytes"])),
    ("timeout", "while :; do :; done", lambda r: r["timed_out"] and not r["alive"]),
]



def _auth() -> dict:
    """The shared secret, when the served sandbox enforces one (`ZERO_SANDBOX_TOKEN`)."""
    token = (os.environ.get("ZERO_SANDBOX_TOKEN") or "").strip()
    return {"Authorization": "Bearer " + token} if token else {}

def io_report(url: str, text: str, errors: list, grant: dict | None = None) -> dict:
    """Is I/O starved, and does the per-action `/work` scan tax a big workspace?

    `elapsed_ms` is server-side and INCLUDES the metadata scan, so two identical no-ops over
    workspaces of different sizes isolate what the scan costs per action -- the sandbox reads
    `/work` twice per action to report the file delta, and that is the one unbounded thing on the
    request path.
    """
    sid, out, size = "perf-io-" + secrets.token_hex(6), {}, 64

    def act(code: str, label: str, **request) -> dict:
        reply = execute(url, text, code, session_id=sid, wait_s=request.pop("wait_s", 120),
                        timeout_s=180, resources=grant or RESOURCES, **request)
        if "http" in reply:
            errors.append(f"io:{label}:HTTP {reply['http']}:{reply.get('error')}")
        elif reply["returncode"] != 0:
            errors.append(f"io:{label}:rc {reply['returncode']}:{reply['err'][:150]}")
        return reply

    def rate(reply: dict, megabytes: float) -> float | None:
        elapsed = reply.get("elapsed_ms") or 0
        return round(megabytes / (elapsed / 1000), 1) if elapsed else None

    wrote = act(f"dd if=/dev/zero of=blob bs=1M count={size} conv=fsync 2>/dev/null", "write")
    out["workdir_write_mb_s"] = rate(wrote, size)
    out["workdir_write_ms"] = wrote.get("elapsed_ms")
    read = act("dd if=blob of=/dev/null bs=1M 2>/dev/null", "read")
    out["workdir_read_mb_s"] = rate(read, size)
    streamed = act(f"dd if=/dev/zero bs=1M count={size} 2>/dev/null | tr '\\0' x", "stdout",
                   io_cap_kb=64)
    out["stdout_mb_s"] = rate(streamed, (streamed.get("stdout_total_bytes") or 0) / 2**20)
    out["stdout_retained_kb"] = (streamed.get("stdout_bytes") or 0) // 1024
    out["stdout_total_mb"] = round((streamed.get("stdout_total_bytes") or 0) / 2**20, 1)

    small = act("true", "noop-small")
    act("mkdir -p many && cd many && for i in $(seq 1 3000); do : > f$i; done", "fill")
    large = act("true", "noop-large")
    out["noop_ms_1_file"] = small.get("elapsed_ms")
    out["noop_ms_3000_files"] = large.get("elapsed_ms")
    out["scan_files"] = (large.get("files") or {}).get("scanned")
    if small.get("elapsed_ms") and large.get("elapsed_ms"):
        out["scan_overhead_ms"] = large["elapsed_ms"] - small["elapsed_ms"]
    act("rm -rf many blob", "cleanup", wait_s=0)
    return out


def dockerfile(name: str) -> str:
    return DYNAMIC.format(name=name, extra="", egress="")


def api(url: str, path: str, body: dict | None = None, timeout: float = 300) -> dict:
    request = urllib.request.Request(
        url.rstrip("/") + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", **_auth()})
    with urllib.request.urlopen(request, timeout=timeout) as reply:
        return json.load(reply)


def execute(url: str, text: str, code: str, **request) -> dict:
    body = {"runtime": {"dockerfile": text, "documentation": ""},
            "session_id": request.pop("session_id", None) or "p" + secrets.token_hex(8),
            "code": code, "cwd": "/work", "timeout_s": request.pop("timeout_s", 60),
            "wait_s": request.pop("wait_s", 0), "io_cap_kb": request.pop("io_cap_kb", 200),
            "resources": request.pop("resources", RESOURCES), **request}
    try:
        return api(url, "/execute", body, body["timeout_s"] + 900)
    except urllib.error.HTTPError as error:
        return {"http": error.code, **json.loads(error.read() or b"{}")}


def _stats(samples: list[float]) -> dict:
    """Latency in milliseconds. An empty phase reports nothing rather than a zero."""
    if not samples:
        return {"n": 0}
    ordered = sorted(samples)
    pick = lambda p: ordered[min(len(ordered) - 1, int(len(ordered) * p))]
    return {"n": len(ordered), "mean_ms": round(1000 * statistics.fmean(ordered)),
            "p50_ms": round(1000 * pick(.5)), "p95_ms": round(1000 * pick(.95)),
            "p99_ms": round(1000 * pick(.99)), "max_ms": round(1000 * ordered[-1])}


class Discovery:
    """Polls both discovery routes in the background. The point is the MAX, not the median: a
    single blocking call inside the gateway's event loop is what a deadline catches."""

    def __init__(self, url: str, every: float = 0.25):
        self.url, self.every, self.samples, self.errors = url, every, [], []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self):
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_exc):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        return False

    def _poll(self) -> None:
        while not self._stop.is_set():
            route = "/health" if len(self.samples) % 2 else "/runtimes"
            started = time.monotonic()
            try:
                api(self.url, route, timeout=30)
                self.samples.append(time.monotonic() - started)
            except Exception as error:
                self.errors.append(f"{route}:{type(error).__name__}:{error}")
            self._stop.wait(self.every)

    def report(self) -> dict:
        return {**_stats(self.samples), "routes": ["/health", "/runtimes"],
                "errors": self.errors}


def measure(url: str, *, runtimes: int = 4, actions: int = 8, n: int = 24,
            concurrency: int = 8, seed: int | None = None,
            submitted: str | None = None, code: str = "echo PASS",
            resources: dict | None = None) -> dict:
    """``submitted`` profiles ONE real runtime instead of throwaway alpine ones.

    A heavy image is the case the cheap ones cannot stand in for: its cold phase is a real build,
    and its warm and session phases say whether tens of thousands of small files cost anything at
    launch. The flood and hostile phases still use their own runtimes -- confinement and admission
    are properties of the sandbox, not of the image.
    """
    # ALWAYS a fresh name, never derived from `seed`: a rerun that reuses digests finds the
    # images already built and the cold phase measures a cache hit -- it read 220 ms against a
    # real 1300 ms, and `cold` is the only number that contains a build.
    grant = {**RESOURCES, **(resources or {})}
    tag = secrets.token_hex(4)
    texts = ([submitted] if submitted else
             [dockerfile(f"perf{tag}x{i}") for i in range(runtimes)])
    errors: list[str] = []
    phases: dict[str, dict] = {}

    def once(text: str, code: str, expect_zero: bool = True, label: str = "", **request):
        started = time.monotonic()
        reply = execute(url, text, code, resources=request.pop("resources", grant), **request)
        elapsed = time.monotonic() - started
        if "http" in reply:
            errors.append(f"{label}:HTTP {reply['http']}:{reply.get('error')}")
        elif expect_zero and reply["returncode"] != 0:
            errors.append(f"{label}:returncode {reply['returncode']}:{reply['err'][:200]}")
        return elapsed, reply

    before = api(url, "/health")

    # --- cold: the build IS the cost, so these run one at a time and are never reused ---------
    phases["cold"] = _stats([once(text, code, label=f"cold{i}")[0]
                             for i, text in enumerate(texts)])

    # --- warm: the same hashes again. Anything above the stateless number is not the cache ----
    phases["warm"] = _stats([once(text, code, label=f"warm{i}")[0]
                             for i, text in enumerate(texts)])

    # --- stateless: one container carrying its command, repeated ------------------------------
    phases["stateless"] = _stats([once(texts[0], code, label=f"stateless{i}")[0]
                                  for i in range(actions)])

    # --- session: launch once, exec per action. The gap to `stateless` is what a session buys -
    sid = "perf-" + secrets.token_hex(8)
    session = [once(texts[0], code, session_id=sid, wait_s=60,
                    label=f"session{i}")[0] for i in range(actions)]
    closing, reply = once(texts[0], code, session_id=sid, wait_s=0, label="session-close")
    if reply.get("alive"):
        errors.append("session-close: session stayed alive after wait_s=0")
    phases["session"] = _stats(session)

    # --- flood: mixed good and failing actions at concurrency, discovery polled throughout ----
    flooded = [dockerfile(f"perf{tag}f{i}") for i in range(runtimes)] if submitted else texts
    work = [i % len(flooded) for i in range(n)]
    latencies: list[float] = []
    lock = threading.Lock()

    def job(index: int) -> None:
        good = index % 2 == 0
        # The flood and the hostile matrix measure the SANDBOX, not the image, so they keep the
        # baseline grant: a bigger one would let `memory-flood` allocate what it was meant to be
        # denied, and would oversubscribe the node with throwaway runtimes.
        elapsed, reply = once(flooded[index % len(flooded)],
                              "echo PASS" if good else "echo EXPECTED >&2; exit 7",
                              expect_zero=good, label=f"flood{index}", resources=RESOURCES)
        if not good and reply.get("returncode") != 7:
            errors.append(f"flood{index}: expected returncode 7: {reply.get('returncode')}")
        with lock:
            latencies.append(elapsed)

    with Discovery(url) as discovery:
        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            for future in [pool.submit(job, index) for index in work]:
                future.result()
        wall = time.monotonic() - started
    phases["flood"] = {**_stats(latencies), "wall_s": round(wall, 2),
                       "concurrency": concurrency,
                       "requests_s": round(len(latencies) / wall, 2)}

    # --- hostile: the confinement envelope, and what holding it costs --------------------------
    attacks, failed = [], []
    for name, code, ok in HOSTILE:
        text = dockerfile(f"perf{tag}h{name}")
        started = time.monotonic()
        reply = execute(url, text, code, timeout_s=8 if name == "timeout" else 60,
                        io_cap_kb=1 if name == "stdio-tail" else 200)
        attacks.append(time.monotonic() - started)
        try:
            held = "http" not in reply and ok(reply)
        except Exception as error:
            held = False
            reply = {"error": f"{type(error).__name__}:{error}", **reply}
        if not held:
            failed.append(name)
            errors.append(f"hostile:{name}: {json.dumps(reply)[:300]}")
    phases["hostile"] = {**_stats(attacks), "cases": [name for name, _c, _o in HOSTILE],
                         "failed": failed}

    # --- io: throughput through the workspace and the streams, and the scan it costs ----------
    io = io_report(url, texts[0], errors, grant)

    seen = discovery.report()
    after = api(url, "/health")
    tiers: dict[str, int] = {}
    for facts in after["runtimes"].values():
        tiers[facts["tier"]] = tiers.get(facts["tier"], 0) + 1
    return {
        "url": url, "run": tag, "seed": seed, "at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "phases": phases, "io": io,
        "discovery": seen,
        "server": {"served": after["served"] - before["served"],
                   "uptime_s": after["uptime_s"], "sessions": after["sessions"],
                   "runtimes": len(after["runtimes"]), "tiers": tiers,
                   "cache": after["cache"], "failures": after["failures"]},
        "errors": errors + seen["errors"],
    }


COLUMNS = ("n", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms")


def render(report: dict) -> str:
    rows = [("phase", *COLUMNS, "note")]
    notes = {"flood": lambda p: f"{p['requests_s']}/s at c={p['concurrency']}",
             "hostile": lambda p: ("all held" if not p["failed"]
                                   else "FAILED: " + ",".join(p["failed"]))}
    for name, phase in report["phases"].items():
        rows.append((name, *[str(phase.get(key, "-")) for key in COLUMNS],
                     notes.get(name, lambda _p: "")(phase)))
    discovery = report["discovery"]
    rows.append(("discovery", *[str(discovery.get(key, "-")) for key in COLUMNS],
                 "/health + /runtimes while flooding"))
    width = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    out = [" ".join(str(cell).ljust(width[i]) for i, cell in enumerate(rows[0])).rstrip(),
           " ".join("-" * w for w in width)]
    out += [" ".join(str(cell).ljust(width[i]) for i, cell in enumerate(row)).rstrip()
            for row in rows[1:]]
    io = report["io"]
    out += ["", "io  workdir {write} MB/s write, {read} MB/s read | stdout {stream} MB/s "
                "({total} MB produced, {kept} KiB kept)".format(
                    write=io["workdir_write_mb_s"], read=io["workdir_read_mb_s"],
                    stream=io["stdout_mb_s"], total=io["stdout_total_mb"],
                    kept=io["stdout_retained_kb"]),
            "    /work scan {files} files costs {cost} ms per action "
            "(no-op {small} ms -> {large} ms)".format(
                files=io["scan_files"], cost=io.get("scan_overhead_ms"),
                small=io["noop_ms_1_file"], large=io["noop_ms_3000_files"])]
    server = report["server"]
    out += ["", f"served {server['served']} actions | {server['runtimes']} runtimes known "
                f"| tiers {server['tiers']} | ram {server['cache']['ram_used_bytes']}"
                f"/{server['cache']['ram_budget_bytes']} bytes"]
    out += [f"errors: {len(report['errors'])}"] + [f"  {e}" for e in report["errors"][:20]]
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description="measure a served sandbox")
    parser.add_argument("--url", default="http://127.0.0.1:8070")
    parser.add_argument("--runtimes", type=int, default=4)
    parser.add_argument("--actions", type=int, default=8)
    parser.add_argument("--n", type=int, default=24)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--dockerfile", default="",
                        help="profile this real runtime instead of throwaway alpine ones")
    parser.add_argument("--code", default="echo PASS",
                        help="the terminal action to time in every phase")
    # A real runtime is not served by the grant a busybox one needs: Mathlib alone wants GiB.
    parser.add_argument("--cpus", type=float, default=RESOURCES["cpus"])
    parser.add_argument("--memory-gb", type=float, default=RESOURCES["memory_gb"])
    parser.add_argument("--workdir-gb", type=float, default=RESOURCES["workdir_gb"])
    parser.add_argument("--pids", type=int, default=RESOURCES["pids"])
    parser.add_argument("--json", default="")
    args = parser.parse_args()
    submitted = (open(args.dockerfile, encoding="utf-8").read() if args.dockerfile else None)
    report = measure(args.url, runtimes=args.runtimes, actions=args.actions, n=args.n,
                     concurrency=args.concurrency, seed=args.seed, submitted=submitted,
                     code=args.code,
                     resources={"cpus": args.cpus, "memory_gb": args.memory_gb,
                                "workdir_gb": args.workdir_gb, "pids": args.pids})
    if args.json:
        with open(args.json, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(report, indent=2) + "\n")
    print(render(report))
    return bool(report["errors"])


if __name__ == "__main__":
    raise SystemExit(main())
