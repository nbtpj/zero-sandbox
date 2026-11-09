"""Lean/Mathlib as the heavy end of the flood: a real IO-bound runtime under concurrency.

A 12.7 GB image of ~11,000 `.olean` files is the load the light alpine runtimes cannot stand in
for -- its cold phase is a real multi-minute build, its launch assembles a large overlay, and
`import Mathlib` reads much of the library. This runs it AS a flood, mixed with light actions, and
asserts the heavy runtime neither starves the light ones nor makes discovery unanswerable.

Gated: it builds a 12.7 GB image and each proof reloads Mathlib (~2 min), so it is off unless
`ZERO_LEAN_BENCH=1`. Point `ZERO_LEAN_DOCKERFILE` at another heavy runtime to benchmark that one.
"""
from __future__ import annotations

import concurrent.futures
import hashlib
import os
import pathlib
import threading
import time

import pytest

from conftest import RESOURCES, api, dynamic, submit

BENCH = pytest.mark.skipif(
    not os.environ.get("ZERO_LEAN_BENCH"),
    reason="set ZERO_LEAN_BENCH=1 to run the heavy Lean/Mathlib benchmark")

LEAN_DOCKERFILE = os.environ.get(
    "ZERO_LEAN_DOCKERFILE",
    str(pathlib.Path(__file__).resolve().parent / "runtimes" / "lean-proof.Dockerfile"))
LEAN_GRANT = {"cpus": 3, "memory_gb": 6, "workdir_gb": 2, "workdir_medium": "temp",
              "gpus": 0, "pids": 512}
PROOF = ("printf 'import Mathlib\\nexample : (1:Nat)+1=2 := by norm_num\\n' > p.lean "
         "&& lean-runtime p.lean && echo LEAN_OK")
LEAN_ACTIONS = int(os.environ.get("ZERO_LEAN_ACTIONS", "4"))
LIGHT_ACTIONS = int(os.environ.get("ZERO_LEAN_LIGHT_ACTIONS", "40"))


def lean_text() -> str:
    return pathlib.Path(LEAN_DOCKERFILE).read_text(encoding="utf-8")


LIGHT = [dynamic(f"lightbench{i}") for i in range(4)]


@pytest.fixture(scope="module")
def lean_built(url):
    """Pay every cold build once, up front: the flood must measure serving, not building. The
    light runtimes are warmed too, so a starved light action is starved by ADMISSION behind Lean,
    not by its own build competing for the same slots."""
    text = lean_text()
    started = time.monotonic()
    reply = submit(url, text, PROOF, name="lean-warm", timeout_s=1800, resources=LEAN_GRANT)
    assert reply["returncode"] == 0 and "LEAN_OK" in reply["stdout"], reply
    print(f"\n  lean cold build+proof: {time.monotonic() - started:.0f}s, "
          f"server {reply['elapsed_ms']}ms")
    for light in LIGHT:
        assert submit(url, light, "echo PASS", name="light-warm",
                      timeout_s=120)["returncode"] == 0
    return text


@BENCH
@pytest.mark.slow
def test_lean_runs_and_reports_its_io(url, lean_built):
    """The benchmark numbers: launch overlay assembly vs the Mathlib load, and the inventory."""
    text = lean_built
    launch = submit(url, text, "echo LAUNCHED", name="lean", timeout_s=600, resources=LEAN_GRANT)
    inv = submit(url, text, "find / -name '*.olean' 2>/dev/null | wc -l; "
                            "du -sh /opt/lean-libs 2>/dev/null | cut -f1",
                 name="lean", timeout_s=600, resources=LEAN_GRANT)
    oleans = inv["stdout"].split()
    print(f"\n  launch (overlay assembly, no import): server {launch['elapsed_ms']}ms"
          f"\n  olean inventory: {oleans}")
    assert launch["returncode"] == 0 and launch["stdout"].strip() == "LAUNCHED"
    assert int(oleans[0]) > 1000, inv["stdout"]


@BENCH
@pytest.mark.slow
def test_a_heavy_runtime_does_not_starve_the_light_ones_or_discovery(url, lean_built):
    """The mixed flood: Lean proofs and light actions at once, discovery polled throughout. The
    heavy runtime must not make a light action fail, nor push discovery past its deadline."""
    lean = lean_built
    light = LIGHT
    deadline = float(os.environ.get("ZERO_HEALTH_DEADLINE_S", "1.0"))
    errors, latencies, polls = [], [], []
    stop = threading.Event()

    def poll():
        while not stop.is_set():
            route = "/health" if len(polls) % 2 else "/runtimes"
            t = time.monotonic()
            try:
                api(url, route, timeout=30)
                polls.append(time.monotonic() - t)
            except Exception as error:
                errors.append(f"{route}:{type(error).__name__}:{error}")
            stop.wait(0.25)

    def one_lean(i):
        t = time.monotonic()
        r = submit(url, lean, PROOF, name="lean", timeout_s=1800, resources=LEAN_GRANT)
        latencies.append(("lean", time.monotonic() - t))
        if r["returncode"] != 0 or "LEAN_OK" not in r["stdout"]:
            errors.append(f"lean{i}: rc {r['returncode']}: {r['err'][:150]}")

    def one_light(i):
        t = time.monotonic()
        r = submit(url, light[i % len(light)], "echo PASS", name="light", timeout_s=120)
        latencies.append(("light", time.monotonic() - t))
        if r["returncode"] != 0 or r["stdout"].strip() != "PASS":
            errors.append(f"light{i}: rc {r['returncode']}: {r['stdout'][:80]}")

    watcher = threading.Thread(target=poll, daemon=True)
    watcher.start()
    started = time.monotonic()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            jobs = ([pool.submit(one_lean, i) for i in range(LEAN_ACTIONS)] +
                    [pool.submit(one_light, i) for i in range(LIGHT_ACTIONS)])
            for job in jobs:
                job.result()
    finally:
        stop.set()
        watcher.join(timeout=5)
    wall = time.monotonic() - started

    def at(values, q):
        return values[min(len(values) - 1, int(len(values) * q))]

    lean_ms = sorted(1000 * d for kind, d in latencies if kind == "lean")
    light_ms = sorted(1000 * d for kind, d in latencies if kind == "light")
    disc = sorted(polls)
    print(f"\n  mixed flood: {LEAN_ACTIONS} lean + {LIGHT_ACTIONS} light in {wall:.0f}s"
          f"\n  lean  p50 {at(lean_ms, .5):.0f}ms  max {lean_ms[-1]:.0f}ms"
          f"\n  light p50 {at(light_ms, .5):.0f}ms  p95 {at(light_ms, .95):.0f}ms  "
          f"max {light_ms[-1]:.0f}ms"
          f"\n  discovery {len(disc)} polls  p50 {1000 * at(disc, .5):.0f}ms  "
          f"p95 {1000 * at(disc, .95):.0f}ms  max {1000 * disc[-1]:.0f}ms")

    # Correctness first: under a heavy mixed load EVERYTHING must complete. No proof failed, no
    # light action failed, no discovery poll errored.
    assert not errors, errors
    assert api(url, "/health")["ok"]
    # Discovery is answered from gateway facts, so its TYPICAL cost stays flat while Lean launches
    # -- the p50/p95 assert that. A rare max spike is the node not scheduling the gateway PROCESS
    # under a saturated single VM, not an engine call on the request path; it is reported, not
    # asserted, and the p95 bound still catches a genuine stall.
    assert disc, "discovery was never polled"
    assert 1000 * at(disc, .5) < deadline * 1000, f"discovery p50 {1000 * at(disc, .5):.0f}ms"
    assert 1000 * at(disc, .95) < deadline * 1000, f"discovery p95 {1000 * at(disc, .95):.0f}ms"
    # A light action's TYPICAL latency stays interactive even while proofs run. Its tail is the
    # admission cost of queueing behind a 3-CPU Lean job (FIFO, deliberate) and is reported.
    assert at(light_ms, .5) < deadline * 5000, f"light p50 {at(light_ms, .5):.0f}ms"
