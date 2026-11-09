"""The measured claims. `tests/perf.py` produces the report; this asserts what it must say.

Run the full profile by hand for a number to quote:

    python tests/perf.py --url http://127.0.0.1:8070 --n 120 --concurrency 8 --json report.json
"""
from __future__ import annotations

import json
import os

import pytest

import perf
from conftest import live


@pytest.fixture(scope="module")
def report(url) -> dict:
    """One measured run, shared by every assertion below; it costs real builds."""
    out = perf.measure(url, runtimes=3, actions=6, n=18, concurrency=6, seed=20260828)
    print("\n" + perf.render(out))
    if path := os.environ.get("ZERO_PERF_JSON"):
        with open(path, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(out, indent=2) + "\n")
    return out


@live
def test_the_sandbox_serves_every_phase_without_an_error(report):
    assert not report["errors"], report["errors"]


@live
def test_the_confinement_envelope_holds_under_every_attack(report):
    """A faster sandbox that stopped confining is not a faster sandbox."""
    assert report["phases"]["hostile"]["failed"] == []


@live
def test_a_cached_image_is_paid_for_once(report):
    """The second request for a hash must not pay a build: that is the whole cache."""
    cold, warm = report["phases"]["cold"], report["phases"]["warm"]
    assert warm["p50_ms"] < cold["p50_ms"], (cold, warm)
    assert warm["max_ms"] < cold["p50_ms"], (cold, warm)


@live
def test_a_session_action_is_cheaper_than_a_fresh_container(report):
    """A live session pays launch once; a stateless action pays it every time."""
    session, stateless = report["phases"]["session"], report["phases"]["stateless"]
    assert session["p50_ms"] <= stateless["p50_ms"], (session, stateless)


@live
def test_a_warm_runtime_costs_what_a_repeated_one_costs(report):
    """The pooled session worker is not bound to a runtime, so the FIRST action on a warm image
    costs what a repeated one does. While the pool was keyed by digest this read 1636 ms
    against 212 ms, and the gap was a fresh Python process per runtime."""
    warm, stateless = report["phases"]["warm"], report["phases"]["stateless"]
    assert warm["p50_ms"] < 4 * stateless["p50_ms"], (warm, stateless)


@live
def test_discovery_stays_answerable_while_the_flood_runs(report):
    """Builds and executions in flight must not make /health or /runtimes unanswerable."""
    deadline = float(os.environ.get("ZERO_HEALTH_DEADLINE_S", "1.0")) * 1000
    discovery = report["discovery"]
    assert discovery["n"] > 5, discovery
    assert not discovery["errors"], discovery["errors"]
    assert discovery["max_ms"] < deadline, discovery


@live
def test_the_flood_makes_progress_on_every_request(report):
    flood = report["phases"]["flood"]
    assert flood["n"] == 18 and flood["requests_s"] > 0, flood


@live
def test_io_is_not_what_limits_an_action(report):
    """Throughput through the workspace and the streams, and the one unbounded thing on the
    request path: the sandbox reads `/work` twice per action to report its file delta."""
    io = report["io"]
    assert io["workdir_write_mb_s"] and io["workdir_read_mb_s"], io
    assert io["stdout_total_mb"] >= 32 and io["stdout_retained_kb"] <= 64, io
    assert io["stdout_mb_s"] and io["stdout_mb_s"] > 1, io
    # `_files` is capped at 10_000 entries; 3000 of them may not dominate an action.
    assert io["scan_files"] >= 3000, io
    assert io["scan_overhead_ms"] is not None and io["scan_overhead_ms"] < 250, io
