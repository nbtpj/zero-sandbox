"""Concurrency as a correctness question, not only a throughput one.

`tests/perf.py` times a flood; these cases assert what must remain TRUE during one. A sandbox
that stays fast while handing a reply to the wrong runtime, leaking one session's workspace into
another, or letting a failed build poison the gateway is not a sandbox that works.
"""
from __future__ import annotations

import concurrent.futures
import hashlib
import os
import secrets
import threading
import time

import pytest

from conftest import RESOURCES, api, dynamic, live, submit

# A flood is HUNDREDS of actions, not a dozen: a handful never queues behind admission, never
# refills the worker pool, and never lets the reaper run while requests are in flight. Overridable
# so a slow host can still run the suite.
FLOOD = int(os.environ.get("ZERO_FLOOD_N", "240"))
CONCURRENCY = int(os.environ.get("ZERO_FLOOD_CONCURRENCY", "8"))
RUNTIMES = int(os.environ.get("ZERO_FLOOD_RUNTIMES", "4"))


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _in_parallel(jobs, workers: int):
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        return [future.result() for future in [pool.submit(job) for job in jobs]]


@live
def test_a_flood_never_answers_for_the_wrong_runtime(url):
    """Distinct submitted Dockerfiles are distinct images, and every reply must both name the
    hash it was asked for and come out of the image that hash built.

    The container reports `TEST_RUNTIME_NAME`, not `/stamp`: the fixture's `RUN` line is
    byte-identical across these Dockerfiles, so BuildKit reuses one cached layer and all four
    stamps agree. The images are still distinct -- it is the trailing `ENV` that differs -- and
    that ENV is therefore the only thing in the container that can identify its own build.
    """
    tag = secrets.token_hex(4)
    names = [f"flood{tag}x{i}" for i in range(RUNTIMES)]
    texts = [dynamic(name) for name in names]
    order = [i % len(texts) for i in range(FLOOD)]

    def job(index: int):
        text, good = texts[order[index]], index % 2 == 0
        code = ("printf %s \"$TEST_RUNTIME_NAME\"" if good else
                "printf %s \"$TEST_RUNTIME_NAME\" >&2; exit 7")
        return order[index], submit(url, text, code, name="flood", timeout_s=90)

    started = time.monotonic()
    replies = _in_parallel([lambda i=i: job(i) for i in range(len(order))], CONCURRENCY)
    wall = time.monotonic() - started
    print(f"\n  {len(order)} actions over {RUNTIMES} runtimes at c={CONCURRENCY}: "
          f"{wall:.1f}s, {len(order) / wall:.1f}/s")
    for position, (which, reply) in enumerate(replies):
        assert reply["runtime"]["hash"] == _digest(texts[which]), (position, which)
        assert reply["returncode"] == (0 if position % 2 == 0 else 7), reply
        assert (reply["stdout"] or reply["err"]).strip() == names[which], (
            "a reply came out of another runtime's image", position, reply)


@live
def test_concurrent_sessions_of_one_runtime_never_see_each_other(url):
    """Sessions share an image and must share nothing else. Ordered actions in one session, many
    sessions at once, and each may only ever read back its own token."""
    text = dynamic("floodsess" + secrets.token_hex(4))
    submit(url, text, "true", name="warm", timeout_s=120)  # build once, outside the timing

    def job(index: int):
        sid, token = f"fl{secrets.token_hex(6)}", secrets.token_hex(8)
        first = submit(url, text, f"printf %s {token} > mine", name="s", session_id=sid,
                       wait_s=45)
        assert first["alive"] and first["files"]["created"] == 1, first
        second = submit(url, text, "ls | tr '\\n' ' '; cat mine", name="s", session_id=sid,
                        wait_s=0)
        return token, second

    sessions = max(6, CONCURRENCY)
    for token, reply in _in_parallel([lambda i=i: job(i) for i in range(sessions)], sessions):
        assert reply["stdout"].strip() == f"mine {token}", reply
        assert not reply["alive"]


@live
def test_discovery_stays_responsive_under_a_flood(url):
    """Builds, executions and hostile cases in flight must not make discovery unanswerable."""
    import os

    deadline = float(os.environ.get("ZERO_HEALTH_DEADLINE_S", "1.0"))
    tag = secrets.token_hex(4)
    texts = [dynamic(f"flooddisc{tag}x{i}") for i in range(RUNTIMES)]
    polls, errors, stop = [], [], threading.Event()

    def poll():
        while not stop.is_set():
            route = "/health" if len(polls) % 2 else "/runtimes"
            started = time.monotonic()
            try:
                api(url, route, timeout=30)
                polls.append(time.monotonic() - started)
            except Exception as error:
                errors.append(f"{route}:{type(error).__name__}:{error}")
            stop.wait(0.25)

    watcher = threading.Thread(target=poll, daemon=True)
    watcher.start()
    try:
        _in_parallel([lambda i=i: submit(url, texts[i % len(texts)], "echo PASS", name="f",
                                         timeout_s=90) for i in range(FLOOD)], CONCURRENCY)
    finally:
        stop.set()
        watcher.join(timeout=5)
    ordered = sorted(polls)
    print(f"\n  discovery over {FLOOD} actions: {len(polls)} polls, "
          f"p50 {1000 * ordered[len(ordered) // 2]:.0f}ms, max {1000 * ordered[-1]:.0f}ms")
    assert not errors, errors
    assert len(polls) > 5, polls
    assert max(polls) < deadline, f"discovery max {max(polls):.3f}s over {deadline}s"


@live
def test_a_flood_of_failing_builds_does_not_poison_the_gateway(url):
    """A submitted Dockerfile is executable input and some of it will not build. Each failure is
    its own 503, is reported against its own hash, and costs a good runtime nothing."""
    tag = secrets.token_hex(4)
    broken = [dynamic(f"floodbad{tag}x{i}", extra=f"RUN exit {3 + i}\n") for i in range(4)]
    healthy = dynamic(f"floodok{tag}")

    def job(index: int):
        if index % 2:
            text = broken[(index // 2) % len(broken)]
            return "bad", text, submit(url, text, "true", name="bad", timeout_s=90, expect=503)
        return "good", healthy, submit(url, healthy, "echo PASS", name="good", timeout_s=90)

    results = _in_parallel([lambda i=i: job(i) for i in range(max(8, FLOOD // 8))],
                           CONCURRENCY // 2 or 1)
    for kind, _text, reply in results:
        if kind == "bad":
            assert reply["error"].startswith("unavailable:"), reply
        else:
            assert reply["returncode"] == 0 and reply["stdout"].strip() == "PASS", reply

    health = api(url, "/health")
    attempted = {text for kind, text, _reply in results if kind == "bad"}
    assert attempted, "the flood never submitted a failing build"
    for text in attempted:
        facts = health["runtimes"].get(_digest(text))
        assert facts and facts["build"] == "failed" and facts["failure"], facts
    assert health["runtimes"][_digest(healthy)]["build"] == "cached"
    # A failure is remembered against its own hash and never against the healthy one.
    assert _digest(healthy) not in health["failures"]


@live
def test_admission_serializes_a_flood_it_cannot_run_at_once(url):
    """The gateway grants CPU FIFO and by weight. More concurrent requests than the server has
    admission for must queue, not fail."""
    text = dynamic("floodadmit" + secrets.token_hex(4))
    submit(url, text, "true", name="warm", timeout_s=120)
    wide = max(12, CONCURRENCY * 4)
    replies = _in_parallel(
        [lambda: submit(url, text, "echo PASS", name="admit", timeout_s=120,
                        resources={**RESOURCES, "cpus": 1}) for _ in range(wide)], wide)
    assert all(reply["returncode"] == 0 for reply in replies), replies
    assert all(reply["stdout"].strip() == "PASS" for reply in replies)


@live
def test_a_grant_larger_than_admission_is_refused_not_queued(url):
    """An impossible grant must be a 400 at once, never a request that waits for ever."""
    text = dynamic("floodbig" + secrets.token_hex(4))
    reply = submit(url, text, "true", name="big", timeout_s=60, expect=400,
                   resources={**RESOURCES, "cpus": 4096})
    assert "CPUs" in reply["error"], reply


@live
def test_startup_contention_does_not_time_out_deterministic_commands(url):
    """`proc.wait(timeout_s)` used to wrap the whole stateless run, so container startup
    ate the command budget. Under concurrent admission past the host's CPUs, identical
    deterministic code then timed out or not depending on launch contention -- a stochastic
    verifier score. With startup separated, a short command with a short timeout must return the
    SAME thing every time, however contended the launch."""
    import os

    text = dynamic("determ" + secrets.token_hex(4))
    submit(url, text, "true", name="warm", timeout_s=120)     # build once, off the clock
    cpus = os.cpu_count() or 4
    n, concurrency = max(24, cpus * 4), max(16, cpus * 4)     # deliberately oversubscribe

    def one(i):
        # A trivial deterministic command with a SHORT budget. If startup counted against it,
        # this would time out under contention; it must not.
        return submit(url, text, "printf 0.50", name="determ", timeout_s=5, resources=RESOURCES)

    replies = _in_parallel([lambda i=i: one(i) for i in range(n)], concurrency)
    codes = {r["returncode"] for r in replies}
    outs = {r["stdout"] for r in replies}
    timed = [r for r in replies if r["timed_out"]]
    assert codes == {0}, f"nondeterministic return codes under contention: {codes}"
    assert outs == {"0.50"}, f"nondeterministic output: {outs}"
    assert not timed, f"{len(timed)}/{n} timed out on a 5s budget -- startup ate the budget"
    # And the split is reported: startup shows up as start_ms/build_ms, not in elapsed_ms.
    assert all("start_ms" in r and "build_ms" in r for r in replies)
    assert max(r["elapsed_ms"] for r in replies) < 5000, "elapsed_ms must be the command only"

