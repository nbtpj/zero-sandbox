"""The live HTTP contract. Needs a served sandbox in ZERO_SANDBOX_URL.

Every case submits its own throwaway Dockerfile and depends on no runtime catalog.
"""
from __future__ import annotations

import concurrent.futures
import hashlib
import json
import secrets
import time
import urllib.error
import urllib.request

import pytest

from conftest import RESOURCES, RESPONSE, api, dynamic, live, submit


@live
def test_a_dockerfile_outside_the_catalog_is_built_on_demand_and_served(url):
    name = "dyn" + secrets.token_hex(4)
    reply = submit(url, dynamic(name), "id -u; cat /stamp", name=name)
    assert reply["returncode"] == 0 and reply["stdout"].split()[0] == "65532"
    assert len(reply["runtime"]["hash"]) == 64

@live
def test_concurrent_cold_requests_build_the_image_once(url):
    name = "cold" + secrets.token_hex(4)
    text = dynamic(name)
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        stamps = {reply["stdout"] for reply in pool.map(
            lambda _: submit(url, text, "cat /stamp", name=name), range(8))}
    assert len(stamps) == 1, stamps

@live
def test_two_dockerfile_hashes_stay_distinct_and_both_cached(url):
    name = "twin" + secrets.token_hex(4)
    one = submit(url, dynamic(name, extra="ENV E=1\n"), "cat /stamp; echo $E", name=name)
    two = submit(url, dynamic(name, extra="ENV E=2\n"), "cat /stamp; echo $E", name=name)
    assert one["stdout"] != two["stdout"] and one["runtime"]["hash"] != two["runtime"]["hash"]
    cached = api(url, "/health")["runtimes"]
    for reply in (one, two):
        state = cached[reply["runtime"]["hash"]]
        assert state["build"] == "cached" and state["bytes"] > 0 and state["tier"] != "none"

@live
def test_resource_grants_change_sessions_without_rebuilding_the_image(url):
    name, text = "grant" + secrets.token_hex(4), None
    text = dynamic(name)
    small = submit(url, text, "true", name=name)
    bigger = {**RESOURCES, "memory_gb": 1.5, "pids": 96}
    large = submit(url, text, "true", name=name, resources=bigger)
    assert small["runtime"]["hash"] == large["runtime"]["hash"]
    assert small["resources"] == {**RESOURCES, "cpus": 1.0, "memory_gb": 1.0,
                                  "workdir_gb": 0.25}
    assert large["resources"]["memory_gb"] == 1.5 and large["resources"]["pids"] == 96

@live
def test_documentation_changes_the_served_metadata_but_not_the_image(url):
    name = "docs" + secrets.token_hex(4)
    text = dynamic(name)
    first = submit(url, text, "cat /stamp", name=name, documentation="v1")
    second = submit(url, text, "cat /stamp", name=name, documentation="v2")
    assert first["stdout"] == second["stdout"]
    assert first["runtime"]["hash"] == second["runtime"]["hash"]
    key = first["runtime"]["hash"]
    assert api(url, "/runtimes")["runtimes"][key]["documentation"] == "v2"

@live
def test_health_reports_build_state_size_tier_and_failures(url):
    name = "sick" + secrets.token_hex(4)
    submit(url, dynamic(name, extra="RUN exit 3\n"), "true", name=name, expect=503)
    health = api(url, "/health")
    key = hashlib.sha256(dynamic(name, extra="RUN exit 3\n").encode()).hexdigest()
    assert health["runtimes"][key]["build"] == "failed"
    assert key in health["failures"] and health["failures"][key]
    assert {"runtimes", "sessions", "served", "uptime_s"} <= health.keys()

@live
def test_the_reply_never_echoes_a_submitted_secret(url):
    name, secret = "sec" + secrets.token_hex(4), "sk-" + secrets.token_hex(20)
    reply = submit(url, dynamic(name), "echo ok", name=name, env={"OPENAI_API_KEY": secret})
    assert secret not in json.dumps(reply)

@live
@pytest.mark.parametrize("case,code,ok", [
    ("read-only-root", "touch /MUST_NOT_EXIST", lambda r: r["returncode"] != 0),
    ("workspace-flood", "set -e; dd if=/dev/zero of=fill bs=1M count=512 status=none; echo BAD",
     lambda r: r["returncode"] == 153 and "BAD" not in r["stdout"]
     and r["files"]["bytes_delta"] <= 256 * 2**20),
    ("pid-flood", "for i in $(seq 1 400); do sleep 30 & done; wait",
     lambda r: "Resource temporarily unavailable" in r["err"]),
    ("memory-flood", "dd if=/dev/zero of=/dev/null bs=1500M count=1",
     lambda r: r["returncode"] == 137 and not r["timed_out"]),
    ("no-network", "wget -T2 -q -O- http://1.1.1.1/ && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED" and not r["network"]["outbound"]),
])
def test_a_submitted_runtime_is_confined(url, case, code, ok):
    name = "jail" + secrets.token_hex(4)
    assert ok(submit(url, dynamic(name), code, name=name, timeout_s=45)), case

@live
def test_a_timeout_destroys_the_whole_session(url):
    name, sid = "slow" + secrets.token_hex(4), "s" + secrets.token_hex(8)
    text = dynamic(name)
    submit(url, text, "printf alpha > note", name=name, session_id=sid, wait_s=60)
    killed = submit(url, text, "while :; do :; done", name=name, session_id=sid,
                    timeout_s=2, wait_s=60)
    assert killed["timed_out"] and not killed["alive"]
    fresh = submit(url, text, "cat note 2>/dev/null || echo GONE", name=name, session_id=sid)
    assert fresh["stdout"].strip() == "GONE"

@live
def test_output_is_tail_capped_at_the_requested_size(url):
    name = "cap" + secrets.token_hex(4)
    text = dynamic(name)
    code = "dd if=/dev/zero bs=1k count=400 2>/dev/null | tr '\\0' x; printf OUT_END"
    small = submit(url, text, code, name=name, io_cap_kb=1)
    assert (small["stdout"].endswith("OUT_END") and small["stdout_bytes"] <= 1024
            and small["stdout_truncated"] and small["stdout_total_bytes"] > 400_000)
    assert submit(url, text, "printf X", name=name, io_cap_kb=1024)["returncode"] == 0
    assert submit(url, text, "printf X", name=name, io_cap_kb=2048,
                  expect=400)["error"].startswith("bad_request:io_cap_kb")

@live
def test_a_session_is_ordered_bound_and_expires_when_idle(url):
    name, sid = "sess" + secrets.token_hex(4), "s" + secrets.token_hex(8)
    text = dynamic(name)
    first = submit(url, text, "printf alpha > note", name=name, session_id=sid, wait_s=20)
    assert first["alive"] and first["files"]["created"] >= 1
    second = submit(url, text, "printf beta >> note; cat note", name=name, session_id=sid,
                    wait_s=3)
    assert second["stdout"] == "alphabeta"
    other = submit(url, dynamic(name, extra="ENV E=1\n"), "true", name=name, session_id=sid,
                   expect=409)
    assert other["error"].startswith("conflict:")
    time.sleep(10)
    assert submit(url, text, "cat note 2>/dev/null || echo GONE", name=name,
                  session_id=sid)["stdout"].strip() == "GONE"

@live
def test_endpoint_egress_reaches_the_requested_host_and_nothing_else(url):
    name, sid = "net" + secrets.token_hex(4), "n" + secrets.token_hex(8)
    text = dynamic(name, extra="RUN apk add --no-cache curl iproute2\n", egress=True)
    endpoint = "https://openrouter.ai/api/v1"
    web = lambda code: submit(url, text, code, name=name, session_id=sid, wait_s=100,
                              timeout_s=60, network=endpoint)["stdout"]
    status = "curl -sS -m 20 -o /dev/null -w '%{http_code}' "
    assert web(status + "https://openrouter.ai/api/v1/models").strip() == "200"
    assert "200" not in web(status + "https://example.com/")
    assert "200" not in web(status + "--noproxy '*' https://1.1.1.1/")
    assert "200" not in web(status + "--noproxy '*' https://openrouter.ai/api/v1/models")
    assert "not permitted" in web("ip addr add 10.9.9.9/24 dev eth0 2>&1").lower()
    assert submit(url, dynamic(name + "x"), "true", name=name + "x", network=endpoint,
                  expect=403)["error"].startswith("forbidden:")

@live
def test_two_sessions_of_one_runtime_share_no_state(url):
    """The image is shared read-only; each session gets its own writable layer and /work."""
    name = "pair" + secrets.token_hex(4)
    text = dynamic(name)
    one, two = "a" + secrets.token_hex(6), "b" + secrets.token_hex(6)
    submit(url, text, "printf mine > /work/note; printf mine > /tmp/note", name=name,
           session_id=one, wait_s=30)
    other = submit(url, text, "cat /work/note /tmp/note 2>/dev/null || echo EMPTY", name=name,
                   session_id=two)
    assert other["stdout"].strip() == "EMPTY"
    assert submit(url, text, "cat /work/note", name=name,
                  session_id=one)["stdout"] == "mine"

@live
def test_a_second_request_reuses_the_cached_image(url):
    name = "warm" + secrets.token_hex(4)
    text = dynamic(name)
    first = submit(url, text, "cat /stamp", name=name)
    before = api(url, "/health")["runtimes"][first["runtime"]["hash"]]
    second = submit(url, text, "cat /stamp", name=name)
    after = api(url, "/health")["runtimes"][second["runtime"]["hash"]]
    assert first["stdout"] == second["stdout"] and before["bytes"] == after["bytes"] > 0
