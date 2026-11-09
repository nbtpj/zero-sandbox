"""Flood a served sandbox with checked work across runtimes while injecting infrastructure faults.

The assertion is the requirement itself: while the service is up, every valid request gets its
CORRECT result -- never an infrastructure error (`unavailable:`, `server_error:`), never a bare
5xx answered while up, never a wrong or partial result -- and a candidate's own failure (exit
code, signal, timeout, OOM, truncated output) comes back as exactly that result. "Down" (refused,
cut, or Serve's bare 5xx) is counted separately: the client waits and re-sends, as `sandbox.route`
does.

Run by hand against a TEST deployment, never production:

    python tests/chaos.py --url http://HOST:PORT --token-file TOKEN \\
        --runtimes /path/to/runtimes --seconds 1800 --concurrency 48 \\
        --chaos actor,container,stop,engine,disconnect \\
        --host-cmd 'srun --overlap --jobid J --ntasks=1 bash -c' \\
        --engine-cmd 'srun --overlap --jobid J --ntasks=1 bash /path/vm.sh' \\
        --socket /tmp/.../podman.sock --json /tmp/chaos.json

`--host-cmd` runs a shell snippet on the service host; `--engine-cmd` runs podman against the
engine. Without `--runtimes` a small built-in workload is used.
"""
from __future__ import annotations

import argparse
import base64
import collections
import concurrent.futures
import hashlib
import http.client
import json
import os
import random
import secrets
import shlex
import subprocess
import threading
import time
import urllib.error
import urllib.request

R = collections.namedtuple("R", "cpus memory_gb workdir_gb pids")
RES = {"python-runtime": R(2, 2, 1, 512), "cpp-runtime": R(4, 2, 0.5, 512),
       "js-runtime": R(2, 1, 0.5, 512), "shell-runtime": R(1, 1, 0.25, 256),
       "smt-proof": R(2, 1, 0.5, 512), "lean-proof": R(4, 2, 0.5, 256)}
BUILTIN = {"debian": "FROM docker.io/library/debian:bookworm-slim\nENV ZERO_RUNTIME_SHELL=/bin/bash\n"}


def _write(path: str, text: str | bytes) -> str:
    data = text if isinstance(text, bytes) else text.encode()
    return f"printf %s {shlex.quote(base64.b64encode(data).decode())} | base64 -d > {path}"


def _fnv(text: str) -> int:
    h = 1469598103934665603
    for byte in text.encode():
        h = ((h ^ byte) * 1099511628211) % 2**64
    return h


def job(name: str, nonce: str, kind: str):
    """(code, timeout_s, resources, check) for one request; ``check(reply) -> str|None`` says
    what is wrong with a 200 reply, or None when it is the correct result."""
    res = RES.get(name, R(1, 1, 0.25, 256))
    digest = hashlib.sha256(nonce.encode()).hexdigest()

    def stdout_is(expected):
        return lambda r: None if r.get("returncode") == 0 and r.get("stdout", "").strip() == expected \
            else f"want rc0 {expected!r}, got rc{r.get('returncode')} {r.get('stdout', '')[-200:]!r} " \
                 f"err={r.get('err', '')[-300:]!r}"
    if kind == "big":
        payload = secrets.token_bytes(random.randint(100_000, 450_000))
        code = "\n".join([_write("/work/payload.bin", payload), _write(
            "/work/main.py", "import hashlib;print(hashlib.sha256(open('/work/payload.bin','rb')"
            ".read()).hexdigest())"), "exec python3 -I -B /work/main.py"])
        return code, 60, res, stdout_is(hashlib.sha256(payload).hexdigest())
    if kind == "exit3":
        return ("python3 -c 'import sys; print(\"x\"); sys.exit(3)'", 30, res,
                lambda r: None if (r["returncode"], r["stdout"]) == (3, "x\n") else f"exit3 {r}")
    if kind == "segv":
        return ("python3 -c 'import os,signal; os.kill(os.getpid(), signal.SIGSEGV)'", 30, res,
                lambda r: None if r["returncode"] == 139 and r["signal"] == 11 else f"segv {r['returncode']}")
    if kind == "timeout":
        return ("echo before; sleep 30", 2, res,
                lambda r: None if r["timed_out"] and r["returncode"] == 124 and r["stdout"] == "before\n"
                else f"timeout {r['returncode']} {r['timed_out']} {r['stdout']!r}")
    if kind == "oom":
        return ("python3 -c 'b = bytearray(6 * 1024**3); print(len(b))'", 60, res,
                lambda r: None if r["returncode"] in (137, 1) and not r["timed_out"]
                and "6442450944" not in r["stdout"] else f"oom {r['returncode']} {r['stdout'][:80]!r}")
    if kind == "flood-out":
        return ("python3 -c 'import sys; sys.stdout.write(\"a\" * 5000000)'", 60, res,
                lambda r: None if r["returncode"] == 0 and r["stdout_truncated"] and
                r["stdout_total_bytes"] == 5000000 and len(r["stdout"]) == 200 * 1024
                else f"flood-out {r['returncode']} {r['stdout_total_bytes']} {len(r['stdout'])}")
    if name == "python-runtime" or name == "debian":
        if name == "debian":
            return f"printf %s {nonce} | sha256sum | cut -d' ' -f1", 30, res, stdout_is(digest)
        code = "\n".join([_write("/work/main.py", f'import hashlib\nprint(hashlib.sha256(b"{nonce}").hexdigest())'),
                          "exec python3 -I -B /work/main.py"])
        return code, 30, res, stdout_is(digest)
    if name == "cpp-runtime":
        src = ('#include <cstdio>\nint main(){unsigned long long h=1469598103934665603ULL;'
               f'const char*s="{nonce}";for(const char*p=s;*p;++p){{h^=(unsigned char)*p;'
               'h*=1099511628211ULL;}printf("%llu\\n",h);return 0;}')
        code = "\n".join([_write("/work/main.cpp", src),
                          "g++ -std=c++20 -O2 -pipe -o /work/main.bin /work/main.cpp",
                          'rc=$?; [ "$rc" -eq 0 ] || exit "$rc"', "exec /work/main.bin"])
        return code, 150, res, stdout_is(str(_fnv(nonce)))
    if name == "js-runtime":
        src = f"const c=require('crypto');console.log(c.createHash('sha256').update('{nonce}').digest('hex'))"
        return "\n".join([_write("/work/main.js", src), "exec node /work/main.js"]), 30, res, \
            stdout_is(digest)
    if name == "shell-runtime":
        src = f"printf %s {nonce} | sha256sum | cut -d' ' -f1\n"
        return "\n".join([_write("/work/main.sh", src), "exec bash /work/main.sh"]), 30, res, \
            stdout_is(digest)
    if name == "smt-proof":
        k = int(digest[:6], 16)
        src = f"(declare-const x Int)\n(assert (= (* 7 x) {7 * k}))\n(check-sat)\n(get-value (x))\n"
        return "\n".join([_write("/work/main.smt2", src), "exec z3 -smt2 /work/main.smt2"]), 60, \
            res, stdout_is(f"sat\n(({'x'} {k}))")
    if name == "lean-proof":
        k = int(digest[:6], 16)
        heavy = random.random() < 0.15
        src = (("import Mathlib\nexample : (2 : ℕ) + 2 = 4 := by norm_num\n" if heavy else "")
               + f"#eval ({k} : Nat) * 3\n")
        return "\n".join([_write("/work/main.lean", src),
                          "exec /usr/local/bin/lean-runtime /work/main.lean"]), 600, res, \
            stdout_is(str(3 * k))
    raise KeyError(name)


class Client:
    def __init__(self, url: str, token: str):
        self.url, self.token = url.rstrip("/"), token

    def post(self, body: dict, timeout: float, cut_after: float | None = None):
        """('ok', reply) | ('error', status, text) | ('down', why) | ('hung', why)."""
        data = json.dumps(body).encode()
        request = urllib.request.Request(self.url + "/execute", data=data, headers={
            "Content-Type": "application/json", "Authorization": "Bearer " + self.token})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as reply:
                return ("ok", json.loads(reply.read()))
        except urllib.error.HTTPError as error:
            raw = error.read() or b""
            try:
                return ("error", error.code, json.loads(raw).get("error", ""))
            except Exception:
                return ("down", f"bare HTTP {error.code} {raw[:60]!r}") if error.code >= 500 else \
                    ("error", error.code, raw[:200].decode(errors="replace"))
        except (TimeoutError, OSError) as error:
            reason = getattr(error, "reason", error)
            if isinstance(reason, TimeoutError) or "timed out" in str(error):
                return ("hung", f"{type(error).__name__}: {error}")
            return ("down", f"{type(reason).__name__}: {reason}")
        except (http.client.RemoteDisconnected, http.client.IncompleteRead,
                http.client.BadStatusLine) as error:
            return ("down", f"{type(error).__name__}: {error}")

    def health(self):
        try:
            # Without the token /health says only `ok`; the counters this compares need it.
            request = urllib.request.Request(self.url + "/health", headers={
                "Authorization": "Bearer " + self.token} if self.token else {})
            with urllib.request.urlopen(request, timeout=20) as reply:
                return json.loads(reply.read())
        except Exception as error:
            return {"error": repr(error)}


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts = collections.Counter()
        self.examples = collections.defaultdict(list)
        self.latency = collections.defaultdict(list)
        self.events = []

    def add(self, key, example=None, latency=None, label=""):
        with self.lock:
            self.counts[key] += 1
            if example is not None and len(self.examples[key]) < 8:
                self.examples[key].append(example)
            if latency is not None:
                self.latency[label].append(latency)

    def event(self, what):
        with self.lock:
            self.events.append((round(time.time(), 1), what))
        print(f"[chaos] {time.strftime('%H:%M:%S')} {what}", flush=True)


def worker(client, dockerfiles, mix, stats, stop, args):
    names = list(dockerfiles)
    while not stop.is_set():
        name = random.choice(names)
        kind = random.choices(list(mix), weights=list(mix.values()))[0]
        if kind != "normal":
            name = "python-runtime" if "python-runtime" in dockerfiles else names[0]
        nonce = secrets.token_hex(8)
        code, timeout_s, res, check = job(name, nonce, kind)
        body = {"runtime": {"dockerfile": dockerfiles[name]}, "session_id": "chaos-" + nonce,
                "code": code, "timeout_s": timeout_s, "wait_s": 0, "io_cap_kb": 200,
                "resources": {"cpus": res.cpus, "memory_gb": res.memory_gb,
                              "workdir_gb": res.workdir_gb, "workdir_medium": "temp",
                              "gpus": 0, "pids": res.pids}}
        label, started, downs = f"{name}/{kind}", time.monotonic(), 0
        while True:
            outcome = client.post(body, timeout=args.http_timeout)
            if outcome[0] != "down":
                break
            downs += 1
            stats.add("down-replies", (label, outcome[1]))
            if stop.is_set() and downs > 3:
                break
            time.sleep(min(30, 2 * downs))
        dt = time.monotonic() - started
        if outcome[0] == "ok":
            wrong = check(outcome[1])
            if wrong:
                stats.add("WRONG-RESULT", (label, body["session_id"], wrong))
            else:
                stats.add("ok", latency=dt, label=label)
                stats.add("ok:" + kind)
                if downs:
                    stats.add("ok-after-down")
        elif outcome[0] == "error":
            status, text = outcome[1], outcome[2]
            key = ("INFRA-ERROR" if text.startswith(("unavailable:", "server_error:"))
                   or status >= 500 else "REQUEST-ERROR")
            stats.add(key, (label, body["session_id"], status, text[:400]))
        elif outcome[0] == "hung":
            stats.add("HUNG", (label, body["session_id"], outcome[1]))
        else:
            stats.add("gave-up-down", (label, outcome[1]))


def host(args, snippet: str, timeout=120) -> str:
    argv = shlex.split(args.host_cmd) + [snippet]
    out = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    return out.stdout


def engine(args, *podman, timeout=120) -> str:
    argv = shlex.split(args.engine_cmd) + list(podman)
    out = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    return out.stdout


def _mine(pattern: str, cgroup: str) -> str:
    """A shell snippet listing pids of THIS user whose argv[0] matches ``pattern`` and, when
    ``cgroup`` is given, whose /proc/<pid>/cgroup names it (e.g. the Slurm job of the test
    deployment) -- never another user's or another job's process."""
    keep = f'grep -q {shlex.quote(cgroup)} /proc/$p/cgroup 2>/dev/null && ' if cgroup else ""
    return (f"for p in $(ps -u $(id -u) -o pid=,args= | awk '$2 ~ /{pattern}/ {{print $1}}'); "
            f"do {keep}echo $p; done")


def chaos(args, stats, stop, client, dockerfiles):
    kinds = [k for k in args.chaos.split(",") if k]
    while not stop.wait(random.uniform(args.chaos_every * 0.5, args.chaos_every * 1.5)):
        kind = random.choice(kinds)
        try:
            if kind == "actor":
                # Session workers only, of THIS deployment (the host command scopes it), by pid.
                listing = host(args, _mine("^ray::_SessionActor", args.cgroup))
                pids = listing.split()
                victims = random.sample(pids, min(len(pids), random.randint(1, 3)))
                if victims:
                    host(args, "kill -9 " + " ".join(victims))
                stats.event(f"killed session workers {victims} of {len(pids)}")
            elif kind == "cacheactor":
                # Image cache workers, typically mid-load of the cold runtimes `disconnect` mints.
                listing = host(args, _mine("^ray::_CacheActor", args.cgroup))
                pids = listing.split()
                victims = random.sample(pids, min(len(pids), random.randint(1, 2)))
                if victims:
                    host(args, "kill -9 " + " ".join(victims))
                stats.event(f"killed cache workers {victims} of {len(pids)}")
            elif kind in ("container", "stop"):
                names = engine(args, "ps", "-q", "--filter", "name=zero-session-").split()
                victims = random.sample(names, min(len(names), random.randint(1, 3)))
                for victim in victims:
                    engine(args, *(["rm", "-f", "-t", "0"] if kind == "container" else
                                   ["stop", "-t", "0"]), victim)
                stats.event(f"{'removed' if kind == 'container' else 'stopped'} containers "
                            f"{victims} of {len(names)}")
            elif kind == "engine":
                hold = random.uniform(5, 25)
                sock = shlex.quote(args.socket)
                host(args, f"mv {sock} {sock}.chaos && sleep {hold:.1f}; mv {sock}.chaos {sock}",
                     timeout=hold + 60)
                stats.event(f"engine socket unreachable for {hold:.0f}s")
            elif kind == "disconnect":
                # A FRESH runtime (new digest => a cold load) and clients that leave mid-load:
                # incident 2. Afterwards that runtime must still serve.
                base = dockerfiles.get("shell-runtime") or next(iter(dockerfiles.values()))
                fresh = base + f"\nRUN sleep 8 # chaos {secrets.token_hex(4)}\n"
                body = {"runtime": {"dockerfile": fresh}, "code": "echo fresh", "timeout_s": 30,
                        "wait_s": 0, "resources": {"cpus": 1, "memory_gb": 1, "workdir_gb": 0.25,
                                                   "workdir_medium": "temp", "gpus": 0, "pids": 64}}
                leavers = []
                for _ in range(5):
                    body["session_id"] = "chaos-leave-" + secrets.token_hex(6)
                    leavers.append(threading.Thread(target=client.post, args=(dict(body), 2)))
                for thread in leavers:
                    thread.start()
                for thread in leavers:
                    thread.join()
                time.sleep(12)
                body["session_id"] = "chaos-after-" + secrets.token_hex(6)
                outcome = client.post(body, timeout=args.http_timeout)
                good = outcome[0] == "ok" and outcome[1].get("stdout") == "fresh\n"
                stats.add("disconnect-check-ok" if good else "DISCONNECT-CHECK-FAILED",
                          None if good else str(outcome)[:300])
                stats.event(f"disconnect storm on a cold runtime; afterwards: {str(outcome)[:120]}")
            elif kind == "replica":
                listing = host(args, _mine("^ray::ServeReplica:default:Gateway", args.cgroup))
                victims = listing.split()[:1]
                if victims:
                    host(args, "kill -9 " + victims[0])
                stats.event(f"killed the gateway replica {victims}")
        except Exception as error:
            stats.event(f"chaos {kind} failed to inject: {error!r}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--token-file", default="")
    parser.add_argument("--runtimes", default="")
    parser.add_argument("--only", default="", help="comma-separated runtime names")
    parser.add_argument("--seconds", type=float, default=600)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--http-timeout", type=float, default=3600)
    parser.add_argument("--chaos", default="")
    parser.add_argument("--chaos-every", type=float, default=30)
    parser.add_argument("--host-cmd", default="bash -c")
    parser.add_argument("--engine-cmd", default="podman")
    parser.add_argument("--socket", default="")
    parser.add_argument("--cgroup", default="", help="only touch processes in this cgroup")
    parser.add_argument("--mix", default="normal=70,big=12,exit3=3,segv=3,timeout=4,oom=4,flood-out=4")
    parser.add_argument("--json", default="")
    args = parser.parse_args()
    token = open(args.token_file).read().strip() if args.token_file else ""
    client = Client(args.url, token)
    if args.runtimes:
        dockerfiles = {d: open(os.path.join(args.runtimes, d, "Dockerfile")).read()
                       for d in sorted(os.listdir(args.runtimes))
                       if os.path.isfile(os.path.join(args.runtimes, d, "Dockerfile"))}
    else:
        dockerfiles = dict(BUILTIN)
    if args.only:
        dockerfiles = {k: v for k, v in dockerfiles.items() if k in args.only.split(",")}
    mix = {k: float(v) for k, v in (x.split("=") for x in args.mix.split(","))}
    stats, stop = Stats(), threading.Event()
    before = client.health()
    stats.event(f"start: {len(dockerfiles)} runtimes, concurrency {args.concurrency}, "
                f"{args.seconds:g}s, chaos={args.chaos or 'none'}")
    threads = [threading.Thread(target=worker, args=(client, dockerfiles, mix, stats, stop, args),
                                daemon=True) for _ in range(args.concurrency)]
    for thread in threads:
        thread.start()
    if args.chaos:
        threading.Thread(target=chaos, args=(args, stats, stop, client, dockerfiles),
                         daemon=True).start()
    deadline = time.monotonic() + args.seconds
    while time.monotonic() < deadline:
        time.sleep(min(30, max(0.1, deadline - time.monotonic())))
        print(f"[chaos] {time.strftime('%H:%M:%S')} {dict(stats.counts)}", flush=True)
    stop.set()
    for thread in threads:
        thread.join(timeout=args.http_timeout + 60)
    after = client.health()
    p = lambda xs, q: round(sorted(xs)[int(q * (len(xs) - 1))], 2) if xs else None
    report = {"counts": dict(stats.counts), "examples": dict(stats.examples),
              "events": stats.events,
              "latency_s": {k: {"n": len(v), "p50": p(v, .5), "p95": p(v, .95), "max": max(v)}
                            for k, v in sorted(stats.latency.items())},
              "health_before": {k: before.get(k) for k in ("served", "infra", "selfcheck", "uptime_s")},
              "health_after": {k: after.get(k) for k in ("served", "infra", "selfcheck", "uptime_s",
                                                         "sessions", "error")}}
    bad = sum(stats.counts[k] for k in ("WRONG-RESULT", "INFRA-ERROR", "HUNG",
                                        "DISCONNECT-CHECK-FAILED"))
    report["verdict"] = "PASS" if bad == 0 else f"FAIL ({bad} violations)"
    text = json.dumps(report, indent=1, default=str)
    if args.json:
        open(args.json, "w").write(text)
    print(text)


if __name__ == "__main__":
    main()
