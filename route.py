"""Async client for the raw sandbox runtime API."""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import os
import threading
import urllib.error
import urllib.request


_CLIENT_THREADS = max(1, int(os.environ.get("ZERO_SANDBOX_CLIENT_THREADS", "256")))
_HTTP_TIMEOUT = float(os.environ.get("ZERO_SANDBOX_TIMEOUT_S", "930"))
_ALLOW_LOCAL = os.environ.get("ZERO_ALLOW_LOCAL_EXEC", "").lower() not in ("", "0", "false")
_POOL = None
_POOL_PID = None
_TOKEN_CACHE: str | None = None


def _token() -> str:
    """The shared secret, resolved once per process. The env var is primary, but Ray's
    runtime_env plumbing does not reliably reach every forked worker/subprocess (the freeze panel
    is one), so fall back to the well-known file the launcher also reads -- it is on shared storage,
    so any client process can authenticate. Empty when neither exists (dev, or genuinely no secret):
    the server then either does not enforce, or replies 403 and the caller raises loudly."""
    global _TOKEN_CACHE
    if _TOKEN_CACHE is None:
        token = (os.environ.get("ZERO_SANDBOX_TOKEN") or "").strip()
        if not token:
            path = os.environ.get("ZERO_SANDBOX_TOKEN_FILE") or os.path.expanduser(
                "~/.zero_sandbox_token")
            try:
                with open(path, encoding="ascii") as stream:
                    token = stream.read().strip()
            except OSError:
                token = ""
        _TOKEN_CACHE = token
    return _TOKEN_CACHE
_LOCK = threading.Lock()


def service_url() -> str | None:
    return (os.environ.get("ZERO_SANDBOX_URL") or "").rstrip("/") or None


def _pool():
    global _POOL, _POOL_PID
    with _LOCK:
        if _POOL is None or _POOL_PID != os.getpid():
            _POOL = concurrent.futures.ThreadPoolExecutor(
                max_workers=_CLIENT_THREADS, thread_name_prefix="zero-sandbox")
            _POOL_PID = os.getpid()
        return _POOL


class _Down(Exception):
    """The service did not answer the request at all: it is down, or going down."""


def _down(error: BaseException) -> bool:
    """Whether a transport failure means the service is DOWN, as opposed to slow.

    The service has exactly two states for a valid request: it answers with the result, or it is
    not there (sandbox README, "Failure semantics"). Not there is: the port refuses (stopped,
    restarting), the connection is cut before a reply (the process exited under the request), or
    Serve's own front answers without the gateway -- a bare, non-JSON 5xx (the app is starting,
    or its replica died). A TIMEOUT is not "down": the service accepted the request and is still
    working on it, so it stays an error, exactly as before."""
    import http.client
    if isinstance(error, urllib.error.HTTPError):
        return False
    reason = getattr(error, "reason", error)
    return isinstance(reason, (ConnectionRefusedError, ConnectionResetError, BrokenPipeError,
                               ConnectionAbortedError, http.client.RemoteDisconnected,
                               http.client.IncompleteRead, http.client.BadStatusLine))


async def _request(path: str, payload: dict | None = None) -> dict:
    url = service_url()
    if not url:
        raise RuntimeError("ZERO_SANDBOX_URL is not configured")

    def call():
        body = None if payload is None else json.dumps(payload).encode()
        headers = {"Content-Type": "application/json"} if body else {}
        # Shared secret for the 0.0.0.0-bound service. Sent when configured; the server enforces
        # only when it too has one, so an unset token is a no-op on both sides.
        token = _token()
        if token:
            headers["Authorization"] = "Bearer " + token
        request = urllib.request.Request(url + path, data=body, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=_HTTP_TIMEOUT) as reply:
                return json.loads(reply.read() or b"{}")
        except urllib.error.HTTPError as error:
            raw = error.read() or b""
            try:
                detail = json.loads(raw or b"{}")
            except Exception:
                # Every answer of the GATEWAY is JSON. A bare 5xx is Serve's front speaking
                # without it -- starting up, or its replica gone -- i.e. the service is down.
                if error.code >= 500:
                    raise _Down(f"HTTP {error.code} {raw[:120]!r}") from None
                detail = {"error": f"HTTP {error.code}"}
            raise RuntimeError(str(detail.get("error") or detail)) from None
        except Exception as error:
            if _down(error):
                raise _Down(f"{type(error).__name__}: {error}") from None
            raise RuntimeError(f"sandbox {type(error).__name__}: {error}") from None

    # WAIT WHILE IT IS DOWN, bounded. The service retries its own infrastructure failures and
    # takes itself down (and its supervisor back up) rather than answer with one, so "down" is a
    # pause, not a verdict: the request is sent again once the service answers. Sending it again
    # is safe because the request got NO answer -- whatever it did, nobody saw a result -- and no
    # submitted program can take the service down to buy itself another run.
    loop = asyncio.get_running_loop()
    wait_s = float(os.environ.get("ZERO_SANDBOX_DOWN_WAIT_S", "1800"))
    give_up, pause = loop.time() + wait_s, 2.0
    while True:
        try:
            return await loop.run_in_executor(_pool(), call)
        except _Down as down:
            if loop.time() + pause > give_up:
                raise RuntimeError(f"sandbox down for over {wait_s:g}s: {down}") from None
            await asyncio.sleep(pause)
            pause = min(30.0, pause * 1.5)


async def aexecute(runtime: object, code: str, *, session_id: str, timeout_s: float,
                   resources: object,
                   wait_s: float = 0, cwd: str = "/work", io_cap_kb: int = 200,
                   image: str | None = None, network: str | None = None,
                   env: dict[str, str] | None = None) -> dict:
    """Run one ordered terminal action; ``wait_s=0`` makes it stateless.

    ``runtime`` is a Dockerfile path or a full descriptor. Resources are a per-session grant.
    """
    from sandbox.runtime import Resources, as_descriptor
    grant = resources if isinstance(resources, Resources) else Resources.from_wire(resources)
    payload = {"runtime": as_descriptor(runtime), "session_id": session_id, "code": code,
               "cwd": cwd, "timeout_s": timeout_s, "wait_s": wait_s,
               "io_cap_kb": io_cap_kb, "resources": grant.wire()}
    if image:
        payload["image"] = image
    if network:
        payload["network"] = network
    if env:
        payload["env"] = env
    if service_url():
        return await _request("/execute", payload)
    if not _ALLOW_LOCAL:
        raise RuntimeError(
            "refusing local execution: configure ZERO_SANDBOX_URL or explicitly set "
            "ZERO_ALLOW_LOCAL_EXEC=1 for development")
    from sandbox.service import execute_local
    return await asyncio.to_thread(execute_local, payload)


def run_sync(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()
