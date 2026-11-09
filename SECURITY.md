# Security policy

This service runs code submitted over HTTP. A vulnerability here can mean code execution on the
host, so please report one privately and give us time to fix it before disclosing it.

## Reporting

Use GitHub's private vulnerability reporting for this repository (**Security → Report a
vulnerability**). If that is not available to you, open an issue that says only that you have a
security report and asks for a private channel; do not include details, a proof of concept or an
exploit in the issue.

Useful in a report: the commit, how the service was started, the request that triggers the
problem, and what you observed.

## What is in scope

- Escaping a session's container, reading another session's workspace or environment, or
  exceeding a granted resource limit in a way that harms the host or other sessions.
- Bypassing the shared-secret check on any route other than `/health`.
- The endpoint-egress relay reaching an address it is meant to refuse (loopback, private,
  link-local, cloud metadata) or an origin other than the one declared.

## What is not a vulnerability

- Running arbitrary code inside a session: that is what the service is for.
- An unauthenticated service started on purpose with `ZERO_SANDBOX_ALLOW_NO_AUTH=1`.
- A denial of service by a caller who holds the token and is allowed to request large resource
  grants; the grants are the operator's to cap.

## Hardening notes for operators

- Always set `ZERO_SANDBOX_TOKEN`; the service refuses to start on a non-loopback address without
  it. The token is a single shared secret: rotate it if it may have leaked.
- Isolation is container-level (rootless, unprivileged user, read-only root, capabilities
  dropped, no-new-privileges). It is not a virtual-machine boundary. Run the service on a
  dedicated machine or VM, and keep credentials you cannot afford to lose off it.
- `ZERO_BUILD_SHARE_HF_TOKEN=1` passes the service's `HF_TOKEN` to image builds. Any caller's
  Dockerfile can then read it, so set it only when every token holder may use that credential.
- Put the service behind your own network controls; `/health` is unauthenticated by design, and
  without the token it reports only `{"ok": true}`.
