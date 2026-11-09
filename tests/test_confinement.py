"""What submitted code CANNOT do.

`test_contract.py` checks that the resource envelope binds -- memory, PIDs, file size, time. This
asks the other question: a submitted program is hostile input, and these are the things one would
actually reach for. Every case runs real code in a real container and asserts the attempt failed,
because a source review cannot tell you whether a flag was accepted by the engine.
"""
from __future__ import annotations

import secrets

import pytest

from conftest import RESOURCES, dynamic, live, submit


def attempt(url: str, code: str, **request) -> dict:
    return submit(url, dynamic("jail" + secrets.token_hex(4)), code, name="jail",
                  timeout_s=request.pop("timeout_s", 45), **request)


# Each case is (name, code, predicate). The code must be busybox-portable: the runtime is
# whatever was submitted, and the fixture is alpine.
ESCAPES = [
    # The control mount carries the session deadline and its environment file. If a program could
    # write there it could extend its own life past the watchdog, which is the only thing that
    # stops a session the gateway has lost.
    ("control-mount-is-read-only",
     "echo 9999999999 > /zero-control/deadline 2>/dev/null && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED"),
    ("control-mount-cannot-be-added-to",
     "touch /zero-control/x 2>/dev/null && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED"),
    # The one escape that ends the argument: reach the engine and start a privileged container.
    ("no-container-engine-socket",
     "ls /var/run/docker.sock /run/docker.sock /var/run/podman/podman.sock "
     "/run/user/*/podman/podman.sock 2>/dev/null | wc -l",
     lambda r: r["stdout"].strip() == "0"),
    # --cap-drop=ALL. Mounting is how a namespace is escaped from the inside.
    ("cannot-mount",
     "mount -t tmpfs none /mnt 2>/dev/null && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED"),
    # CAP_CHOWN would let a workspace file be handed to another uid.
    ("cannot-chown",
     "touch f && chown 0:0 f 2>/dev/null && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED"),
    # Read the capability sets rather than inferring them. A behavioural probe misleads here:
    # busybox `ping` falls back to an ICMP DATAGRAM socket, which needs no CAP_NET_RAW wherever
    # `net.ipv4.ping_group_range` covers the gid -- it succeeds against loopback with every
    # capability dropped, and loopback is all `--network=none` leaves reachable anyway.
    ("every-capability-is-dropped",
     "grep -E '^Cap(Prm|Eff|Bnd|Inh):' /proc/self/status | tr -s '\t' ' '",
     lambda r: all(line.split()[1] == "0000000000000000"
                   for line in r["stdout"].strip().splitlines())),
    ("no-new-privileges-is-set",
     "grep '^NoNewPrivs:' /proc/self/status | tr -s '\t' ' '",
     lambda r: r["stdout"].split()[1] == "1"),
    ("a-seccomp-filter-is-installed",
     "grep '^Seccomp:' /proc/self/status | tr -s '\t' ' '",
     lambda r: r["stdout"].split()[1] in ("1", "2")),
    ("root-filesystem-is-read-only",
     "touch /MUST_NOT_EXIST 2>/dev/null && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED"),
    ("etc-is-read-only",
     "echo x >> /etc/passwd 2>/dev/null && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED"),
    # The kernel knobs are shared with every other tenant on the node.
    ("sysctl-is-not-writable",
     "echo 1 > /proc/sys/kernel/panic 2>/dev/null && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED"),
    ("cgroup-is-not-writable",
     "echo 1 > /sys/fs/cgroup/cgroup.procs 2>/dev/null && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED"),
    # Host devices: raw memory and raw disks are the other way out.
    ("host-devices-are-absent",
     "ls /dev/mem /dev/kmem /dev/sda /dev/vda /dev/nvme0n1 2>/dev/null | wc -l",
     lambda r: r["stdout"].strip() == "0"),
    # /tmp and /dev/shm are tmpfs, which is RAM: an uncapped one is a node-wide memory attack
    # that the session's own --memory limit would never see.
    ("tmp-is-capped", "dd if=/dev/zero of=/tmp/fill bs=1M count=512 2>/dev/null; "
                      "du -sm /tmp 2>/dev/null | cut -f1",
     lambda r: int(r["stdout"].strip() or 0) <= 260),
    ("dev-shm-is-capped", "dd if=/dev/zero of=/dev/shm/fill bs=1M count=512 2>/dev/null; "
                          "du -sm /dev/shm 2>/dev/null | cut -f1",
     lambda r: int(r["stdout"].strip() or 0) <= 260),
    # Nothing of the node's process table may be visible.
    ("only-our-own-processes-are-visible",
     "ls /proc | grep -c '^[0-9][0-9]*$'",
     lambda r: 0 < int(r["stdout"].strip()) < 30),
    # No route at all, so name resolution cannot even begin.
    ("no-dns-without-an-egress-grant",
     "nslookup example.com >/dev/null 2>&1 && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED"),
    ("no-route-without-an-egress-grant",
     "wget -T2 -q -O- http://1.1.1.1/ >/dev/null 2>&1 && echo BAD || echo BLOCKED",
     lambda r: r["stdout"].strip() == "BLOCKED" and not r["network"]["outbound"]),
]


@live
@pytest.mark.parametrize("name,code,held", ESCAPES, ids=[case[0] for case in ESCAPES])
def test_a_submitted_program_cannot_break_out(url, name, code, held):
    reply = attempt(url, code)
    assert held(reply), (name, reply["returncode"], reply["stdout"][:200], reply["err"][:200])


@live
def test_the_session_runs_as_the_unprivileged_uid_it_was_given(url):
    """Everything above rests on this: the process is not root, in its namespace or outside it."""
    reply = attempt(url, "id -u; id -g; id -G")
    uid, gid, groups = reply["stdout"].split("\n")[:3]
    assert (uid, gid) == ("65532", "65532"), reply["stdout"]
    assert "0" not in groups.split(), groups


@live
def test_a_workspace_symlink_cannot_widen_the_reported_delta(url):
    """`/work` is reported by metadata delta. A link to `/` would make the reply a directory
    listing of the image, and on a bind-mounted workspace, of the node."""
    sid = "esc" + secrets.token_hex(8)
    text = dynamic("escape" + secrets.token_hex(4))
    reply = submit(url, text, "ln -s / root-link; ln -s /etc etc-link; ls -l | wc -l",
                   name="esc", session_id=sid, wait_s=30, timeout_s=45)
    assert reply["returncode"] == 0, reply
    assert reply["files"]["created"] == 2, reply["files"]
    assert reply["files"]["scanned"] == 2, "the scan followed a link out of the workspace"
    submit(url, text, "true", name="esc", session_id=sid, wait_s=0)


@live
def test_an_endpoint_grant_does_not_open_any_other_host(url):
    """The relay filters on the granted host. A program that asks for another one gets nothing,
    and the reply names only the origin that was granted."""
    text = dynamic("egress" + secrets.token_hex(4), egress=True)
    granted = submit(url, text, "wget -T5 -q -O- https://example.com/ >/dev/null 2>&1 "
                                "&& echo REACHED || echo BLOCKED",
                     name="eg", timeout_s=90, network="https://example.com")
    other = submit(url, text, "wget -T5 -q -O- https://api.github.com/ >/dev/null 2>&1 "
                              "&& echo BAD || echo BLOCKED",
                   name="eg", timeout_s=90, network="https://example.com")
    assert granted["network"] == {"mode": "endpoint", "outbound": True,
                                  "origin": "https://example.com:443"}, granted["network"]
    assert other["stdout"].strip() == "BLOCKED", other
    # And a runtime that never declared egress cannot ask for any of it.
    plain = submit(url, dynamic("plain" + secrets.token_hex(4)), "true", name="p",
                   network="https://example.com", expect=403)
    assert plain["error"].startswith("forbidden:"), plain


@live
@pytest.mark.parametrize("host", ["localhost", "localtest.me"])
def test_an_egress_grant_cannot_point_at_a_non_global_address(url, host):
    """The grant demands a DNS name, not an IP literal -- but a NAME resolves, and nothing stopped
    it resolving to loopback, the node's private network, or the cloud metadata service at
    169.254.169.254. `localtest.me` is a public name that resolves to 127.0.0.1; the relay would
    have connected to it. The grant must refuse before a relay is ever opened."""
    text = dynamic("ssrf" + secrets.token_hex(4), egress=True)
    reply = submit(url, text, "true", name="ssrf", timeout_s=45,
                   network=f"http://{host}", expect=403)
    assert reply["error"].startswith("forbidden:"), reply
    assert "non-global" in reply["error"], reply


@live
def test_a_session_cannot_reach_another_session_workspace(url):
    """Two live sessions of one runtime, at the same time, each holding a secret."""
    text = dynamic("twosess" + secrets.token_hex(4))
    first, second = "a" + secrets.token_hex(6), "b" + secrets.token_hex(6)
    token = secrets.token_hex(8)
    submit(url, text, f"printf %s {token} > secret", name="s", session_id=first, wait_s=60)
    hunt = submit(url, text,
                  "find / -name secret -not -path '/proc/*' 2>/dev/null | head -5; echo END",
                  name="s", session_id=second, wait_s=0, timeout_s=60)
    assert token not in hunt["stdout"], hunt["stdout"][:400]
    assert hunt["stdout"].strip() == "END", hunt["stdout"][:400]
    submit(url, text, "true", name="s", session_id=first, wait_s=0)
