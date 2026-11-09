"""Submitted container images and per-session resource grants."""
from __future__ import annotations

import hashlib
import os
import re
import shlex
from dataclasses import dataclass


_PREFIX = "ZERO_RUNTIME_"
MAX_DOCKERFILE_BYTES = 256 * 1024
MAX_DOCUMENTATION_BYTES = 256 * 1024


@dataclass(frozen=True)
class RuntimeSpec:
    """Image capabilities declared by its Dockerfile; never a hardware allocation."""

    version: int = 1
    shell: str = "/bin/bash"
    egress: str = "none"
    image_prefixes: tuple[str, ...] = ()
    image_required: bool = False

    def wire_facts(self, *, available: bool, why: str) -> dict:
        return {"version": self.version, "available": bool(available),
                "why": str(why), "shell": self.shell, "egress": self.egress,
                "image_required": self.image_required,
                "image_prefixes": list(self.image_prefixes)}


@dataclass(frozen=True)
class Resources:
    """Hardware requested for one session. It is not part of the image identity."""

    cpus: float
    memory_gb: float
    workdir_gb: float
    workdir_medium: str
    gpus: float
    pids: int

    @classmethod
    def from_wire(cls, raw: object) -> "Resources":
        keys = {"cpus", "memory_gb", "workdir_gb", "workdir_medium", "gpus", "pids"}
        if not isinstance(raw, dict) or set(raw) != keys:
            raise ValueError("bad_request:resources must declare cpus, memory_gb, workdir_gb, "
                             "workdir_medium, gpus and pids")
        try:
            out = cls(float(raw["cpus"]), float(raw["memory_gb"]),
                      float(raw["workdir_gb"]), str(raw["workdir_medium"]),
                      float(raw["gpus"]), int(raw["pids"]))
        except (TypeError, ValueError):
            raise ValueError("bad_request:resource values have invalid types") from None
        if min(out.cpus, out.memory_gb, out.workdir_gb, out.pids) <= 0 or out.gpus < 0:
            raise ValueError("bad_request:resource values must be positive (gpus may be zero)")
        if out.workdir_medium not in ("temp", "ram"):
            raise ValueError("bad_request:workdir_medium must be temp or ram")
        return out

    def wire(self) -> dict:
        return {"cpus": self.cpus, "memory_gb": self.memory_gb,
                "workdir_gb": self.workdir_gb, "workdir_medium": self.workdir_medium,
                "gpus": self.gpus, "pids": self.pids}


def declarations(text: str) -> dict[str, str]:
    """Read only ``ENV ZERO_RUNTIME_*=...`` declarations from Dockerfile contents."""
    env: dict[str, str] = {}
    logical = ""
    for line in text.splitlines():
        logical += line[:-1] + " " if line.rstrip().endswith("\\") else line
        if line.rstrip().endswith("\\"):
            continue
        match = re.match(r"\s*ENV\s+(.+)$", logical)
        logical = ""
        if not match:
            continue
        for item in shlex.split(match.group(1), posix=True):
            if "=" in item:
                key, value = item.split("=", 1)
                if key.startswith(_PREFIX):
                    env[key] = value
    return env


def spec_from(env: dict[str, str], source: str = "") -> RuntimeSpec:
    def value(key: str, cast, default=None):
        raw = env.get(_PREFIX + key)
        return default if raw is None or raw == "" else cast(raw)

    prefixes = tuple(x.strip() for x in value("IMAGE_PREFIXES", str, "").split(",")
                     if x.strip())
    spec = RuntimeSpec(version=value("VERSION", int, 1),
                       shell=value("SHELL", str, "/bin/bash"),
                       egress=value("EGRESS", str, "none"), image_prefixes=prefixes,
                       image_required=bool(value("IMAGE_REQUIRED", int, 0)))
    if not spec.shell.startswith("/") or "\x00" in spec.shell:
        raise ValueError(f"runtime {source!r}: SHELL must be an absolute image path")
    if spec.egress not in ("none", "endpoint"):
        raise ValueError(f"runtime {source!r}: EGRESS must be none or endpoint")
    if spec.image_required and not prefixes:
        raise ValueError(f"runtime {source!r}: IMAGE_REQUIRED needs IMAGE_PREFIXES")
    return spec


def as_descriptor(runtime: object) -> dict[str, str]:
    """Read a client-local Dockerfile path or accept a complete descriptor."""
    if isinstance(runtime, dict):
        return dict(runtime)
    try:
        path = os.fspath(runtime)
    except TypeError:
        raise TypeError("runtime must be a Dockerfile path or descriptor") from None
    if not os.path.isfile(path):
        raise ValueError(f"runtime Dockerfile does not exist: {path}")
    text = open(path, encoding="utf-8").read()
    readme = os.path.join(os.path.dirname(path), "README.md")
    return {"dockerfile": text, "documentation": open(readme, encoding="utf-8").read()
            if os.path.isfile(readme) else ""}


@dataclass(frozen=True)
class Submitted:
    spec: RuntimeSpec
    digest: str
    dockerfile: str
    documentation: str

    @property
    def key(self) -> str:
        return self.digest


def resolve(raw: object) -> Submitted:
    """Accept any submitted Dockerfile; its declaration supplies image capabilities only."""
    if not isinstance(raw, dict) or not set(raw) <= {"dockerfile", "documentation"}:
        raise ValueError("bad_request:runtime takes dockerfile and optional documentation")
    text, docs = raw.get("dockerfile"), raw.get("documentation", "")
    if not isinstance(text, str) or not isinstance(docs, str):
        raise ValueError("bad_request:runtime fields must be strings")
    if len(text) > MAX_DOCKERFILE_BYTES or len(docs) > MAX_DOCUMENTATION_BYTES:
        raise ValueError("bad_request:runtime declaration is too large")
    try:
        digest = hashlib.sha256(text.encode()).hexdigest()
        spec = spec_from(declarations(text), digest)
    except ValueError as error:
        raise ValueError(f"bad_request:{error}") from None
    return Submitted(spec, digest, text, docs)


__all__ = ["MAX_DOCKERFILE_BYTES", "MAX_DOCUMENTATION_BYTES", "Resources", "RuntimeSpec",
           "Submitted", "as_descriptor", "declarations", "resolve", "spec_from"]
