#!/usr/bin/env bash
# Configure one compute-local Podman cache; runtimes build on first request.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_ROOT="${ZERO_LOCAL_ROOT:-${SLURM_TMPDIR:-/tmp}/zero-sandbox-$(id -u)}"
BUILD="${ZERO_BUILD_IMAGE_STORE:-$LOCAL_ROOT/images}"
RAM="${ZERO_RAM_IMAGE_STORE:-/dev/shm/zero-images-$(id -u)${SLURM_JOB_ID:+-$SLURM_JOB_ID}}"
STORE="${ZERO_RUNTIME_STORE:-$LOCAL_ROOT/runtime}"
TOOLS="$HERE/.toolchain/bin"
PODMAN="$TOOLS/podman"

die() { printf 'FATAL: %s\n' "$*" >&2; exit 1; }

# A REMOTE ENGINE OWNS ITS STORAGE, so none of the local store configuration below applies to it:
# `--root`, `--storage-driver` and `--storage-opt` are invalid with `--remote`, and every host path
# named here is invisible inside the VM. Exporting them anyway is not harmless -- `ZERO_RAM_IMAGE_STORE`
# made `_stage` save an image out of the VM and load it straight back into the same VM, then report
# a `ram` tier for an image sitting on the VM's disk. `service.py::_preflight_store` refuses to
# start in that configuration; this is the other half of the same fix. Measured 2026-08-30: the
# the production host deployment had been running with exactly this set.
remote() { [ -n "${ZERO_PODMAN_URL:-}${ZERO_PODMAN_REMOTE:-}" ]; }

prepare() {
  if remote; then
    echo "[sandbox] remote engine configured; the VM owns storage, so no local store is prepared" >&2
    # Still ours: host-side scratch for the service's own bookkeeping. A remote session binds
    # nothing from it, but `root()` reads it and the package path below is derived from it.
    mkdir -p "$LOCAL_ROOT"
    export ZERO_LOCAL_ROOT="$LOCAL_ROOT" ZERO_CONTAINER_RUNTIME=podman
    unset ZERO_RAM_IMAGE_STORE ZERO_BUILD_IMAGE_STORE ZERO_IMAGE_STORE \
          ZERO_PODMAN_ROOT ZERO_PODMAN_RUNROOT ZERO_PODMAN_IMAGE_STORES
    return 0
  fi
  [ -x "$PODMAN" ] || die "missing pinned Podman; run: bash sandbox/build.sh --bootstrap"
  [ "$(stat -f -c %T /dev/shm)" = tmpfs ] || die "/dev/shm is not tmpfs"
  mkdir -p "$BUILD"/{containers/storage,runroot} "$RAM"
  [ "$(stat -f -c %T "$RAM")" = tmpfs ] || die "RAM image store is not on tmpfs: $RAM"
  mkdir -p "$STORE"/{containers/storage,runroot,tmp,cache,config/containers,home/.config/containers}
  printf '{"default":[{"type":"insecureAcceptAnything"}]}\n' \
    >"$STORE/config/containers/policy.json"
  cp "$STORE/config/containers/policy.json" "$STORE/home/.config/containers/policy.json"
  cat >"$STORE/storage.conf" <<EOF
[storage]
driver = "overlay"
graphroot = "$STORE/containers/storage"
runroot = "$STORE/runroot"
[storage.options.overlay]
mount_program = "$TOOLS/fuse-overlayfs"
EOF
  cat >"$STORE/config/containers/containers.conf" <<EOF
[engine]
conmon_path = ["$TOOLS/conmon"]
helper_binaries_dir = ["$TOOLS"]
runtime = "${ZERO_OCI_RUNTIME:-$TOOLS/crun}"
cgroup_manager = "${ZERO_CGROUP_MANAGER:-cgroupfs}"
events_logger = "file"

[network]
network_backend = "cni"
cni_plugin_dirs = ["$TOOLS"]
EOF
  export ZERO_LOCAL_ROOT="$LOCAL_ROOT" ZERO_IMAGE_STORE="$STORE"
  export ZERO_BUILD_IMAGE_STORE="$BUILD" ZERO_RAM_IMAGE_STORE="$RAM"
  export CONTAINERS_STORAGE_CONF="$STORE/storage.conf"
  export CONTAINERS_CONF="$STORE/config/containers/containers.conf"
  export CONTAINERS_POLICY="$STORE/config/containers/policy.json"
  export XDG_CONFIG_HOME="$STORE/config" XDG_DATA_HOME="$STORE"
  export XDG_CACHE_HOME="$STORE/cache" XDG_RUNTIME_DIR="$STORE/runroot" TMPDIR="$STORE/tmp"
  export ZERO_ENGINE_HOME="$STORE/home" ZERO_CONTAINER_RUNTIME=podman
  export ZERO_PODMAN_ROOT="$STORE/containers/storage" ZERO_PODMAN_RUNROOT="$STORE/runroot"
  export ZERO_PODMAN_IMAGE_STORES="$BUILD/containers/storage"
}

case "${1:-serve}" in
  serve)
    shift || true
    prepare
    # A submodule named `sandbox` imports directly from its parent. A standalone clone is normally
    # named `zero-sandbox`; expose the same package name from compute-local scratch without writing
    # to the checkout or requiring an editable install.
    PACKAGE_ROOT="$LOCAL_ROOT/python"
    mkdir -p "$PACKAGE_ROOT"
    if [ "$(readlink -f "$PACKAGE_ROOT/sandbox" 2>/dev/null || true)" != "$HERE" ]; then
      if [ -L "$PACKAGE_ROOT/sandbox" ]; then
        rm "$PACKAGE_ROOT/sandbox"
      elif [ -e "$PACKAGE_ROOT/sandbox" ]; then
        die "package path already exists and is not this checkout: $PACKAGE_ROOT/sandbox"
      fi
      ln -s "$HERE" "$PACKAGE_ROOT/sandbox"
    fi
    export PYTHONPATH="$PACKAGE_ROOT:$HERE/..${PYTHONPATH:+:$PYTHONPATH}"
    cd "$LOCAL_ROOT"
    exec python -c 'from sandbox.service import main; raise SystemExit(main())' "$@"
    ;;
  status)
    prepare
    env HOME="$STORE/home" "$PODMAN" --root "$STORE/containers/storage" \
      --runroot "$STORE/runroot" --storage-driver overlay \
      --storage-opt "overlay.mount_program=$TOOLS/fuse-overlayfs" \
      --storage-opt "additionalimagestore=$BUILD/containers/storage" \
      images --format '{{.Repository}}:{{.Tag}} {{.Size}}'
    ;;
  *) die "usage: sandbox/launch-server.sh [serve [options] | status]" ;;
esac
