#!/usr/bin/env bash
# Bootstrap and validate the executor toolchain. Runtime images build only from API submissions.
#
#   build.sh                          bootstrap and preflight only
#   build.sh --bootstrap              just fetch podman/crun/conmon into sandbox/.toolchain
#   build.sh --preflight              just the checks: builder, subuid/userns, setuid helpers
#   build.sh --prune                  recover storage retained by an interrupted build
#
# Builder choice: rootless `buildah` if present (the HPC/cluster standard, no daemon, no root),
# else `podman`, else `docker` -- on Linux, in that order. On macOS (dev only) this uses `docker`,
# which is the colima-provided rootless VM on this machine.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# EVERY BYTE THE CONTAINER ENGINE WRITES STAYS UNDER THIS DIRECTORY. Podman scatters state across
# FOUR places in $HOME by default -- the layer store, its config, its runtime dir and its cache --
# none of which this repo owns or cleans, and on the cluster $HOME is CephFS, where tens of GB of
# image layers is the exact I/O shape that caused the metadata storms. All four are pinned here.
#
# ONE VARIABLE, HONOURED BY BOTH SIDES: the service reads the same store (`service.py`), because a
# store the builder writes and the runtime cannot see makes `image_present()` report "image not
# present" with the images sitting on disk.
#
# The unpacked image graph is tens of thousands of small files (especially Mathlib). It must never
# default beneath the checkout because $HOME is commonly NAS on Slurm. SLURM_TMPDIR is the
# allocation's compute-local scratch; ordinary Linux/WSL uses native /tmp. Explicit overrides
# remain available, but build and launch must resolve to the same image store.
LOCAL_ROOT="${ZERO_LOCAL_ROOT:-${SLURM_TMPDIR:-/tmp}/zero-sandbox-$(id -u)}"
ZERO_IMAGE_STORE="${ZERO_IMAGE_STORE:-$LOCAL_ROOT/images}"
ENGINE_TMP=""

remove_engine_tmp() {
  local path="$1"
  rm -rf -- "$path" 2>/dev/null && return 0
  # Rootless builders can leave files owned by subordinate UIDs. Remove those through the same
  # user namespace that created them; a Docker-only/macOS build does not need this fallback.
  command -v podman >/dev/null 2>&1 && without_store_lock podman unshare rm -rf -- "$path"
}

cleanup_engine_tmp() {
  [ -n "$ENGINE_TMP" ] || return 0
  case "$ENGINE_TMP" in
    "$ZERO_IMAGE_STORE"/tmp/[0-9]*) remove_engine_tmp "$ENGINE_TMP" ;;
    *) printf 'WARN: refusing to clean unexpected engine tmp path: %s\n' "$ENGINE_TMP" >&2 ;;
  esac
  ENGINE_TMP=""
}

cleanup_stale_engine_tmp() {
  # The image-store lock excludes another build.sh process. PID directories can nevertheless
  # survive SIGKILL, WSL shutdown, or an older build.sh that had no EXIT trap. They contain only
  # Podman's transient image archives; completed layers live under containers/storage.
  local path pid
  for path in "$ZERO_IMAGE_STORE"/tmp/[0-9]*; do
    [ -d "$path" ] || continue
    [ "$path" = "$ENGINE_TMP" ] && continue
    pid="${path##*/}"
    kill -0 "$pid" 2>/dev/null || remove_engine_tmp "$path"
  done
}

# AND THE ENGINE ITSELF LIVES HERE TOO. Nothing outside this directory, the binaries included: the
# cluster has no podman, no root to install one, and a tool picked up from $PATH is a tool this repo
# neither pinned nor controls. `--bootstrap` fetches static builds into .toolchain/bin and every
# later command prefers them, so a fresh checkout on a bare node is one command from building.
#
# PINNED TO 4.9.4, NOT `latest`, AND THE REASON IS GLIBC. podman 5.x resolves `netavark` at
# startup even for `--network=host` and `--network=none`, and the netavark binary needs glibc
# >= 2.39; a node on 2.35 (Ubuntu 22.04, which this cluster is) then cannot build or run anything,
# failing with `could not find "netavark"` no matter what the network flags say. 4.9.4 uses
# slirp4netns, which ships in the same tarball and has no such floor. Verified working end to end
# on glibc 2.35: `build.sh --preflight` all-OK. Override with ZERO_PODMAN_URL on a newer node.
TOOLCHAIN="$HERE/.toolchain"
PODMAN_VERSION="${ZERO_PODMAN_VERSION:-v4.9.4}"
PODMAN_STATIC_URL="${ZERO_PODMAN_URL:-https://github.com/mgoltzsche/podman-static/releases/download/$PODMAN_VERSION/podman-linux-amd64.tar.gz}"
CONMON_URL="${ZERO_CONMON_URL:-https://github.com/containers/conmon/releases/latest/download/conmon.amd64}"
CNI_VERSION="${ZERO_CNI_VERSION:-v1.5.1}"
CNI_URL="${ZERO_CNI_URL:-https://github.com/containernetworking/plugins/releases/download/$CNI_VERSION/cni-plugins-linux-amd64-$CNI_VERSION.tgz}"

use_toolchain() { [ -d "$TOOLCHAIN/bin" ] && PATH="$TOOLCHAIN/bin:$PATH" && export PATH; return 0; }

bootstrap() {
  # NOT AN INSTALL: nothing is written outside $TOOLCHAIN, nothing needs root, and `rm -rf` on that
  # one directory undoes it completely.
  command -v curl >/dev/null 2>&1 || die "curl is required to bootstrap the container toolchain"
  mkdir -p "$TOOLCHAIN/bin" "$TOOLCHAIN/dl"
  if [ ! -x "$TOOLCHAIN/bin/podman" ]; then
    log "fetching podman-static -> $TOOLCHAIN/bin"
    curl -fsSL -o "$TOOLCHAIN/dl/podman.tgz" "$PODMAN_STATIC_URL" || die "podman download failed"
    tar xzf "$TOOLCHAIN/dl/podman.tgz" -C "$TOOLCHAIN/dl" || die "podman tarball is not readable"
    find "$TOOLCHAIN/dl" -type f -perm -u+x -path '*/usr/local/bin/*' -exec cp -f {} "$TOOLCHAIN/bin/" \;
    find "$TOOLCHAIN/dl" -type f -name conmon -exec cp -f {} "$TOOLCHAIN/bin/" \;
    find "$TOOLCHAIN/dl" -type f -perm -u+x -name 'slirp4netns' -exec cp -f {} "$TOOLCHAIN/bin/" \;
  fi
  if [ ! -x "$TOOLCHAIN/bin/conmon" ]; then
    log "fetching conmon -> $TOOLCHAIN/bin"
    curl -fsSL -o "$TOOLCHAIN/bin/conmon" "$CONMON_URL" || die "conmon download failed"
    chmod +x "$TOOLCHAIN/bin/conmon"
  fi
  if [ ! -x "$TOOLCHAIN/bin/bridge" ]; then
    log "fetching CNI plugins $CNI_VERSION -> $TOOLCHAIN/bin"
    mkdir -p "$TOOLCHAIN/dl/cni"
    curl -fsSL -o "$TOOLCHAIN/dl/cni.tgz" "$CNI_URL" || die "CNI download failed"
    tar xzf "$TOOLCHAIN/dl/cni.tgz" -C "$TOOLCHAIN/dl/cni" || die "CNI tarball is not readable"
    for b in bridge firewall host-local loopback portmap tuning; do
      [ -x "$TOOLCHAIN/dl/cni/$b" ] || die "CNI plugin $b is missing"
      cp -f "$TOOLCHAIN/dl/cni/$b" "$TOOLCHAIN/bin/$b"
    done
  fi
  rm -rf "$TOOLCHAIN/dl"
  use_toolchain
  # REPORT WHAT LANDED, never assume the pin: these are the versions the rest of this file was
  # verified against, and a silent drift is what makes a later failure unattributable.
  for b in podman crun conmon; do
    [ -x "$TOOLCHAIN/bin/$b" ] || die "$b missing after bootstrap -- the artefact layout changed"
    ok "$b: $("$TOOLCHAIN/bin/$b" --version 2>&1 | head -1)"
  done
  ok "toolchain: $TOOLCHAIN/bin (nothing installed outside it; rm -rf undoes this)"
}

image_store_env() {
  # BuildKit-style heredocs are materialized beneath TMPDIR.  A shared directory lets concurrent
  # podman builds unlink one another's temporary COPY sources, so give every build process its own
  # bounded namespace while retaining all engine writes inside ZERO_IMAGE_STORE.
  ENGINE_TMP="$ZERO_IMAGE_STORE/tmp/$$"
  mkdir -p "$ZERO_IMAGE_STORE"/{storage,runroot,config/containers,cache,home/.config/containers} "$ENGINE_TMP"
  trap cleanup_engine_tmp EXIT
  cat > "$ZERO_IMAGE_STORE/storage.conf" <<EOF
[storage]
driver = "${ZERO_IMAGE_DRIVER:-overlay}"
graphroot = "$ZERO_IMAGE_STORE/containers/storage"
runroot = "$ZERO_IMAGE_STORE/runroot"
[storage.options.overlay]
# Rootless overlay needs a FUSE mount program; ours is pinned in the toolchain beside podman.
mount_program = "$TOOLCHAIN/bin/fuse-overlayfs"
EOF
  # ignore_chown_errors is deliberately NOT set. It is the single-uid workaround (only one uid
  # mapped -> any chown to another is EINVAL, which is what killed kaniko on /etc/gshadow), and it
  # silently produces images whose file ownership is wrong. `--preflight` requires
  # newuidmap/newgidmap instead, so the mapping is real and the chowns land.
  # A policy file is MANDATORY for podman/buildah and is otherwise looked up in $HOME.
  [ -f "$ZERO_IMAGE_STORE/config/containers/policy.json" ] ||     printf '{"default":[{"type":"insecureAcceptAnything"}]}\n' \
      > "$ZERO_IMAGE_STORE/config/containers/policy.json"
  cp "$ZERO_IMAGE_STORE/config/containers/policy.json" \
    "$ZERO_IMAGE_STORE/home/.config/containers/policy.json"
  cat > "$ZERO_IMAGE_STORE/config/containers/containers.conf" <<EOF
[engine]
# HELPERS FROM OUR TOOLCHAIN, not the host's: conmon and the OCI runtime are pinned with podman.
conmon_path = ["$TOOLCHAIN/bin/conmon"]
helper_binaries_dir = ["$TOOLCHAIN/bin"]
runtime = "$TOOLCHAIN/bin/crun"
cgroup_manager = "cgroupfs"
events_logger = "file"

[network]
network_backend = "cni"
cni_plugin_dirs = ["$TOOLCHAIN/bin"]
EOF
  export CONTAINERS_STORAGE_CONF="$ZERO_IMAGE_STORE/storage.conf"
  export CONTAINERS_CONF="$ZERO_IMAGE_STORE/config/containers/containers.conf"
  export CONTAINERS_POLICY="$ZERO_IMAGE_STORE/config/containers/policy.json"
  export XDG_CONFIG_HOME="$ZERO_IMAGE_STORE/config"     # containers.conf, registries.conf
  export XDG_DATA_HOME="$ZERO_IMAGE_STORE"              # anything else libpod would put in ~/.local/share
  export XDG_CACHE_HOME="$ZERO_IMAGE_STORE/cache"
  export XDG_RUNTIME_DIR="$ZERO_IMAGE_STORE/runroot"    # podman's per-user runtime state
  export TMPDIR="$ENGINE_TMP"
}

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
ok()  { printf '\033[1;32mOK: %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mFATAL: %s\033[0m\n' "$*" >&2; exit 1; }

BUILDER=""
STORE_LOCK_FD=""

lock_image_store() {
  [ -n "$STORE_LOCK_FD" ] && return
  command -v flock >/dev/null 2>&1 || die "flock is required to serialize image builds"
  mkdir -p "$ZERO_IMAGE_STORE"
  exec {STORE_LOCK_FD}>"$ZERO_IMAGE_STORE/build.lock"
  flock "$STORE_LOCK_FD"
}

without_store_lock() (
  # The parent shell owns serialization. Podman may leave fuse-overlayfs helpers behind after an
  # interrupt; they must not inherit this descriptor and keep the build lock alive forever.
  if [ -n "$STORE_LOCK_FD" ]; then
    case "$STORE_LOCK_FD" in *[!0-9]*) die "invalid build-lock descriptor" ;; esac
    eval "exec ${STORE_LOCK_FD}>&-"
  fi
  exec "$@"
)

detect_builder() {
  [ -n "$BUILDER" ] && return
  case "$(uname -s)" in
    Linux)
      if command -v buildah >/dev/null 2>&1; then BUILDER=buildah
      elif command -v podman >/dev/null 2>&1; then BUILDER=podman
      elif command -v docker >/dev/null 2>&1; then BUILDER=docker
      else die "no builder found (buildah, podman or docker). Rootless: 'conda install -c conda-forge buildah' or podman."
      fi ;;
    Darwin)
      command -v docker >/dev/null 2>&1 || die "no docker found (mac dev target: colima + docker)"
      BUILDER=docker ;;
    *) die "unsupported OS $(uname -s)" ;;
  esac
  echo "[build.sh] builder: $BUILDER" >&2
}
#: ZERO_BUILD_BUILDER pins the builder the way ZERO_CONTAINER_RUNTIME pins the engine.
[ -n "${ZERO_BUILD_BUILDER:-}" ] && BUILDER="$ZERO_BUILD_BUILDER"

preflight() {
  use_toolchain
  detect_builder
  image_store_env
  ok "image store: $ZERO_IMAGE_STORE (compute-local scratch; never the engine or HOME default)"
  # OVERLAY OR NOTHING, AND THE HOST IS ASKED RATHER THAN THE OPERATOR REMINDED. The store defaults
  # into the working directory and stays there -- that is the point of it. But overlayfs cannot be
  # stacked on CephFS/NFS/fuse, and podman does not fail there: it silently falls back to `vfs`,
  # which gives EVERY image its own full copy of the base instead of sharing one (~15 GB becomes
  # ~80 GB) and writes the layer tree onto a network filesystem, which is the metadata load the
  # cluster operators complained about. A silent 5x is exactly the substitution this project
  # refuses, so it is refused here, with the one line that fixes it.
  local fstype; fstype="$(stat -f -c %T "$ZERO_IMAGE_STORE" 2>/dev/null || echo unknown)"
  case "$fstype" in
    ceph|nfs|nfs4|fuseblk|fuse|smb2|cifs)
      die "the image store is on '$fstype' ($ZERO_IMAGE_STORE). overlayfs cannot stack on a network
       filesystem, and podman would fall back to vfs: every image gets its own copy of the base
       (~15 GB -> ~80 GB) on shared storage. Point the store at NODE-LOCAL disk and use the SAME
       value for the build and the service:
           export ZERO_IMAGE_STORE=/tmp/zero_images_\$(id -u)" ;;
    *) ok "image store filesystem: $fstype (overlay-capable)" ;;
  esac
  ok "builder: $BUILDER ($($BUILDER --version 2>&1 | head -1))"
  # THE BUILDER MUST ACTUALLY WORK, not merely exist: a docker shim
  # with no daemon passes `which` and fails every build with an error that names the wrong layer.
  if ! "$BUILDER" info >/dev/null 2>&1; then
    die "$BUILDER info failed -- the builder is on PATH but its engine is not usable. Fix the engine or pin another with ZERO_BUILD_BUILDER."
  fi
  ok "builder engine reachable: $BUILDER info"
  if [ "$(uname -s)" != Linux ]; then
    ok "mac dev host: rootless subuid/userns are a linux-only requirement, skipped here"
    return
  fi
  local me; me="$(id -un)"
  if grep -q "^$me:" /etc/subuid 2>/dev/null && grep -q "^$me:" /etc/subgid 2>/dev/null; then
    ok "subuid/subgid range present for '$me'"
    # A RANGE IS ONLY USABLE THROUGH THE setuid HELPERS: podman/buildah need more than one
    # uid mapped for any `chown` inside a build, and widening needs newuidmap/newgidmap -- which
    # are setuid-root and CANNOT be installed without root. Their absence was reported all-OK here
    # and every build then died naming the helper, not the check.
    if ! command -v newuidmap >/dev/null 2>&1 || ! command -v newgidmap >/dev/null 2>&1; then
      cat >&2 <<'EOF'
FATAL: /etc/subuid,/etc/subgid carry a range but newuidmap/newgidmap are not on PATH.
       The helpers are setuid-root; without them a rootless builder can map only ONE uid, so any
       chown inside a build fails (podman: 'command required for rootless mode with multiple IDs').
       Admin: apt install uidmap   (or build where the helpers exist, or use a single-uid build).
EOF
      exit 1
    fi
    ok "newuidmap/newgidmap present"
  else
    echo "WARN: no /etc/subuid,/etc/subgid for '$me' -- multi-uid rootless builds unavailable." >&2
    echo "      Builds will only work single-uid (chroot isolation, no chown to other uids)." >&2
  fi
  local maxns; maxns="$(cat /proc/sys/user/max_user_namespaces 2>/dev/null || echo 0)"
  { [ "$maxns" -gt 0 ] 2>/dev/null && ok "unprivileged user namespaces enabled (max=$maxns)"; } \
    || die "unprivileged user namespaces disabled (max=$maxns). Admin: sysctl -w user.max_user_namespaces=63846"
}

prune_dangling() {
  use_toolchain; detect_builder; image_store_env; lock_image_store
  case "$BUILDER" in
    buildah)
      local dangling=()
      mapfile -t dangling < <(without_store_lock buildah images --filter dangling=true -q)
      ((${#dangling[@]} == 0)) || without_store_lock buildah rmi "${dangling[@]}"
      ;;
    podman)
      # Interrupted `podman build` processes leave Buildah working containers behind. They are
      # visible only with --external and pin every intermediate layer; system prune --external
      # does not remove them on our pinned Podman 4.9.4.
      local abandoned=()
      mapfile -t abandoned < <(without_store_lock podman ps -aq --external)
      if ((${#abandoned[@]})); then
        log "removing ${#abandoned[@]} abandoned build containers"
        without_store_lock podman rm -f "${abandoned[@]}"
      fi
      without_store_lock podman image prune -f
      ;;
    docker) without_store_lock docker image prune -f ;;
  esac
  cleanup_stale_engine_tmp
}

case "${1:-}" in
  --bootstrap) bootstrap ;;
  --preflight) preflight ;;
  --prune) prune_dangling ;;
  "")
    bootstrap; preflight ;;
  *) die "unknown argument '$1'. usage: build.sh --bootstrap | --preflight | --prune" ;;
esac
