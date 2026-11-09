FROM docker.io/library/debian:bookworm-slim AS mathlib-builder
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git zstd \
    && rm -rf /var/lib/apt/lists/*
ENV ELAN_HOME=/opt/elan PATH=/opt/elan/bin:$PATH
RUN curl -fsSL https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
    | sh -s -- -y --default-toolchain none --no-modify-path \
    && /opt/elan/bin/elan toolchain install leanprover/lean4:v4.33.0 \
    && /opt/elan/bin/elan default leanprover/lean4:v4.33.0
RUN git clone --depth 1 --branch v4.33.0 https://github.com/leanprover-community/mathlib4.git /opt/mathlib4 \
    && cd /opt/mathlib4 \
    && /opt/elan/toolchains/leanprover--lean4---v4.33.0/bin/lake exe cache get \
    && test -f /opt/mathlib4/.lake/build/lib/lean/Mathlib.olean \
    && mkdir -p /opt/lean-runtime/libs \
    && n=0 \
    && for lib in /opt/mathlib4/.lake/build/lib/lean \
                  /opt/mathlib4/.lake/packages/*/.lake/build/lib/lean; do \
         if [ -d "$lib" ]; then \
           mv "$lib" "/opt/lean-runtime/libs/$n"; \
           printf '%s\n' "/opt/lean-libs/$n" >> /opt/lean-runtime/path; \
           n=$((n + 1)); \
         fi; \
       done \
    && test -s /opt/lean-runtime/path \
    && chmod -R a+rX /opt/lean-runtime/libs \
    && rm -rf /opt/mathlib4

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
ENV PATH=/opt/lean/bin:$PATH
COPY --from=mathlib-builder /opt/elan/toolchains/leanprover--lean4---v4.33.0 /opt/lean
COPY --from=mathlib-builder /opt/lean-runtime/libs /opt/lean-libs
COPY --from=mathlib-builder /opt/lean-runtime/path /opt/lean-path
COPY <<'LEAN' /usr/local/bin/lean-runtime
#!/bin/bash
set -euo pipefail
lean_path="$(paste -sd: /opt/lean-path)"
LEAN_PATH="$lean_path${LEAN_PATH:+:$LEAN_PATH}" exec /opt/lean/bin/lean "$@"
LEAN
RUN chmod +x /usr/local/bin/lean-runtime \
    && printf 'import Mathlib\nexample : (1 : Nat) + 1 = 2 := by norm_num\n' >/tmp/smoke.lean \
    && /usr/local/bin/lean-runtime /tmp/smoke.lean \
    && rm /tmp/smoke.lean
WORKDIR /work
ENV ZERO_RUNTIME_VERSION=3
