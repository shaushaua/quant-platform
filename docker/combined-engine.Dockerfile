# Combined Engine: feeder_client sidecar + pymdl SDK + factor computation.
# Extends backtest-base for the full Python/runtime dependency set.
# Build context must include:
#   vendor/pymdl/pymdl-2.13.232-py3.tar.gz
#   vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz
#   vendor/mdl_parser/*.whl  (pre-built, see below)
#
# Pre-build mdl_parser wheel on the build host (one-time or when lib.rs changes):
#   export RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static
#   export RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup
#   curl -sSf https://mirrors.ustc.edu.cn/rust-static/rustup/rustup-init.sh | sh -s -- -y
#   . "$HOME/.cargo/env"
#   pip install maturin
#   mkdir -p vendor/mdl_parser
#   cd mdl_parser && maturin build --release --strip
#   cp target/wheels/*.whl ../vendor/mdl_parser/

FROM 172.24.99.176:5000/quant-platform/backtest-base:latest

LABEL description="Combined engine: MDL client sidecar + pymdl SDK + MemoryStore + factor computation"

WORKDIR /app

# jemalloc is intentionally installed here instead of depending on sdk-collector.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*

# Install pymdl SDK
COPY vendor/pymdl/pymdl-2.13.232-py3.tar.gz /tmp/
RUN pip install --no-cache-dir "setuptools<70" \
    && pip install --no-cache-dir --no-build-isolation --force-reinstall /tmp/pymdl-2.13.232-py3.tar.gz \
    && pip install --no-cache-dir "setuptools>=70" \
    && python -c "import pymdl; print('pymdl import ok')" \
    && rm -f /tmp/pymdl-2.13.232-py3.tar.gz

# Install MDL Linux client (feeder_client sidecar)
COPY vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz /tmp/
RUN mkdir -p /opt/mdl-client \
    && tar xzf /tmp/mdl_forward_2.13.232_linux.tar.gz -C /opt/mdl-client \
    && chmod +x /opt/mdl-client/feeder_client \
    && rm -f /tmp/mdl_forward_2.13.232_linux.tar.gz

# Install pre-built mdl_parser wheel (built on host, no Rust toolchain in Docker)
COPY vendor/mdl_parser/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl \
    && python -c "import mdl_parser; print('mdl_parser import ok')" \
    && rm -f /tmp/*.whl

# Copy entrypoint script (starts feeder_client then Python engine)
COPY docker/entrypoint-combined.sh /app/entrypoint-combined.sh
RUN chmod +x /app/entrypoint-combined.sh

# Copy latest framework code over the base image copy.
COPY quant_platform/ ./quant_platform/

ENV LD_LIBRARY_PATH=/usr/local/lib/python3.11/site-packages/pymdl:/opt/mdl-client:${LD_LIBRARY_PATH}
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
ENV MALLOC_ARENA_MAX=1

# MDL client log directory
RUN mkdir -p /data/quant/mdl_logs/client

ENTRYPOINT ["/app/entrypoint-combined.sh"]
