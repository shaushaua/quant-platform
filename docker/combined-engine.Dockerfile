# Stage 1: Build Rust mdl_parser extension (cached by Docker unless mdl_parser/ changes)
FROM 172.24.99.176:5000/quant-platform/backtest-base:latest AS rust-builder

ENV RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static
ENV RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup

# Layer 1: Install Rust toolchain + maturin (cached unless Dockerfile changes)
RUN --mount=type=cache,target=/root/.cache/pip \
    curl --proto '=https' --tlsv1.2 -sSf https://mirrors.ustc.edu.cn/rust-static/rustup/rustup-init.sh | sh -s -- -y --default-toolchain stable \
    && . "$HOME/.cargo/env" \
    && pip install maturin \
    && mkdir -p /root/.cargo \
    && printf '[source.crates-io]\nreplace-with = "ustc"\n\n[source.ustc]\nregistry = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"\n' > /root/.cargo/config.toml

# Layer 2: Build mdl_parser (only re-run when mdl_parser/ changes)
COPY mdl_parser/ /app/mdl_parser/
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/app/mdl_parser/target \
    . "$HOME/.cargo/env" && cd /app/mdl_parser && maturin build --release --strip && mkdir -p /app/wheels && cp target/wheels/*.whl /app/wheels/

# Stage 2: Final image (no Rust toolchain, only the wheel)
FROM 172.24.99.176:5000/quant-platform/backtest-base:latest

LABEL description="Combined engine: MDL client sidecar + pymdl SDK + MemoryStore + factor computation"

WORKDIR /app

# jemalloc is intentionally installed here instead of depending on sdk-collector.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*

# Install pymdl SDK
COPY scripts/verify_mdl_layout.py /tmp/verify_mdl_layout.py
RUN --mount=type=bind,source=vendor/pymdl/pymdl-2.13.232-py3.tar.gz,target=/tmp/pymdl-2.13.232-py3.tar.gz,readonly \
    --mount=type=cache,target=/root/.cache/pip \
    python /tmp/verify_mdl_layout.py --sdk /tmp/pymdl-2.13.232-py3.tar.gz \
    && pip install "setuptools<70" \
    && pip install --no-build-isolation --force-reinstall /tmp/pymdl-2.13.232-py3.tar.gz \
    && pip install "setuptools>=70" \
    && python -c "import pymdl; print('pymdl import ok')" \
    && rm -f /tmp/verify_mdl_layout.py

# Install MDL Linux client (feeder_client sidecar)
RUN --mount=type=bind,source=vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz,target=/tmp/mdl_forward_2.13.232_linux.tar.gz,readonly \
    mkdir -p /opt/mdl-client \
    && tar xzf /tmp/mdl_forward_2.13.232_linux.tar.gz -C /opt/mdl-client \
    && chmod +x /opt/mdl-client/feeder_client

# Install pre-built Rust extension from builder stage (no Rust toolchain in final image)
COPY --from=rust-builder /app/wheels/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl \
    && python -c "import mdl_parser; print('mdl_parser import ok')" \
    && rm -f /tmp/*.whl

# Copy entrypoint script (starts feeder_client then Python engine)
COPY docker/entrypoint-combined.sh /app/entrypoint-combined.sh
RUN chmod +x /app/entrypoint-combined.sh

# Copy latest framework code over the base image copy.
COPY quant_platform/ ./quant_platform/

ENV LD_LIBRARY_PATH=/usr/local/lib/python3.11/site-packages/pymdl:/opt/mdl-client
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
ENV MALLOC_ARENA_MAX=1

# MDL client log directory
RUN mkdir -p /data/quant/mdl_logs/client

ENTRYPOINT ["/app/entrypoint-combined.sh"]
