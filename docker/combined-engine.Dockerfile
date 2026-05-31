# Combined Engine: feeder_client sidecar + pymdl SDK + factor computation.
# Extends backtest-base for the full Python/runtime dependency set.
# Build context must include:
#   vendor/pymdl/pymdl-2.13.232-py3.tar.gz
#   vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz

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

# Build Rust mdl_parser extension (binary parser for MDL messages, ~10x faster than Python)
# Use Chinese mirrors for rustup and cargo (direct rustup.sh is too slow in China)
ENV RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static
ENV RUSTUP_UPDATE_ROOT=https://mirrors.ustc.edu.cn/rust-static/rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://mirrors.ustc.edu.cn/rust-static/rustup/rustup-init.sh | sh -s -- -y --default-toolchain stable \
    && . "$HOME/.cargo/env" \
    && pip install --no-cache-dir maturin
COPY mdl_parser/ /app/mdl_parser/
RUN mkdir -p /root/.cargo \
    && echo '[source.crates-io]\nreplace-with = "ustc"\n\n[source.ustc]\nregistry = "sparse+https://mirrors.ustc.edu.cn/crates.io-index/"' > /root/.cargo/config.toml \
    && . "$HOME/.cargo/env" \
    && cd /app/mdl_parser && maturin build --release --strip \
    && pip install --no-cache-dir /app/mdl_parser/target/wheels/*.whl \
    && python -c "import mdl_parser; print('mdl_parser import ok')" \
    && rm -rf /app/mdl_parser/target /root/.cargo/registry
# Remove Rust toolchain after build to save ~500MB
RUN rm -rf /root/.rustup /root/.cargo

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
