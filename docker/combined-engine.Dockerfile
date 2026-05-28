# Combined Engine: pymdl SDK + factor computation in single process.
# Extends backtest-base for the full Python/runtime dependency set, but does
# not depend on sdk-collector.
# Build context must include vendor/pymdl/pymdl-2.13.232-py3.tar.gz.

FROM 172.24.99.176:5000/quant-platform/backtest-base:latest

LABEL description="Combined engine: pymdl SDK + MemoryStore + factor computation"

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

# Copy latest framework code over the base image copy.
COPY quant_platform/ ./quant_platform/

ENV LD_LIBRARY_PATH=/usr/local/lib/python3.11/site-packages/pymdl:${LD_LIBRARY_PATH}
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
ENV MALLOC_ARENA_MAX=1

CMD ["python", "-m", "quant_platform.live_engine.combined_engine"]
