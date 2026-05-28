# SDK collector image: extends the existing runtime with pymdl.
# Build context must include vendor/pymdl/pymdl-2.13.232-py3.tar.gz.
FROM 172.24.99.176:5000/quant-platform/backtest-base:latest

WORKDIR /app

# Install jemalloc — much better than glibc malloc at reclaiming fragmented memory
RUN apt-get update && apt-get install -y --no-install-recommends libjemalloc2 && rm -rf /var/lib/apt/lists/*

COPY quant_platform/ ./quant_platform/
COPY vendor/pymdl/pymdl-2.13.232-py3.tar.gz /tmp/

RUN pip install --no-cache-dir "setuptools<70" \
    && pip install --no-cache-dir --no-build-isolation --force-reinstall /tmp/pymdl-2.13.232-py3.tar.gz \
    && pip install --no-cache-dir "setuptools>=70" \
    && python -c "import pymdl; print('pymdl import ok')"

ENV LD_LIBRARY_PATH=/usr/local/lib/python3.11/site-packages/pymdl:${LD_LIBRARY_PATH}
# Use jemalloc for all memory allocations (Python + pymdl C++)
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
# Remove PYTHONMALLOC=malloc — jemalloc handles fragmentation natively
ENV MALLOC_ARENA_MAX=1

CMD ["python", "-m", "quant_platform.collector.sdk_collector"]
