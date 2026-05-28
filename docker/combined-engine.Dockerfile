# Combined engine: pymdl SDK + factor computation in single process.
# Extends sdk-collector image (which has pymdl + jemalloc).
FROM 172.24.99.176:5000/quant-platform/sdk-collector:latest

WORKDIR /app

COPY quant_platform/ ./quant_platform/

CMD ["python", "-m", "quant_platform.live_engine.combined_engine"]
