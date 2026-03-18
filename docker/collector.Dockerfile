FROM localhost:5000/quant-platform-base:latest

WORKDIR /app

COPY requirements.txt /app/
COPY qlib_factor_platform /app/qlib_factor_platform

RUN mkdir -p /data /data/tenant_a /data/tenant_b /data/shared

EXPOSE 9000

HEALTHCHECK --interval=60s --timeout=15s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9000/health || exit 1

CMD ["python", "-m", "qlib_factor_platform.tongliang.collector"]
