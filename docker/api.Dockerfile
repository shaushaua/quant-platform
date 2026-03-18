FROM localhost:5000/quant-platform-base:latest

WORKDIR /app

COPY requirements.txt /app/
COPY qlib_factor_platform /app/qlib_factor_platform

RUN mkdir -p /data /data/tenant_a /data/tenant_b /data/shared

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "qlib_factor_platform.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
