FROM localhost:5000/quant-platform-base:latest

WORKDIR /app

ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

COPY requirements.txt /app/
COPY qlib_factor_platform /app/qlib_factor_platform
COPY app.py /app/

RUN mkdir -p /data /data/tenant_a /data/tenant_b /data/shared

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py"]
