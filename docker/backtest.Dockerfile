FROM 172.24.99.176:5000/quant-platform/base:latest

WORKDIR /app

COPY requirements.txt /app/
COPY quant_platform /app/quant_platform

ENV PYTHONUNBUFFERED=1
