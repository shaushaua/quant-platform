FROM 172.24.99.176:5000/quant-platform-base:latest

WORKDIR /app

# 复制项目文件
COPY quant_platform/ ./quant_platform/

# 把包路径加入 PYTHONPATH，无需构建
ENV PYTHONPATH=/app
