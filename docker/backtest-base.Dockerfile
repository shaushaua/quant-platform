FROM 172.24.99.176:5000/quant-platform-base:latest

WORKDIR /app

# 复制项目文件
COPY pyproject.toml ./
COPY quant_platform/ ./quant_platform/

# 安装 quant_platform 包（依赖已在基础镜像中）
RUN pip install --no-cache-dir . --no-deps
