FROM hub-mirror.c.163.com/library/python:3.11-slim

WORKDIR /app

# 复制项目文件
COPY pyproject.toml ./
COPY quant_platform/ ./quant_platform/

# 安装依赖（使用清华源）
RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple \
    numpy pandas pyarrow \
    oss2 pymysql \
    requests python-dateutil \
    pyyaml python-dotenv \
    && pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -e .
