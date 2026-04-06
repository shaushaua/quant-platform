# quant-platform 全量镜像：包含 collector / live_engine / backtest worker
FROM 172.24.99.176:5000/quant-platform-base:latest

WORKDIR /app

COPY quant_platform/ ./quant_platform/

# 策略代码挂载点（kaniko 构建策略镜像时 COPY strategy.py 到此处）
RUN mkdir -p /app/strategy

ENV PYTHONPATH=/app

# pymysql（daily_basic 从 MySQL 加载）
RUN pip install pymysql --no-cache-dir

# 预装 DuckDB httpfs 扩展
RUN python3 -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs'); con.execute('LOAD httpfs'); con.close()"
