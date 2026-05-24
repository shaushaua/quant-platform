FROM docker.m.daocloud.io/library/python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple

# Common native libraries used by pandas/pyarrow/duckdb and optional ML packages.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        gcc \
        g++ \
        git \
        libgomp1 \
        libhdf5-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Runtime dependencies shared by backtest, platform, and collector images.
# pymdl is intentionally excluded: it is a proprietary SDK copied in sdk-collector.
RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install \
        numpy>=1.24.0 \
        pandas>=2.0.0 \
        scipy>=1.10.0 \
        pyarrow>=12.0.0 \
        duckdb>=0.10.0 \
        oss2>=2.18.0 \
        pymysql>=1.1.0 \
        fastapi>=0.110.0 \
        "uvicorn[standard]>=0.27.0" \
        "pydantic>=2.0.0,<3.0.0" \
        "pydantic-settings>=2.0.0,<3.0.0" \
        python-multipart>=0.0.6 \
        requests>=2.31.0 \
        python-dateutil>=2.8.2 \
        prometheus-client>=0.19.0 \
        pyyaml>=6.0 \
        python-dotenv>=1.0.0 \
        tqdm>=4.66.0 \
        httpx>=0.24.0 \
        joblib>=1.3.0 \
        watchdog>=3.0.0 \
        readerwriterlock>=1.0.9 \
        multiprocessing-logging>=0.3.4 \
        aliyun-log-python-sdk

# Pre-install DuckDB httpfs so worker pods do not need outbound internet at runtime.
RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs'); con.execute('LOAD httpfs'); con.close()"

CMD ["python", "--version"]
