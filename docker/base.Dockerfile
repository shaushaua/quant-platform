# Base Dockerfile for Quant Platform
# Contains Python 3.11 + quant_platform framework.
# Strategy images are built FROM this base.

FROM docker.m.daocloud.io/library/python:3.11-slim

LABEL maintainer="Quant Platform Team"
LABEL description="Base image for distributed backtest workers"

# Use Chinese mirror for faster download in China.
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip config set global.trusted-host pypi.tuna.tsinghua.edu.cn

# Use Chinese Debian mirror only.
RUN rm -rf /etc/apt/sources.list.d/* && \
    echo 'deb https://mirrors.tuna.tsinghua.edu.cn/debian trixie main contrib non-free non-free-firmware' > /etc/apt/sources.list && \
    echo 'deb https://mirrors.tuna.tsinghua.edu.cn/debian trixie-updates main contrib non-free non-free-firmware' >> /etc/apt/sources.list

# Install system dependencies and ossfs 2.0.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    gnupg \
    fuse \
    libfuse2 \
    unzip \
    build-essential \
    ca-certificates \
    && wget -q https://gosspublic.alicdn.com/ossfs/ossfs2_2.0.6_linux_x86_64.deb -O /tmp/ossfs.deb \
    && apt-get install -y /tmp/ossfs.deb \
    && rm -f /tmp/ossfs.deb \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/local/bin/ossfs2 /usr/local/bin/ossfs \
    && which ossfs && ossfs --version

# Install ossutil for file operations.
RUN set -ex && \
    wget -q https://gosspublic.alicdn.com/ossutil/1.7.17/ossutil-v1.7.17-linux-amd64.zip -O /tmp/ossutil.zip && \
    unzip /tmp/ossutil.zip -d /tmp/ && \
    find /tmp -name "ossutil*" -type f -executable -exec cp {} /usr/local/bin/ossutil \; && \
    chmod +x /usr/local/bin/ossutil && \
    rm -rf /tmp/ossutil.zip /tmp/ossutil* && \
    which ossutil && ossutil --version

WORKDIR /app

# Copy requirements first for caching.
COPY requirements.txt .

# Install Python dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Copy framework code.
COPY quant_platform/ ./quant_platform/

# Set Python path.
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default environment variables. Runtime secrets override credentials.
ENV OSS_DATA_BUCKET=quant-historical-data
ENV OSS_RESULT_BUCKET=stock-mdl-data-result
ENV OSS_ENDPOINT=oss-cn-hangzhou-internal.aliyuncs.com

# Create directories.
RUN mkdir -p /data /results /logs

# Default command. Strategy images override this.
CMD ["python", "-c", "print('Quant Platform Base Image Ready')"]
