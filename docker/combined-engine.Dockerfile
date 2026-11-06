# Stage 0: Build C++ native-mdl-collector (cached by Docker unless native_mdl_collector/ changes)
FROM 172.24.99.176:5000/quant-platform/backtest-base:latest AS cpp-builder

RUN apt-get update && apt-get install -y --no-install-recommends cmake g++ make && rm -rf /var/lib/apt/lists/*

# Copy MDL C++ SDK headers + static libs (small, tracked in git)
COPY vendor/mdl-sdk/ /opt/mdl-sdk/

RUN chmod +x /opt/mdl-sdk/libs/linux/libmdl_api.so

# Copy native collector source
COPY native_mdl_collector/ /app/native_mdl_collector/

RUN mkdir -p /app/native_mdl_collector/build && \
    cd /app/native_mdl_collector/build && \
    cmake .. -DMDL_SDK_ROOT=/opt/mdl-sdk && \
    make -j$(nproc)

# Stage 1: Final native runtime image
FROM 172.24.99.176:5000/quant-platform/backtest-base:latest

LABEL description="Native combined engine: feeder_client + C++ MDL collector + Python factor computation"

WORKDIR /app

# jemalloc is installed in this runtime image for the native collector and Python engine.
# zstd: SHM raw archive compression (50GB mmap → tar.zst, 比 gzip 快 5-10x).
RUN apt-get update \
    && apt-get install -y --no-install-recommends libjemalloc2 zstd \
    && rm -rf /var/lib/apt/lists/*

# Install MDL Linux client (feeder_client sidecar) from local vendor cache.
COPY vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz /tmp/
RUN mkdir -p /opt/mdl-client \
    && tar xzf /tmp/mdl_forward_2.13.232_linux.tar.gz -C /opt/mdl-client \
    && chmod +x /opt/mdl-client/feeder_client \
    && rm -f /tmp/mdl_forward_2.13.232_linux.tar.gz

# Copy C++ native-mdl-collector binary and MDL SDK shared library (from cpp-builder stage)
RUN mkdir -p /opt/native-mdl-collector/bin /opt/native-mdl-collector/lib
COPY --from=cpp-builder /app/native_mdl_collector/build/native-mdl-collector /opt/native-mdl-collector/bin/
COPY --from=cpp-builder /opt/mdl-sdk/libs/linux/libmdl_api.so /opt/native-mdl-collector/lib/
RUN chmod +x /opt/native-mdl-collector/bin/native-mdl-collector

# Copy entrypoint script (starts feeder_client then Python engine)
COPY docker/entrypoint-combined.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# ── Python 依赖层(必须在 COPY quant_platform/ 之前!) ─────────────
# 依赖层放在代码 COPY 之后会导致: 每次改代码 → 代码层失效 → pip 层
# 跟着失效 → 600MB 依赖全量重下(还叠加 xgboost 2.x 的 nvidia 硬依赖,
# 2026-11-06 构建卡死实例)。放在前面后, 只有依赖清单变化才重下,
# 日常改代码的构建跳过本层(秒级)。
#
# Inference dependencies (for trader-provided inference modules)
# paramiko: optional SFTP utilities
# lightgbm/xgboost: model artifacts deserialization (tree_model_inference)
# cvxpy: position optimizer inside calc_predict_tree_model.so (USE_OPTIMIZER=True)
# polars: required by .so for lazy frame operations
#
# xgboost 必须 --no-deps: 2.x manylinux wheel 把 nvidia-nccl/cublas(~1GB,
# GPU 专用, CPU 推理完全不用)声明成硬依赖。ldd libxgboost.so 证实主库
# NEEDED 表无任何 NVIDIA 库(仅 pthread/dl/libgomp/libstdc++/libc)。
# 其真实运行依赖 numpy/scipy 由本层显式提供。
# 阿里云镜像: 打包机在阿里云内网, 直连 pypi.org 慢且时段性断流。
RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ \
        "lightgbm>=4.0,<5" "cvxpy>=1.4,<2" \
        "joblib>=1.3,<2" "cloudpickle>=2.2,<4" "scikit-learn>=1.3,<2" "paramiko>=3.0,<4" \
        "polars>=1.0,<2" "scipy>=1.10" && \
    pip install --no-cache-dir --no-deps -i https://mirrors.aliyun.com/pypi/simple/ \
        "xgboost>=1.7,<3"

# Copy latest framework code over the base image copy.
COPY quant_platform/ ./quant_platform/

ENV LD_LIBRARY_PATH=/opt/native-mdl-collector/lib:/opt/mdl-client
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
ENV MALLOC_ARENA_MAX=1

# MDL client log directory
RUN mkdir -p /data/quant/mdl_logs/client

ENTRYPOINT ["/app/entrypoint-combined.sh"]
