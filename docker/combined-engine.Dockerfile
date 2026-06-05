# Stage 0: Build C++ native-mdl-collector (cached by Docker unless native_mdl_collector/ changes)
FROM 172.24.99.176:5000/quant-platform/backtest-base:latest AS cpp-builder

RUN apt-get update && apt-get install -y --no-install-recommends cmake g++ make && rm -rf /var/lib/apt/lists/*

# Copy MDL C++ SDK (headers + shared lib)
COPY vendor/mdl-sdk/ /opt/mdl-sdk/

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

# jemalloc is intentionally installed here instead of depending on sdk-collector.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libjemalloc2 \
    && rm -rf /var/lib/apt/lists/*

# Install MDL Linux client (feeder_client sidecar)
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
COPY docker/entrypoint-combined.sh /app/entrypoint-combined.sh
RUN chmod +x /app/entrypoint-combined.sh

# Copy latest framework code over the base image copy.
COPY quant_platform/ ./quant_platform/

# Inference dependencies (for trader-provided inference modules)
RUN pip install --no-cache-dir joblib>=1.3,<2 cloudpickle>=2.2,<4 scikit-learn>=1.3,<2

ENV LD_LIBRARY_PATH=/opt/native-mdl-collector/lib:/opt/mdl-client
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
ENV MALLOC_ARENA_MAX=1

# MDL client log directory
RUN mkdir -p /data/quant/mdl_logs/client

ENTRYPOINT ["/app/entrypoint-combined.sh"]
