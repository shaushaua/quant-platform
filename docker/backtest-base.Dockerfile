FROM 172.24.99.176:5000/quant-platform-base:latest

WORKDIR /app

# 复制项目文件
COPY quant_platform/ ./quant_platform/

# 策略代码挂载点（kaniko 构建策略镜像时 COPY strategy.py 到此处）
RUN mkdir -p /app/strategy

# 把包路径加入 PYTHONPATH，无需构建
ENV PYTHONPATH=/app

# 默认入口：由 backtest-operator worker job 调用
CMD ["python", "-m", "quant_platform.backtest.worker_entrypoint"]
