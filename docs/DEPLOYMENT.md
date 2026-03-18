# -*- coding: utf-8 -*-
"""
部署文档
说明如何部署量化因子平台
"""

# 部署指南

## 目录

- [环境要求](#环境要求)
- [本地开发部署](#本地开发部署)
- [Docker 部署](#docker-部署)
- [生产环境部署](#生产环境部署)
- [配置说明](#配置说明)
- [监控和日志](#监控和日志)
- [故障排查](#故障排查)

---

## 环境要求

### 基础要求

- Python 3.9+
- Docker 20.10+
- Docker Compose 2.0+
- 4GB+ 可用内存
- 10GB+ 可用磁盘空间

### 依赖服务

- Redis 6.0+ (用于缓存)
- 阿里云 OSS (用于数据存储)
- 通联数据源 Token (用于实时行情)

---

## 本地开发部署

### 1. 克隆项目

```bash
git clone https://github.com/your-org/qlib-factor-platform.git
cd qlib-factor-platform
```

### 2. 创建虚拟环境

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或 venv\Scripts\activate  # Windows
```

### 3. 安装依赖

```bash
pip install -r requirements.txt
```

### 4. 配置环境变量

复制 `.env.example` 到 `.env` 并修改配置：

```bash
cp .env.example .env
```

编辑 `.env` 文件，配置以下参数：

```bash
# Redis 配置
REDIS_HOST=localhost
REDIS_PORT=6379

# OSS 配置
OSS_ACCESS_KEY_ID=your_access_key
OSS_ACCESS_KEY_SECRET=your_secret
OSS_BUCKET_NAME=your_bucket
OSS_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com

# 通联配置
TONGLIANG_TOKEN=your_32_char_token
```

### 5. 初始化 QLib 数据

```bash
python -c "import qlib; qlib.init(provider_uri='~/.qlib/qlib_data/cn_data', region='cn')"
```

### 6. 启动服务

#### 启动 API 服务

```bash
uvicorn qlib_factor_platform.api.main:app --reload --host 0.0.0.0 --port 8000
```

#### 启动 UI 服务

```bash
streamlit run app.py
```

#### 启动数据采集服务（可选）

```bash
python -m qlib_factor_platform.tongliang.collector
```

### 7. 访问服务

- UI: http://localhost:8501
- API: http://localhost:8000
- API 文档: http://localhost:8000/docs (debug 模式)

---

## Docker 部署

### 1. 使用 Docker Compose 一键部署

```bash
# 构建并启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f
```

### 2. 访问服务

- UI: http://localhost:8501
- API: http://localhost:8000
- API 健康检查: http://localhost:8000/health

### 3. 停止服务

```bash
# 停止所有服务
docker-compose down

# 停止并删除卷
docker-compose down -v
```

### 4. 独立构建和运行服务

#### 构建 API 服务

```bash
docker build -f docker/api.Dockerfile -t qlib-api .
docker run -p 8000:8000 --env-file .env qlib-api
```

---

## 控制面宿主机部署

适用：镜像构建、分布式回测调度控制面。需要本机 Docker CLI。

### 1. 安装依赖

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 配置环境变量

```bash
cp .env.example .env
```

### 3. 启动控制面 API

```bash
./start_api.sh
```

### 4. 实盘数据与策略执行

实时采集与策略执行建议同机部署：

```bash
python -m qlib_factor_platform.scripts.run_realtime --strategy /path/to/strategy.py
```

使用 xxl_job 调度单次执行：

```bash
python -m qlib_factor_platform.scripts.run_realtime --strategy /path/to/strategy.py --once --no-collector
```

#### 构建 UI 服务

```bash
docker build -f docker/ui.Dockerfile -t qlib-ui .
docker run -p 8501:8501 --env-file .env qlib-ui
```

#### 构建数据采集服务

```bash
docker build -f docker/collector.Dockerfile -t qlib-collector .
docker run --env-file .env qlib-collector
```

---

## 生产环境部署

### 1. 准备生产环境配置

创建 `production.env` 文件：

```bash
# 环境配置
ENVIRONMENT=production
DEBUG=false

# Redis 配置（生产环境使用外部 Redis）
REDIS_HOST=your-redis-host
REDIS_PORT=6379
REDIS_PASSWORD=your-redis-password
REDIS_MAX_MEMORY_MB=100.0
REDIS_POOL_SIZE=10

# OSS 配置
OSS_ACCESS_KEY_ID=your_access_key
OSS_ACCESS_KEY_SECRET=your_secret
OSS_BUCKET_NAME=your_bucket
OSS_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com

# 通联配置
TONGLIANG_TOKEN=your_token
TONGLIANG_SH_L1_HOST=mdl-cloud-sh.datayes.com
TONGLIANG_SH_L1_PORT=19011
TONGLIANG_SZ_L1_HOST=mdl-cloud-sz.datayes.com
TONGLIANG_SZ_L1_PORT=19011

# API 配置
API_BASE_URL=https://api.yourdomain.com
API_CORS_ORIGINS=https://yourdomain.com

# 回测配置
MAX_BACKTEST_WORKERS=4
```

### 2. 使用 Docker Compose 部署生产环境

```bash
# 使用生产环境配置启动
docker-compose --env-file production.env up -d

# 查看服务状态
docker-compose ps
```

### 3. 配置 Nginx 反向代理

创建 Nginx 配置文件 `nginx.conf`：

```nginx
upstream api_backend {
    server api:8000;
}

upstream ui_backend {
    server ui:8501;
}

server {
    listen 80;
    server_name yourdomain.com;

    # API 服务
    location /api/ {
        proxy_pass http://api_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # UI 服务
    location / {
        proxy_pass http://ui_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_http_version 1.1;
    }
}
```

启动 Nginx：

```bash
docker run -d \
  --name qlib-nginx \
  -p 80:80 \
  -p 443:443 \
  -v ./nginx.conf:/etc/nginx/nginx.conf:ro \
  -v ./ssl:/etc/nginx/ssl:ro \
  nginx:alpine
```

### 4. 配置 HTTPS（推荐）

使用 Let's Encrypt 获取免费 SSL 证书：

```bash
docker run -d \
  --name qlib-certbot \
  -v ./letsencrypt:/etc/letsencrypt \
  -p 80:80 \
  -p 443:443 \
  certbot/certbot certonly --standalone \
  -d yourdomain.com
```

更新 Nginx 配置以使用 SSL：

```nginx
server {
    listen 443 ssl http2;
    server_name yourdomain.com;

    ssl_certificate /etc/nginx/ssl/fullchain.pem;
    ssl_certificate_key /etc/nginx/ssl/privkey.pem;

    # ... 其他配置保持不变
}

server {
    listen 80;
    server_name yourdomain.com;
    return 301 https://$server_name$request_uri;
}
```

---

## 配置说明

### 环境变量

| 变量名 | 说明 | 默认值 | 必需 |
|--------|------|--------|------|
| `ENVIRONMENT` | 环境类型 | development | 否 |
| `DEBUG` | 调试模式 | true | 否 |
| `REDIS_HOST` | Redis 主机 | localhost | 是 |
| `REDIS_PORT` | Redis 端口 | 6379 | 否 |
| `REDIS_PASSWORD` | Redis 密码 | - | 否 |
| `REDIS_MAX_MEMORY_MB` | Redis 最大内存 | 10.0 | 否 |
| `OSS_ACCESS_KEY_ID` | OSS AccessKey | - | 是 |
| `OSS_ACCESS_KEY_SECRET` | OSS Secret | - | 是 |
| `OSS_BUCKET_NAME` | OSS 存储桶名 | - | 是 |
| `OSS_ENDPOINT` | OSS 端点 | - | 是 |
| `OSS_DATA_PATH` | OSS 数据挂载路径 | /2025 | 否 |
| `REALTIME_PARQUET_PATH` | 实盘分钟数据落盘路径 | OSS_DATA_PATH | 否 |
| `TONGLIANG_TOKEN` | 通联 Token | - | 是 |
| `API_BASE_URL` | API 基础 URL | http://localhost:8000 | 否 |
| `MAX_BACKTEST_WORKERS` | 最大回测工作线程 | 2 | 否 |

### Redis 配置

- **缓存策略**: allkeys-lru (最近最少使用淘汰)
- **持久化**: AOF (仅追加文件)
- **最大内存**: 128MB (容器内)，生产环境建议 1GB+

### OSS 配置

- **存储结构**:
  ```
  /YYYY/
    /YYYYMM/
      /YYYYMMDD/
        YYYYMMDD_HHMM_tick.parquet
        YYYYMMDD_HHMM_order.parquet
        YYYYMMDD_HHMM_deal.parquet
        YYYYMMDD_HHMM_kline_1min.parquet
        YYYYMMDD_kline_5min.parquet
        YYYYMMDD_kline_10min.parquet
        YYYYMMDD_daily_basic_data.parquet
  ```

---

## 监控和日志

### 健康检查

所有服务都内置健康检查：

```bash
# API 健康检查
curl http://localhost:8000/health

# Redis 健康检查
docker exec qlib-redis redis-cli ping

# 查看所有服务健康状态
docker-compose ps
```

### 查看日志

```bash
# 查看所有服务日志
docker-compose logs -f

# 查看特定服务日志
docker-compose logs -f api
docker-compose logs -f ui
docker-compose logs -f collector
docker-compose logs -f redis

# 查看最近 100 行日志
docker-compose logs --tail=100 api
```

### 日志级别

日志级别可通过环境变量配置：

```bash
# DEBUG 级别（开发环境）
LOG_LEVEL=DEBUG

# INFO 级别（生产环境）
LOG_LEVEL=INFO

# WARNING 级别
LOG_LEVEL=WARNING

# ERROR 级别
LOG_LEVEL=ERROR
```

---

## 故障排查

### 常见问题

#### 1. 服务无法启动

**症状**: `docker-compose up` 后服务立即退出

**解决**:
```bash
# 查看详细日志
docker-compose logs api

# 检查配置文件
docker-compose config

# 检查端口占用
netstat -tuln | grep -E '8000|8501'
```

#### 2. Redis 连接失败

**症状**: API 服务报错 "Redis connection refused"

**解决**:
```bash
# 检查 Redis 容器状态
docker ps | grep redis

# 进入 Redis 容器检查
docker exec -it qlib-redis redis-cli ping

# 检查 Redis 日志
docker-compose logs redis
```

#### 3. OSS 上传失败

**症状**: 数据上传到 OSS 失败

**解决**:
```bash
# 检查 OSS 凭证
echo $OSS_ACCESS_KEY_ID
echo $OSS_ACCESS_KEY_SECRET

# 测试 OSS 连接
python -c "
from oss2 import Auth
auth = Auth('your_id', 'your_secret')
bucket = auth.get_bucket('your_bucket')
print(bucket.get_bucket_info())
"
```

#### 4. 通联连接失败

**症状**: 数据采集服务无法连接通联

**解决**:
```bash
# 检查 Token 格式（应为 32 字符）
echo $TONGLIANG_TOKEN | wc -c

# 检查网络连通性
ping mdl-cloud-sh.datayes.com
telnet mdl-cloud-sh.datayes.com 19011
```

#### 5. 内存不足

**症状**: 服务因 OOM 崩溃

**解决**:
```bash
# 增加 Docker 内存限制
# 在 docker-compose.yml 中添加：
services:
  api:
    mem_limit: 2g

# 清理未使用的 Docker 资源
docker system prune -a
```

### 性能优化

1. **Redis 优化**:
   - 生产环境使用专用 Redis 实例
   - 配置合适的 maxmemory
   - 使用合适的淘汰策略

2. **数据库优化**:
   - 定期清理过期数据
   - 使用索引优化查询
   - 考虑读写分离

3. **API 优化**:
   - 使用 Gunicorn 或 Uvicorn workers
   - 启用响应压缩
   - 配置合理的超时时间

---

## 备份和恢复

### 数据备份

```bash
# 备份 Redis 数据
docker exec qlib-redis redis-cli BGSAVE
docker cp qlib-redis:/data/dump.rdb ./backup/

# 备份 OSS 数据
# 使用 OSS 命令行工具或控制台进行备份

# 备份配置文件
tar -czf config-backup-$(date +%Y%m%d).tar.gz .env docker-compose.yml
```

### 数据恢复

```bash
# 恢复 Redis 数据
docker cp ./backup/dump.rdb qlib-redis:/data/dump.rdb
docker restart qlib-redis

# 恢复配置文件
tar -xzf config-backup-2024MMDD.tar.gz
```

---

## 更新部署

### 滚动更新

```bash
# 1. 拉取最新代码
git pull origin main

# 2. 构建新镜像
docker-compose build

# 3. 重启服务（无停机）
docker-compose up -d --no-deps --build api

# 4. 等待新服务就绪后更新其他服务
docker-compose up -d --no-deps --build ui
docker-compose up -d --no-deps --build collector
```

### 蓝绿部署

```bash
# 1. 构建新版本
docker build -f docker/api.Dockerfile -t qlib-api:blue .

# 2. 启动新版本（蓝）
docker run -d --name qlib-api-blue -p 8001:8000 qlib-api:blue

# 3. 健康检查后切换流量
# 4. 停止旧版本（绿）
docker stop qlib-api-green

# 5. 清理旧容器
docker rm qlib-api-green
```

---

## 安全建议

1. **不要在代码中硬编码敏感信息**
2. **使用强密码和安全的 Token**
3. **定期更新依赖包**
4. **启用 HTTPS**
5. **配置防火墙规则**
6. **定期备份数据**
7. **监控异常访问**
8. **实施速率限制**

---

## 联系支持

如有部署问题，请联系：

- Email: support@yourdomain.com
- GitHub Issues: https://github.com/your-org/qlib-factor-platform/issues
- 文档: https://docs.yourdomain.com
