REGISTRY ?= 172.24.99.176:5000/quant-platform
IMAGE ?= combined-engine
TAG ?= $(shell date +%Y%m%d%H%M%S)

.PHONY: build push deploy all

# ============================================================
# Full build: docker image → push
# ============================================================
all: build push

# ============================================================
# Build Docker image (Rust build happens inside Docker)
# ============================================================
build:
	@echo "==> Building Docker image..."
	docker build -f docker/combined-engine.Dockerfile \
	    -t $(REGISTRY)/$(IMAGE):latest \
	    -t $(REGISTRY)/$(IMAGE):$(TAG) \
	    .

# ============================================================
# Push Docker image to registry
# ============================================================
push:
	@echo "==> Pushing $(REGISTRY)/$(IMAGE):$(TAG) ..."
	docker push $(REGISTRY)/$(IMAGE):$(TAG)
	docker push $(REGISTRY)/$(IMAGE):latest
	@echo "==> Done: $(REGISTRY)/$(IMAGE):$(TAG)"

# ============================================================
# Deploy: rollout restart on K8s
# ============================================================
deploy: push
	kubectl rollout restart deployment/combined-engine
	@echo "==> Deployment restarted"
