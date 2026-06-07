REGISTRY ?= 172.24.99.176:5000/quant-platform
IMAGE ?= combined-engine

.PHONY: build push deploy all

# ============================================================
# Full build: docker image → push
# ============================================================
all: build push

# ============================================================
# Build Docker image (native C++ collector build happens inside Docker)
# ============================================================
build:
	@echo "==> Building Docker image..."
	scripts/ensure_mdl_vendor.sh
	docker build -f docker/combined-engine.Dockerfile \
	    -t $(REGISTRY)/$(IMAGE):latest \
	    .

# ============================================================
# Push Docker image to registry
# ============================================================
push:
	@echo "==> Pushing $(REGISTRY)/$(IMAGE):latest ..."
	docker push $(REGISTRY)/$(IMAGE):latest
	@echo "==> Done: $(REGISTRY)/$(IMAGE):latest"

# ============================================================
# Deploy: rollout restart on K8s
# ============================================================
deploy: push
	kubectl rollout restart deployment/combined-engine
	@echo "==> Deployment restarted"
