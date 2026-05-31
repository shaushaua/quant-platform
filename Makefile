REGISTRY ?= 172.24.99.176:5000/quant-platform
IMAGE ?= combined-engine
TAG ?= $(shell date +%Y%m%d%H%M%S)

RUSTUP_MIRROR = https://mirrors.ustc.edu.cn/rust-static
CRATES_MIRROR = sparse+https://mirrors.ustc.edu.cn/crates.io-index/

.PHONY: wheel build push deploy all clean

# ============================================================
# Full build: wheel → docker image → push
# ============================================================
all: wheel build push

# ============================================================
# Build mdl_parser wheel on host (requires Rust toolchain)
# Only needed when mdl_parser/src/lib.rs changes.
# ============================================================
wheel: vendor/mdl_parser/.built

vendor/mdl_parser/.built: mdl_parser/src/lib.rs mdl_parser/Cargo.toml
	@echo "==> Building mdl_parser wheel..."
	@export RUSTUP_DIST_SERVER=$(RUSTUP_MIRROR) \
	    && export RUSTUP_UPDATE_ROOT=$(RUSTUP_MIRROR)/rustup \
	    && if ! command -v cargo >/dev/null 2>&1; then \
	        echo "    Installing Rust via rustup (Chinese mirror)..." \
	        && curl -sSf $(RUSTUP_MIRROR)/rustup/rustup-init.sh | sh -s -- -y \
	        && . "$$HOME/.cargo/env"; \
	    fi \
	    && . "$$HOME/.cargo/env" \
	    && pip install -q maturin \
	    && mkdir -p vendor/mdl_parser \
	    && cd mdl_parser \
	    && printf '[source.crates-io]\nreplace-with = "ustc"\n\n[source.ustc]\nregistry = "$(CRATES_MIRROR)"\n' > .cargo/config.toml \
	    && maturin build --release --strip \
	    && cp target/wheels/*.whl ../vendor/mdl_parser/ \
	    && cd .. \
	    && touch vendor/mdl_parser/.built
	@echo "==> Wheel ready: $$(ls vendor/mdl_parser/*.whl)"

# ============================================================
# Build Docker image
# ============================================================
build: wheel
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

# ============================================================
# Clean
# ============================================================
clean:
	rm -f vendor/mdl_parser/*.whl vendor/mdl_parser/.built
	rm -rf mdl_parser/target
	@echo "==> Cleaned wheel and Rust build artifacts"
