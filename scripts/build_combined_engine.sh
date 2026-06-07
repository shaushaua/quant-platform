#!/usr/bin/env bash
set -euo pipefail

REGISTRY="${REGISTRY:-172.24.99.176:5000/quant-platform}"
IMAGE="${IMAGE:-combined-engine}"
TAG="${1:-${TAG:-latest}}"
DOCKERFILE="${DOCKERFILE:-docker/combined-engine.Dockerfile}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[build] image: ${REGISTRY}/${IMAGE}:${TAG}"
echo "[build] dockerfile: ${DOCKERFILE}"

scripts/ensure_mdl_vendor.sh

if [ "$TAG" = "latest" ]; then
  docker build \
    -f "$DOCKERFILE" \
    -t "${REGISTRY}/${IMAGE}:latest" \
    .
  docker push "${REGISTRY}/${IMAGE}:latest"
  echo "[build] pushed: ${REGISTRY}/${IMAGE}:latest"
else
  docker build \
    -f "$DOCKERFILE" \
    -t "${REGISTRY}/${IMAGE}:${TAG}" \
    -t "${REGISTRY}/${IMAGE}:latest" \
    .
  docker push "${REGISTRY}/${IMAGE}:${TAG}"
  docker push "${REGISTRY}/${IMAGE}:latest"
  echo "[build] pushed: ${REGISTRY}/${IMAGE}:${TAG}"
  echo "[build] pushed: ${REGISTRY}/${IMAGE}:latest"
fi
