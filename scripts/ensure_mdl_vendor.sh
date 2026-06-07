#!/usr/bin/env bash
set -euo pipefail

MDL_SDK_URL="${MDL_SDK_URL:-https://quant-mdl-data.oss-cn-hangzhou.aliyuncs.com/vendor/libmdl_api.so}"
MDL_CLIENT_URL="${MDL_CLIENT_URL:-https://quant-mdl-data.oss-cn-hangzhou.aliyuncs.com/vendor/mdl_forward_2.13.232_linux.tar.gz}"
MDL_SDK_OSS_URI="${MDL_SDK_OSS_URI:-oss://quant-mdl-data/vendor/libmdl_api.so}"
MDL_CLIENT_OSS_URI="${MDL_CLIENT_OSS_URI:-oss://quant-mdl-data/vendor/mdl_forward_2.13.232_linux.tar.gz}"

mkdir -p vendor/mdl-sdk/libs/linux vendor/mdl-client

download_artifact() {
  local path="$1"
  local url="$2"
  local oss_uri="$3"
  local name="$4"

  if [ -f "${path}" ]; then
    return 0
  fi

  echo "[vendor] downloading ${name}"
  if curl -fSL -o "${path}" "${url}"; then
    return 0
  fi

  rm -f "${path}"
  if command -v ossutil >/dev/null 2>&1; then
    echo "[vendor] curl failed; trying ossutil: ${oss_uri}"
    if ossutil cp "${oss_uri}" "${path}"; then
      return 0
    fi
  fi

  cat >&2 <<EOF
[vendor] failed to download ${name}
  URL: ${url}
  OSS: ${oss_uri}

If the OSS object is private, either:
  1. configure ossutil on this build machine and retry; or
  2. export ${name}_URL to a signed URL/public URL; or
  3. upload the file locally to ${path}.
EOF
  return 1
}

download_artifact \
  vendor/mdl-sdk/libs/linux/libmdl_api.so \
  "${MDL_SDK_URL}" \
  "${MDL_SDK_OSS_URI}" \
  "MDL_SDK"

download_artifact \
  vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz \
  "${MDL_CLIENT_URL}" \
  "${MDL_CLIENT_OSS_URI}" \
  "MDL_CLIENT"

test -f vendor/mdl-sdk/libs/linux/libmdl_api.so
test -f vendor/mdl-sdk/libs/linux/libjson.a
test -f vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz
