#!/usr/bin/env bash
set -euo pipefail

MDL_SDK_URL="${MDL_SDK_URL:-https://quant-mdl-data.oss-cn-hangzhou.aliyuncs.com/vendor/libmdl_api.so}"
MDL_CLIENT_URL="${MDL_CLIENT_URL:-https://quant-mdl-data.oss-cn-hangzhou.aliyuncs.com/vendor/mdl_forward_2.13.232_linux.tar.gz}"

mkdir -p vendor/mdl-sdk/libs/linux vendor/mdl-client

if [ ! -f vendor/mdl-sdk/libs/linux/libmdl_api.so ]; then
  echo "[vendor] downloading libmdl_api.so"
  curl -fSL -o vendor/mdl-sdk/libs/linux/libmdl_api.so "${MDL_SDK_URL}"
fi

if [ ! -f vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz ]; then
  echo "[vendor] downloading mdl_forward_2.13.232_linux.tar.gz"
  curl -fSL -o vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz "${MDL_CLIENT_URL}"
fi

test -f vendor/mdl-sdk/libs/linux/libmdl_api.so
test -f vendor/mdl-sdk/libs/linux/libjson.a
test -f vendor/mdl-client/mdl_forward_2.13.232_linux.tar.gz
