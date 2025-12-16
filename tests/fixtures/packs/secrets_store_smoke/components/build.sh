#!/usr/bin/env bash
set -euo pipefail

dir=$(cd "$(dirname "$0")" && pwd)

cd "$dir"

export WIT_BINDGEN_WIT_SEARCH_PATH="$dir/wit/echo-secret/deps"

RUSTFLAGS="-C opt-level=z" cargo build \
  --target wasm32-wasip2 \
  --release

# Move wasm to components directory
cp "$dir/../../../../../target/wasm32-wasip2/release/echo_secret.wasm" "$dir/echo_secret.wasm"
