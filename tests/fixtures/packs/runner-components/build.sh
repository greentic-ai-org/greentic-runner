#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMP_ROOT="${ROOT}/../../runner-components"
OUT_DIR="${ROOT}/components"
TARGET_DIR="${COMP_ROOT}/target/wasm32-wasip2/release"

mkdir -p "${OUT_DIR}"

# Build component crates
cargo build --manifest-path "${COMP_ROOT}/qa_process/Cargo.toml" --target wasm32-wasip2 --release
cargo build --manifest-path "${COMP_ROOT}/templating_handlebars/Cargo.toml" --target wasm32-wasip2 --release

cp "${TARGET_DIR}/qa_process.wasm" "${OUT_DIR}/qa_process.wasm"
cp "${TARGET_DIR}/templating_handlebars.wasm" "${OUT_DIR}/templating_handlebars.wasm"

echo "Components copied to ${OUT_DIR}"

# Build .gtpack via local packager helper
cargo run --manifest-path "${COMP_ROOT}/packager/Cargo.toml" -- --root "${ROOT}" --out "${ROOT}/runner-components.gtpack" --manifest-out "${ROOT}/manifest.cbor"
