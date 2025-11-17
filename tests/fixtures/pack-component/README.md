This directory contains a minimal WIT component implementing the `greentic:pack-export@0.2.0`
world. The compiled artifact `pack-component.wasm` is checked in so host tests can drive
`PackRuntime::run_flow` without rebuilding the component.

To regenerate the component (requires `wasm32-wasip2` target and cached dependencies):

```bash
cd tests/fixtures/pack-component
CARGO_NET_OFFLINE=true cargo build --target wasm32-wasip2 --release
cp target/wasm32-wasip2/release/pack_component.wasm ./pack-component.wasm
```
