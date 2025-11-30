# greentic-runner – Current Behaviour (Snapshot)

## 1. Flow model and node types
- Flow representation: pack flows are stored as `greentic_flow::ir::FlowIR` (see `greentic-flow` crate) with nodes keyed by string IDs. The runner loads `FlowIR` from packs (`crates/greentic-runner-host/src/pack.rs` → `PackRuntime::load_flow_ir`) and executes them in `FlowEngine` (`crates/greentic-runner-host/src/runner/engine.rs`).
- Node dispatch is hard-coded in `FlowEngine::dispatch_node`:
  - `component.exec` – generic path that resolves a pack component by `component_ref` and calls its `greentic:component@0.4.0` exports via Wasmtime.
  - `flow.call` – builtin; recursively invokes another flow by ID (`execute_flow_call`).
  - `session.wait` – builtin; yields a wait snapshot (`DispatchOutcome::wait`), enabling pause/resume.
  - `emit*` (any component starting with `emit`) – builtin; pushes payload to egress buffer.
  - Any other component string → error (`unsupported node component`).
- Node kinds are inferred solely by these string literals; the only generic path for domain logic is `component.exec`.
- Flow engine retry/backoff is derived from host `retry` config (`FlowRetryConfig` → `RetryConfig`) and applied to all node executions in the engine.

## 2. Component loading and invocation
- Pack ingestion: `RunnerHost` loads packs (`crates/greentic-runner-host/src/host.rs`) using `TenantRuntime::load`/`PackRuntime::load`, which:
  - Verifies archives (optional), reads `.gtpack` or `.component.wasm`, parses pack metadata/flows, and caches `FlowIR` if present (`pack.rs`).
  - Instantiates a Wasmtime `Component` for the pack to expose host imports (HTTP, secrets, state, session, telemetry; MCP removed).
  - Favors the embedded `manifest.cbor` (via `greentic-pack::open_pack`) as a fast path for flow/component descriptors, with a legacy archive scan fallback when the manifest is missing or invalid.
- Execution path:
  - Ingress adapters normalize activities to `IngressEnvelope` and call `StateMachineRuntime::handle` (`engine/runtime.rs`), which wraps the flow engine via the `PackFlowAdapter`.
  - The `PackFlowAdapter` calls `FlowEngine::execute`/`resume` with the ingress payload; flow nodes run as builtins (above).
  - `component.exec` resolves the target pack component (from `manifest.components`) and invokes `greentic:component/node@0.4.0#invoke`, passing an `ExecCtx`, node payload as input JSON, and optional config JSON.
- Host imports: `ComponentState` implements `greentic-interfaces-host` v0.6 imports (`HostImports` in `pack.rs`), wiring HTTP, secrets, telemetry, state/session, OAuth. MCP bridging is removed; pack components rely on host imports only.
  - Component invocation caches a prepared `InstancePre` per component (fresh `Store` per call) to avoid redundant linking work; a shared blocking HTTP client is reused across invocations to avoid spawning per-call clients.

## 3. MCP-specific logic (legacy)
- Removed. The MCP bridge (`greentic_mcp`, `mcp.exec` node, host `mcp_exec`/`tool_invoke`, `HostConfig::mcp`) is stripped out. MCP is expected to be handled via pre-composed components invoked through `component.exec`.

## 4. Hard-coded vs pack-driven behaviour
- Builtins (runner code): control-flow + egress only (`flow.call`, `session.wait`, `emit*`).
- Pack-driven: `component.exec` invokes pack components generically via `greentic:component@0.4.0`. MCP is expected to arrive as a pre-composed component; no MCP-specific host logic remains. QA/templating logic now lives in pack components (runner-components fixture).

## 5. Host configuration model
- Defined in `crates/greentic-runner-host/src/config.rs`.
- Top-level `HostConfig`: `tenant`, `bindings_path`, `flow_type_bindings`, `rate_limits`, `retry`, `http_enabled`, `secrets_policy`, `webhook_policy`, `timers`, `oauth`.
- `BindingsFile` maps YAML to `HostConfig`; `flow_type_bindings` declare adapters per flow type (e.g., messaging/webhook) with `config` and `secrets`.
- Retry config is now protocol-neutral (`FlowRetryConfig`) and feeds the flow-engine retry policy.
- OAuth block maps to `OAuthBrokerConfig`.
- MCP-specific config is removed; existing bindings should omit `mcp` blocks.

## 6. Tests and fixtures
- Integration tests (crate `crates/tests`):
  - `host_integration.rs` – loads demo pack, runs a flow end-to-end via RunnerHost; exercises pack loading and flow execution.
  - `host_wasi_p2_roundtrip.rs` – validates WASI preview2 policy enforcement when instantiating a component.
  - `multiturn.rs`, `webhook_timer.rs` – exercise session/wait/timers via state machine.
  - `weather_smoke.rs` (ignored) – placeholder for live pack call.
- Unit tests:
  - `FlowEngine` templating/rendering/finalize tests (`runner/engine.rs`).
  - `StateMachine` pause/resume adapter tests (`engine/state_machine.rs`).
  - Config OAuth mapping tests (`config.rs`).
- MCP coverage: none; no MCP bridge remains; future coverage should exercise `component.exec` with an MCP component.
- `component.exec` coverage: integration tests now include `component_exec.rs` plus `pack_manifest.rs` (loads the runner-components `.gtpack` and calls its components) to guard the generic path.
- Fixtures: demo pack under `examples/packs/demo.gtpack`; runner-components pack fixture under `tests/fixtures/packs/runner-components` (with `.gtpack`, manifest, and component WASMs); bindings in `examples/bindings/default.bindings.yaml` omit MCP blocks.

## 7. Observed design mismatches
- Remaining builtins (`flow.call`, `session.wait`, `emit*`) are still hard-coded; they should eventually be migrated into components and routed via `component.exec`.
- Flow fixtures and bindings should stay aligned with the component-based model; MCP references have been removed from shipped hints/configs.
