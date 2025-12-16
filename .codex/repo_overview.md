# Repository Overview

## 1. High-Level Purpose
- Monorepo for the Greentic runner: a Rust/Wasmtime-based runtime that loads signed “packs”, exposes canonical messaging/webhook/timer ingress adapters, and handles sessions/state/telemetry for multi-tenant flows.
- Provides the production host, CLI wrapper, desktop/dev harness, core pack resolution/cache logic, and integration tests plus example bindings/packs.

## 2. Main Components and Functionality
- **Path:** `crates/greentic-runner`  
  **Role:** CLI and embedding surface.  
  **Key functionality:** Parses bindings + port from CLI/env, constructs `RunnerConfig`, and launches the host via `run_http_host`; library exports `start_embedded_host` for API-only embedding and re-exports host modules. Ships binaries `greentic-runner` (HTTP host), `greentic-gen-bindings`, and `greentic-runner-cli` (desktop pack runner that executes a pack locally and prints `RunResult` JSON).

- **Path:** `crates/greentic-runner-host`  
  **Role:** Production runtime.  
  **Key functionality:** Loads tenant bindings, bootstraps secrets/OAuth/telemetry, and builds `RunnerHost`; pack watcher resolves/caches packs (including overlays) via `runner-core`; HTTP server (Axum) exposes canonical ingress adapters (Telegram, Teams, Slack, WebChat, Webex, WhatsApp, generic webhook, cron/timer) plus `/healthz` and `/admin/*`; normalizes payloads and session keys, persists/resumes sessions/state, verifies pack signatures/digests, and invokes Wasmtime components with configurable WASI (preview2) policy.  
  **Key dependencies / integration points:** Uses `runner-core` for pack resolution/verification, `greentic-pack` for manifests, `greentic-session/state` for persistence, and optional `greentic-oauth-host` for broker wiring.

- **Path:** `crates/greentic-runner-desktop`  
  **Role:** Desktop/dev harness to run packs locally without the HTTP server.  
  **Key functionality:** Builds host config + WASI policy, loads packs from disk, executes flows with mocks for HTTP/KV/secrets/time/tools/telemetry, captures transcripts/artifacts, enforces signing policy (strict/dev), and supports OTLP emission hooks; provides `Runner` convenience builder with profiles and tenant context overrides. Runtime errors and unexpected pauses are now surfaced in `RunResult` (error field plus `_runtime` failure entry) and also emitted to stderr for visibility without a tracing subscriber.

- **Path:** `crates/runner-core`  
  **Role:** Pack resolution/cache/verification utilities shared by hosts.  
  **Key functionality:** Parses JSON pack indexes (file or remote), resolves locators with source fallbacks, downloads artifacts via resolver registry, verifies digests/signatures, stores them in a content-addressed cache, and exposes `PackManager`/`ResolvedSet` for tenants.  
  **Key dependencies / integration points:** Relies on `greentic-pack` for manifests/build artifacts and optional public-key verification.

- **Path:** `crates/tests`  
  **Role:** Integration/regression suite.  
  **Key functionality:** Exercises host loading of demo packs, pack watcher reload and overlay handling, WASI preview2 policy enforcement for components, multi-turn flow pause/resume via in-memory adapter/hosts, and smoke scaffolding for webhook/cron adapters; includes ignored placeholder for live Telegram weather flow.

- **Path:** `examples/`  
  **Role:** Reference assets.  
  **Key functionality:** Example bindings/index/packs used by integration tests and quick-start scenarios.

## 3. Work In Progress, TODOs, and Stubs
- **Location:** `crates/tests/tests/weather_smoke.rs:6`  
  **Status:** Ignored smoke test (env-gated)  
  **Short description:** Spawns the `greentic-runner` binary against a user-supplied pack + bindings and hits `/webhook/{WEATHER_FLOW_ID}`; requires `WEATHER_PACK_PATH` and optional `WEATHER_BINDINGS_PATH`/`WEATHER_WEBHOOK_PAYLOAD`/`TELEGRAM_BOT_TOKEN`.

## 4. Broken, Failing, or Conflicting Areas
- No known broken areas after updating `PackMeta` initializers; `cargo test -p runner-core --tests --examples` now passes.

## 5. Notes for Future Work
- Run the full workspace test suite to confirm broader coverage beyond runner-core.
