# Runner / Host Inventory

This document captures the current runtime surface inside the `greentic-runner`
workspace so we can verify that no capabilities disappear while unifying the
host. It enumerates every crate that participates in pack execution, the entry
points they expose, CLI / env knobs, and how packs, tenants, telemetry, and
secrets are wired today.

## Crate Overview

### `greentic-runner` (CLI + public API re-export)

- **Role** – wraps `greentic-runner-host` and exposes two binaries:
  - `greentic-runner` launches the host HTTP server.
  - `greentic-gen-bindings` inspects pack metadata/components and emits
    `bindings.generated.yaml`.
- **Library surface** – `src/lib.rs` re-exports the entire host API
  (`Activity`, `HostBuilder`, `RunnerHost`, `HostServer`, `RunnerConfig`,
  `HostConfig`, `TenantHandle`, etc.) so downstream crates can embed the host
  without depending on `greentic-runner-host` directly, and provides two stable
  helpers: `run_http_host` (CLI-equivalent server) and `start_embedded_host`
  (build a `RunnerHost` without wiring the HTTP server).
- **Features** – none. The CLI/bin + bindings generator always compile; all
  runtime behaviour is driven by the re-exported host APIs and environment.
- **CLI flags**
  - `--bindings <PATH>` (repeatable) – absolute or relative paths to tenant
    bindings files that `RunnerConfig::from_env` loads.
  - `--port <u16>` – overrides the HTTP port before calling
    `greentic_runner_host::run`.
- **Additional tooling** – `greentic-gen-bindings` accepts `--pack`, `--component`,
  `--out`, `--complete`, `--strict`, and `--pretty` to control pack inspection.

### `greentic-runner-host` (canonical runtime shim)

- **Role** – owns tenant bindings, pack loading, Wasmtime runtime glue, ingress
  adapters, admin/health HTTP server, telemetry bootstrapping, and session/state
  backends.
- **Top-level API**
  - `RunnerConfig` encapsulates CLI/env derived settings (`bindings`, pack
    watcher `PackConfig`, HTTP port, `RoutingConfig`, admin auth, telemetry,
    secrets backend, `RunnerWasiPolicy`). `RunnerConfig::from_env` reads
    `PACK_*`, `PORT`, `PACK_REFRESH_INTERVAL`, `TENANT_RESOLVER`,
    `DEFAULT_TENANT`, `ADMIN_TOKEN`, and `SECRETS_BACKEND`.
  - `run(cfg)` orchestrates host creation, telemetry/secrets boot, pack watcher,
    HTTP server startup, and Ctrl+C shutdown.
  - `HostBuilder`/`RunnerHost` allow programmatic embedding: load tenant configs,
    start/stop, `handle_activity`, and inspect `TenantHandle`s.
  - Public types such as `Activity`, `ActivityKind`, `HostConfig`, `HostServer`,
    `PreopenSpec`, and `RunnerWasiPolicy` are re-exported for downstream use.
- **Features** – `verify` (default) enforces pack file presence and optional
  signatures/digests, `telemetry` wires OTLP exporters via
  `greentic-telemetry`. (The legacy `mcp` bridge feature has been removed;
  MCP components are expected to be pre-composed and invoked via `component.exec`.)
- **Tenant bindings (`HostConfig::load_from_path`)**
  - Each YAML file declares `tenant`, `flow_type_bindings` (map of flow kinds to
    adapter ID + adapter config + allowed secret names), optional `rate_limits`,
    `timers`, `retry` policy, and optionally an `oauth` block. (No `mcp` block
    in the new model.)
  - The optional `oauth` block enables the Greentic OAuth broker integration:
    ```yaml
    oauth:
      http_base_url: https://oauth.api.greentic.net/
      nats_url: nats://oauth-broker:4222
      provider: greentic.oauth.default
      env: prod          # optional, falls back to GREENTIC_ENV/local
      team: platform     # optional logical team hint
    ```
    When present, the runner initialises the SDK client once per tenant and
    exposes the `greentic:oauth-broker@1.0.0/world broker` WIT world to
    components that import it.
  - `SecretsPolicy` and `WebhookPolicy` are derived from bindings to enforce
    outbound secret usage and web-hook path allow/deny rules.
- **Pack ingestion / watcher**
  - `watcher::start_pack_watcher` builds a `runner_core::PackManager`, loads the
    JSON index from `PACK_INDEX_URL`, resolves the main pack plus overlays per
    tenant, and spawns a hot-reload loop using `PACK_REFRESH_INTERVAL`.
  - Reloads call `PackRuntime::load` for each pack (with the tenant’s
    `HostConfig`, optional archive metadata, and shared session/state stores).
  - Manual reloads hit `/admin/packs/reload` and use `PackReloadHandle`.
- **Runtime wiring**
  - Active packs live in `ActivePacks` (Arc-swap). Each tenant has a
    `TenantRuntime` bundling `FlowEngine`, Wasmtime objects, session/state
    hosts (`greentic-session`, `greentic-state`), and optional `MockLayer`.
  - `RunnerHost::handle_activity` accepts `Activity` structs, normalises payloads
    into the canonical `IngressEnvelope` (tenant/provider/session metadata) and
    jumps into the flow state machine. `TenantCtx` values from
    `greentic-types` are threaded through HTTP handlers and host interfaces.
  - `HostServer::serve` (Axum) exposes ingress routes for adapters (Telegram,
    Teams/Bot Framework, Slack Events + Interactive, WebChat/DirectLine, Webex,
    WhatsApp Cloud, generic webhook, timers) plus `/healthz` and `/admin`
    endpoints protected by `AdminAuth`.
    When a tenant config includes the `oauth` block, the Wasmtime linker also
    registers the `greentic:oauth-broker@1.0.0` world, allowing deployment or
    channel components to request consent URLs/tokens directly from the host.
  - HTTP routing is governed by `RoutingConfig::from_env`, supporting host,
    header, JWT, or fixed-tenant resolution. Admin requests require
    `ADMIN_TOKEN` unless originating from loopback.
- **Ingress & actions**
  - `ingress.rs` converts HTTP payloads into `Activity`/`IngressEnvelope`s with
    deterministic session keys (`{tenant}:{provider}:{conversation}:{user}`).
  - `runner::adapt_*` modules implement provider-specific features (Slack button
    payload parsing, WhatsApp verify/app-secret, Webhook path policies, timer
    cron wrappers). Provider secrets come either from env (`WHATSAPP_VERIFY_TOKEN`,
    `WHATSAPP_APP_SECRET`, `SLACK_SIGNING_SECRET`, etc.) or from the secrets
    host via adapters (`TELEGRAM_BOT_TOKEN`, `WEBEX_WEBHOOK_SECRET`, etc.).
- **Telemetry / secrets**
  - `boot::init` wires OTLP exporters (if `telemetry` feature is enabled) and
    `greentic_secrets::init` initialises the requested `SecretsBackend`.
  - Host exposes `TelemetryCfg`/`with_telemetry` so embedders can supply OTLP
    endpoints or sampling rules.
- **Admin + health**
  - `/healthz` returns watcher status, telemetry init, and secrets backend state.
  - `/admin/packs/status` lists tenants, pack versions/digests, and last reload
    timestamp/errors. `/admin/packs/reload` triggers a reload (subject to admin
    auth). Both live behind the `AdminGuard`.

### `runner-core`

- **Role** – shared pack ingestion utilities used by the host, including pack
  index parsing, remote fetching, signature/digest verification, and caching.
- **Modules**
  - `env` defines `PackConfig`, `PackSource`, and `IndexLocation`. The host reads
    `PACK_SOURCE`, `PACK_INDEX_URL`, `PACK_CACHE_DIR`, and `PACK_PUBLIC_KEY`
    through this API.
  - `packs` implements the resolver stack:
    - `PackManager` orchestrates the resolver registry (filesystem, HTTPS, OCI,
      S3, GCS, Azure blob), downloads artifacts, writes them into the cache, and
      invokes `PackVerifier` when a public key or `PACK_VERIFY_STRICT` is set.
    - `Index`/`PackEntry`/`TenantPacks` mirror the JSON index schema that maps
      tenants to `main_pack` + overlay list.
    - `ResolvedSet` is what the watcher converts into ready-to-load packs.

### `greentic-runner-desktop`

- **Role** – developer harness that embeds `greentic-runner-host` for local,
  single-tenant execution with mocks, transcripts, and artifact capture.
- **Surface**
  - Provides `RunOptions`, `Profile::Dev`, `TenantContext`, transcript hooks,
    OTLP hooks, and a `MocksConfig` that reuses the host’s mock layer
    (HTTP/time/telemetry/tools).
  - Loads `.gtpack` files via `PackRuntime::load` and drives the same
    `FlowEngine`, but runs inside a `tokio::Runtime` owned by the library.
  - Emits transcripts (JSON) per node via `ExecutionObserver` callbacks,
    supports redaction, attaches metadata (duration, CPU, mem), and can export
    artifacts (e.g., generated WASI files, IaC outputs) to disk.
  - Useful knobs: per-node/per-run wallclock limits, tenant/team/user overrides,
    manual entry-flow selection, `MocksConfig` toggles for HTTP/telemetry/time,
    and optional OTLP streaming.

### `greentic-secrets`

- **Role** – very small helper crate that currently logs/validates secrets
  backend choices (`Env`, `Aws`, `Gcp`, `Azure`) and exposes `init` +
  `SecretsBackend::from_env`.
- **Usage** – `RunnerConfig::from_env` selects a backend based on the
  `SECRETS_BACKEND` env var and `run()` always calls `greentic_secrets::init`
  before starting the host. Swapping in a real provider simply requires
  extending this crate.

## CLI Flags (current binaries)

| Binary | Flags |
| --- | --- |
| `greentic-runner` | `--bindings <PATH>` (repeat per tenant bindings file), `--port <u16>` |
| `greentic-gen-bindings` | `--pack <DIR>`, `--component <FILE>`, `--out <FILE>`, `--complete`, `--strict`, `--pretty` |

All other runtime behaviour is controlled via env vars read by `RunnerConfig`,
`RoutingConfig`, `AdminAuth`, adapters, or the watcher.

## Environment Variables & Host Wiring

| Variable | Consumed by | Purpose |
| --- | --- | --- |
| `PACK_SOURCE` | `runner_core::env::PackConfig` | Default resolver scheme when pack locators omit a protocol (`fs`, `http`, `oci`, `s3`, `gcs`, `azblob`). |
| `PACK_INDEX_URL` | `PackConfig` | Points to the tenant index JSON (filesystem path, `file://`, or `http(s)://`). Required. |
| `PACK_CACHE_DIR` | `PackConfig` | Directory for cached pack artifacts (`.packs` default). |
| `PACK_PUBLIC_KEY` | `PackConfig` / `verify` | Enables strict signature verification; combined with `PACK_VERIFY_STRICT`. |
| `PACK_VERIFY_STRICT` | `verify` feature | Forces `SigningPolicy::Strict` even without a public key, or relaxes enforcement when false. |
| `PACK_REFRESH_INTERVAL` | `RunnerConfig::from_env` | Duration string for the hot-reload ticker (default `30s`). |
| `PORT` | `RunnerConfig::from_env` | Optional override for the HTTP server port (defaults to 8080; also overridden by CLI `--port`). |
| `TENANT_RESOLVER` | `RoutingConfig::from_env` | Chooses tenant routing strategy: `env`, `host`, `header`, or `jwt`. |
| `DEFAULT_TENANT` | `RoutingConfig::from_env` | Fallback tenant when routing heuristics fail (`demo`). |
| `ADMIN_TOKEN` | `AdminAuth::from_env` | Required bearer token for `/admin/*` when set; otherwise admin access is loopback-only. |
| `SECRETS_BACKEND` | `RunnerConfig::from_env` / `greentic-secrets` | Selects `Env`, `Aws`, `Gcp`, or `Azure` placeholder. |
| `GREENTIC_ENV` | `RunnerHost::handle_activity` and `FlowEngine` defaults | Marks the logical deployment environment inserted into `TenantCtx`. Defaults to `local`. |
| `OTEL_*` (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_RESOURCE_ATTRIBUTES`, etc.) | `telemetry` feature | Standard OTLP exporter configuration for spans/metrics. |
| Provider-specific secrets (`SLACK_SIGNING_SECRET`, `WEBEX_WEBHOOK_SECRET`, `WHATSAPP_VERIFY_TOKEN`, `WHATSAPP_APP_SECRET`, `TELEGRAM_BOT_TOKEN`, etc.) | Adapter modules | Enable signature verification, API credentials, and message sending for the corresponding adapters. |

Bindings files also list secret IDs under each flow binding. Those IDs must be
whitelisted in `secrets` arrays and match what components call via the host
interfaces. The host enforces this via `SecretsPolicy`.

## Pack Loading & Tenant Context Recap

- **Pack discovery** – `runner_core::PackManager::resolve_all_for_index` loads
  the tenant JSON index, fetches each pack via the registered resolver, enforces
  digests/signatures, and hands the resolved set to the watcher.
- **Tenant runtimes** – `PackRuntime::load` reads pack metadata (flows,
  components, bindings) and instantiates Wasmtime modules with the host
  interfaces (secrets, state, session, messaging, events, HTTP, telemetry, etc.).
  Overlays are applied in order so later overlays can replace flows/components.
- **TenantCtx / session wiring** – HTTP ingress builds `IngressEnvelope`s that
  include `tenant`, optional `env` (`GREENTIC_ENV`), provider/channel/user IDs,
  and `session_hint`. The flow state machine creates `TenantCtx` structures for
  each call into the Wasm host interfaces, so per-tenant policies, session stores,
  and telemetry attributes are consistent across adapters.
- **State & session stores** – `HostBuilder::build` always creates shared
  `greentic-session` and `greentic-state` stores (with in-memory defaults today,
  swap-able later). The state machine persists `FlowSnapshot`s under the
  canonical session key to implement pause/resume.

With this inventory a reviewer can diff any subsequent refactor (e.g., folding
the “new runner” path into the canonical host) against the list of crates,
entrypoints, env knobs, adapters, and pack wiring described above.
