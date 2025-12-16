#![forbid(unsafe_code)]
//! Canonical entrypoint for embedding the Greentic runner.
//!
//! This crate provides two supported integration paths:
//! - [`run_http_host`] mirrors the CLI and starts the HTTP server that exposes
//!   ingress adapters, admin endpoints, and the pack watcher.
//! - [`start_embedded_host`] constructs a [`RunnerHost`] without spinning up the
//!   HTTP server so callers can drive `handle_activity` manually (desktop/dev
//!   harnesses, tests, etc.).

use anyhow::Result;

pub use greentic_runner_host::{
    self as host, Activity, ActivityKind, HostBuilder, HostServer, RunnerConfig, RunnerHost,
    TenantHandle, config, http, pack, routing, runner, runtime, runtime_wasmtime, telemetry,
    verify, watcher,
};

pub mod desktop {
    pub use greentic_runner_desktop::*;
}

pub mod gen_bindings;

/// Launch the canonical HTTP host. This is equivalent to running the
/// `greentic-runner` binary with the provided [`RunnerConfig`].
pub async fn run_http_host(cfg: RunnerConfig) -> Result<()> {
    greentic_runner_host::run(cfg).await
}

/// Build and start a [`RunnerHost`] without wiring the HTTP ingress server.
/// Callers are responsible for loading packs via [`RunnerHost::load_pack`] and
/// invoking [`RunnerHost::handle_activity`] directly.
pub async fn start_embedded_host(builder: HostBuilder) -> Result<RunnerHost> {
    let host = builder.build()?;
    host.start().await?;
    Ok(host)
}
