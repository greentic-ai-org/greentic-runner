#![deny(unsafe_code)]
//! Canonical Greentic host runtime.
//!
//! This crate owns tenant bindings, pack ingestion/watchers, ingress adapters,
//! Wasmtime glue, session/state storage, and the HTTP server used by the
//! `greentic-runner` CLI. Downstream crates embed it either through
//! [`RunnerConfig`] + [`run`] (HTTP host) or [`HostBuilder`] (direct API access).

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::secrets::SecretsBackend;
use anyhow::{Context, Result, anyhow};
use greentic_config::ResolvedConfig;
#[cfg(feature = "telemetry")]
use greentic_config_types::TelemetryExporterKind;
use greentic_config_types::{
    NetworkConfig, PackSourceConfig, PacksConfig, PathsConfig, TelemetryConfig,
};
#[cfg(feature = "telemetry")]
use greentic_telemetry::export::{ExportConfig as TelemetryExportConfig, ExportMode, Sampling};
use runner_core::env::PackConfig;
use tokio::signal;

pub mod boot;
pub mod component_api;
pub mod config;
pub mod engine;
pub mod http;
pub mod ingress;
pub mod pack;
pub mod routing;
pub mod runner;
pub mod runtime;
pub mod runtime_wasmtime;
pub mod secrets;
pub mod storage;
pub mod telemetry;
pub mod verify;
pub mod wasi;
pub mod watcher;

mod activity;
mod host;
pub mod oauth;

pub use activity::{Activity, ActivityKind};
pub use config::HostConfig;
pub use host::TelemetryCfg;
pub use host::{HostBuilder, RunnerHost, TenantHandle};
pub use wasi::{PreopenSpec, RunnerWasiPolicy};

pub use greentic_types::{EnvId, FlowId, PackId, TenantCtx, TenantId};

pub use http::auth::AdminAuth;
pub use routing::RoutingConfig;
use routing::TenantRouting;
pub use runner::HostServer;

/// User-facing configuration for running the unified host.
#[derive(Clone)]
pub struct RunnerConfig {
    pub bindings: Vec<PathBuf>,
    pub pack: PackConfig,
    pub port: u16,
    pub refresh_interval: Duration,
    pub routing: RoutingConfig,
    pub admin: AdminAuth,
    pub telemetry: Option<TelemetryCfg>,
    pub secrets_backend: SecretsBackend,
    pub wasi_policy: RunnerWasiPolicy,
    pub resolved_config: ResolvedConfig,
}

impl RunnerConfig {
    /// Build a [`RunnerConfig`] from a resolved greentic-config and the provided binding files.
    pub fn from_config(resolved_config: ResolvedConfig, bindings: Vec<PathBuf>) -> Result<Self> {
        if bindings.is_empty() {
            anyhow::bail!("at least one bindings file is required");
        }
        let pack = pack_config_from(
            &resolved_config.config.packs,
            &resolved_config.config.paths,
            &resolved_config.config.network,
        )?;
        let refresh = parse_refresh_interval(std::env::var("PACK_REFRESH_INTERVAL").ok())?;
        let port = std::env::var("PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8080);
        let default_tenant = resolved_config
            .config
            .dev
            .as_ref()
            .map(|dev| dev.default_tenant.clone())
            .unwrap_or_else(|| "demo".into());
        let routing = RoutingConfig::from_env_with_default(default_tenant);
        let paths = &resolved_config.config.paths;
        ensure_paths_exist(paths)?;
        let wasi_policy = default_wasi_policy(paths);

        let admin = AdminAuth::new(resolved_config.config.services.as_ref().and_then(|s| {
            s.events
                .as_ref()
                .and_then(|svc| svc.headers.as_ref())
                .and_then(|headers| headers.get("x-admin-token").cloned())
        }));
        let secrets_backend = SecretsBackend::from_config(&resolved_config.config.secrets)?;
        Ok(Self {
            bindings,
            pack,
            port,
            refresh_interval: refresh,
            routing,
            admin,
            telemetry: telemetry_from(&resolved_config.config.telemetry),
            secrets_backend,
            wasi_policy,
            resolved_config,
        })
    }

    /// Override the HTTP port used by the host server.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_wasi_policy(mut self, policy: RunnerWasiPolicy) -> Self {
        self.wasi_policy = policy;
        self
    }
}

fn parse_refresh_interval(value: Option<String>) -> Result<Duration> {
    let raw = value.unwrap_or_else(|| "30s".into());
    humantime::parse_duration(&raw).map_err(|err| anyhow!("invalid PACK_REFRESH_INTERVAL: {err}"))
}

fn default_wasi_policy(paths: &PathsConfig) -> RunnerWasiPolicy {
    let mut policy = RunnerWasiPolicy::default()
        .with_env("GREENTIC_ROOT", paths.greentic_root.display().to_string())
        .with_env("GREENTIC_STATE_DIR", paths.state_dir.display().to_string())
        .with_env("GREENTIC_CACHE_DIR", paths.cache_dir.display().to_string())
        .with_env("GREENTIC_LOGS_DIR", paths.logs_dir.display().to_string());
    policy = policy
        .with_preopen(PreopenSpec::new(&paths.state_dir, "/state"))
        .with_preopen(PreopenSpec::new(&paths.cache_dir, "/cache"))
        .with_preopen(PreopenSpec::new(&paths.logs_dir, "/logs"));
    policy
}

fn ensure_paths_exist(paths: &PathsConfig) -> Result<()> {
    for dir in [
        &paths.greentic_root,
        &paths.state_dir,
        &paths.cache_dir,
        &paths.logs_dir,
    ] {
        fs::create_dir_all(dir)
            .with_context(|| format!("failed to ensure directory {}", dir.display()))?;
    }
    Ok(())
}

fn pack_config_from(
    packs: &Option<PacksConfig>,
    paths: &PathsConfig,
    network: &NetworkConfig,
) -> Result<PackConfig> {
    if let Some(cfg) = packs {
        let cache_dir = cfg.cache_dir.clone();
        let index_location = match &cfg.source {
            PackSourceConfig::LocalIndex { path } => {
                runner_core::env::IndexLocation::File(path.clone())
            }
            PackSourceConfig::HttpIndex { url } => {
                runner_core::env::IndexLocation::from_value(url)?
            }
            PackSourceConfig::OciRegistry { reference } => {
                runner_core::env::IndexLocation::from_value(reference)?
            }
        };
        let public_key = cfg
            .trust
            .as_ref()
            .and_then(|trust| trust.public_keys.first().cloned());
        return Ok(PackConfig {
            source: runner_core::env::PackSource::Fs,
            index_location,
            cache_dir,
            public_key,
            network: Some(network.clone()),
        });
    }
    let mut cfg = PackConfig::default_for_paths(paths)?;
    cfg.network = Some(network.clone());
    Ok(cfg)
}

#[cfg(feature = "telemetry")]
fn telemetry_from(cfg: &TelemetryConfig) -> Option<TelemetryCfg> {
    if !cfg.enabled || matches!(cfg.exporter, TelemetryExporterKind::None) {
        return None;
    }
    let mut export = TelemetryExportConfig::json_default();
    export.mode = match cfg.exporter {
        TelemetryExporterKind::Otlp => ExportMode::OtlpGrpc,
        TelemetryExporterKind::Stdout => ExportMode::JsonStdout,
        TelemetryExporterKind::None => return None,
    };
    export.endpoint = cfg.endpoint.clone();
    export.sampling = Sampling::TraceIdRatio(cfg.sampling as f64);
    Some(TelemetryCfg {
        config: greentic_telemetry::TelemetryConfig {
            service_name: "greentic-runner".into(),
        },
        export,
    })
}

#[cfg(not(feature = "telemetry"))]
fn telemetry_from(_cfg: &TelemetryConfig) -> Option<TelemetryCfg> {
    None
}

/// Run the unified Greentic runner host until shutdown.
pub async fn run(cfg: RunnerConfig) -> Result<()> {
    let RunnerConfig {
        bindings,
        pack,
        port,
        refresh_interval,
        routing,
        admin,
        telemetry,
        secrets_backend,
        wasi_policy,
        resolved_config: _resolved_config,
    } = cfg;
    #[cfg(not(feature = "telemetry"))]
    let _ = telemetry;

    let mut builder = HostBuilder::new();
    for path in &bindings {
        let host_config = HostConfig::load_from_path(path)
            .with_context(|| format!("failed to load host bindings {}", path.display()))?;
        builder = builder.with_config(host_config);
    }
    #[cfg(feature = "telemetry")]
    if let Some(telemetry) = telemetry.clone() {
        builder = builder.with_telemetry(telemetry);
    }
    builder = builder
        .with_wasi_policy(wasi_policy.clone())
        .with_secrets_manager(
            secrets_backend
                .build_manager()
                .context("failed to initialise secrets backend")?,
        );

    let host = Arc::new(builder.build()?);
    host.start().await?;

    let (watcher, reload_handle) =
        watcher::start_pack_watcher(Arc::clone(&host), pack.clone(), refresh_interval).await?;

    let routing = TenantRouting::new(routing.clone());
    let server = HostServer::new(
        port,
        host.active_packs(),
        routing,
        host.health_state(),
        Some(reload_handle),
        admin.clone(),
    )?;

    tokio::select! {
        result = server.serve() => {
            result?;
        }
        _ = signal::ctrl_c() => {
            tracing::info!("received shutdown signal");
        }
    }

    drop(watcher);
    host.stop().await?;
    Ok(())
}
