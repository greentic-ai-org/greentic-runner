#![deny(unsafe_code)]
//! Canonical Greentic host runtime.
//!
//! This crate owns tenant bindings, pack ingestion/watchers, ingress adapters,
//! Wasmtime glue, session/state storage, and the HTTP server used by the
//! `greentic-runner` CLI. Downstream crates embed it either through
//! [`RunnerConfig`] + [`run`] (HTTP host) or [`HostBuilder`] (direct API access).

use std::collections::{HashMap, HashSet};
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
use serde_json::json;
use tokio::signal;

pub mod boot;
pub mod cache;
pub mod component_api;
pub mod config;
pub mod engine;
pub mod fault;
pub mod gtbind;
pub mod http;
pub mod ingress;
pub mod pack;
pub mod provider;
pub mod provider_core;
pub mod provider_core_only;
pub mod routing;
pub mod runner;
pub mod runtime;
pub mod runtime_wasmtime;
pub mod secrets;
pub mod storage;
pub mod telemetry;
pub mod trace;
pub mod validate;
pub mod verify;
pub mod wasi;
pub mod watcher;

mod activity;
mod host;
pub mod oauth;

pub use activity::{Activity, ActivityKind};
pub use config::HostConfig;
pub use gtbind::{PackBinding, TenantBindings};
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
    pub tenant_bindings: HashMap<String, TenantBindings>,
    pub pack: PackConfig,
    pub port: u16,
    pub refresh_interval: Duration,
    pub routing: RoutingConfig,
    pub admin: AdminAuth,
    pub telemetry: Option<TelemetryCfg>,
    pub secrets_backend: SecretsBackend,
    pub wasi_policy: RunnerWasiPolicy,
    pub resolved_config: ResolvedConfig,
    pub trace: trace::TraceConfig,
    pub validation: validate::ValidationConfig,
}

impl RunnerConfig {
    /// Build a [`RunnerConfig`] from a resolved greentic-config and the provided binding files.
    pub fn from_config(resolved_config: ResolvedConfig, bindings: Vec<PathBuf>) -> Result<Self> {
        if bindings.is_empty() {
            anyhow::bail!("at least one gtbind file is required");
        }
        let tenant_bindings = gtbind::load_gtbinds(&bindings)?;
        if tenant_bindings.is_empty() {
            anyhow::bail!("no gtbind files loaded");
        }
        let mut pack = pack_config_from(
            &resolved_config.config.packs,
            &resolved_config.config.paths,
            &resolved_config.config.network,
        )?;
        maybe_write_gtbind_index(&tenant_bindings, &resolved_config.config.paths, &mut pack)?;
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
        let mut wasi_policy = default_wasi_policy(paths);
        let mut env_allow = HashSet::new();
        for binding in tenant_bindings.values() {
            env_allow.extend(binding.env_passthrough.iter().cloned());
        }
        for key in env_allow {
            wasi_policy = wasi_policy.allow_env(key);
        }

        let admin = AdminAuth::new(resolved_config.config.services.as_ref().and_then(|s| {
            s.events
                .as_ref()
                .and_then(|svc| svc.headers.as_ref())
                .and_then(|headers| headers.get("x-admin-token").cloned())
        }));
        let secrets_backend = SecretsBackend::from_config(&resolved_config.config.secrets)?;
        Ok(Self {
            tenant_bindings,
            pack,
            port,
            refresh_interval: refresh,
            routing,
            admin,
            telemetry: telemetry_from(&resolved_config.config.telemetry),
            secrets_backend,
            wasi_policy,
            resolved_config,
            trace: trace::TraceConfig::from_env(),
            validation: validate::ValidationConfig::from_env(),
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

fn maybe_write_gtbind_index(
    tenant_bindings: &HashMap<String, TenantBindings>,
    paths: &PathsConfig,
    pack: &mut PackConfig,
) -> Result<()> {
    let mut uses_locators = false;
    for binding in tenant_bindings.values() {
        for pack_binding in &binding.packs {
            if pack_binding.pack_locator.is_some() {
                uses_locators = true;
            }
        }
    }
    if !uses_locators {
        return Ok(());
    }

    let mut entries = serde_json::Map::new();
    for binding in tenant_bindings.values() {
        let mut packs = Vec::new();
        for pack_binding in &binding.packs {
            let locator = pack_binding.pack_locator.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "gtbind {} missing pack_locator for pack {}",
                    binding.tenant,
                    pack_binding.pack_id
                )
            })?;
            let (name, version_or_digest) =
                pack_binding.pack_ref.split_once('@').ok_or_else(|| {
                    anyhow::anyhow!(
                        "gtbind {} invalid pack_ref {} (expected name@version)",
                        binding.tenant,
                        pack_binding.pack_ref
                    )
                })?;
            if name != pack_binding.pack_id {
                anyhow::bail!(
                    "gtbind {} pack_ref {} does not match pack_id {}",
                    binding.tenant,
                    pack_binding.pack_ref,
                    pack_binding.pack_id
                );
            }
            let mut entry = serde_json::Map::new();
            entry.insert("name".to_string(), json!(name));
            if version_or_digest.contains(':') {
                entry.insert("digest".to_string(), json!(version_or_digest));
            } else {
                entry.insert("version".to_string(), json!(version_or_digest));
            }
            entry.insert("locator".to_string(), json!(locator));
            packs.push(serde_json::Value::Object(entry));
        }
        let main_pack = packs
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("gtbind {} has no packs", binding.tenant))?;
        let overlays = packs.into_iter().skip(1).collect::<Vec<_>>();
        entries.insert(
            binding.tenant.clone(),
            json!({
                "main_pack": main_pack,
                "overlays": overlays,
            }),
        );
    }

    let index_path = paths.greentic_root.join("packs").join("gtbind.index.json");
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let serialized = serde_json::to_vec_pretty(&serde_json::Value::Object(entries))?;
    fs::write(&index_path, serialized)
        .with_context(|| format!("failed to write {}", index_path.display()))?;
    pack.index_location = runner_core::env::IndexLocation::File(index_path);
    Ok(())
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
        tenant_bindings,
        pack,
        port,
        refresh_interval,
        routing,
        admin,
        telemetry,
        secrets_backend,
        wasi_policy,
        resolved_config: _resolved_config,
        trace,
        validation,
    } = cfg;
    #[cfg(not(feature = "telemetry"))]
    let _ = telemetry;

    let mut builder = HostBuilder::new();
    for bindings in tenant_bindings.into_values() {
        let mut host_config = HostConfig::from_gtbind(bindings);
        host_config.trace = trace.clone();
        host_config.validation = validation.clone();
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
