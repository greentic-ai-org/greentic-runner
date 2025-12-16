use anyhow::Result;
#[cfg(feature = "telemetry")]
use anyhow::anyhow;

use crate::TelemetryCfg;
use crate::http::health::HealthState;

#[cfg(feature = "telemetry")]
use greentic_telemetry::{TelemetryConfig, init_telemetry};

#[cfg(feature = "telemetry")]
use tracing::info;

pub fn init(health: &HealthState, otlp_cfg: Option<&TelemetryCfg>) -> Result<()> {
    configure_telemetry(otlp_cfg)?;
    health.mark_telemetry_ready();
    health.mark_secrets_ready();
    Ok(())
}

#[cfg(feature = "telemetry")]
fn configure_telemetry(config: Option<&crate::TelemetryCfg>) -> Result<()> {
    apply_preset_from_env();
    let cfg = config.cloned().unwrap_or_else(|| TelemetryConfig {
        service_name: std::env::var("OTEL_SERVICE_NAME")
            .unwrap_or_else(|_| "greentic-runner-host".into()),
    });
    info!(
        service = cfg.service_name,
        "initialising telemetry pipeline"
    );
    init_telemetry(cfg).map_err(|err| anyhow!(err.to_string()))
}

#[cfg(not(feature = "telemetry"))]
fn configure_telemetry(_config: Option<&TelemetryCfg>) -> Result<()> {
    Ok(())
}

#[cfg(feature = "telemetry")]
fn apply_preset_from_env() {
    if let Ok(preset) = std::env::var("CLOUD_PRESET") {
        info!(preset = %preset, "telemetry preset requested");
    }
}
