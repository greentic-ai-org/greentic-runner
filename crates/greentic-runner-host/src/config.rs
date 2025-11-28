use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(feature = "mcp")]
use std::time::Duration;

use crate::oauth::OAuthBrokerConfig;
use anyhow::{Context, Result};
#[cfg(feature = "mcp")]
use greentic_mcp::{ExecConfig, RuntimePolicy, ToolStore, VerifyPolicy};
use serde::Deserialize;
use serde_yaml_bw as serde_yaml;
use std::env;

#[derive(Debug, Clone)]
pub struct HostConfig {
    pub tenant: String,
    pub bindings_path: PathBuf,
    pub flow_type_bindings: HashMap<String, FlowBinding>,
    pub mcp: McpConfig,
    pub rate_limits: RateLimits,
    pub http_enabled: bool,
    pub secrets_policy: SecretsPolicy,
    pub webhook_policy: WebhookPolicy,
    pub timers: Vec<TimerBinding>,
    pub oauth: Option<OAuthConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BindingsFile {
    pub tenant: String,
    #[serde(default)]
    pub flow_type_bindings: HashMap<String, FlowBinding>,
    pub mcp: McpConfig,
    #[serde(default)]
    pub rate_limits: RateLimits,
    #[serde(default)]
    pub timers: Vec<TimerBinding>,
    #[serde(default)]
    pub oauth: Option<OAuthConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FlowBinding {
    pub adapter: String,
    #[serde(default)]
    pub config: serde_yaml::Value,
    #[serde(default)]
    pub secrets: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpConfig {
    pub store: serde_yaml::Value,
    #[serde(default)]
    pub security: serde_yaml::Value,
    #[serde(default)]
    pub runtime: serde_yaml::Value,
    #[serde(default)]
    pub http_enabled: Option<bool>,
    #[serde(default)]
    pub retry: Option<McpRetryConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RateLimits {
    #[serde(default = "default_messaging_qps")]
    pub messaging_send_qps: u32,
    #[serde(default = "default_messaging_burst")]
    pub messaging_burst: u32,
}

#[derive(Debug, Clone)]
pub struct SecretsPolicy {
    allowed: HashSet<String>,
    allow_all: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct McpRetryConfig {
    #[serde(default = "default_mcp_retry_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_mcp_retry_base_delay_ms")]
    pub base_delay_ms: u64,
}

#[derive(Debug, Clone, Default)]
pub struct WebhookPolicy {
    allow_paths: Vec<String>,
    deny_paths: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebhookBindingConfig {
    #[serde(default)]
    pub allow_paths: Vec<String>,
    #[serde(default)]
    pub deny_paths: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimerBinding {
    pub flow_id: String,
    pub cron: String,
    #[serde(default)]
    pub schedule_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OAuthConfig {
    pub http_base_url: String,
    pub nats_url: String,
    pub provider: String,
    #[serde(default)]
    pub env: Option<String>,
    #[serde(default)]
    pub team: Option<String>,
}

impl HostConfig {
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("failed to read bindings file {path:?}"))?;
        let bindings: BindingsFile = serde_yaml::from_str(&content)
            .with_context(|| format!("failed to parse bindings file {path:?}"))?;

        let secrets_policy = SecretsPolicy::from_bindings(&bindings);
        let http_enabled = bindings
            .mcp
            .http_enabled
            .unwrap_or(bindings.flow_type_bindings.contains_key("messaging"));
        let webhook_policy = bindings
            .flow_type_bindings
            .get("webhook")
            .and_then(|binding| {
                serde_yaml::from_value::<WebhookBindingConfig>(binding.config.clone())
                    .map(WebhookPolicy::from)
                    .map_err(|err| {
                        tracing::warn!(error = %err, "failed to parse webhook binding config");
                        err
                    })
                    .ok()
            })
            .unwrap_or_default();

        Ok(Self {
            tenant: bindings.tenant.clone(),
            bindings_path: path.to_path_buf(),
            flow_type_bindings: bindings.flow_type_bindings.clone(),
            mcp: bindings.mcp.clone(),
            rate_limits: bindings.rate_limits.clone(),
            http_enabled,
            secrets_policy,
            webhook_policy,
            timers: bindings.timers.clone(),
            oauth: bindings.oauth.clone(),
        })
    }

    pub fn messaging_binding(&self) -> Option<&FlowBinding> {
        self.flow_type_bindings.get("messaging")
    }

    pub fn mcp_retry_config(&self) -> McpRetryConfig {
        self.mcp.retry.clone().unwrap_or_default()
    }

    #[cfg(feature = "mcp")]
    pub fn mcp_exec_config(&self) -> Result<ExecConfig> {
        self.mcp
            .to_exec_config(self.bindings_path.parent())
            .context("failed to build MCP exec configuration")
    }

    pub fn oauth_broker_config(&self) -> Option<OAuthBrokerConfig> {
        let oauth = self.oauth.as_ref()?;
        let mut cfg = OAuthBrokerConfig::new(&oauth.http_base_url, &oauth.nats_url);
        if !oauth.provider.is_empty() {
            cfg.default_provider = Some(oauth.provider.clone());
        }
        if let Some(team) = &oauth.team
            && !team.is_empty()
        {
            cfg.team = Some(team.clone());
        }
        Some(cfg)
    }
}

impl SecretsPolicy {
    fn from_bindings(bindings: &BindingsFile) -> Self {
        let allowed = bindings
            .flow_type_bindings
            .values()
            .flat_map(|binding| binding.secrets.iter().cloned())
            .collect::<HashSet<_>>();
        Self {
            allowed,
            allow_all: false,
        }
    }

    pub fn is_allowed(&self, key: &str) -> bool {
        self.allow_all || self.allowed.contains(key)
    }

    pub fn allow_all() -> Self {
        Self {
            allowed: HashSet::new(),
            allow_all: true,
        }
    }

    pub fn from_allowed<I, S>(iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            allowed: iter.into_iter().map(Into::into).collect(),
            allow_all: false,
        }
    }
}

impl Default for RateLimits {
    fn default() -> Self {
        Self {
            messaging_send_qps: default_messaging_qps(),
            messaging_burst: default_messaging_burst(),
        }
    }
}

fn default_messaging_qps() -> u32 {
    10
}

fn default_messaging_burst() -> u32 {
    20
}

impl From<WebhookBindingConfig> for WebhookPolicy {
    fn from(value: WebhookBindingConfig) -> Self {
        Self {
            allow_paths: value.allow_paths,
            deny_paths: value.deny_paths,
        }
    }
}

impl WebhookPolicy {
    pub fn is_allowed(&self, path: &str) -> bool {
        if self
            .deny_paths
            .iter()
            .any(|prefix| path.starts_with(prefix))
        {
            return false;
        }

        if self.allow_paths.is_empty() {
            return true;
        }

        self.allow_paths
            .iter()
            .any(|prefix| path.starts_with(prefix))
    }
}

impl TimerBinding {
    pub fn schedule_id(&self) -> &str {
        self.schedule_id.as_deref().unwrap_or(self.flow_id.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn host_config_with_oauth(oauth: Option<OAuthConfig>) -> HostConfig {
        HostConfig {
            tenant: "tenant-a".to_string(),
            bindings_path: PathBuf::from("/tmp/bindings.yaml"),
            flow_type_bindings: HashMap::new(),
            mcp: McpConfig {
                store: Value::Null(None),
                security: Value::Null(None),
                runtime: Value::Null(None),
                http_enabled: Some(false),
                retry: None,
            },
            rate_limits: RateLimits::default(),
            http_enabled: false,
            secrets_policy: SecretsPolicy::allow_all(),
            webhook_policy: WebhookPolicy::default(),
            timers: Vec::new(),
            oauth,
        }
    }

    #[test]
    fn oauth_broker_config_absent_without_block() {
        let cfg = host_config_with_oauth(None);
        assert!(cfg.oauth_broker_config().is_none());
    }

    #[test]
    fn oauth_broker_config_maps_fields() {
        let cfg = host_config_with_oauth(Some(OAuthConfig {
            http_base_url: "https://oauth.example/".into(),
            nats_url: "nats://broker:4222".into(),
            provider: "demo".into(),
            env: None,
            team: Some("ops".into()),
        }));
        let broker = cfg.oauth_broker_config().expect("missing broker config");
        assert_eq!(broker.http_base_url, "https://oauth.example/");
        assert_eq!(broker.nats_url, "nats://broker:4222");
        assert_eq!(broker.default_provider.as_deref(), Some("demo"));
        assert_eq!(broker.team.as_deref(), Some("ops"));
    }
}

#[cfg(feature = "mcp")]
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum StoreBinding {
    #[serde(rename = "http-single")]
    HttpSingle {
        name: String,
        url: String,
        #[serde(default)]
        cache_dir: Option<String>,
    },
    #[serde(rename = "local-dir")]
    LocalDir { path: String },
}

#[cfg(feature = "mcp")]
#[derive(Debug, Default, Deserialize)]
struct RuntimeBinding {
    #[serde(default)]
    max_memory_mb: Option<u64>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    fuel: Option<u64>,
    #[serde(default)]
    per_call_timeout_ms: Option<u64>,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    base_backoff_ms: Option<u64>,
}

#[cfg(feature = "mcp")]
#[derive(Debug, Default, Deserialize)]
struct SecurityBinding {
    #[serde(default)]
    require_signature: bool,
    #[serde(default)]
    required_digests: HashMap<String, String>,
    #[serde(default)]
    trusted_signers: Vec<String>,
}

#[cfg(feature = "mcp")]
impl McpConfig {
    fn to_exec_config(&self, base_dir: Option<&Path>) -> Result<ExecConfig> {
        let store_cfg: StoreBinding = serde_yaml::from_value(self.store.clone())
            .context("invalid MCP store configuration")?;
        let runtime_cfg: RuntimeBinding =
            serde_yaml::from_value(self.runtime.clone()).unwrap_or_default();
        let security_cfg: SecurityBinding =
            serde_yaml::from_value(self.security.clone()).unwrap_or_default();

        let store = match store_cfg {
            StoreBinding::HttpSingle {
                name,
                url,
                cache_dir,
            } => ToolStore::HttpSingleFile {
                name,
                url,
                cache_dir: resolve_optional_path(base_dir, cache_dir)
                    .unwrap_or_else(|| default_cache_dir(base_dir)),
            },
            StoreBinding::LocalDir { path } => {
                ToolStore::LocalDir(resolve_required_path(base_dir, path))
            }
        };

        let runtime = RuntimePolicy {
            fuel: runtime_cfg.fuel,
            max_memory: runtime_cfg.max_memory_mb.map(|mb| mb * 1024 * 1024),
            wallclock_timeout: Duration::from_millis(runtime_cfg.timeout_ms.unwrap_or(30_000)),
            per_call_timeout: Duration::from_millis(
                runtime_cfg.per_call_timeout_ms.unwrap_or(10_000),
            ),
            max_attempts: runtime_cfg.max_attempts.unwrap_or(1),
            base_backoff: Duration::from_millis(runtime_cfg.base_backoff_ms.unwrap_or(100)),
        };

        let security = VerifyPolicy {
            allow_unverified: !security_cfg.require_signature,
            required_digests: security_cfg.required_digests,
            trusted_signers: security_cfg.trusted_signers,
        };

        Ok(ExecConfig {
            store,
            security,
            runtime,
            http_enabled: self.http_enabled.unwrap_or(false),
        })
    }
}

impl Default for McpRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_mcp_retry_attempts(),
            base_delay_ms: default_mcp_retry_base_delay_ms(),
        }
    }
}

fn default_mcp_retry_attempts() -> u32 {
    3
}

fn default_mcp_retry_base_delay_ms() -> u64 {
    250
}

#[cfg(feature = "mcp")]
fn resolve_required_path(base: Option<&Path>, value: String) -> PathBuf {
    let candidate = PathBuf::from(&value);
    if candidate.is_absolute() {
        candidate
    } else if let Some(base) = base {
        base.join(candidate)
    } else {
        PathBuf::from(value)
    }
}

#[cfg(feature = "mcp")]
fn resolve_optional_path(base: Option<&Path>, value: Option<String>) -> Option<PathBuf> {
    value.map(|v| resolve_required_path(base, v))
}

#[cfg(feature = "mcp")]
fn default_cache_dir(base: Option<&Path>) -> PathBuf {
    if let Some(dir) = env::var_os("GREENTIC_CACHE_DIR") {
        PathBuf::from(dir)
    } else if let Some(base) = base {
        base.join(".greentic/tool-cache")
    } else {
        env::temp_dir().join("greentic-tool-cache")
    }
}
