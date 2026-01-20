use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::{ArtifactKey, CacheConfig, CacheManager, CpuPolicy, EngineProfile};
use crate::component_api::{
    self, node::ExecCtx as ComponentExecCtx, node::InvokeResult, node::NodeError,
};
use crate::oauth::{OAuthBrokerConfig, OAuthBrokerHost, OAuthHostContext};
use crate::provider::{ProviderBinding, ProviderRegistry};
use crate::provider_core::SchemaCorePre as ProviderComponentPre;
use crate::provider_core_only;
use crate::runtime_wasmtime::{Component, Engine, InstancePre, Linker, ResourceTable};
use anyhow::{Context, Result, anyhow, bail};
use greentic_distributor_client::dist::{DistClient, DistError, DistOptions};
use greentic_interfaces_wasmtime::host_helpers::v1::{
    self as host_v1, HostFns, add_all_v1_to_linker,
    runner_host_http::RunnerHostHttp,
    runner_host_kv::RunnerHostKv,
    secrets_store::{SecretsError, SecretsStoreHost},
    state_store::{
        OpAck as StateOpAck, StateKey as HostStateKey, StateStoreError as StateError,
        StateStoreHost, TenantCtx as StateTenantCtx,
    },
    telemetry_logger::{
        OpAck as TelemetryAck, SpanContext as TelemetrySpanContext,
        TelemetryLoggerError as TelemetryError, TelemetryLoggerHost,
        TenantCtx as TelemetryTenantCtx,
    },
};
use greentic_interfaces_wasmtime::http_client_client_v1_0::greentic::interfaces_types::types::Impersonation as ImpersonationV1_0;
use greentic_interfaces_wasmtime::http_client_client_v1_1::greentic::interfaces_types::types::Impersonation as ImpersonationV1_1;
use greentic_pack::builder as legacy_pack;
use greentic_types::flow::FlowHasher;
use greentic_types::{
    ArtifactLocationV1, ComponentId, ComponentManifest, ComponentSourceRef, ComponentSourcesV1,
    EXT_COMPONENT_SOURCES_V1, EnvId, ExtensionRef, Flow, FlowComponentRef, FlowId, FlowKind,
    FlowMetadata, InputMapping, Node, NodeId, OutputMapping, Routing, StateKey as StoreStateKey,
    TeamId, TelemetryHints, TenantCtx as TypesTenantCtx, TenantId, UserId, decode_pack_manifest,
    pack_manifest::ExtensionInline,
};
use host_v1::http_client::{
    HttpClientError, HttpClientErrorV1_1, HttpClientHost, HttpClientHostV1_1,
    Request as HttpRequest, RequestOptionsV1_1 as HttpRequestOptionsV1_1,
    RequestV1_1 as HttpRequestV1_1, Response as HttpResponse, ResponseV1_1 as HttpResponseV1_1,
    TenantCtx as HttpTenantCtx, TenantCtxV1_1 as HttpTenantCtxV1_1,
};
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use reqwest::blocking::Client as BlockingClient;
use runner_core::normalize_under_root;
use serde::{Deserialize, Serialize};
use serde_cbor;
use serde_json::{self, Value};
use sha2::Digest;
use tokio::fs;
use wasmparser::{Parser, Payload};
use wasmtime::StoreContextMut;
use zip::ZipArchive;

use crate::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use crate::runner::flow_adapter::{FlowIR, flow_doc_to_ir, flow_ir_to_flow};
use crate::runner::mocks::{HttpDecision, HttpMockRequest, HttpMockResponse, MockLayer};

use crate::config::HostConfig;
use crate::fault;
use crate::secrets::{DynSecretsManager, read_secret_blocking};
use crate::storage::state::STATE_PREFIX;
use crate::storage::{DynSessionStore, DynStateStore};
use crate::verify;
use crate::wasi::RunnerWasiPolicy;
use tracing::warn;
use wasmtime_wasi::p2::add_to_linker_sync as add_wasi_to_linker;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use greentic_flow::model::FlowDoc;

#[allow(dead_code)]
pub struct PackRuntime {
    /// Component artifact path (wasm file).
    path: PathBuf,
    /// Optional archive (.gtpack) used to load flows/manifests.
    archive_path: Option<PathBuf>,
    config: Arc<HostConfig>,
    engine: Engine,
    metadata: PackMetadata,
    manifest: Option<greentic_types::PackManifest>,
    legacy_manifest: Option<Box<legacy_pack::PackManifest>>,
    component_manifests: HashMap<String, ComponentManifest>,
    mocks: Option<Arc<MockLayer>>,
    flows: Option<PackFlows>,
    components: HashMap<String, PackComponent>,
    http_client: Arc<BlockingClient>,
    pre_cache: Mutex<HashMap<String, InstancePre<ComponentState>>>,
    session_store: Option<DynSessionStore>,
    state_store: Option<DynStateStore>,
    wasi_policy: Arc<RunnerWasiPolicy>,
    provider_registry: RwLock<Option<ProviderRegistry>>,
    secrets: DynSecretsManager,
    oauth_config: Option<OAuthBrokerConfig>,
    cache: CacheManager,
}

struct PackComponent {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    version: String,
    component: Arc<Component>,
}

#[derive(Debug, Default, Clone)]
pub struct ComponentResolution {
    /// Root of a materialized pack directory containing `manifest.cbor` and `components/`.
    pub materialized_root: Option<PathBuf>,
    /// Explicit overrides mapping component id -> wasm path.
    pub overrides: HashMap<String, PathBuf>,
    /// If true, do not fetch remote components; require cached artifacts.
    pub dist_offline: bool,
    /// Optional cache directory for resolved remote components.
    pub dist_cache_dir: Option<PathBuf>,
    /// Allow bundled components without wasm_sha256 (dev-only escape hatch).
    pub allow_missing_hash: bool,
}

fn build_blocking_client() -> BlockingClient {
    std::thread::spawn(|| {
        BlockingClient::builder()
            .no_proxy()
            .build()
            .expect("blocking client")
    })
    .join()
    .expect("client build thread panicked")
}

fn normalize_pack_path(path: &Path) -> Result<(PathBuf, PathBuf)> {
    let (root, candidate) = if path.is_absolute() {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("pack path {} has no parent", path.display()))?;
        let root = parent
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", parent.display()))?;
        let file = path
            .file_name()
            .ok_or_else(|| anyhow!("pack path {} has no file name", path.display()))?;
        (root, PathBuf::from(file))
    } else {
        let cwd = std::env::current_dir().context("failed to resolve current directory")?;
        let base = if let Some(parent) = path.parent() {
            cwd.join(parent)
        } else {
            cwd
        };
        let root = base
            .canonicalize()
            .with_context(|| format!("failed to canonicalize {}", base.display()))?;
        let file = path
            .file_name()
            .ok_or_else(|| anyhow!("pack path {} has no file name", path.display()))?;
        (root, PathBuf::from(file))
    };
    let safe = normalize_under_root(&root, &candidate)?;
    Ok((root, safe))
}

static HTTP_CLIENT: Lazy<Arc<BlockingClient>> = Lazy::new(|| Arc::new(build_blocking_client()));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowDescriptor {
    pub id: String,
    #[serde(rename = "type")]
    pub flow_type: String,
    pub pack_id: String,
    pub profile: String,
    pub version: String,
    #[serde(default)]
    pub description: Option<String>,
}

pub struct HostState {
    config: Arc<HostConfig>,
    http_client: Arc<BlockingClient>,
    default_env: String,
    #[allow(dead_code)]
    session_store: Option<DynSessionStore>,
    state_store: Option<DynStateStore>,
    mocks: Option<Arc<MockLayer>>,
    secrets: DynSecretsManager,
    oauth_config: Option<OAuthBrokerConfig>,
    oauth_host: OAuthBrokerHost,
    exec_ctx: Option<ComponentExecCtx>,
}

impl HostState {
    #[allow(clippy::default_constructed_unit_structs)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<HostConfig>,
        http_client: Arc<BlockingClient>,
        mocks: Option<Arc<MockLayer>>,
        session_store: Option<DynSessionStore>,
        state_store: Option<DynStateStore>,
        secrets: DynSecretsManager,
        oauth_config: Option<OAuthBrokerConfig>,
        exec_ctx: Option<ComponentExecCtx>,
    ) -> Result<Self> {
        let default_env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
        Ok(Self {
            config,
            http_client,
            default_env,
            session_store,
            state_store,
            mocks,
            secrets,
            oauth_config,
            oauth_host: OAuthBrokerHost::default(),
            exec_ctx,
        })
    }

    pub fn get_secret(&self, key: &str) -> Result<String> {
        if provider_core_only::is_enabled() {
            bail!(provider_core_only::blocked_message("secrets"))
        }
        if !self.config.secrets_policy.is_allowed(key) {
            bail!("secret {key} is not permitted by bindings policy");
        }
        if let Some(mock) = &self.mocks
            && let Some(value) = mock.secrets_lookup(key)
        {
            return Ok(value);
        }
        let ctx = self.config.tenant_ctx();
        let bytes = read_secret_blocking(&self.secrets, &ctx, key)
            .context("failed to read secret from manager")?;
        let value = String::from_utf8(bytes).context("secret value is not valid UTF-8")?;
        Ok(value)
    }

    fn tenant_ctx_from_v1(&self, ctx: Option<StateTenantCtx>) -> Result<TypesTenantCtx> {
        let tenant_raw = ctx
            .as_ref()
            .map(|ctx| ctx.tenant.clone())
            .or_else(|| self.exec_ctx.as_ref().map(|ctx| ctx.tenant.tenant.clone()))
            .unwrap_or_else(|| self.config.tenant.clone());
        let env_raw = ctx
            .as_ref()
            .map(|ctx| ctx.env.clone())
            .unwrap_or_else(|| self.default_env.clone());
        let tenant_id = TenantId::from_str(&tenant_raw)
            .with_context(|| format!("invalid tenant id `{tenant_raw}`"))?;
        let env_id = EnvId::from_str(&env_raw)
            .unwrap_or_else(|_| EnvId::from_str("local").expect("default env must be valid"));
        let mut tenant_ctx = TypesTenantCtx::new(env_id, tenant_id);
        if let Some(exec_ctx) = self.exec_ctx.as_ref() {
            if let Some(team) = exec_ctx.tenant.team.as_ref() {
                let team_id =
                    TeamId::from_str(team).with_context(|| format!("invalid team id `{team}`"))?;
                tenant_ctx = tenant_ctx.with_team(Some(team_id));
            }
            if let Some(user) = exec_ctx.tenant.user.as_ref() {
                let user_id =
                    UserId::from_str(user).with_context(|| format!("invalid user id `{user}`"))?;
                tenant_ctx = tenant_ctx.with_user(Some(user_id));
            }
            tenant_ctx = tenant_ctx.with_flow(exec_ctx.flow_id.clone());
            if let Some(node) = exec_ctx.node_id.as_ref() {
                tenant_ctx = tenant_ctx.with_node(node.clone());
            }
            if let Some(session) = exec_ctx.tenant.correlation_id.as_ref() {
                tenant_ctx = tenant_ctx.with_session(session.clone());
            }
            tenant_ctx.trace_id = exec_ctx.tenant.trace_id.clone();
        }

        if let Some(ctx) = ctx {
            if let Some(team) = ctx.team.or(ctx.team_id) {
                let team_id =
                    TeamId::from_str(&team).with_context(|| format!("invalid team id `{team}`"))?;
                tenant_ctx = tenant_ctx.with_team(Some(team_id));
            }
            if let Some(user) = ctx.user.or(ctx.user_id) {
                let user_id =
                    UserId::from_str(&user).with_context(|| format!("invalid user id `{user}`"))?;
                tenant_ctx = tenant_ctx.with_user(Some(user_id));
            }
            if let Some(flow) = ctx.flow_id {
                tenant_ctx = tenant_ctx.with_flow(flow);
            }
            if let Some(node) = ctx.node_id {
                tenant_ctx = tenant_ctx.with_node(node);
            }
            if let Some(provider) = ctx.provider_id {
                tenant_ctx = tenant_ctx.with_provider(provider);
            }
            if let Some(session) = ctx.session_id {
                tenant_ctx = tenant_ctx.with_session(session);
            }
            tenant_ctx.trace_id = ctx.trace_id;
        }
        Ok(tenant_ctx)
    }

    fn send_http_request(
        &mut self,
        req: HttpRequest,
        opts: Option<HttpRequestOptionsV1_1>,
        _ctx: Option<HttpTenantCtx>,
    ) -> Result<HttpResponse, HttpClientError> {
        if !self.config.http_enabled {
            return Err(HttpClientError {
                code: "denied".into(),
                message: "http client disabled by policy".into(),
            });
        }

        let mut mock_state = None;
        let raw_body = req.body.clone();
        if let Some(mock) = &self.mocks
            && let Ok(meta) = HttpMockRequest::new(&req.method, &req.url, raw_body.as_deref())
        {
            match mock.http_begin(&meta) {
                HttpDecision::Mock(response) => {
                    let headers = response
                        .headers
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    return Ok(HttpResponse {
                        status: response.status,
                        headers,
                        body: response.body.clone().map(|b| b.into_bytes()),
                    });
                }
                HttpDecision::Deny(reason) => {
                    return Err(HttpClientError {
                        code: "denied".into(),
                        message: reason,
                    });
                }
                HttpDecision::Passthrough { record } => {
                    mock_state = Some((meta, record));
                }
            }
        }

        let method = req.method.parse().unwrap_or(reqwest::Method::GET);
        let mut builder = self.http_client.request(method, &req.url);
        for (key, value) in req.headers {
            if let Ok(header) = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                && let Ok(header_value) = reqwest::header::HeaderValue::from_str(&value)
            {
                builder = builder.header(header, header_value);
            }
        }

        if let Some(body) = raw_body.clone() {
            builder = builder.body(body);
        }

        if let Some(opts) = opts {
            if let Some(timeout_ms) = opts.timeout_ms {
                builder = builder.timeout(Duration::from_millis(timeout_ms as u64));
            }
            if opts.allow_insecure == Some(true) {
                warn!(url = %req.url, "allow-insecure not supported; using default TLS validation");
            }
            if let Some(follow_redirects) = opts.follow_redirects
                && !follow_redirects
            {
                warn!(url = %req.url, "follow-redirects=false not supported; using default client behaviour");
            }
        }

        let response = match builder.send() {
            Ok(resp) => resp,
            Err(err) => {
                warn!(url = %req.url, error = %err, "http client request failed");
                return Err(HttpClientError {
                    code: "unavailable".into(),
                    message: err.to_string(),
                });
            }
        };

        let status = response.status().as_u16();
        let headers_vec = response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_string(),
                    v.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let body_bytes = response.bytes().ok().map(|b| b.to_vec());

        if let Some((meta, true)) = mock_state.take()
            && let Some(mock) = &self.mocks
        {
            let recorded = HttpMockResponse::new(
                status,
                headers_vec.clone().into_iter().collect(),
                body_bytes
                    .as_ref()
                    .map(|b| String::from_utf8_lossy(b).into_owned()),
            );
            mock.http_record(&meta, &recorded);
        }

        Ok(HttpResponse {
            status,
            headers: headers_vec,
            body: body_bytes,
        })
    }
}

impl SecretsStoreHost for HostState {
    fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, SecretsError> {
        if provider_core_only::is_enabled() {
            warn!(secret = %key, "provider-core only mode enabled; blocking secrets store");
            return Err(SecretsError::Denied);
        }
        if !self.config.secrets_policy.is_allowed(&key) {
            return Err(SecretsError::Denied);
        }
        if let Some(mock) = &self.mocks
            && let Some(value) = mock.secrets_lookup(&key)
        {
            return Ok(Some(value.into_bytes()));
        }
        let ctx = self.config.tenant_ctx();
        match read_secret_blocking(&self.secrets, &ctx, &key) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) => {
                warn!(secret = %key, error = %err, "secret lookup failed");
                Err(SecretsError::NotFound)
            }
        }
    }
}

impl HttpClientHost for HostState {
    fn send(
        &mut self,
        req: HttpRequest,
        ctx: Option<HttpTenantCtx>,
    ) -> Result<HttpResponse, HttpClientError> {
        self.send_http_request(req, None, ctx)
    }
}

impl HttpClientHostV1_1 for HostState {
    fn send(
        &mut self,
        req: HttpRequestV1_1,
        opts: Option<HttpRequestOptionsV1_1>,
        ctx: Option<HttpTenantCtxV1_1>,
    ) -> Result<HttpResponseV1_1, HttpClientErrorV1_1> {
        let legacy_req = HttpRequest {
            method: req.method,
            url: req.url,
            headers: req.headers,
            body: req.body,
        };
        let legacy_ctx = ctx.map(|ctx| HttpTenantCtx {
            env: ctx.env,
            tenant: ctx.tenant,
            tenant_id: ctx.tenant_id,
            team: ctx.team,
            team_id: ctx.team_id,
            user: ctx.user,
            user_id: ctx.user_id,
            trace_id: ctx.trace_id,
            correlation_id: ctx.correlation_id,
            attributes: ctx.attributes,
            session_id: ctx.session_id,
            flow_id: ctx.flow_id,
            node_id: ctx.node_id,
            provider_id: ctx.provider_id,
            deadline_ms: ctx.deadline_ms,
            attempt: ctx.attempt,
            idempotency_key: ctx.idempotency_key,
            impersonation: ctx
                .impersonation
                .map(|ImpersonationV1_1 { actor_id, reason }| ImpersonationV1_0 {
                    actor_id,
                    reason,
                }),
        });

        self.send_http_request(legacy_req, opts, legacy_ctx)
            .map(|resp| HttpResponseV1_1 {
                status: resp.status,
                headers: resp.headers,
                body: resp.body,
            })
            .map_err(|err| HttpClientErrorV1_1 {
                code: err.code,
                message: err.message,
            })
    }
}

impl StateStoreHost for HostState {
    fn read(
        &mut self,
        key: HostStateKey,
        ctx: Option<StateTenantCtx>,
    ) -> Result<Vec<u8>, StateError> {
        let store = match self.state_store.as_ref() {
            Some(store) => store.clone(),
            None => {
                return Err(StateError {
                    code: "unavailable".into(),
                    message: "state store not configured".into(),
                });
            }
        };
        let tenant_ctx = match self.tenant_ctx_from_v1(ctx) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(StateError {
                    code: "invalid-ctx".into(),
                    message: err.to_string(),
                });
            }
        };
        let key = StoreStateKey::from(key);
        match store.get_json(&tenant_ctx, STATE_PREFIX, &key, None) {
            Ok(Some(value)) => Ok(serde_json::to_vec(&value).unwrap_or_else(|_| Vec::new())),
            Ok(None) => Err(StateError {
                code: "not_found".into(),
                message: "state key not found".into(),
            }),
            Err(err) => Err(StateError {
                code: "internal".into(),
                message: err.to_string(),
            }),
        }
    }

    fn write(
        &mut self,
        key: HostStateKey,
        bytes: Vec<u8>,
        ctx: Option<StateTenantCtx>,
    ) -> Result<StateOpAck, StateError> {
        let store = match self.state_store.as_ref() {
            Some(store) => store.clone(),
            None => {
                return Err(StateError {
                    code: "unavailable".into(),
                    message: "state store not configured".into(),
                });
            }
        };
        let tenant_ctx = match self.tenant_ctx_from_v1(ctx) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(StateError {
                    code: "invalid-ctx".into(),
                    message: err.to_string(),
                });
            }
        };
        let key = StoreStateKey::from(key);
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()));
        match store.set_json(&tenant_ctx, STATE_PREFIX, &key, None, &value, None) {
            Ok(()) => Ok(StateOpAck::Ok),
            Err(err) => Err(StateError {
                code: "internal".into(),
                message: err.to_string(),
            }),
        }
    }

    fn delete(
        &mut self,
        key: HostStateKey,
        ctx: Option<StateTenantCtx>,
    ) -> Result<StateOpAck, StateError> {
        let store = match self.state_store.as_ref() {
            Some(store) => store.clone(),
            None => {
                return Err(StateError {
                    code: "unavailable".into(),
                    message: "state store not configured".into(),
                });
            }
        };
        let tenant_ctx = match self.tenant_ctx_from_v1(ctx) {
            Ok(ctx) => ctx,
            Err(err) => {
                return Err(StateError {
                    code: "invalid-ctx".into(),
                    message: err.to_string(),
                });
            }
        };
        let key = StoreStateKey::from(key);
        match store.del(&tenant_ctx, STATE_PREFIX, &key) {
            Ok(_) => Ok(StateOpAck::Ok),
            Err(err) => Err(StateError {
                code: "internal".into(),
                message: err.to_string(),
            }),
        }
    }
}

impl TelemetryLoggerHost for HostState {
    fn log(
        &mut self,
        span: TelemetrySpanContext,
        fields: Vec<(String, String)>,
        _ctx: Option<TelemetryTenantCtx>,
    ) -> Result<TelemetryAck, TelemetryError> {
        if let Some(mock) = &self.mocks
            && mock.telemetry_drain(&[("span_json", span.flow_id.as_str())])
        {
            return Ok(TelemetryAck::Ok);
        }
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert(k, Value::String(v));
        }
        tracing::info!(
            tenant = %span.tenant,
            flow_id = %span.flow_id,
            node = ?span.node_id,
            provider = %span.provider,
            fields = %serde_json::Value::Object(map.clone()),
            "telemetry log from pack"
        );
        Ok(TelemetryAck::Ok)
    }
}

impl RunnerHostHttp for HostState {
    fn request(
        &mut self,
        method: String,
        url: String,
        headers: Vec<String>,
        body: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, String> {
        let req = HttpRequest {
            method,
            url,
            headers: headers
                .chunks(2)
                .filter_map(|chunk| {
                    if chunk.len() == 2 {
                        Some((chunk[0].clone(), chunk[1].clone()))
                    } else {
                        None
                    }
                })
                .collect(),
            body,
        };
        match HttpClientHost::send(self, req, None) {
            Ok(resp) => Ok(resp.body.unwrap_or_default()),
            Err(err) => Err(err.message),
        }
    }
}

impl RunnerHostKv for HostState {
    fn get(&mut self, _ns: String, _key: String) -> Option<String> {
        None
    }

    fn put(&mut self, _ns: String, _key: String, _val: String) {}
}

enum ManifestLoad {
    New {
        manifest: Box<greentic_types::PackManifest>,
        flows: PackFlows,
    },
    Legacy {
        manifest: Box<legacy_pack::PackManifest>,
        flows: PackFlows,
    },
}

fn load_manifest_and_flows(path: &Path) -> Result<ManifestLoad> {
    let mut archive = ZipArchive::new(File::open(path)?)
        .with_context(|| format!("{} is not a valid gtpack", path.display()))?;
    let bytes = read_entry(&mut archive, "manifest.cbor")
        .with_context(|| format!("missing manifest.cbor in {}", path.display()))?;
    match decode_pack_manifest(&bytes) {
        Ok(manifest) => {
            let cache = PackFlows::from_manifest(manifest.clone());
            Ok(ManifestLoad::New {
                manifest: Box::new(manifest),
                flows: cache,
            })
        }
        Err(err) => {
            tracing::debug!(error = %err, pack = %path.display(), "decode_pack_manifest failed; trying legacy manifest");
            // Fall back to legacy pack manifest
            let legacy: legacy_pack::PackManifest = serde_cbor::from_slice(&bytes)
                .context("failed to decode legacy pack manifest from manifest.cbor")?;
            let flows = load_legacy_flows(&mut archive, &legacy)?;
            Ok(ManifestLoad::Legacy {
                manifest: Box::new(legacy),
                flows,
            })
        }
    }
}

fn load_manifest_and_flows_from_dir(root: &Path) -> Result<ManifestLoad> {
    let manifest_path = root.join("manifest.cbor");
    let bytes = std::fs::read(&manifest_path)
        .with_context(|| format!("missing manifest.cbor in {}", root.display()))?;
    match decode_pack_manifest(&bytes) {
        Ok(manifest) => {
            let cache = PackFlows::from_manifest(manifest.clone());
            Ok(ManifestLoad::New {
                manifest: Box::new(manifest),
                flows: cache,
            })
        }
        Err(err) => {
            tracing::debug!(
                error = %err,
                pack = %root.display(),
                "decode_pack_manifest failed for materialized pack; trying legacy manifest"
            );
            let legacy: legacy_pack::PackManifest = serde_cbor::from_slice(&bytes)
                .context("failed to decode legacy pack manifest from manifest.cbor")?;
            let flows = load_legacy_flows_from_dir(root, &legacy)?;
            Ok(ManifestLoad::Legacy {
                manifest: Box::new(legacy),
                flows,
            })
        }
    }
}

fn load_legacy_flows(
    archive: &mut ZipArchive<File>,
    manifest: &legacy_pack::PackManifest,
) -> Result<PackFlows> {
    let mut flows = HashMap::new();
    let mut descriptors = Vec::new();

    for entry in &manifest.flows {
        let bytes = read_entry(archive, &entry.file_json)
            .with_context(|| format!("missing flow json {}", entry.file_json))?;
        let doc: FlowDoc = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to decode flow doc {}", entry.file_json))?;
        let normalized = normalize_flow_doc(doc);
        let flow_ir = flow_doc_to_ir(normalized)?;
        let flow = flow_ir_to_flow(flow_ir)?;

        descriptors.push(FlowDescriptor {
            id: entry.id.clone(),
            flow_type: entry.kind.clone(),
            pack_id: manifest.meta.pack_id.clone(),
            profile: manifest.meta.pack_id.clone(),
            version: manifest.meta.version.to_string(),
            description: None,
        });
        flows.insert(entry.id.clone(), flow);
    }

    let mut entry_flows = manifest.meta.entry_flows.clone();
    if entry_flows.is_empty() {
        entry_flows = manifest.flows.iter().map(|f| f.id.clone()).collect();
    }
    let metadata = PackMetadata {
        pack_id: manifest.meta.pack_id.clone(),
        version: manifest.meta.version.to_string(),
        entry_flows,
        secret_requirements: Vec::new(),
    };

    Ok(PackFlows {
        descriptors,
        flows,
        metadata,
    })
}

fn load_legacy_flows_from_dir(
    root: &Path,
    manifest: &legacy_pack::PackManifest,
) -> Result<PackFlows> {
    let mut flows = HashMap::new();
    let mut descriptors = Vec::new();

    for entry in &manifest.flows {
        let path = root.join(&entry.file_json);
        let bytes = std::fs::read(&path)
            .with_context(|| format!("missing flow json {}", path.display()))?;
        let doc: FlowDoc = serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to decode flow doc {}", path.display()))?;
        let normalized = normalize_flow_doc(doc);
        let flow_ir = flow_doc_to_ir(normalized)?;
        let flow = flow_ir_to_flow(flow_ir)?;

        descriptors.push(FlowDescriptor {
            id: entry.id.clone(),
            flow_type: entry.kind.clone(),
            pack_id: manifest.meta.pack_id.clone(),
            profile: manifest.meta.pack_id.clone(),
            version: manifest.meta.version.to_string(),
            description: None,
        });
        flows.insert(entry.id.clone(), flow);
    }

    let mut entry_flows = manifest.meta.entry_flows.clone();
    if entry_flows.is_empty() {
        entry_flows = manifest.flows.iter().map(|f| f.id.clone()).collect();
    }
    let metadata = PackMetadata {
        pack_id: manifest.meta.pack_id.clone(),
        version: manifest.meta.version.to_string(),
        entry_flows,
        secret_requirements: Vec::new(),
    };

    Ok(PackFlows {
        descriptors,
        flows,
        metadata,
    })
}

pub struct ComponentState {
    pub host: HostState,
    wasi_ctx: WasiCtx,
    resource_table: ResourceTable,
}

impl ComponentState {
    pub fn new(host: HostState, policy: Arc<RunnerWasiPolicy>) -> Result<Self> {
        let wasi_ctx = policy
            .instantiate()
            .context("failed to build WASI context")?;
        Ok(Self {
            host,
            wasi_ctx,
            resource_table: ResourceTable::new(),
        })
    }

    fn host_mut(&mut self) -> &mut HostState {
        &mut self.host
    }

    fn should_cancel_host(&mut self) -> bool {
        false
    }

    fn yield_now_host(&mut self) {
        // no-op cooperative yield
    }
}

impl component_api::v0_4::greentic::component::control::Host for ComponentState {
    fn should_cancel(&mut self) -> bool {
        self.should_cancel_host()
    }

    fn yield_now(&mut self) {
        self.yield_now_host();
    }
}

impl component_api::v0_5::greentic::component::control::Host for ComponentState {
    fn should_cancel(&mut self) -> bool {
        self.should_cancel_host()
    }

    fn yield_now(&mut self) {
        self.yield_now_host();
    }
}

fn add_component_control_instance(
    linker: &mut Linker<ComponentState>,
    name: &str,
) -> wasmtime::Result<()> {
    let mut inst = linker.instance(name)?;
    inst.func_wrap(
        "should-cancel",
        |mut caller: StoreContextMut<'_, ComponentState>, (): ()| {
            let host = caller.data_mut();
            Ok((host.should_cancel_host(),))
        },
    )?;
    inst.func_wrap(
        "yield-now",
        |mut caller: StoreContextMut<'_, ComponentState>, (): ()| {
            let host = caller.data_mut();
            host.yield_now_host();
            Ok(())
        },
    )?;
    Ok(())
}

fn add_component_control_to_linker(linker: &mut Linker<ComponentState>) -> wasmtime::Result<()> {
    add_component_control_instance(linker, "greentic:component/control@0.5.0")?;
    add_component_control_instance(linker, "greentic:component/control@0.4.0")?;
    Ok(())
}

pub fn register_all(linker: &mut Linker<ComponentState>, allow_state_store: bool) -> Result<()> {
    add_wasi_to_linker(linker)?;
    add_all_v1_to_linker(
        linker,
        HostFns {
            http_client_v1_1: Some(|state| state.host_mut()),
            http_client: Some(|state| state.host_mut()),
            oauth_broker: None,
            runner_host_http: Some(|state| state.host_mut()),
            runner_host_kv: Some(|state| state.host_mut()),
            telemetry_logger: Some(|state| state.host_mut()),
            state_store: allow_state_store.then_some(|state| state.host_mut()),
            secrets_store: Some(|state| state.host_mut()),
        },
    )?;
    Ok(())
}

impl OAuthHostContext for ComponentState {
    fn tenant_id(&self) -> &str {
        &self.host.config.tenant
    }

    fn env(&self) -> &str {
        &self.host.default_env
    }

    fn oauth_broker_host(&mut self) -> &mut OAuthBrokerHost {
        &mut self.host.oauth_host
    }

    fn oauth_config(&self) -> Option<&OAuthBrokerConfig> {
        self.host.oauth_config.as_ref()
    }
}

impl WasiView for ComponentState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

#[allow(unsafe_code)]
unsafe impl Send for ComponentState {}
#[allow(unsafe_code)]
unsafe impl Sync for ComponentState {}

impl PackRuntime {
    fn allows_state_store(&self, component_ref: &str) -> bool {
        if self.state_store.is_none() {
            return false;
        }
        if !self.config.state_store_policy.allow {
            return false;
        }
        let Some(manifest) = self.component_manifests.get(component_ref) else {
            return false;
        };
        manifest
            .capabilities
            .host
            .state
            .as_ref()
            .map(|caps| caps.read || caps.write)
            .unwrap_or(false)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load(
        path: impl AsRef<Path>,
        config: Arc<HostConfig>,
        mocks: Option<Arc<MockLayer>>,
        archive_source: Option<&Path>,
        session_store: Option<DynSessionStore>,
        state_store: Option<DynStateStore>,
        wasi_policy: Arc<RunnerWasiPolicy>,
        secrets: DynSecretsManager,
        oauth_config: Option<OAuthBrokerConfig>,
        verify_archive: bool,
        component_resolution: ComponentResolution,
    ) -> Result<Self> {
        let path = path.as_ref();
        let (_pack_root, safe_path) = normalize_pack_path(path)?;
        let path_meta = std::fs::metadata(&safe_path).ok();
        let is_dir = path_meta
            .as_ref()
            .map(|meta| meta.is_dir())
            .unwrap_or(false);
        let is_component = !is_dir
            && safe_path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("wasm"))
                .unwrap_or(false);
        let archive_hint_path = if let Some(source) = archive_source {
            let (_, normalized) = normalize_pack_path(source)?;
            Some(normalized)
        } else if is_component || is_dir {
            None
        } else {
            Some(safe_path.clone())
        };
        let archive_hint = archive_hint_path.as_deref();
        if verify_archive {
            if let Some(verify_target) = archive_hint.and_then(|p| {
                std::fs::metadata(p)
                    .ok()
                    .filter(|meta| meta.is_file())
                    .map(|_| p)
            }) {
                verify::verify_pack(verify_target).await?;
                tracing::info!(pack_path = %verify_target.display(), "pack verification complete");
            } else {
                tracing::debug!("skipping archive verification (no archive source)");
            }
        }
        let engine = Engine::default();
        let engine_profile =
            EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
        let cache = CacheManager::new(CacheConfig::default(), engine_profile);
        let mut metadata = PackMetadata::fallback(&safe_path);
        let mut manifest = None;
        let mut legacy_manifest: Option<Box<legacy_pack::PackManifest>> = None;
        let mut flows = None;
        let materialized_root = component_resolution.materialized_root.clone().or_else(|| {
            if is_dir {
                Some(safe_path.clone())
            } else {
                None
            }
        });

        if let Some(root) = materialized_root.as_ref() {
            match load_manifest_and_flows_from_dir(root) {
                Ok(ManifestLoad::New {
                    manifest: m,
                    flows: cache,
                }) => {
                    metadata = cache.metadata.clone();
                    manifest = Some(*m);
                    flows = Some(cache);
                }
                Ok(ManifestLoad::Legacy {
                    manifest: m,
                    flows: cache,
                }) => {
                    metadata = cache.metadata.clone();
                    legacy_manifest = Some(m);
                    flows = Some(cache);
                }
                Err(err) => {
                    warn!(error = %err, pack = %root.display(), "failed to parse materialized pack manifest");
                }
            }
        }

        if manifest.is_none()
            && legacy_manifest.is_none()
            && let Some(archive_path) = archive_hint
        {
            match load_manifest_and_flows(archive_path) {
                Ok(ManifestLoad::New {
                    manifest: m,
                    flows: cache,
                }) => {
                    metadata = cache.metadata.clone();
                    manifest = Some(*m);
                    flows = Some(cache);
                }
                Ok(ManifestLoad::Legacy {
                    manifest: m,
                    flows: cache,
                }) => {
                    metadata = cache.metadata.clone();
                    legacy_manifest = Some(m);
                    flows = Some(cache);
                }
                Err(err) => {
                    warn!(error = %err, pack = %archive_path.display(), "failed to parse pack manifest; skipping flows");
                }
            }
        }
        let mut pack_lock = None;
        for root in find_pack_lock_roots(&safe_path, is_dir, archive_hint) {
            pack_lock = load_pack_lock(&root)?;
            if pack_lock.is_some() {
                break;
            }
        }
        let component_sources_payload = if pack_lock.is_none() {
            if let Some(manifest) = manifest.as_ref() {
                manifest
                    .get_component_sources_v1()
                    .context("invalid component sources extension")?
            } else {
                None
            }
        } else {
            None
        };
        let component_sources = if let Some(lock) = pack_lock.as_ref() {
            Some(component_sources_table_from_pack_lock(
                lock,
                component_resolution.allow_missing_hash,
            )?)
        } else {
            component_sources_table(component_sources_payload.as_ref())?
        };
        let components = if is_component {
            let wasm_bytes = fs::read(&safe_path).await?;
            metadata = PackMetadata::from_wasm(&wasm_bytes)
                .unwrap_or_else(|| PackMetadata::fallback(&safe_path));
            let name = safe_path
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "component".to_string());
            let component = compile_component_with_cache(&cache, &engine, None, wasm_bytes).await?;
            let mut map = HashMap::new();
            map.insert(
                name.clone(),
                PackComponent {
                    name,
                    version: metadata.version.clone(),
                    component,
                },
            );
            map
        } else {
            let specs = component_specs(
                manifest.as_ref(),
                legacy_manifest.as_deref(),
                component_sources_payload.as_ref(),
                pack_lock.as_ref(),
            );
            if specs.is_empty() {
                HashMap::new()
            } else {
                let mut loaded = HashMap::new();
                let mut missing: HashSet<String> =
                    specs.iter().map(|spec| spec.id.clone()).collect();
                let mut searched = Vec::new();

                if !component_resolution.overrides.is_empty() {
                    load_components_from_overrides(
                        &cache,
                        &engine,
                        &component_resolution.overrides,
                        &specs,
                        &mut missing,
                        &mut loaded,
                    )
                    .await?;
                    searched.push("override map".to_string());
                }

                if let Some(component_sources) = component_sources.as_ref() {
                    load_components_from_sources(
                        &cache,
                        &engine,
                        component_sources,
                        &component_resolution,
                        &specs,
                        &mut missing,
                        &mut loaded,
                        materialized_root.as_deref(),
                        archive_hint,
                    )
                    .await?;
                    searched.push(format!("extension {}", EXT_COMPONENT_SOURCES_V1));
                }

                if let Some(root) = materialized_root.as_ref() {
                    load_components_from_dir(
                        &cache,
                        &engine,
                        root,
                        &specs,
                        &mut missing,
                        &mut loaded,
                    )
                    .await?;
                    searched.push(format!("components dir {}", root.display()));
                }

                if let Some(archive_path) = archive_hint {
                    load_components_from_archive(
                        &cache,
                        &engine,
                        archive_path,
                        &specs,
                        &mut missing,
                        &mut loaded,
                    )
                    .await?;
                    searched.push(format!("archive {}", archive_path.display()));
                }

                if !missing.is_empty() {
                    let missing_list = missing.into_iter().collect::<Vec<_>>().join(", ");
                    let sources = if searched.is_empty() {
                        "no component sources".to_string()
                    } else {
                        searched.join(", ")
                    };
                    bail!(
                        "components missing: {}; looked in {}",
                        missing_list,
                        sources
                    );
                }

                loaded
            }
        };
        let http_client = Arc::clone(&HTTP_CLIENT);
        let mut component_manifests = HashMap::new();
        if let Some(manifest) = manifest.as_ref() {
            for component in &manifest.components {
                component_manifests.insert(component.id.as_str().to_string(), component.clone());
            }
        }
        Ok(Self {
            path: safe_path,
            archive_path: archive_hint.map(Path::to_path_buf),
            config,
            engine,
            metadata,
            manifest,
            legacy_manifest,
            component_manifests,
            mocks,
            flows,
            components,
            http_client,
            pre_cache: Mutex::new(HashMap::new()),
            session_store,
            state_store,
            wasi_policy,
            provider_registry: RwLock::new(None),
            secrets,
            oauth_config,
            cache,
        })
    }

    pub async fn list_flows(&self) -> Result<Vec<FlowDescriptor>> {
        if let Some(cache) = &self.flows {
            return Ok(cache.descriptors.clone());
        }
        if let Some(manifest) = &self.manifest {
            let descriptors = manifest
                .flows
                .iter()
                .map(|flow| FlowDescriptor {
                    id: flow.id.as_str().to_string(),
                    flow_type: flow_kind_to_str(flow.kind).to_string(),
                    pack_id: manifest.pack_id.as_str().to_string(),
                    profile: manifest.pack_id.as_str().to_string(),
                    version: manifest.version.to_string(),
                    description: None,
                })
                .collect();
            return Ok(descriptors);
        }
        Ok(Vec::new())
    }

    #[allow(dead_code)]
    pub async fn run_flow(
        &self,
        flow_id: &str,
        input: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let pack = Arc::new(
            PackRuntime::load(
                &self.path,
                Arc::clone(&self.config),
                self.mocks.clone(),
                self.archive_path.as_deref(),
                self.session_store.clone(),
                self.state_store.clone(),
                Arc::clone(&self.wasi_policy),
                self.secrets.clone(),
                self.oauth_config.clone(),
                false,
                ComponentResolution::default(),
            )
            .await?,
        );

        let engine = FlowEngine::new(vec![Arc::clone(&pack)], Arc::clone(&self.config)).await?;
        let retry_config = self.config.retry_config().into();
        let mocks = pack.mocks.as_deref();
        let tenant = self.config.tenant.as_str();

        let ctx = FlowContext {
            tenant,
            pack_id: pack.metadata().pack_id.as_str(),
            flow_id,
            node_id: None,
            tool: None,
            action: None,
            session_id: None,
            provider_id: None,
            retry_config,
            observer: None,
            mocks,
        };

        let execution = engine.execute(ctx, input).await?;
        match execution.status {
            FlowStatus::Completed => Ok(execution.output),
            FlowStatus::Waiting(wait) => Ok(serde_json::json!({
                "status": "pending",
                "reason": wait.reason,
                "resume": wait.snapshot,
                "response": execution.output,
            })),
        }
    }

    pub async fn invoke_component(
        &self,
        component_ref: &str,
        ctx: ComponentExecCtx,
        operation: &str,
        _config_json: Option<String>,
        input_json: String,
    ) -> Result<Value> {
        let pack_component = self
            .components
            .get(component_ref)
            .with_context(|| format!("component '{component_ref}' not found in pack"))?;

        let mut linker = Linker::new(&self.engine);
        let allow_state_store = self.allows_state_store(component_ref);
        register_all(&mut linker, allow_state_store)?;
        add_component_control_to_linker(&mut linker)?;

        let host_state = HostState::new(
            Arc::clone(&self.config),
            Arc::clone(&self.http_client),
            self.mocks.clone(),
            self.session_store.clone(),
            self.state_store.clone(),
            Arc::clone(&self.secrets),
            self.oauth_config.clone(),
            Some(ctx.clone()),
        )?;
        let store_state = ComponentState::new(host_state, Arc::clone(&self.wasi_policy))?;
        let mut store = wasmtime::Store::new(&self.engine, store_state);
        let pre_instance = linker.instantiate_pre(pack_component.component.as_ref())?;
        let result = match component_api::v0_5::ComponentPre::new(pre_instance) {
            Ok(pre) => {
                let bindings = pre.instantiate_async(&mut store).await?;
                let node = bindings.greentic_component_node();
                let ctx_v05 = component_api::exec_ctx_v0_5(&ctx);
                let result = node.call_invoke(&mut store, &ctx_v05, operation, &input_json)?;
                component_api::invoke_result_from_v0_5(result)
            }
            Err(err) => {
                if is_missing_node_export(&err, "0.5.0") {
                    let pre_instance = linker.instantiate_pre(pack_component.component.as_ref())?;
                    let pre: component_api::v0_4::ComponentPre<ComponentState> =
                        component_api::v0_4::ComponentPre::new(pre_instance)?;
                    let bindings = pre.instantiate_async(&mut store).await?;
                    let node = bindings.greentic_component_node();
                    let ctx_v04 = component_api::exec_ctx_v0_4(&ctx);
                    let result = node.call_invoke(&mut store, &ctx_v04, operation, &input_json)?;
                    component_api::invoke_result_from_v0_4(result)
                } else {
                    return Err(err);
                }
            }
        };

        match result {
            InvokeResult::Ok(body) => {
                if body.is_empty() {
                    return Ok(Value::Null);
                }
                serde_json::from_str(&body).or_else(|_| Ok(Value::String(body)))
            }
            InvokeResult::Err(NodeError {
                code,
                message,
                retryable,
                backoff_ms,
                details,
            }) => {
                let mut obj = serde_json::Map::new();
                obj.insert("ok".into(), Value::Bool(false));
                let mut error = serde_json::Map::new();
                error.insert("code".into(), Value::String(code));
                error.insert("message".into(), Value::String(message));
                error.insert("retryable".into(), Value::Bool(retryable));
                if let Some(backoff) = backoff_ms {
                    error.insert("backoff_ms".into(), Value::Number(backoff.into()));
                }
                if let Some(details) = details {
                    error.insert(
                        "details".into(),
                        serde_json::from_str(&details).unwrap_or(Value::String(details)),
                    );
                }
                obj.insert("error".into(), Value::Object(error));
                Ok(Value::Object(obj))
            }
        }
    }

    pub fn resolve_provider(
        &self,
        provider_id: Option<&str>,
        provider_type: Option<&str>,
    ) -> Result<ProviderBinding> {
        let registry = self.provider_registry()?;
        registry.resolve(provider_id, provider_type)
    }

    pub async fn invoke_provider(
        &self,
        binding: &ProviderBinding,
        ctx: ComponentExecCtx,
        op: &str,
        input_json: Vec<u8>,
    ) -> Result<Value> {
        let component_ref = &binding.component_ref;
        let pack_component = self
            .components
            .get(component_ref)
            .with_context(|| format!("provider component '{component_ref}' not found in pack"))?;

        let mut linker = Linker::new(&self.engine);
        let allow_state_store = self.allows_state_store(component_ref);
        register_all(&mut linker, allow_state_store)?;
        add_component_control_to_linker(&mut linker)?;
        let pre_instance = linker.instantiate_pre(pack_component.component.as_ref())?;
        let pre: ProviderComponentPre<ComponentState> = ProviderComponentPre::new(pre_instance)?;

        let host_state = HostState::new(
            Arc::clone(&self.config),
            Arc::clone(&self.http_client),
            self.mocks.clone(),
            self.session_store.clone(),
            self.state_store.clone(),
            Arc::clone(&self.secrets),
            self.oauth_config.clone(),
            Some(ctx),
        )?;
        let store_state = ComponentState::new(host_state, Arc::clone(&self.wasi_policy))?;
        let mut store = wasmtime::Store::new(&self.engine, store_state);
        let bindings: crate::provider_core::SchemaCore = pre.instantiate_async(&mut store).await?;
        let provider = bindings.greentic_provider_core_schema_core_api();

        let result = provider.call_invoke(&mut store, op, &input_json)?;
        deserialize_json_bytes(result)
    }

    fn provider_registry(&self) -> Result<ProviderRegistry> {
        if let Some(registry) = self.provider_registry.read().clone() {
            return Ok(registry);
        }
        let manifest = self
            .manifest
            .as_ref()
            .context("pack manifest required for provider resolution")?;
        let env = std::env::var("GREENTIC_ENV").unwrap_or_else(|_| "local".to_string());
        let registry = ProviderRegistry::new(
            manifest,
            self.state_store.clone(),
            &self.config.tenant,
            &env,
        )?;
        *self.provider_registry.write() = Some(registry.clone());
        Ok(registry)
    }

    pub fn load_flow(&self, flow_id: &str) -> Result<Flow> {
        if let Some(cache) = &self.flows {
            return cache
                .flows
                .get(flow_id)
                .cloned()
                .ok_or_else(|| anyhow!("flow '{flow_id}' not found in pack"));
        }
        if let Some(manifest) = &self.manifest {
            let entry = manifest
                .flows
                .iter()
                .find(|f| f.id.as_str() == flow_id)
                .ok_or_else(|| anyhow!("flow '{flow_id}' not found in manifest"))?;
            return Ok(entry.flow.clone());
        }
        bail!("flow '{flow_id}' not available (pack exports disabled)")
    }

    pub fn metadata(&self) -> &PackMetadata {
        &self.metadata
    }

    pub fn required_secrets(&self) -> &[greentic_types::SecretRequirement] {
        &self.metadata.secret_requirements
    }

    pub fn missing_secrets(
        &self,
        tenant_ctx: &TypesTenantCtx,
    ) -> Vec<greentic_types::SecretRequirement> {
        let env = tenant_ctx.env.as_str().to_string();
        let tenant = tenant_ctx.tenant.as_str().to_string();
        let team = tenant_ctx.team.as_ref().map(|t| t.as_str().to_string());
        self.required_secrets()
            .iter()
            .filter(|req| {
                // scope must match current context if provided
                if let Some(scope) = &req.scope {
                    if scope.env != env {
                        return false;
                    }
                    if scope.tenant != tenant {
                        return false;
                    }
                    if let Some(ref team_req) = scope.team
                        && team.as_ref() != Some(team_req)
                    {
                        return false;
                    }
                }
                let ctx = self.config.tenant_ctx();
                read_secret_blocking(&self.secrets, &ctx, req.key.as_str()).is_err()
            })
            .cloned()
            .collect()
    }

    pub fn for_component_test(
        components: Vec<(String, PathBuf)>,
        flows: HashMap<String, FlowIR>,
        pack_id: &str,
        config: Arc<HostConfig>,
    ) -> Result<Self> {
        let engine = Engine::default();
        let engine_profile =
            EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
        let cache = CacheManager::new(CacheConfig::default(), engine_profile);
        let mut component_map = HashMap::new();
        for (name, path) in components {
            if !path.exists() {
                bail!("component artifact missing: {}", path.display());
            }
            let wasm_bytes = std::fs::read(&path)?;
            let component = Arc::new(
                Component::from_binary(&engine, &wasm_bytes)
                    .with_context(|| format!("failed to compile component {}", path.display()))?,
            );
            component_map.insert(
                name.clone(),
                PackComponent {
                    name,
                    version: "0.0.0".into(),
                    component,
                },
            );
        }

        let mut flow_map = HashMap::new();
        let mut descriptors = Vec::new();
        for (id, ir) in flows {
            let flow_type = ir.flow_type.clone();
            let flow = flow_ir_to_flow(ir)?;
            flow_map.insert(id.clone(), flow);
            descriptors.push(FlowDescriptor {
                id: id.clone(),
                flow_type,
                pack_id: pack_id.to_string(),
                profile: "test".into(),
                version: "0.0.0".into(),
                description: None,
            });
        }
        let entry_flows = descriptors.iter().map(|flow| flow.id.clone()).collect();
        let metadata = PackMetadata {
            pack_id: pack_id.to_string(),
            version: "0.0.0".into(),
            entry_flows,
            secret_requirements: Vec::new(),
        };
        let flows_cache = PackFlows {
            descriptors: descriptors.clone(),
            flows: flow_map,
            metadata: metadata.clone(),
        };

        Ok(Self {
            path: PathBuf::new(),
            archive_path: None,
            config,
            engine,
            metadata,
            manifest: None,
            legacy_manifest: None,
            component_manifests: HashMap::new(),
            mocks: None,
            flows: Some(flows_cache),
            components: component_map,
            http_client: Arc::clone(&HTTP_CLIENT),
            pre_cache: Mutex::new(HashMap::new()),
            session_store: None,
            state_store: None,
            wasi_policy: Arc::new(RunnerWasiPolicy::new()),
            provider_registry: RwLock::new(None),
            secrets: crate::secrets::default_manager()?,
            oauth_config: None,
            cache,
        })
    }
}

fn is_missing_node_export(err: &wasmtime::Error, version: &str) -> bool {
    let message = err.to_string();
    message.contains("no exported instance named")
        && message.contains(&format!("greentic:component/node@{version}"))
}

struct PackFlows {
    descriptors: Vec<FlowDescriptor>,
    flows: HashMap<String, Flow>,
    metadata: PackMetadata,
}

const RUNTIME_FLOW_EXTENSION_IDS: [&str; 3] = [
    "greentic.pack.runtime_flow",
    "greentic.pack.flow_runtime",
    "greentic.pack.runtime_flows",
];

#[derive(Debug, Deserialize)]
struct RuntimeFlowBundle {
    flows: Vec<RuntimeFlow>,
}

#[derive(Debug, Deserialize)]
struct RuntimeFlow {
    id: String,
    #[serde(alias = "flow_type")]
    kind: FlowKind,
    #[serde(default)]
    schema_version: Option<String>,
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    entrypoints: BTreeMap<String, Value>,
    nodes: BTreeMap<String, RuntimeNode>,
    #[serde(default)]
    metadata: Option<FlowMetadata>,
}

#[derive(Debug, Deserialize)]
struct RuntimeNode {
    #[serde(alias = "component")]
    component_id: String,
    #[serde(default, alias = "operation")]
    operation_name: Option<String>,
    #[serde(default, alias = "payload", alias = "input")]
    operation_payload: Value,
    #[serde(default)]
    routing: Option<Routing>,
    #[serde(default)]
    telemetry: Option<TelemetryHints>,
}

fn deserialize_json_bytes(bytes: Vec<u8>) -> Result<Value> {
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&bytes).or_else(|_| {
        String::from_utf8(bytes)
            .map(Value::String)
            .map_err(|err| anyhow!(err))
    })
}

impl PackFlows {
    fn from_manifest(manifest: greentic_types::PackManifest) -> Self {
        if let Some(flows) = flows_from_runtime_extension(&manifest) {
            return flows;
        }
        let descriptors = manifest
            .flows
            .iter()
            .map(|entry| FlowDescriptor {
                id: entry.id.as_str().to_string(),
                flow_type: flow_kind_to_str(entry.kind).to_string(),
                pack_id: manifest.pack_id.as_str().to_string(),
                profile: manifest.pack_id.as_str().to_string(),
                version: manifest.version.to_string(),
                description: None,
            })
            .collect();
        let mut flows = HashMap::new();
        for entry in &manifest.flows {
            flows.insert(entry.id.as_str().to_string(), entry.flow.clone());
        }
        Self {
            metadata: PackMetadata::from_manifest(&manifest),
            descriptors,
            flows,
        }
    }
}

fn flows_from_runtime_extension(manifest: &greentic_types::PackManifest) -> Option<PackFlows> {
    let extensions = manifest.extensions.as_ref()?;
    let extension = extensions.iter().find_map(|(key, ext)| {
        if RUNTIME_FLOW_EXTENSION_IDS
            .iter()
            .any(|candidate| candidate == key)
        {
            Some(ext)
        } else {
            None
        }
    })?;
    let runtime_flows = match decode_runtime_flow_extension(extension) {
        Some(flows) if !flows.is_empty() => flows,
        _ => return None,
    };

    let descriptors = runtime_flows
        .iter()
        .map(|flow| FlowDescriptor {
            id: flow.id.as_str().to_string(),
            flow_type: flow_kind_to_str(flow.kind).to_string(),
            pack_id: manifest.pack_id.as_str().to_string(),
            profile: manifest.pack_id.as_str().to_string(),
            version: manifest.version.to_string(),
            description: None,
        })
        .collect::<Vec<_>>();
    let flows = runtime_flows
        .into_iter()
        .map(|flow| (flow.id.as_str().to_string(), flow))
        .collect();

    Some(PackFlows {
        metadata: PackMetadata::from_manifest(manifest),
        descriptors,
        flows,
    })
}

fn decode_runtime_flow_extension(extension: &ExtensionRef) -> Option<Vec<Flow>> {
    let value = match extension.inline.as_ref()? {
        ExtensionInline::Other(value) => value.clone(),
        _ => return None,
    };

    if let Ok(bundle) = serde_json::from_value::<RuntimeFlowBundle>(value.clone()) {
        return Some(collect_runtime_flows(bundle.flows));
    }

    if let Ok(flows) = serde_json::from_value::<Vec<RuntimeFlow>>(value.clone()) {
        return Some(collect_runtime_flows(flows));
    }

    if let Ok(flows) = serde_json::from_value::<Vec<Flow>>(value) {
        return Some(flows);
    }

    warn!(
        extension = %extension.kind,
        version = %extension.version,
        "runtime flow extension present but could not be decoded"
    );
    None
}

fn collect_runtime_flows(flows: Vec<RuntimeFlow>) -> Vec<Flow> {
    flows
        .into_iter()
        .filter_map(|flow| match runtime_flow_to_flow(flow) {
            Ok(flow) => Some(flow),
            Err(err) => {
                warn!(error = %err, "failed to decode runtime flow");
                None
            }
        })
        .collect()
}

fn runtime_flow_to_flow(runtime: RuntimeFlow) -> Result<Flow> {
    let flow_id = FlowId::from_str(&runtime.id)
        .with_context(|| format!("invalid flow id `{}`", runtime.id))?;
    let mut entrypoints = runtime.entrypoints;
    if entrypoints.is_empty()
        && let Some(start) = &runtime.start
    {
        entrypoints.insert("default".into(), Value::String(start.clone()));
    }

    let mut nodes: IndexMap<NodeId, Node, FlowHasher> = IndexMap::default();
    for (id, node) in runtime.nodes {
        let node_id = NodeId::from_str(&id).with_context(|| format!("invalid node id `{id}`"))?;
        let component_id = ComponentId::from_str(&node.component_id)
            .with_context(|| format!("invalid component id `{}`", node.component_id))?;
        let component = FlowComponentRef {
            id: component_id,
            pack_alias: None,
            operation: node.operation_name,
        };
        let routing = node.routing.unwrap_or(Routing::End);
        let telemetry = node.telemetry.unwrap_or_default();
        nodes.insert(
            node_id.clone(),
            Node {
                id: node_id,
                component,
                input: InputMapping {
                    mapping: node.operation_payload,
                },
                output: OutputMapping {
                    mapping: Value::Null,
                },
                routing,
                telemetry,
            },
        );
    }

    Ok(Flow {
        schema_version: runtime.schema_version.unwrap_or_else(|| "1.0".to_string()),
        id: flow_id,
        kind: runtime.kind,
        entrypoints,
        nodes,
        metadata: runtime.metadata.unwrap_or_default(),
    })
}

fn flow_kind_to_str(kind: greentic_types::FlowKind) -> &'static str {
    match kind {
        greentic_types::FlowKind::Messaging => "messaging",
        greentic_types::FlowKind::Event => "event",
        greentic_types::FlowKind::ComponentConfig => "component-config",
        greentic_types::FlowKind::Job => "job",
        greentic_types::FlowKind::Http => "http",
    }
}

fn read_entry(archive: &mut ZipArchive<File>, name: &str) -> Result<Vec<u8>> {
    let mut file = archive
        .by_name(name)
        .with_context(|| format!("entry {name} missing from archive"))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

fn normalize_flow_doc(mut doc: FlowDoc) -> FlowDoc {
    for node in doc.nodes.values_mut() {
        let Some((component_ref, payload)) = node
            .raw
            .iter()
            .next()
            .map(|(key, value)| (key.clone(), value.clone()))
        else {
            continue;
        };
        if component_ref.starts_with("emit.") {
            node.operation = Some(component_ref);
            node.payload = payload;
            node.raw.clear();
            continue;
        }
        let (target_component, operation, input, config) =
            infer_component_exec(&payload, &component_ref);
        let mut payload_obj = serde_json::Map::new();
        // component.exec is meta; ensure the payload carries the actual target component.
        payload_obj.insert("component".into(), Value::String(target_component));
        payload_obj.insert("operation".into(), Value::String(operation));
        payload_obj.insert("input".into(), input);
        if let Some(cfg) = config {
            payload_obj.insert("config".into(), cfg);
        }
        node.operation = Some("component.exec".to_string());
        node.payload = Value::Object(payload_obj);
        node.raw.clear();
    }
    doc
}

fn infer_component_exec(
    payload: &Value,
    component_ref: &str,
) -> (String, String, Value, Option<Value>) {
    let default_op = if component_ref.starts_with("templating.") {
        "render"
    } else {
        "invoke"
    }
    .to_string();

    if let Value::Object(map) = payload {
        let op = map
            .get("op")
            .or_else(|| map.get("operation"))
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .unwrap_or_else(|| default_op.clone());

        let mut input = map.clone();
        let config = input.remove("config");
        let component = input
            .get("component")
            .or_else(|| input.get("component_ref"))
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .unwrap_or_else(|| component_ref.to_string());
        input.remove("component");
        input.remove("component_ref");
        input.remove("op");
        input.remove("operation");
        return (component, op, Value::Object(input), config);
    }

    (component_ref.to_string(), default_op, payload.clone(), None)
}

#[derive(Clone, Debug)]
struct ComponentSpec {
    id: String,
    version: String,
    legacy_path: Option<String>,
}

#[derive(Clone, Debug)]
struct ComponentSourceInfo {
    digest: Option<String>,
    source: ComponentSourceRef,
    artifact: ComponentArtifactLocation,
    expected_wasm_sha256: Option<String>,
    skip_digest_verification: bool,
}

#[derive(Clone, Debug)]
enum ComponentArtifactLocation {
    Inline { wasm_path: String },
    Remote,
}

#[derive(Clone, Debug, Deserialize)]
struct PackLockV1 {
    schema_version: u32,
    components: Vec<PackLockComponent>,
}

#[derive(Clone, Debug, Deserialize)]
struct PackLockComponent {
    name: String,
    #[serde(default, rename = "source_ref")]
    source_ref: Option<String>,
    #[serde(default, rename = "ref")]
    legacy_ref: Option<String>,
    #[serde(default)]
    component_id: Option<ComponentId>,
    #[serde(default)]
    bundled: Option<bool>,
    #[serde(default, rename = "bundled_path")]
    bundled_path: Option<String>,
    #[serde(default, rename = "path")]
    legacy_path: Option<String>,
    #[serde(default)]
    wasm_sha256: Option<String>,
    #[serde(default, rename = "sha256")]
    legacy_sha256: Option<String>,
    #[serde(default)]
    resolved_digest: Option<String>,
    #[serde(default)]
    digest: Option<String>,
}

fn component_specs(
    manifest: Option<&greentic_types::PackManifest>,
    legacy_manifest: Option<&legacy_pack::PackManifest>,
    component_sources: Option<&ComponentSourcesV1>,
    pack_lock: Option<&PackLockV1>,
) -> Vec<ComponentSpec> {
    if let Some(manifest) = manifest {
        if !manifest.components.is_empty() {
            return manifest
                .components
                .iter()
                .map(|entry| ComponentSpec {
                    id: entry.id.as_str().to_string(),
                    version: entry.version.to_string(),
                    legacy_path: None,
                })
                .collect();
        }
        if let Some(lock) = pack_lock {
            let mut seen = HashSet::new();
            let mut specs = Vec::new();
            for entry in &lock.components {
                let id = entry
                    .component_id
                    .as_ref()
                    .map(|id| id.as_str())
                    .unwrap_or(entry.name.as_str());
                if seen.insert(id.to_string()) {
                    specs.push(ComponentSpec {
                        id: id.to_string(),
                        version: "0.0.0".to_string(),
                        legacy_path: None,
                    });
                }
            }
            return specs;
        }
        if let Some(sources) = component_sources {
            let mut seen = HashSet::new();
            let mut specs = Vec::new();
            for entry in &sources.components {
                let id = entry
                    .component_id
                    .as_ref()
                    .map(|id| id.as_str())
                    .unwrap_or(entry.name.as_str());
                if seen.insert(id.to_string()) {
                    specs.push(ComponentSpec {
                        id: id.to_string(),
                        version: "0.0.0".to_string(),
                        legacy_path: None,
                    });
                }
            }
            return specs;
        }
    }
    if let Some(legacy_manifest) = legacy_manifest {
        return legacy_manifest
            .components
            .iter()
            .map(|entry| ComponentSpec {
                id: entry.name.clone(),
                version: entry.version.to_string(),
                legacy_path: Some(entry.file_wasm.clone()),
            })
            .collect();
    }
    Vec::new()
}

fn component_sources_table(
    sources: Option<&ComponentSourcesV1>,
) -> Result<Option<HashMap<String, ComponentSourceInfo>>> {
    let Some(sources) = sources else {
        return Ok(None);
    };
    let mut table = HashMap::new();
    for entry in &sources.components {
        let artifact = match &entry.artifact {
            ArtifactLocationV1::Inline { wasm_path, .. } => ComponentArtifactLocation::Inline {
                wasm_path: wasm_path.clone(),
            },
            ArtifactLocationV1::Remote => ComponentArtifactLocation::Remote,
        };
        let info = ComponentSourceInfo {
            digest: Some(entry.resolved.digest.clone()),
            source: entry.source.clone(),
            artifact,
            expected_wasm_sha256: None,
            skip_digest_verification: false,
        };
        if let Some(component_id) = entry.component_id.as_ref() {
            table.insert(component_id.as_str().to_string(), info.clone());
        }
        table.insert(entry.name.clone(), info);
    }
    Ok(Some(table))
}

fn load_pack_lock(path: &Path) -> Result<Option<PackLockV1>> {
    let lock_path = if path.is_dir() {
        let candidate = path.join("pack.lock");
        if candidate.exists() {
            Some(candidate)
        } else {
            let candidate = path.join("pack.lock.json");
            candidate.exists().then_some(candidate)
        }
    } else {
        None
    };
    let Some(lock_path) = lock_path else {
        return Ok(None);
    };
    let raw = std::fs::read_to_string(&lock_path)
        .with_context(|| format!("failed to read {}", lock_path.display()))?;
    let lock: PackLockV1 = serde_json::from_str(&raw).context("failed to parse pack.lock")?;
    if lock.schema_version != 1 {
        bail!("pack.lock schema_version must be 1");
    }
    Ok(Some(lock))
}

fn find_pack_lock_roots(
    pack_path: &Path,
    is_dir: bool,
    archive_hint: Option<&Path>,
) -> Vec<PathBuf> {
    if is_dir {
        return vec![pack_path.to_path_buf()];
    }
    let mut roots = Vec::new();
    if let Some(archive_path) = archive_hint {
        if let Some(parent) = archive_path.parent() {
            roots.push(parent.to_path_buf());
            if let Some(grandparent) = parent.parent() {
                roots.push(grandparent.to_path_buf());
            }
        }
    } else if let Some(parent) = pack_path.parent() {
        roots.push(parent.to_path_buf());
        if let Some(grandparent) = parent.parent() {
            roots.push(grandparent.to_path_buf());
        }
    }
    roots
}

fn normalize_sha256(digest: &str) -> Result<String> {
    let trimmed = digest.trim();
    if trimmed.is_empty() {
        bail!("sha256 digest cannot be empty");
    }
    if let Some(stripped) = trimmed.strip_prefix("sha256:") {
        if stripped.is_empty() {
            bail!("sha256 digest must include hex bytes after sha256:");
        }
        return Ok(trimmed.to_string());
    }
    if trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
        return Ok(format!("sha256:{trimmed}"));
    }
    bail!("sha256 digest must be hex or sha256:<hex>");
}

fn component_sources_table_from_pack_lock(
    lock: &PackLockV1,
    allow_missing_hash: bool,
) -> Result<HashMap<String, ComponentSourceInfo>> {
    let mut table = HashMap::new();
    let mut names = HashSet::new();
    for entry in &lock.components {
        if !names.insert(entry.name.clone()) {
            bail!(
                "pack.lock contains duplicate component name `{}`",
                entry.name
            );
        }
        let source_ref = match (&entry.source_ref, &entry.legacy_ref) {
            (Some(primary), Some(legacy)) => {
                if primary != legacy {
                    bail!(
                        "pack.lock component {} has conflicting refs: {} vs {}",
                        entry.name,
                        primary,
                        legacy
                    );
                }
                primary.as_str()
            }
            (Some(primary), None) => primary.as_str(),
            (None, Some(legacy)) => legacy.as_str(),
            (None, None) => {
                bail!("pack.lock component {} missing source_ref", entry.name);
            }
        };
        let source: ComponentSourceRef = source_ref
            .parse()
            .with_context(|| format!("invalid component ref `{}`", source_ref))?;
        let bundled_path = match (&entry.bundled_path, &entry.legacy_path) {
            (Some(primary), Some(legacy)) => {
                if primary != legacy {
                    bail!(
                        "pack.lock component {} has conflicting bundled paths: {} vs {}",
                        entry.name,
                        primary,
                        legacy
                    );
                }
                Some(primary.clone())
            }
            (Some(primary), None) => Some(primary.clone()),
            (None, Some(legacy)) => Some(legacy.clone()),
            (None, None) => None,
        };
        let bundled = entry.bundled.unwrap_or(false) || bundled_path.is_some();
        let (artifact, digest, expected_wasm_sha256, skip_digest_verification) = if bundled {
            let wasm_path = bundled_path.ok_or_else(|| {
                anyhow!(
                    "pack.lock component {} marked bundled but bundled_path is missing",
                    entry.name
                )
            })?;
            let expected_raw = match (&entry.wasm_sha256, &entry.legacy_sha256) {
                (Some(primary), Some(legacy)) => {
                    if primary != legacy {
                        bail!(
                            "pack.lock component {} has conflicting wasm_sha256 values: {} vs {}",
                            entry.name,
                            primary,
                            legacy
                        );
                    }
                    Some(primary.as_str())
                }
                (Some(primary), None) => Some(primary.as_str()),
                (None, Some(legacy)) => Some(legacy.as_str()),
                (None, None) => None,
            };
            let expected = match expected_raw {
                Some(value) => Some(normalize_sha256(value)?),
                None => None,
            };
            if expected.is_none() && !allow_missing_hash {
                bail!(
                    "pack.lock component {} missing wasm_sha256 for bundled component",
                    entry.name
                );
            }
            (
                ComponentArtifactLocation::Inline { wasm_path },
                expected.clone(),
                expected,
                allow_missing_hash && expected_raw.is_none(),
            )
        } else {
            if source.is_tag() {
                bail!(
                    "component {} uses tag ref {} but is not bundled; rebuild the pack",
                    entry.name,
                    source
                );
            }
            let expected = entry
                .resolved_digest
                .as_deref()
                .or(entry.digest.as_deref())
                .ok_or_else(|| {
                    anyhow!(
                        "pack.lock component {} missing resolved_digest for remote component",
                        entry.name
                    )
                })?;
            (
                ComponentArtifactLocation::Remote,
                Some(normalize_digest(expected)),
                None,
                false,
            )
        };
        let info = ComponentSourceInfo {
            digest,
            source,
            artifact,
            expected_wasm_sha256,
            skip_digest_verification,
        };
        if let Some(component_id) = entry.component_id.as_ref() {
            let key = component_id.as_str().to_string();
            if table.contains_key(&key) {
                bail!(
                    "pack.lock contains duplicate component id `{}`",
                    component_id.as_str()
                );
            }
            table.insert(key, info.clone());
        }
        if entry.name
            != entry
                .component_id
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or("")
        {
            table.insert(entry.name.clone(), info);
        }
    }
    Ok(table)
}

fn component_path_for_spec(root: &Path, spec: &ComponentSpec) -> PathBuf {
    if let Some(path) = &spec.legacy_path {
        return root.join(path);
    }
    root.join("components").join(format!("{}.wasm", spec.id))
}

fn normalize_digest(digest: &str) -> String {
    if digest.starts_with("sha256:") || digest.starts_with("blake3:") {
        digest.to_string()
    } else {
        format!("sha256:{digest}")
    }
}

fn compute_digest_for(bytes: &[u8], digest: &str) -> Result<String> {
    if digest.starts_with("blake3:") {
        let hash = blake3::hash(bytes);
        return Ok(format!("blake3:{}", hash.to_hex()));
    }
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn compute_sha256_digest_for(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn build_artifact_key(cache: &CacheManager, digest: Option<&str>, bytes: &[u8]) -> ArtifactKey {
    let wasm_digest = digest
        .map(normalize_digest)
        .unwrap_or_else(|| compute_sha256_digest_for(bytes));
    ArtifactKey::new(cache.engine_profile_id().to_string(), wasm_digest)
}

async fn compile_component_with_cache(
    cache: &CacheManager,
    engine: &Engine,
    digest: Option<&str>,
    bytes: Vec<u8>,
) -> Result<Arc<Component>> {
    let key = build_artifact_key(cache, digest, &bytes);
    cache.get_component(engine, &key, || Ok(bytes)).await
}

fn verify_component_digest(component_id: &str, expected: &str, bytes: &[u8]) -> Result<()> {
    let normalized_expected = normalize_digest(expected);
    let actual = compute_digest_for(bytes, &normalized_expected)?;
    if normalize_digest(&actual) != normalized_expected {
        bail!(
            "component {component_id} digest mismatch: expected {normalized_expected}, got {actual}"
        );
    }
    Ok(())
}

fn verify_wasm_sha256(component_id: &str, expected: &str, bytes: &[u8]) -> Result<()> {
    let normalized_expected = normalize_sha256(expected)?;
    let actual = compute_sha256_digest_for(bytes);
    if actual != normalized_expected {
        bail!(
            "component {component_id} bundled digest mismatch: expected {normalized_expected}, got {actual}"
        );
    }
    Ok(())
}

#[cfg(test)]
mod pack_lock_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn pack_lock_tag_ref_requires_bundle() {
        let lock = PackLockV1 {
            schema_version: 1,
            components: vec![PackLockComponent {
                name: "templates".to_string(),
                source_ref: Some("oci://registry.test/templates:latest".to_string()),
                legacy_ref: None,
                component_id: None,
                bundled: Some(false),
                bundled_path: None,
                legacy_path: None,
                wasm_sha256: None,
                legacy_sha256: None,
                resolved_digest: None,
                digest: None,
            }],
        };
        let err = component_sources_table_from_pack_lock(&lock, false).unwrap_err();
        assert!(
            err.to_string().contains("tag ref") && err.to_string().contains("rebuild the pack"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn bundled_hash_mismatch_errors() {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let temp = TempDir::new().expect("temp dir");
        let engine = Engine::default();
        let engine_profile =
            EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
        let cache_config = CacheConfig {
            root: temp.path().join("cache"),
            ..CacheConfig::default()
        };
        let cache = CacheManager::new(cache_config, engine_profile);
        let wasm_path = temp.path().join("component.wasm");
        let fixture_wasm = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/packs/secrets_store_smoke/components/echo_secret.wasm");
        let bytes = std::fs::read(&fixture_wasm).expect("read fixture wasm");
        std::fs::write(&wasm_path, &bytes).expect("write temp wasm");

        let spec = ComponentSpec {
            id: "qa.process".to_string(),
            version: "0.0.0".to_string(),
            legacy_path: None,
        };
        let mut missing = HashSet::new();
        missing.insert(spec.id.clone());

        let mut sources = HashMap::new();
        sources.insert(
            spec.id.clone(),
            ComponentSourceInfo {
                digest: Some("sha256:deadbeef".to_string()),
                source: ComponentSourceRef::Oci("registry.test/qa.process@sha256:deadbeef".into()),
                artifact: ComponentArtifactLocation::Inline {
                    wasm_path: "component.wasm".to_string(),
                },
                expected_wasm_sha256: Some("sha256:deadbeef".to_string()),
                skip_digest_verification: false,
            },
        );

        let mut loaded = HashMap::new();
        let result = rt.block_on(load_components_from_sources(
            &cache,
            &engine,
            &sources,
            &ComponentResolution::default(),
            &[spec],
            &mut missing,
            &mut loaded,
            Some(temp.path()),
            None,
        ));
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("bundled digest mismatch"),
            "unexpected error: {err}"
        );
    }
}

fn dist_options_from(component_resolution: &ComponentResolution) -> DistOptions {
    let mut opts = DistOptions {
        allow_tags: true,
        ..DistOptions::default()
    };
    if let Some(cache_dir) = component_resolution.dist_cache_dir.clone() {
        opts.cache_dir = cache_dir;
    }
    if component_resolution.dist_offline {
        opts.offline = true;
    }
    opts
}

#[allow(clippy::too_many_arguments)]
async fn load_components_from_sources(
    cache: &CacheManager,
    engine: &Engine,
    component_sources: &HashMap<String, ComponentSourceInfo>,
    component_resolution: &ComponentResolution,
    specs: &[ComponentSpec],
    missing: &mut HashSet<String>,
    into: &mut HashMap<String, PackComponent>,
    materialized_root: Option<&Path>,
    archive_hint: Option<&Path>,
) -> Result<()> {
    let mut archive = if let Some(path) = archive_hint {
        Some(
            ZipArchive::new(File::open(path)?)
                .with_context(|| format!("{} is not a valid gtpack", path.display()))?,
        )
    } else {
        None
    };
    let mut dist_client: Option<DistClient> = None;

    for spec in specs {
        if !missing.contains(&spec.id) {
            continue;
        }
        let Some(source) = component_sources.get(&spec.id) else {
            continue;
        };

        let bytes = match &source.artifact {
            ComponentArtifactLocation::Inline { wasm_path } => {
                if let Some(root) = materialized_root {
                    let path = root.join(wasm_path);
                    if path.exists() {
                        std::fs::read(&path).with_context(|| {
                            format!(
                                "failed to read inline component {} from {}",
                                spec.id,
                                path.display()
                            )
                        })?
                    } else if archive.is_none() {
                        bail!("inline component {} missing at {}", spec.id, path.display());
                    } else {
                        read_entry(
                            archive.as_mut().expect("archive present when needed"),
                            wasm_path,
                        )
                        .with_context(|| {
                            format!(
                                "inline component {} missing at {} in pack archive",
                                spec.id, wasm_path
                            )
                        })?
                    }
                } else if let Some(archive) = archive.as_mut() {
                    read_entry(archive, wasm_path).with_context(|| {
                        format!(
                            "inline component {} missing at {} in pack archive",
                            spec.id, wasm_path
                        )
                    })?
                } else {
                    bail!(
                        "inline component {} missing and no pack source available",
                        spec.id
                    );
                }
            }
            ComponentArtifactLocation::Remote => {
                if source.source.is_tag() {
                    bail!(
                        "component {} uses tag ref {} but is not bundled; rebuild the pack",
                        spec.id,
                        source.source
                    );
                }
                let client = dist_client.get_or_insert_with(|| {
                    DistClient::new(dist_options_from(component_resolution))
                });
                let reference = source.source.to_string();
                fault::maybe_fail_asset(&reference)
                    .await
                    .with_context(|| format!("fault injection blocked asset {reference}"))?;
                let digest = source.digest.as_deref().ok_or_else(|| {
                    anyhow!(
                        "component {} missing expected digest for remote component",
                        spec.id
                    )
                })?;
                let cache_path = if component_resolution.dist_offline {
                    client
                        .fetch_digest(digest)
                        .await
                        .map_err(|err| dist_error_for_component(err, &spec.id, &reference))?
                } else {
                    let resolved = client
                        .resolve_ref(&reference)
                        .await
                        .map_err(|err| dist_error_for_component(err, &spec.id, &reference))?;
                    let expected = normalize_digest(digest);
                    let actual = normalize_digest(&resolved.digest);
                    if expected != actual {
                        bail!(
                            "component {} digest mismatch after fetch: expected {}, got {}",
                            spec.id,
                            expected,
                            actual
                        );
                    }
                    resolved.cache_path.ok_or_else(|| {
                        anyhow!(
                            "component {} resolved from {} but cache path is missing",
                            spec.id,
                            reference
                        )
                    })?
                };
                std::fs::read(&cache_path).with_context(|| {
                    format!(
                        "failed to read cached component {} from {}",
                        spec.id,
                        cache_path.display()
                    )
                })?
            }
        };

        if let Some(expected) = source.expected_wasm_sha256.as_deref() {
            verify_wasm_sha256(&spec.id, expected, &bytes)?;
        } else if source.skip_digest_verification {
            let actual = compute_sha256_digest_for(&bytes);
            warn!(
                component_id = %spec.id,
                digest = %actual,
                "bundled component missing wasm_sha256; allowing due to flag"
            );
        } else {
            let expected = source.digest.as_deref().ok_or_else(|| {
                anyhow!(
                    "component {} missing expected digest for verification",
                    spec.id
                )
            })?;
            verify_component_digest(&spec.id, expected, &bytes)?;
        }
        let component =
            compile_component_with_cache(cache, engine, source.digest.as_deref(), bytes)
                .await
                .with_context(|| format!("failed to compile component {}", spec.id))?;
        into.insert(
            spec.id.clone(),
            PackComponent {
                name: spec.id.clone(),
                version: spec.version.clone(),
                component,
            },
        );
        missing.remove(&spec.id);
    }

    Ok(())
}

fn dist_error_for_component(err: DistError, component_id: &str, reference: &str) -> anyhow::Error {
    match err {
        DistError::CacheMiss { reference: missing } => anyhow!(
            "remote component {} is not cached for {}. Run `greentic-dist pull --lock <pack.lock>` or `greentic-dist pull {}`",
            component_id,
            missing,
            reference
        ),
        DistError::Offline { reference: blocked } => anyhow!(
            "offline mode blocked fetching component {} from {}; run `greentic-dist pull --lock <pack.lock>` or `greentic-dist pull {}`",
            component_id,
            blocked,
            reference
        ),
        DistError::AuthRequired { target } => anyhow!(
            "component {} requires authenticated source {}; run `greentic-dist pull --lock <pack.lock>` or `greentic-dist pull {}`",
            component_id,
            target,
            reference
        ),
        other => anyhow!(
            "failed to resolve component {} from {}: {}",
            component_id,
            reference,
            other
        ),
    }
}

async fn load_components_from_overrides(
    cache: &CacheManager,
    engine: &Engine,
    overrides: &HashMap<String, PathBuf>,
    specs: &[ComponentSpec],
    missing: &mut HashSet<String>,
    into: &mut HashMap<String, PackComponent>,
) -> Result<()> {
    for spec in specs {
        if !missing.contains(&spec.id) {
            continue;
        }
        let Some(path) = overrides.get(&spec.id) else {
            continue;
        };
        let bytes = std::fs::read(path)
            .with_context(|| format!("failed to read override component {}", path.display()))?;
        let component = compile_component_with_cache(cache, engine, None, bytes)
            .await
            .with_context(|| {
                format!(
                    "failed to compile component {} from override {}",
                    spec.id,
                    path.display()
                )
            })?;
        into.insert(
            spec.id.clone(),
            PackComponent {
                name: spec.id.clone(),
                version: spec.version.clone(),
                component,
            },
        );
        missing.remove(&spec.id);
    }
    Ok(())
}

async fn load_components_from_dir(
    cache: &CacheManager,
    engine: &Engine,
    root: &Path,
    specs: &[ComponentSpec],
    missing: &mut HashSet<String>,
    into: &mut HashMap<String, PackComponent>,
) -> Result<()> {
    for spec in specs {
        if !missing.contains(&spec.id) {
            continue;
        }
        let path = component_path_for_spec(root, spec);
        if !path.exists() {
            tracing::debug!(component = %spec.id, path = %path.display(), "materialized component missing; will try other sources");
            continue;
        }
        let bytes = std::fs::read(&path)
            .with_context(|| format!("failed to read component {}", path.display()))?;
        let component = compile_component_with_cache(cache, engine, None, bytes)
            .await
            .with_context(|| {
                format!(
                    "failed to compile component {} from {}",
                    spec.id,
                    path.display()
                )
            })?;
        into.insert(
            spec.id.clone(),
            PackComponent {
                name: spec.id.clone(),
                version: spec.version.clone(),
                component,
            },
        );
        missing.remove(&spec.id);
    }
    Ok(())
}

async fn load_components_from_archive(
    cache: &CacheManager,
    engine: &Engine,
    path: &Path,
    specs: &[ComponentSpec],
    missing: &mut HashSet<String>,
    into: &mut HashMap<String, PackComponent>,
) -> Result<()> {
    let mut archive = ZipArchive::new(File::open(path)?)
        .with_context(|| format!("{} is not a valid gtpack", path.display()))?;
    for spec in specs {
        if !missing.contains(&spec.id) {
            continue;
        }
        let file_name = spec
            .legacy_path
            .clone()
            .unwrap_or_else(|| format!("components/{}.wasm", spec.id));
        let bytes = match read_entry(&mut archive, &file_name) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(component = %spec.id, pack = %path.display(), error = %err, "component entry missing in pack archive");
                continue;
            }
        };
        let component = compile_component_with_cache(cache, engine, None, bytes)
            .await
            .with_context(|| format!("failed to compile component {}", spec.id))?;
        into.insert(
            spec.id.clone(),
            PackComponent {
                name: spec.id.clone(),
                version: spec.version.clone(),
                component,
            },
        );
        missing.remove(&spec.id);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_flow::model::{FlowDoc, NodeDoc};
    use indexmap::IndexMap;
    use serde_json::json;

    #[test]
    fn normalizes_raw_component_to_component_exec() {
        let mut nodes = IndexMap::new();
        let mut raw = IndexMap::new();
        raw.insert(
            "templating.handlebars".into(),
            json!({ "template": "Hi {{name}}" }),
        );
        nodes.insert(
            "start".into(),
            NodeDoc {
                raw,
                routing: json!([{"out": true}]),
                ..Default::default()
            },
        );
        let doc = FlowDoc {
            id: "welcome".into(),
            title: None,
            description: None,
            flow_type: "messaging".into(),
            start: Some("start".into()),
            parameters: json!({}),
            tags: Vec::new(),
            schema_version: None,
            entrypoints: IndexMap::new(),
            nodes,
        };

        let normalized = normalize_flow_doc(doc);
        let node = normalized.nodes.get("start").expect("node exists");
        assert_eq!(node.operation.as_deref(), Some("component.exec"));
        assert!(node.raw.is_empty());
        let payload = node.payload.as_object().expect("payload object");
        assert_eq!(
            payload.get("component"),
            Some(&Value::String("templating.handlebars".into()))
        );
        assert_eq!(
            payload.get("operation"),
            Some(&Value::String("render".into()))
        );
        let input = payload.get("input").unwrap();
        assert_eq!(input, &json!({ "template": "Hi {{name}}" }));
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PackMetadata {
    pub pack_id: String,
    pub version: String,
    #[serde(default)]
    pub entry_flows: Vec<String>,
    #[serde(default)]
    pub secret_requirements: Vec<greentic_types::SecretRequirement>,
}

impl PackMetadata {
    fn from_wasm(bytes: &[u8]) -> Option<Self> {
        let parser = Parser::new(0);
        for payload in parser.parse_all(bytes) {
            let payload = payload.ok()?;
            match payload {
                Payload::CustomSection(section) => {
                    if section.name() == "greentic.manifest"
                        && let Ok(meta) = Self::from_bytes(section.data())
                    {
                        return Some(meta);
                    }
                }
                Payload::DataSection(reader) => {
                    for segment in reader.into_iter().flatten() {
                        if let Ok(meta) = Self::from_bytes(segment.data) {
                            return Some(meta);
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, serde_cbor::Error> {
        #[derive(Deserialize)]
        struct RawManifest {
            pack_id: String,
            version: String,
            #[serde(default)]
            entry_flows: Vec<String>,
            #[serde(default)]
            flows: Vec<RawFlow>,
            #[serde(default)]
            secret_requirements: Vec<greentic_types::SecretRequirement>,
        }

        #[derive(Deserialize)]
        struct RawFlow {
            id: String,
        }

        let manifest: RawManifest = serde_cbor::from_slice(bytes)?;
        let mut entry_flows = if manifest.entry_flows.is_empty() {
            manifest.flows.iter().map(|f| f.id.clone()).collect()
        } else {
            manifest.entry_flows.clone()
        };
        entry_flows.retain(|id| !id.is_empty());
        Ok(Self {
            pack_id: manifest.pack_id,
            version: manifest.version,
            entry_flows,
            secret_requirements: manifest.secret_requirements,
        })
    }

    pub fn fallback(path: &Path) -> Self {
        let pack_id = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unknown-pack".to_string());
        Self {
            pack_id,
            version: "0.0.0".to_string(),
            entry_flows: Vec::new(),
            secret_requirements: Vec::new(),
        }
    }

    pub fn from_manifest(manifest: &greentic_types::PackManifest) -> Self {
        let entry_flows = manifest
            .flows
            .iter()
            .map(|flow| flow.id.as_str().to_string())
            .collect::<Vec<_>>();
        Self {
            pack_id: manifest.pack_id.as_str().to_string(),
            version: manifest.version.to_string(),
            entry_flows,
            secret_requirements: manifest.secret_requirements.clone(),
        }
    }
}
