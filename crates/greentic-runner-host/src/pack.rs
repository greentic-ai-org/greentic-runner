use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::component_api::component::greentic::component::control::Host as ComponentControlHost;
use crate::component_api::{
    ComponentPre, control, node::ExecCtx as ComponentExecCtx, node::InvokeResult, node::NodeError,
};
use crate::oauth::{OAuthBrokerConfig, OAuthBrokerHost, OAuthHostContext};
use crate::provider::{ProviderBinding, ProviderRegistry};
use crate::provider_core::SchemaCorePre as ProviderComponentPre;
use crate::provider_core_only;
use crate::runtime_wasmtime::{Component, Engine, Linker, ResourceTable};
use anyhow::{Context, Result, anyhow, bail};
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
use greentic_types::{
    EnvId, Flow, StateKey as StoreStateKey, TeamId, TenantCtx as TypesTenantCtx, TenantId, UserId,
    decode_pack_manifest,
};
use host_v1::http_client::{
    HttpClientError, HttpClientErrorV1_1, HttpClientHost, HttpClientHostV1_1,
    Request as HttpRequest, RequestOptionsV1_1 as HttpRequestOptionsV1_1,
    RequestV1_1 as HttpRequestV1_1, Response as HttpResponse, ResponseV1_1 as HttpResponseV1_1,
    TenantCtx as HttpTenantCtx, TenantCtxV1_1 as HttpTenantCtxV1_1,
};
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use reqwest::blocking::Client as BlockingClient;
use runner_core::normalize_under_root;
use serde::{Deserialize, Serialize};
use serde_cbor;
use serde_json::{self, Value};
use tokio::fs;
use wasmparser::{Parser, Payload};
use wasmtime::StoreContextMut;
use zip::ZipArchive;

use crate::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use crate::runner::flow_adapter::{FlowIR, flow_doc_to_ir, flow_ir_to_flow};
use crate::runner::mocks::{HttpDecision, HttpMockRequest, HttpMockResponse, MockLayer};

use crate::config::HostConfig;
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
    mocks: Option<Arc<MockLayer>>,
    flows: Option<PackFlows>,
    components: HashMap<String, PackComponent>,
    http_client: Arc<BlockingClient>,
    pre_cache: Mutex<HashMap<String, ComponentPre<ComponentState>>>,
    session_store: Option<DynSessionStore>,
    state_store: Option<DynStateStore>,
    wasi_policy: Arc<RunnerWasiPolicy>,
    provider_registry: RwLock<Option<ProviderRegistry>>,
    secrets: DynSecretsManager,
    oauth_config: Option<OAuthBrokerConfig>,
}

struct PackComponent {
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    version: String,
    component: Component,
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
}

impl HostState {
    #[allow(clippy::default_constructed_unit_structs)]
    pub fn new(
        config: Arc<HostConfig>,
        http_client: Arc<BlockingClient>,
        mocks: Option<Arc<MockLayer>>,
        session_store: Option<DynSessionStore>,
        state_store: Option<DynStateStore>,
        secrets: DynSecretsManager,
        oauth_config: Option<OAuthBrokerConfig>,
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
        let bytes = read_secret_blocking(&self.secrets, key)
            .context("failed to read secret from manager")?;
        let value = String::from_utf8(bytes).context("secret value is not valid UTF-8")?;
        Ok(value)
    }

    fn tenant_ctx_from_v1(&self, ctx: Option<StateTenantCtx>) -> Result<TypesTenantCtx> {
        let tenant_raw = ctx
            .as_ref()
            .map(|ctx| ctx.tenant.clone())
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
        match read_secret_blocking(&self.secrets, &key) {
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
}

impl control::Host for ComponentState {
    fn should_cancel(&mut self) -> bool {
        false
    }

    fn yield_now(&mut self) {
        // no-op cooperative yield
    }
}

fn add_component_control_to_linker(linker: &mut Linker<ComponentState>) -> wasmtime::Result<()> {
    let mut inst = linker.instance("greentic:component/control@0.4.0")?;
    inst.func_wrap(
        "should-cancel",
        |mut caller: StoreContextMut<'_, ComponentState>, (): ()| {
            let host = caller.data_mut();
            Ok((ComponentControlHost::should_cancel(host),))
        },
    )?;
    inst.func_wrap(
        "yield-now",
        |mut caller: StoreContextMut<'_, ComponentState>, (): ()| {
            let host = caller.data_mut();
            ComponentControlHost::yield_now(host);
            Ok(())
        },
    )?;
    Ok(())
}

pub fn register_all(linker: &mut Linker<ComponentState>) -> Result<()> {
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
            state_store: Some(|state| state.host_mut()),
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
    ) -> Result<Self> {
        let path = path.as_ref();
        let (_pack_root, safe_path) = normalize_pack_path(path)?;
        let is_component = safe_path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("wasm"))
            .unwrap_or(false);
        let archive_hint_path = if let Some(source) = archive_source {
            let (_, normalized) = normalize_pack_path(source)?;
            Some(normalized)
        } else if is_component {
            None
        } else {
            Some(safe_path.clone())
        };
        let archive_hint = archive_hint_path.as_deref();
        if verify_archive {
            let verify_target = archive_hint.unwrap_or(&safe_path);
            verify::verify_pack(verify_target).await?;
            tracing::info!(pack_path = %verify_target.display(), "pack verification complete");
        }
        let engine = Engine::default();
        let wasm_bytes = fs::read(&safe_path).await?;
        let mut metadata = PackMetadata::from_wasm(&wasm_bytes)
            .unwrap_or_else(|| PackMetadata::fallback(&safe_path));
        let mut manifest = None;
        let mut legacy_manifest: Option<Box<legacy_pack::PackManifest>> = None;
        let flows = if let Some(archive_path) = archive_hint {
            match load_manifest_and_flows(archive_path) {
                Ok(ManifestLoad::New {
                    manifest: m,
                    flows: cache,
                }) => {
                    metadata = cache.metadata.clone();
                    manifest = Some(*m);
                    Some(cache)
                }
                Ok(ManifestLoad::Legacy {
                    manifest: m,
                    flows: cache,
                }) => {
                    metadata = cache.metadata.clone();
                    legacy_manifest = Some(m);
                    Some(cache)
                }
                Err(err) => {
                    warn!(error = %err, pack = %archive_path.display(), "failed to parse pack manifest; skipping flows");
                    None
                }
            }
        } else {
            None
        };
        let components = if let Some(archive_path) = archive_hint {
            if let Some(new_manifest) = manifest.as_ref() {
                match load_components_from_archive(&engine, archive_path, Some(new_manifest)) {
                    Ok(map) => map,
                    Err(err) => {
                        warn!(error = %err, pack = %archive_path.display(), "failed to load components from archive");
                        HashMap::new()
                    }
                }
            } else if let Some(legacy) = legacy_manifest.as_ref() {
                match load_legacy_components_from_archive(&engine, archive_path, legacy) {
                    Ok(map) => map,
                    Err(err) => {
                        warn!(error = %err, pack = %archive_path.display(), "failed to load components from archive");
                        HashMap::new()
                    }
                }
            } else {
                HashMap::new()
            }
        } else if is_component {
            let name = safe_path
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "component".to_string());
            let component = Component::from_binary(&engine, &wasm_bytes)?;
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
            HashMap::new()
        };
        let http_client = Arc::clone(&HTTP_CLIENT);
        Ok(Self {
            path: safe_path,
            archive_path: archive_hint.map(Path::to_path_buf),
            config,
            engine,
            metadata,
            manifest,
            legacy_manifest,
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
            )
            .await?,
        );

        let engine = FlowEngine::new(vec![Arc::clone(&pack)], Arc::clone(&self.config)).await?;
        let retry_config = self.config.retry_config().into();
        let mocks = pack.mocks.as_deref();
        let tenant = self.config.tenant.as_str();

        let ctx = FlowContext {
            tenant,
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
        register_all(&mut linker)?;
        add_component_control_to_linker(&mut linker)?;
        let pre_instance = linker.instantiate_pre(&pack_component.component)?;
        let pre: ComponentPre<ComponentState> = ComponentPre::new(pre_instance)?;

        let host_state = HostState::new(
            Arc::clone(&self.config),
            Arc::clone(&self.http_client),
            self.mocks.clone(),
            self.session_store.clone(),
            self.state_store.clone(),
            Arc::clone(&self.secrets),
            self.oauth_config.clone(),
        )?;
        let store_state = ComponentState::new(host_state, Arc::clone(&self.wasi_policy))?;
        let mut store = wasmtime::Store::new(&self.engine, store_state);
        let bindings: crate::component_api::Component = pre.instantiate_async(&mut store).await?;
        let node = bindings.greentic_component_node();

        let result = node.call_invoke(&mut store, &ctx, operation, &input_json)?;

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
        _ctx: ComponentExecCtx,
        op: &str,
        input_json: Vec<u8>,
    ) -> Result<Value> {
        let component_ref = &binding.component_ref;
        let pack_component = self
            .components
            .get(component_ref)
            .with_context(|| format!("provider component '{component_ref}' not found in pack"))?;

        let mut linker = Linker::new(&self.engine);
        register_all(&mut linker)?;
        add_component_control_to_linker(&mut linker)?;
        let pre_instance = linker.instantiate_pre(&pack_component.component)?;
        let pre: ProviderComponentPre<ComponentState> = ProviderComponentPre::new(pre_instance)?;

        let host_state = HostState::new(
            Arc::clone(&self.config),
            Arc::clone(&self.http_client),
            self.mocks.clone(),
            self.session_store.clone(),
            self.state_store.clone(),
            Arc::clone(&self.secrets),
            self.oauth_config.clone(),
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
                read_secret_blocking(&self.secrets, req.key.as_str()).is_err()
            })
            .cloned()
            .collect()
    }

    pub fn for_component_test(
        components: Vec<(String, PathBuf)>,
        flows: HashMap<String, FlowIR>,
        config: Arc<HostConfig>,
    ) -> Result<Self> {
        let engine = Engine::default();
        let mut component_map = HashMap::new();
        for (name, path) in components {
            if !path.exists() {
                bail!("component artifact missing: {}", path.display());
            }
            let wasm_bytes = std::fs::read(&path)?;
            let component = Component::from_binary(&engine, &wasm_bytes)
                .with_context(|| format!("failed to compile component {}", path.display()))?;
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
                profile: "test".into(),
                version: "0.0.0".into(),
                description: None,
            });
        }
        let flows_cache = PackFlows {
            descriptors: descriptors.clone(),
            flows: flow_map,
            metadata: PackMetadata::fallback(Path::new("component-test")),
        };

        Ok(Self {
            path: PathBuf::new(),
            archive_path: None,
            config,
            engine,
            metadata: PackMetadata::fallback(Path::new("component-test")),
            manifest: None,
            legacy_manifest: None,
            mocks: None,
            flows: Some(flows_cache),
            components: component_map,
            http_client: Arc::clone(&HTTP_CLIENT),
            pre_cache: Mutex::new(HashMap::new()),
            session_store: None,
            state_store: None,
            wasi_policy: Arc::new(RunnerWasiPolicy::new()),
            provider_registry: RwLock::new(None),
            secrets: crate::secrets::default_manager(),
            oauth_config: None,
        })
    }
}

struct PackFlows {
    descriptors: Vec<FlowDescriptor>,
    flows: HashMap<String, Flow>,
    metadata: PackMetadata,
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
        let descriptors = manifest
            .flows
            .iter()
            .map(|entry| FlowDescriptor {
                id: entry.id.as_str().to_string(),
                flow_type: flow_kind_to_str(entry.kind).to_string(),
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
        if node.component.is_empty()
            && let Some((component_ref, payload)) = node.raw.iter().next()
        {
            if component_ref.starts_with("emit.") {
                node.component = component_ref.clone();
                node.payload = payload.clone();
                node.raw.clear();
                continue;
            }
            let (target_component, operation, input, config) =
                infer_component_exec(payload, component_ref);
            let mut payload_obj = serde_json::Map::new();
            // component.exec is meta; ensure the payload carries the actual target component.
            payload_obj.insert("component".into(), Value::String(target_component));
            payload_obj.insert("operation".into(), Value::String(operation));
            payload_obj.insert("input".into(), input);
            if let Some(cfg) = config {
                payload_obj.insert("config".into(), cfg);
            }
            node.component = "component.exec".to_string();
            node.payload = Value::Object(payload_obj);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_flow::model::{FlowDoc, NodeDoc};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn normalizes_raw_component_to_component_exec() {
        let mut nodes = BTreeMap::new();
        let mut raw = BTreeMap::new();
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
            entrypoints: BTreeMap::new(),
            nodes,
        };

        let normalized = normalize_flow_doc(doc);
        let node = normalized.nodes.get("start").expect("node exists");
        assert_eq!(node.component, "component.exec");
        assert!(node.raw.is_empty() || node.raw.contains_key("templating.handlebars"));
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

fn load_components_from_archive(
    engine: &Engine,
    path: &Path,
    manifest: Option<&greentic_types::PackManifest>,
) -> Result<HashMap<String, PackComponent>> {
    let mut archive = ZipArchive::new(File::open(path)?)
        .with_context(|| format!("{} is not a valid gtpack", path.display()))?;
    let mut components = HashMap::new();
    if let Some(manifest) = manifest {
        for entry in &manifest.components {
            let file_name = format!("components/{}.wasm", entry.id.as_str());
            let bytes = read_entry(&mut archive, &file_name)
                .with_context(|| format!("missing component {}", file_name))?;
            let component = Component::from_binary(engine, &bytes)
                .with_context(|| format!("failed to compile component {}", entry.id.as_str()))?;
            components.insert(
                entry.id.as_str().to_string(),
                PackComponent {
                    name: entry.id.as_str().to_string(),
                    version: entry.version.to_string(),
                    component,
                },
            );
        }
    }
    Ok(components)
}

fn load_legacy_components_from_archive(
    engine: &Engine,
    path: &Path,
    manifest: &legacy_pack::PackManifest,
) -> Result<HashMap<String, PackComponent>> {
    let mut archive = ZipArchive::new(File::open(path)?)
        .with_context(|| format!("{} is not a valid gtpack", path.display()))?;
    let mut components = HashMap::new();
    for entry in &manifest.components {
        let bytes = read_entry(&mut archive, &entry.file_wasm)
            .with_context(|| format!("missing component {}", entry.file_wasm))?;
        let component = Component::from_binary(engine, &bytes)
            .with_context(|| format!("failed to compile component {}", entry.name))?;
        components.insert(
            entry.name.clone(),
            PackComponent {
                name: entry.name.clone(),
                version: entry.version.to_string(),
                component,
            },
        );
    }
    Ok(components)
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
