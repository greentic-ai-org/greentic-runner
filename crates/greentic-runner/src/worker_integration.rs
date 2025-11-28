//! Worker world integration helpers for `greentic:worker@1.0.0`.

use std::str::FromStr;
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use greentic_interfaces::worker_v1::exports::greentic::worker::worker_api::{
    WorkerRequest as WitWorkerRequest, WorkerResponse as WitWorkerResponse,
};
use greentic_interfaces::worker_v1::greentic::types_core::types::{
    Cloud as WitCloud, DeploymentCtx as WitDeploymentCtx, Platform as WitPlatform,
    TenantCtx as WitTenantCtx,
};
use greentic_interfaces_host::worker;
use greentic_types::{
    EnvId, GreenticError, TeamId, TenantCtx, TenantId, UserId, WorkerMessage, WorkerRequest,
    WorkerResponse,
};
use wasmtime::Store;
use wasmtime::component::Linker;
use wasmtime_wasi::p2::add_to_linker_sync as add_wasi_to_linker;
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

/// Execute a worker component using the `greentic:worker/worker@1.0.0` world.
pub fn execute_worker(
    engine: &wasmtime::Engine,
    component: &wasmtime::component::Component,
    tenant: &TenantCtx,
    request: &WorkerRequest,
) -> Result<WorkerResponse> {
    let mut store = Store::new(engine, WasiState::new()?);
    let mut linker = Linker::new(engine);
    add_wasi_to_linker(&mut linker).map_err(|err| anyhow!(err))?;
    let instance =
        worker::Worker::instantiate(&mut store, component, &linker).map_err(|err| anyhow!(err))?;
    let exports = instance.greentic_worker_worker_api();
    let wit_req = to_wit_request(request, tenant)?;
    let resp = exports
        .call_exec(&mut store, &wit_req)
        .context("worker exec failed")?;
    match resp {
        Ok(value) => from_wit_response(value),
        Err(err) => Err(anyhow!("worker error {}: {}", err.code, err.message)),
    }
}

/// Convenience helper to build a request with common defaults.
#[allow(clippy::too_many_arguments)]
pub fn execute_worker_payload(
    engine: &wasmtime::Engine,
    component: &wasmtime::component::Component,
    tenant: &TenantCtx,
    worker_id: &str,
    correlation_id: Option<String>,
    session_id: Option<String>,
    thread_id: Option<String>,
    payload_json: String,
) -> Result<WorkerResponse> {
    let request = WorkerRequest {
        version: "1.0".to_string(),
        tenant: tenant.clone(),
        worker_id: worker_id.to_string(),
        correlation_id,
        session_id,
        thread_id,
        payload_json,
        timestamp_utc: DateTime::<Utc>::from(SystemTime::now()).to_rfc3339(),
    };
    execute_worker(engine, component, tenant, &request)
}

fn to_wit_request(request: &WorkerRequest, tenant: &TenantCtx) -> Result<WitWorkerRequest> {
    Ok(WitWorkerRequest {
        version: request.version.clone(),
        tenant: tenant_to_wit(tenant)?,
        worker_id: request.worker_id.clone(),
        correlation_id: request.correlation_id.clone(),
        session_id: request.session_id.clone(),
        thread_id: request.thread_id.clone(),
        payload_json: request.payload_json.clone(),
        timestamp_utc: request.timestamp_utc.clone(),
    })
}

fn from_wit_response(response: WitWorkerResponse) -> Result<WorkerResponse> {
    Ok(WorkerResponse {
        version: response.version,
        tenant: tenant_from_wit(response.tenant)?,
        worker_id: response.worker_id,
        correlation_id: response.correlation_id,
        session_id: response.session_id,
        thread_id: response.thread_id,
        messages: response
            .messages
            .into_iter()
            .map(|m| WorkerMessage {
                kind: m.kind,
                payload_json: m.payload_json,
            })
            .collect(),
        timestamp_utc: response.timestamp_utc,
    })
}

fn tenant_to_wit(tenant: &TenantCtx) -> Result<WitTenantCtx> {
    let deployment = WitDeploymentCtx {
        cloud: WitCloud::Local,
        region: None,
        platform: WitPlatform::Other,
        runtime: None,
    };
    Ok(WitTenantCtx {
        tenant: tenant.tenant.to_string(),
        team: tenant.team.as_ref().map(|v| v.to_string()),
        user: tenant.user.as_ref().map(|v| v.to_string()),
        deployment,
        trace_id: tenant.trace_id.clone(),
        session_id: tenant.session_id.clone(),
        flow_id: tenant.flow_id.clone(),
        node_id: tenant.node_id.clone(),
        provider_id: tenant.provider_id.clone(),
    })
}

fn tenant_from_wit(tenant: WitTenantCtx) -> Result<TenantCtx> {
    let env = EnvId::from_str("default").map_err(|err: GreenticError| anyhow!(err))?;
    let tenant_id = tenant
        .tenant
        .parse::<TenantId>()
        .map_err(|err: GreenticError| anyhow!(err))?;
    let mut ctx = TenantCtx::new(env, tenant_id);
    ctx.tenant = tenant
        .tenant
        .parse::<TenantId>()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.team = tenant
        .team
        .map(|v| v.parse::<TeamId>())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.user = tenant
        .user
        .map(|v| v.parse::<UserId>())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.trace_id = tenant.trace_id;
    ctx.session_id = tenant.session_id;
    ctx.flow_id = tenant.flow_id;
    ctx.node_id = tenant.node_id;
    ctx.provider_id = tenant.provider_id;
    Ok(ctx)
}

struct WasiState {
    table: ResourceTable,
    ctx: WasiCtx,
}

impl WasiState {
    fn new() -> Result<Self> {
        Ok(Self {
            table: ResourceTable::new(),
            ctx: WasiCtxBuilder::new()
                .inherit_stdout()
                .inherit_stderr()
                .build(),
        })
    }
}

impl WasiView for WasiState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_interfaces::worker_v1::exports::greentic::worker::worker_api::WorkerMessage as WitWorkerMessage;
    use greentic_types::{TenantId, WorkerRequest, WorkerResponse};

    fn sample_tenant() -> TenantCtx {
        TenantCtx::new("local".parse().unwrap(), TenantId::new("demo").unwrap())
    }

    #[test]
    fn maps_worker_request() {
        let tenant = sample_tenant();
        let req = WorkerRequest {
            version: "1.0".into(),
            tenant: tenant.clone(),
            worker_id: "worker-1".into(),
            correlation_id: Some("corr".into()),
            session_id: Some("sess".into()),
            thread_id: Some("thread".into()),
            payload_json: r#"{ "ok": true }"#.into(),
            timestamp_utc: "2024-01-01T00:00:00Z".into(),
        };
        let wit = to_wit_request(&req, &tenant).expect("to_wit");
        assert_eq!(wit.worker_id, req.worker_id);
        assert_eq!(wit.correlation_id, req.correlation_id);
        assert_eq!(wit.session_id, req.session_id);
        assert_eq!(wit.thread_id, req.thread_id);
        assert_eq!(wit.payload_json, req.payload_json);
        assert_eq!(wit.timestamp_utc, req.timestamp_utc);
        let roundtrip_tenant = tenant_from_wit(wit.tenant).expect("tenant_from_wit");
        assert_eq!(roundtrip_tenant.tenant_id, tenant.tenant_id);
        assert_eq!(roundtrip_tenant.tenant, tenant.tenant);
        assert_eq!(roundtrip_tenant.team, tenant.team);
        assert_eq!(roundtrip_tenant.user, tenant.user);
    }

    #[test]
    fn maps_worker_response() {
        let tenant = sample_tenant();
        let resp = WorkerResponse {
            version: "1.0".into(),
            tenant: tenant.clone(),
            worker_id: "worker-1".into(),
            correlation_id: Some("corr".into()),
            session_id: Some("sess".into()),
            thread_id: Some("thread".into()),
            messages: vec![WorkerMessage {
                kind: "echo".into(),
                payload_json: r#"{ "ok": true }"#.into(),
            }],
            timestamp_utc: "2024-01-01T00:00:00Z".into(),
        };
        let wit = WitWorkerResponse {
            version: resp.version.clone(),
            tenant: tenant_to_wit(&tenant).expect("tenant_to_wit"),
            worker_id: resp.worker_id.clone(),
            correlation_id: resp.correlation_id.clone(),
            session_id: resp.session_id.clone(),
            thread_id: resp.thread_id.clone(),
            messages: resp
                .messages
                .iter()
                .map(|m| WitWorkerMessage {
                    kind: m.kind.clone(),
                    payload_json: m.payload_json.clone(),
                })
                .collect(),
            timestamp_utc: resp.timestamp_utc.clone(),
        };
        let roundtrip = from_wit_response(wit).expect("from_wit");
        assert_eq!(roundtrip.worker_id, resp.worker_id);
        assert_eq!(roundtrip.correlation_id, resp.correlation_id);
        assert_eq!(roundtrip.session_id, resp.session_id);
        assert_eq!(roundtrip.thread_id, resp.thread_id);
        assert_eq!(roundtrip.messages.len(), 1);
        assert_eq!(roundtrip.messages[0].kind, "echo");
        assert_eq!(roundtrip.messages[0].payload_json, r#"{ "ok": true }"#);
        assert_eq!(roundtrip.tenant.tenant_id, tenant.tenant_id);
        assert_eq!(roundtrip.tenant.tenant, tenant.tenant);
    }
}
