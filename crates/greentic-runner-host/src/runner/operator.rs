use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Response, StatusCode},
};
use serde::{Deserialize, Serialize};
use serde_cbor;
use serde_json::{Map, Value, json};
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{Level, span};

use crate::component_api::node::{ExecCtx as ComponentExecCtx, TenantCtx as ComponentTenantCtx};
use crate::operator_registry::OperatorResolveError;
use crate::provider::ProviderBinding;
use crate::routing::TenantRuntimeHandle;
use crate::runtime::TenantRuntime;

const CONTENT_TYPE_CBOR: &str = "application/cbor";

/// Operator-facing invocation payload (CBOR envelope).
#[derive(Debug, Deserialize)]
pub struct OperatorRequest {
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub provider_type: Option<String>,
    #[serde(default)]
    pub pack_id: Option<String>,
    pub op_id: String,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub flags: Vec<String>,
    #[serde(default)]
    pub op_version: Option<String>,
    #[serde(default)]
    pub schema_hash: Option<String>,
    pub payload: OperatorPayload,
}

impl OperatorRequest {
    pub fn from_cbor(bytes: &[u8]) -> Result<Self, serde_cbor::Error> {
        serde_cbor::from_slice(bytes)
    }
}

#[derive(Debug, Deserialize)]
pub struct OperatorPayload {
    #[serde(default)]
    #[serde(rename = "cbor_input")]
    pub cbor_input: Vec<u8>,
    #[serde(default)]
    pub attachments: Vec<AttachmentRef>,
}

#[derive(Debug, Deserialize)]
pub struct AttachmentRef {
    pub id: String,
    #[serde(default)]
    pub metadata: Option<Value>,
}

/// Operator response envelope serialized back to CBOR.
#[derive(Debug, Serialize)]
pub struct OperatorResponse {
    pub status: OperatorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cbor_output: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<OperatorError>,
}

impl OperatorResponse {
    pub fn ok(output: Vec<u8>) -> Self {
        Self {
            status: OperatorStatus::Ok,
            cbor_output: Some(output),
            error: None,
        }
    }

    pub fn error(code: OperatorErrorCode, message: impl Into<String>) -> Self {
        Self {
            status: OperatorStatus::Error,
            cbor_output: None,
            error: Some(OperatorError {
                code,
                message: message.into(),
                details_cbor: None,
            }),
        }
    }

    pub fn to_cbor(&self) -> Result<Vec<u8>, serde_cbor::Error> {
        serde_cbor::ser::to_vec_packed(self)
    }
}

#[derive(Debug, Serialize)]
pub struct OperatorError {
    pub code: OperatorErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details_cbor: Option<Vec<u8>>,
}

#[derive(Debug, Serialize)]
pub enum OperatorStatus {
    Ok,
    Error,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OperatorErrorCode {
    OpNotFound,
    ProviderNotFound,
    TenantNotAllowed,
    InvalidRequest,
    CborDecode,
    TypeMismatch,
    ComponentLoad,
    InvokeTrap,
    Timeout,
    PolicyDenied,
    HostFailure,
}

impl OperatorErrorCode {
    pub fn reason(&self) -> &'static str {
        match self {
            OperatorErrorCode::OpNotFound => "op not found",
            OperatorErrorCode::ProviderNotFound => "provider not found",
            OperatorErrorCode::TenantNotAllowed => "tenant not allowed",
            OperatorErrorCode::InvalidRequest => "invalid operator request",
            OperatorErrorCode::CborDecode => "failed to decode CBOR payload",
            OperatorErrorCode::TypeMismatch => "type mismatch between CBOR and operation",
            OperatorErrorCode::ComponentLoad => "failed to load component",
            OperatorErrorCode::InvokeTrap => "component trapped during invoke",
            OperatorErrorCode::Timeout => "invocation timed out",
            OperatorErrorCode::PolicyDenied => "policy denied the operation",
            OperatorErrorCode::HostFailure => "internal host failure",
        }
    }
}

/// Invoke an operator request without assuming HTTP transport.
pub async fn invoke_operator(
    runtime: &TenantRuntime,
    request: OperatorRequest,
) -> OperatorResponse {
    if let Some(request_tenant) = request.tenant_id.as_deref()
        && request_tenant != runtime.tenant()
    {
        return OperatorResponse::error(
            OperatorErrorCode::TenantNotAllowed,
            format!(
                "tenant mismatch: routing resolved `{}` but request wants `{request_tenant}`",
                runtime.tenant(),
            ),
        );
    }

    if request.provider_id.is_none() && request.provider_type.is_none() {
        return OperatorResponse::error(
            OperatorErrorCode::InvalidRequest,
            "operator invoke requires provider_id or provider_type".to_string(),
        );
    }

    let tenant = runtime.tenant();
    let root_span = span!(
        Level::INFO,
        "operator.invoke",
        tenant = %tenant,
        op_id = %request.op_id,
        provider_id = ?request.provider_id,
        provider_type = ?request.provider_type
    );
    let _root_guard = root_span.enter();

    let provider_id = request.provider_id.as_deref();
    let provider_type = request.provider_type.as_deref();
    runtime
        .operator_metrics()
        .resolve_attempts
        .fetch_add(1, Ordering::Relaxed);
    let resolve_span = span!(Level::DEBUG, "resolve_op");
    let _resolve_guard = resolve_span.enter();
    let binding =
        match runtime
            .operator_registry()
            .resolve(provider_id, provider_type, &request.op_id)
        {
            Ok(binding) => binding,
            Err(err) => {
                let (code, message) = match err {
                    OperatorResolveError::ProviderNotFound => {
                        let label = provider_id.or(provider_type).unwrap_or("unknown");
                        (
                            OperatorErrorCode::ProviderNotFound,
                            format!("provider `{label}` not registered"),
                        )
                    }
                    OperatorResolveError::OpNotFound => {
                        let label = provider_id.or(provider_type).unwrap_or("unknown provider");
                        (
                            OperatorErrorCode::OpNotFound,
                            format!("op `{}` not found for provider `{label}`", &request.op_id),
                        )
                    }
                };
                runtime
                    .operator_metrics()
                    .resolve_errors
                    .fetch_add(1, Ordering::Relaxed);
                let response = OperatorResponse::error(code, message);
                return response;
            }
        };
    drop(_resolve_guard);

    let policy = &runtime.config().operator_policy;
    if !policy.allows_provider(provider_id, binding.provider_type.as_str()) {
        return OperatorResponse::error(
            OperatorErrorCode::PolicyDenied,
            format!(
                "provider `{}` not allowed for tenant {}",
                binding
                    .provider_id
                    .as_deref()
                    .unwrap_or(&binding.provider_type),
                runtime.config().tenant
            ),
        );
    }
    if !policy.allows_op(provider_id, binding.provider_type.as_str(), &binding.op_id) {
        return OperatorResponse::error(
            OperatorErrorCode::PolicyDenied,
            format!(
                "op `{}` is not permitted for provider `{}` on tenant {}",
                binding.op_id,
                binding
                    .provider_id
                    .as_deref()
                    .unwrap_or(&binding.provider_type),
                runtime.config().tenant
            ),
        );
    }

    if let Some(req_pack) = request.pack_id.as_deref() {
        let binding_pack = binding
            .pack_ref
            .split('@')
            .next()
            .unwrap_or(&binding.pack_ref);
        if binding_pack != req_pack {
            return OperatorResponse::error(
                OperatorErrorCode::PolicyDenied,
                format!(
                    "request bound to pack `{req_pack}`, but op lives in `{}`",
                    binding.pack_ref
                ),
            );
        }
    }

    let attachments = match resolve_attachments(&request.payload, runtime) {
        Ok(map) => map,
        Err(response) => return response,
    };

    let decode_span = span!(Level::DEBUG, "decode_cbor");
    let _decode_guard = decode_span.enter();
    let input_value = match decode_request_payload(&request.payload.cbor_input) {
        Ok(value) => value,
        Err(err) => {
            runtime
                .operator_metrics()
                .cbor_decode_errors
                .fetch_add(1, Ordering::Relaxed);
            return OperatorResponse::error(OperatorErrorCode::CborDecode, format!("{err}"));
        }
    };
    drop(_decode_guard);

    let input_value = merge_input_with_attachments(input_value, attachments);

    let input_json = match serde_json::to_string(&input_value) {
        Ok(json) => json,
        Err(err) => {
            return OperatorResponse::error(
                OperatorErrorCode::TypeMismatch,
                format!("failed to serialise input JSON: {err}"),
            );
        }
    };

    let component_ref = &binding.runtime.component_ref;
    let pack = match runtime.pack_for_component(component_ref) {
        Some(pack) => pack,
        None => {
            return OperatorResponse::error(
                OperatorErrorCode::ComponentLoad,
                format!("component `{}` not found in tenant packs", component_ref),
            );
        }
    };

    let exec_ctx = build_exec_ctx(&request, runtime);
    runtime
        .operator_metrics()
        .invoke_attempts
        .fetch_add(1, Ordering::Relaxed);
    let invoke_span = span!(Level::INFO, "invoke_component", component = %component_ref);
    let _invoke_guard = invoke_span.enter();
    let result = if binding.runtime.world.starts_with("greentic:provider-core") {
        let input_bytes = input_json.clone().into_bytes();
        let provider_binding = ProviderBinding {
            provider_id: binding.provider_id.clone(),
            provider_type: binding.provider_type.clone(),
            component_ref: binding.runtime.component_ref.clone(),
            export: binding.runtime.export.clone(),
            world: binding.runtime.world.clone(),
            config_json: None,
            pack_ref: Some(binding.pack_ref.clone()),
        };
        match pack
            .invoke_provider(&provider_binding, exec_ctx, &binding.op_id, input_bytes)
            .await
        {
            Ok(value) => value,
            Err(err) => {
                runtime
                    .operator_metrics()
                    .invoke_errors
                    .fetch_add(1, Ordering::Relaxed);
                return OperatorResponse::error(
                    OperatorErrorCode::HostFailure,
                    format!("provider invoke failed: {err}"),
                );
            }
        }
    } else {
        match pack
            .invoke_component(
                component_ref,
                exec_ctx,
                &binding.op_id,
                None,
                input_json.clone(),
            )
            .await
        {
            Ok(value) => value,
            Err(err) => {
                runtime
                    .operator_metrics()
                    .invoke_errors
                    .fetch_add(1, Ordering::Relaxed);
                return OperatorResponse::error(
                    OperatorErrorCode::HostFailure,
                    format!("component invoke failed: {err}"),
                );
            }
        }
    };
    drop(_invoke_guard);

    let encode_span = span!(Level::DEBUG, "encode_cbor");
    let _encode_guard = encode_span.enter();
    let output_bytes = match serde_cbor::to_vec(&result) {
        Ok(bytes) => bytes,
        Err(err) => {
            return OperatorResponse::error(
                OperatorErrorCode::HostFailure,
                format!("failed to encode CBOR output: {err}"),
            );
        }
    };
    drop(_encode_guard);

    OperatorResponse::ok(output_bytes)
}

/// Convenience helper that takes CBOR bytes and reuses `invoke_operator`.
pub async fn invoke_operator_cbor(
    runtime: &TenantRuntime,
    req_cbor: &[u8],
) -> Result<Vec<u8>, serde_cbor::Error> {
    let request = OperatorRequest::from_cbor(req_cbor)?;
    let response = invoke_operator(runtime, request).await;
    response.to_cbor()
}

/// Axum handler stub for `/operator/op/invoke`.
pub async fn invoke(
    TenantRuntimeHandle { runtime, .. }: TenantRuntimeHandle,
    _headers: HeaderMap,
    body: Body,
) -> Result<Response<Body>, Response<Body>> {
    let bytes = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Err(bad_request(format!("failed to read body: {err}")));
        }
    };

    let request = match OperatorRequest::from_cbor(&bytes) {
        Ok(request) => request,
        Err(err) => {
            return Err(bad_request(format!("failed to decode request CBOR: {err}")));
        }
    };

    let response = invoke_operator(&runtime, request).await;
    build_cbor_response(response)
}

fn bad_request(message: String) -> Response<Body> {
    let payload = json!({ "error": message });
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .expect("building JSON error response must succeed")
}

#[allow(clippy::result_large_err)]
fn build_cbor_response(response: OperatorResponse) -> Result<Response<Body>, Response<Body>> {
    match response.to_cbor() {
        Ok(bytes) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("content-type", CONTENT_TYPE_CBOR)
            .body(Body::from(bytes))
            .expect("building CBOR response must succeed")),
        Err(err) => Err(bad_request(format!(
            "failed to serialize response CBOR: {err}"
        ))),
    }
}

fn decode_request_payload(bytes: &[u8]) -> Result<Value, serde_cbor::Error> {
    if bytes.is_empty() {
        return Ok(Value::Null);
    }
    serde_cbor::from_slice(bytes)
}

fn build_exec_ctx(request: &OperatorRequest, runtime: &TenantRuntime) -> ComponentExecCtx {
    let deadline_unix_ms = request.timeout.and_then(|timeout_ms| {
        SystemTime::now()
            .checked_add(Duration::from_millis(timeout_ms))
            .and_then(|deadline| deadline.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis() as u64)
    });

    let tenant_ctx = ComponentTenantCtx {
        tenant: runtime.config().tenant.clone(),
        team: None,
        user: None,
        trace_id: request.trace_id.clone(),
        correlation_id: request.correlation_id.clone(),
        deadline_unix_ms,
        attempt: 1,
        idempotency_key: request.correlation_id.clone(),
    };

    ComponentExecCtx {
        tenant: tenant_ctx,
        flow_id: format!("operator/{}", request.op_id),
        node_id: None,
    }
}

fn resolve_attachments(
    payload: &OperatorPayload,
    runtime: &TenantRuntime,
) -> Result<Map<String, Value>, OperatorResponse> {
    let mut attachments = Map::new();
    for attachment in &payload.attachments {
        if let Some(kind) = AttachmentKind::from_metadata(attachment.metadata.as_ref()) {
            match kind {
                AttachmentKind::Secret { key, alias } => {
                    let secret = runtime.get_secret(&key).map_err(|err| {
                        OperatorResponse::error(
                            OperatorErrorCode::PolicyDenied,
                            format!("secret `{key}` access denied: {err}"),
                        )
                    })?;
                    attachments.insert(alias, Value::String(secret));
                }
            }
        }
    }
    Ok(attachments)
}

fn merge_input_with_attachments(input: Value, attachments: Map<String, Value>) -> Value {
    if attachments.is_empty() {
        return input;
    }
    match input {
        Value::Object(mut map) => {
            map.insert("_attachments".into(), Value::Object(attachments));
            Value::Object(map)
        }
        other => {
            let mut map = Map::new();
            map.insert("input".into(), other);
            map.insert("_attachments".into(), Value::Object(attachments));
            Value::Object(map)
        }
    }
}

enum AttachmentKind {
    Secret { key: String, alias: String },
}

impl AttachmentKind {
    fn from_metadata(metadata: Option<&Value>) -> Option<Self> {
        let metadata = metadata?.as_object()?;
        let attachment_type = metadata.get("type")?.as_str()?;
        match attachment_type {
            "secret" => {
                let key = metadata.get("key")?.as_str()?.to_string();
                let alias = metadata
                    .get("alias")
                    .and_then(Value::as_str)
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| key.clone());
                Some(AttachmentKind::Secret { key, alias })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value, json};

    #[test]
    fn merge_input_with_attachments_preserves_map_fields() {
        let mut attachments = Map::new();
        attachments.insert("secret".into(), json!("value"));
        let mut input_map = Map::new();
        input_map.insert("foo".into(), json!("bar"));
        let merged = merge_input_with_attachments(Value::Object(input_map), attachments.clone());
        let obj = merged.as_object().expect("should be object");
        assert_eq!(obj.get("foo"), Some(&json!("bar")));
        assert_eq!(obj.get("_attachments"), Some(&Value::Object(attachments)));
    }

    #[test]
    fn merge_input_with_attachments_wraps_scalar() {
        let mut attachments = Map::new();
        attachments.insert("secret".into(), json!("value"));
        let merged =
            merge_input_with_attachments(Value::String("text".into()), attachments.clone());
        let obj = merged.as_object().expect("should be object");
        assert_eq!(obj.get("input"), Some(&Value::String("text".into())));
        assert_eq!(obj.get("_attachments"), Some(&Value::Object(attachments)));
    }

    #[test]
    fn attachment_kind_secret_requires_type_lock() {
        let metadata = json!({
            "type": "secret",
            "key": "TOKEN"
        });
        if let Some(AttachmentKind::Secret { key, alias }) =
            AttachmentKind::from_metadata(Some(&metadata))
        {
            assert_eq!(key, "TOKEN");
            assert_eq!(alias, "TOKEN");
        } else {
            panic!("expected secret attachment");
        }
    }

    #[test]
    fn attachment_kind_secret_with_alias() {
        let metadata = json!({
            "type": "secret",
            "key": "TOKEN",
            "alias": "api_token"
        });
        if let Some(AttachmentKind::Secret { key, alias }) =
            AttachmentKind::from_metadata(Some(&metadata))
        {
            assert_eq!(key, "TOKEN");
            assert_eq!(alias, "api_token");
        } else {
            panic!("expected secret attachment");
        }
    }
}
