#![allow(clippy::all)]

wit_bindgen::generate!({
    path: "wit/state-store-component",
    world: "component",
    generate_all,
});

use exports::greentic::component::node::{
    ExecCtx, Guest as NodeGuest, InvokeResult, LifecycleStatus, NodeError, StreamEvent,
};
use serde_json::{Value, json};

use crate::greentic::state::state_store as state_store;

struct StateStoreComponent;

impl NodeGuest for StateStoreComponent {
    fn get_manifest() -> String {
        r#"{"name":"state.store","ops":["write","read","delete"]}"#.to_string()
    }

    fn on_start(_ctx: ExecCtx) -> Result<LifecycleStatus, String> {
        Ok(LifecycleStatus::Ok)
    }

    fn on_stop(_ctx: ExecCtx, _reason: String) -> Result<LifecycleStatus, String> {
        Ok(LifecycleStatus::Ok)
    }

    fn invoke(_ctx: ExecCtx, op: String, input: String) -> InvokeResult {
        let parsed: Value = serde_json::from_str(&input).unwrap_or(Value::Null);
        let payload = extract_payload(&parsed);
        let key = payload
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or("default")
            .to_string();

        match op.as_str() {
            "write" => {
                let value = payload.get("value").cloned().unwrap_or(Value::Null);
                let bytes = serde_json::to_vec(&value).unwrap_or_default();
                if let Err(err) = state_store::write(&key, &bytes, None) {
                    return InvokeResult::Err(map_state_error(err));
                }
                InvokeResult::Ok(serde_json::to_string(&json!({"status": "ok"})).unwrap())
            }
            "read" => match state_store::read(&key, None) {
                Ok(bytes) => {
                    let value: Value = serde_json::from_slice(&bytes)
                        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()));
                    InvokeResult::Ok(serde_json::to_string(&json!({"value": value})).unwrap())
                }
                Err(err) => InvokeResult::Err(map_state_error(err)),
            },
            "delete" => {
                if let Err(err) = state_store::delete(&key, None) {
                    return InvokeResult::Err(map_state_error(err));
                }
                InvokeResult::Ok(serde_json::to_string(&json!({"status": "ok"})).unwrap())
            }
            other => InvokeResult::Err(NodeError {
                code: "INVALID_OP".into(),
                message: format!("unsupported op {other}"),
                retryable: false,
                backoff_ms: None,
                details: None,
            }),
        }
    }

    fn invoke_stream(_ctx: ExecCtx, _op: String, _input: String) -> Vec<StreamEvent> {
        Vec::new()
    }
}

fn map_state_error(err: state_store::HostError) -> NodeError {
    NodeError {
        code: err.code,
        message: err.message,
        retryable: false,
        backoff_ms: None,
        details: None,
    }
}

export!(StateStoreComponent);

fn extract_payload(value: &Value) -> Value {
    if let Value::Object(map) = value {
        if let Some(Value::Array(bytes)) = map.get("payload") {
            let maybe_vec: Option<Vec<u8>> = bytes
                .iter()
                .map(|entry| entry.as_u64().map(|num| num as u8))
                .collect();
            if let Some(vec) = maybe_vec {
                if let Ok(decoded) = serde_json::from_slice::<Value>(&vec) {
                    return decoded;
                }
            }
        }
    }
    value.clone()
}
