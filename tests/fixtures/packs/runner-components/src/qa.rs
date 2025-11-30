use serde_json::Value;
use wit_bindgen::generate;

generate!({
    path: "../../../../../tests/fixtures/component-echo/target/tmp.wit",
    world: "component",
});

use exports::greentic::component::node::{
    ExecCtx, Guest as NodeGuest, InvokeResult, NodeError, StreamEvent,
};

struct Qa;

impl NodeGuest for Qa {
    fn get_manifest() -> String {
        r#"{"name":"qa.process","ops":["process"]}"#.to_string()
    }

    fn on_start(_ctx: ExecCtx) -> Result<exports::greentic::component::node::LifecycleStatus, String> {
        Ok(exports::greentic::component::node::LifecycleStatus::Ok)
    }

    fn on_stop(
        _ctx: ExecCtx,
        _reason: String,
    ) -> Result<exports::greentic::component::node::LifecycleStatus, String> {
        Ok(exports::greentic::component::node::LifecycleStatus::Ok)
    }

    fn invoke(_ctx: ExecCtx, op: String, input: String) -> InvokeResult {
        if op != "process" {
            return InvokeResult::Err(NodeError {
                code: "INVALID_OP".into(),
                message: format!("unsupported op {op}"),
                retryable: false,
                backoff_ms: None,
                details: None,
            });
        }
        let parsed: Value = serde_json::from_str(&input).unwrap_or(Value::Null);
        InvokeResult::Ok(serde_json::to_string(&parsed).unwrap())
    }

    fn invoke_stream(_ctx: ExecCtx, _op: String, _input: String) -> Vec<StreamEvent> {
        Vec::new()
    }
}

export!(Qa);
