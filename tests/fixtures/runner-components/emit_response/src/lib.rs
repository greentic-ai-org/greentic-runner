#![allow(clippy::all)]

wit_bindgen::generate!({
    inline: r#"
    package greentic:component@0.4.0;

    interface control {
      should-cancel: func() -> bool;
      yield-now: func();
    }

    interface node {
      type json = string;

      record tenant-ctx {
        tenant: string,
        team: option<string>,
        user: option<string>,
        trace-id: option<string>,
        correlation-id: option<string>,
        deadline-unix-ms: option<u64>,
        attempt: u32,
        idempotency-key: option<string>,
      }

      record exec-ctx {
        tenant: tenant-ctx,
        flow-id: string,
        node-id: option<string>,
      }

      record node-error {
        code: string,
        message: string,
        retryable: bool,
        backoff-ms: option<u64>,
        details: option<json>,
      }

      variant invoke-result {
        ok(json),
        err(node-error),
      }

      variant stream-event {
        data(json),
        progress(u8),
        done,
        error(string),
      }

      enum lifecycle-status { ok }

      get-manifest: func() -> json;
      on-start: func(ctx: exec-ctx) -> result<lifecycle-status, string>;
      on-stop: func(ctx: exec-ctx, reason: string) -> result<lifecycle-status, string>;
      invoke: func(ctx: exec-ctx, op: string, input: json) -> invoke-result;
      invoke-stream: func(ctx: exec-ctx, op: string, input: json) -> list<stream-event>;
    }

    world component {
      import control;
      export node;
    }
    "#,
    world: "component",
});

use exports::greentic::component::node::{
    ExecCtx, Guest as NodeGuest, InvokeResult, LifecycleStatus, NodeError, StreamEvent,
};
use serde_json::Value;

struct Emit;

impl NodeGuest for Emit {
    fn get_manifest() -> String {
        r#"{"name":"emit.response","ops":["emit"]}"#.to_string()
    }

    fn on_start(_ctx: ExecCtx) -> Result<LifecycleStatus, String> {
        Ok(LifecycleStatus::Ok)
    }

    fn on_stop(_ctx: ExecCtx, _reason: String) -> Result<LifecycleStatus, String> {
        Ok(LifecycleStatus::Ok)
    }

    fn invoke(_ctx: ExecCtx, op: String, input: String) -> InvokeResult {
        if op != "emit" {
            return InvokeResult::Err(NodeError {
                code: "INVALID_OP".into(),
                message: format!("unsupported op {op}"),
                retryable: false,
                backoff_ms: None,
                details: None,
            });
        }
        let mut value: Value = serde_json::from_str(&input).unwrap_or(Value::Null);
        if let Value::Object(map) = &mut value {
            if let Some(Value::String(node)) = map.get("from_node") {
                if let Some(state) = map.get("state") {
                    if let Some(nodes) = state.get("nodes") {
                        if let Some(payload) = nodes.get(node).and_then(|n| n.get("payload")) {
                            return InvokeResult::Ok(serde_json::to_string(payload).unwrap());
                        }
                    }
                }
            }
        }
        InvokeResult::Ok(serde_json::to_string(&value).unwrap_or_default())
    }

    fn invoke_stream(_ctx: ExecCtx, _op: String, _input: String) -> Vec<StreamEvent> {
        Vec::new()
    }
}

export!(Emit);
