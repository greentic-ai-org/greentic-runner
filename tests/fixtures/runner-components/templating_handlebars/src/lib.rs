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
use handlebars::Handlebars;
use serde_json::Value;

struct Templating;

impl NodeGuest for Templating {
    fn get_manifest() -> String {
        r#"{"name":"templating.handlebars","ops":["render"]}"#.to_string()
    }

    fn on_start(_ctx: ExecCtx) -> Result<LifecycleStatus, String> {
        Ok(LifecycleStatus::Ok)
    }

    fn on_stop(_ctx: ExecCtx, _reason: String) -> Result<LifecycleStatus, String> {
        Ok(LifecycleStatus::Ok)
    }

    fn invoke(_ctx: ExecCtx, op: String, input: String) -> InvokeResult {
        if op != "render" {
            return InvokeResult::Err(NodeError {
                code: "INVALID_OP".into(),
                message: format!("unsupported op {op}"),
                retryable: false,
                backoff_ms: None,
                details: None,
            });
        }
        let input: Value = serde_json::from_str(&input).unwrap_or(Value::Null);
        let template = input
            .get("template")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let partials = input
            .get("partials")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let data = input.get("data").cloned().unwrap_or(Value::Null);
        let state = input.get("state").cloned().unwrap_or(Value::Null);

        let mut ctx = state;
        merge_values(&mut ctx, data);

        let mut engine = Handlebars::new();
        engine.set_strict_mode(false);
        for (name, body) in partials {
            if let Some(text) = body.as_str() {
                let _ = engine.register_template_string(&name, text);
            }
        }
        let rendered = engine.render_template(template, &ctx).unwrap_or_default();
        let payload = serde_json::json!({ "text": rendered });
        InvokeResult::Ok(serde_json::to_string(&payload).unwrap())
    }

    fn invoke_stream(_ctx: ExecCtx, _op: String, _input: String) -> Vec<StreamEvent> {
        Vec::new()
    }
}

fn merge_values(target: &mut Value, addition: Value) {
    match (target, addition) {
        (Value::Object(target_map), Value::Object(add_map)) => {
            for (key, value) in add_map {
                merge_values(target_map.entry(key).or_insert(Value::Null), value);
            }
        }
        (slot, value) => {
            *slot = value;
        }
    }
}

export!(Templating);
