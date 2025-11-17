#![allow(clippy::needless_return)]

wit_bindgen::generate!({
    path: "wit/greentic/pack-export@0.2.0",
    world: "pack-exports",
});

use exports::greentic::pack_export::exports::{
    FlowInfo, IfaceError, RunResult, SchemaDoc,
};

struct Component;

const FLOW_ID: &str = "demo.flow";

impl exports::greentic::pack_export::exports::Guest for Component {
    fn list_flows() -> Result<Vec<FlowInfo>, IfaceError> {
        Ok(vec![FlowInfo {
            id: FLOW_ID.into(),
            profile: "default".into(),
            version: "1.0.0".into(),
            flow_type: "messaging".into(),
            private: false,
        }])
    }

    fn get_flow_schema(flow: String) -> Result<SchemaDoc, IfaceError> {
        if flow != FLOW_ID {
            return Err(IfaceError::NotFound);
        }
        Ok(SchemaDoc {
            input_jsonschema: r#"{"type":"object"}"#.into(),
            output_jsonschema: r#"{"type":"object"}"#.into(),
        })
    }

    fn flow_metadata(flow: String) -> Result<String, IfaceError> {
        if flow != FLOW_ID {
            return Err(IfaceError::NotFound);
        }
        Ok(r#"{"kind":"messaging","id":"demo.flow","nodes":{"reply":{"kind":"qa.process","routing":{"out":true}}}}"#.into())
    }

    fn run_flow(
        flow: String,
        input_json: String,
        _opts: Option<exports::greentic::pack_export::exports::RunOpts>,
    ) -> Result<RunResult, IfaceError> {
        if flow != FLOW_ID {
            return Err(IfaceError::NotFound);
        }
        Ok(RunResult {
            status: "done".into(),
            output_json: Some(input_json),
            error: None,
            logs_json: Some(r#"["pack-component invoked"]"#.into()),
            metrics_json: None,
        })
    }

    fn prepare_flow(
        flow: String,
        _hints: Option<String>,
    ) -> Result<String, IfaceError> {
        if flow != FLOW_ID {
            return Err(IfaceError::NotFound);
        }
        Ok(r#"{"prepared":true}"#.into())
    }

    fn a2a_search(
        _model: String,
        _query_embedding: Vec<f32>,
        _k: u32,
    ) -> Result<Vec<exports::greentic::pack_export::exports::A2aCandidate>, IfaceError> {
        Ok(Vec::new())
    }
}

export!(Component);
