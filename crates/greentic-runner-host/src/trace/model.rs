use crate::validate::ValidationIssue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceEnvelope {
    pub trace_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runner_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_sha: Option<String>,
    pub pack: TracePack,
    pub flow: TraceFlow,
    pub steps: Vec<TraceStep>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TracePack {
    pub pack_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_digest: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceFlow {
    pub id: String,
    pub version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceStep {
    pub node_id: String,
    pub component_id: String,
    pub operation: String,
    pub input_hash: TraceHash,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invocation_json: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invocation_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_hash: Option<TraceHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_delta_hash: Option<TraceHash>,
    pub duration_ms: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validation_issues: Vec<ValidationIssue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<TraceError>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceError {
    pub code: String,
    pub message: String,
    pub details: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceHash {
    pub algorithm: String,
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trace_serializes_with_version() {
        let trace = TraceEnvelope {
            trace_version: 1,
            runner_version: Some("0.0.0".to_string()),
            git_sha: None,
            pack: TracePack {
                pack_ref: "pack@0.0.0".to_string(),
                resolved_digest: Some("sha256:deadbeef".to_string()),
            },
            flow: TraceFlow {
                id: "flow.test".to_string(),
                version: "0.0.1".to_string(),
            },
            steps: vec![TraceStep {
                node_id: "node-1".to_string(),
                component_id: "component.exec".to_string(),
                operation: "render".to_string(),
                input_hash: TraceHash {
                    algorithm: "blake3".to_string(),
                    value: "hash".to_string(),
                },
                invocation_json: None,
                invocation_path: None,
                output_hash: None,
                state_delta_hash: None,
                duration_ms: 5,
                validation_issues: Vec::new(),
                error: Some(TraceError {
                    code: "node_error".to_string(),
                    message: "boom".to_string(),
                    details: json!({ "detail": "value" }),
                }),
            }],
        };

        let serialized = serde_json::to_string(&trace).expect("trace json");
        assert!(serialized.contains("\"trace_version\":1"));
    }
}
