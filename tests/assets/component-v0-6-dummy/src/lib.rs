#![allow(clippy::all)]

use greentic_interfaces_guest::component_v0_6::{
    component_descriptor, component_i18n, component_qa, component_runtime, component_schema,
};

struct ComponentV06Descriptor;

impl component_descriptor::Guest for ComponentV06Descriptor {
    fn get_component_info() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "name": "component-v0-6-dummy",
            "version": "0.1.0"
        }))
        .unwrap_or_default()
    }

    fn describe() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "operations": [
                {
                    "id": "run",
                    "input": {
                        "schema": {
                            "type": "object",
                            "required": ["message"],
                            "properties": {
                                "message": { "type": "string" }
                            },
                            "additionalProperties": false
                        }
                    },
                    "output": {
                        "schema": {
                            "type": "object",
                            "required": ["result"],
                            "properties": {
                                "result": { "type": "string" }
                            },
                            "additionalProperties": false
                        }
                    }
                }
            ],
            "config_schema": {
                "type": "object",
                "properties": {
                    "state_id": { "type": "string" }
                },
                "additionalProperties": true
            }
        }))
        .unwrap_or_else(|_| b"{\"operations\":[]}".to_vec())
    }
}

impl component_schema::Guest for ComponentV06Descriptor {
    fn input_schema() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "type": "object",
            "required": ["message"],
            "properties": {
                "message": { "type": "string" }
            },
            "additionalProperties": false
        }))
        .unwrap_or_default()
    }

    fn output_schema() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "type": "object",
            "required": ["result"],
            "properties": {
                "result": { "type": "string" }
            },
            "additionalProperties": false
        }))
        .unwrap_or_default()
    }

    fn config_schema() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "type": "object",
            "properties": {
                "state_id": { "type": "string" }
            },
            "additionalProperties": true
        }))
        .unwrap_or_default()
    }
}

impl component_runtime::Guest for ComponentV06Descriptor {
    fn run(input: Vec<u8>, _state: Vec<u8>) -> component_runtime::RunResult {
        let message = serde_json::from_slice::<serde_json::Value>(&input)
            .ok()
            .and_then(|value| {
                value
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| "ok".to_string());

        component_runtime::RunResult {
            output: serde_json::to_vec(&serde_json::json!({ "result": message }))
                .unwrap_or_default(),
            new_state: Vec::new(),
        }
    }
}

impl component_qa::Guest for ComponentV06Descriptor {
    fn qa_spec(_mode: component_qa::QaMode) -> Vec<u8> {
        Vec::new()
    }

    fn apply_answers(
        _mode: component_qa::QaMode,
        current_config: Vec<u8>,
        _answers: Vec<u8>,
    ) -> Vec<u8> {
        current_config
    }
}

impl component_i18n::Guest for ComponentV06Descriptor {
    fn i18n_keys() -> Vec<String> {
        Vec::new()
    }
}

greentic_interfaces_guest::export_component_v060!(ComponentV06Descriptor);
