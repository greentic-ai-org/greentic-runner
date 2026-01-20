use std::collections::VecDeque;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde_json::Value;

use crate::runner::engine::{ExecutionObserver, NodeEvent};
use crate::validate::ValidationIssue;

use super::model::{TraceEnvelope, TraceError, TraceFlow, TraceHash, TracePack, TraceStep};

const DEFAULT_TRACE_FILE: &str = "trace.json";
const DEFAULT_BUFFER_SIZE: usize = 20;
const HASH_ALGORITHM: &str = "blake3";

#[derive(Clone, Debug)]
pub struct TraceConfig {
    pub mode: TraceMode,
    pub out_path: PathBuf,
    pub buffer_size: usize,
    pub capture_inputs: bool,
}

impl TraceConfig {
    pub fn from_env() -> Self {
        let out_path = env::var_os("GREENTIC_TRACE_OUT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_TRACE_FILE));
        Self {
            mode: TraceMode::On,
            out_path,
            buffer_size: DEFAULT_BUFFER_SIZE,
            capture_inputs: env::var("GREENTIC_TRACE_CAPTURE_INPUTS").ok().as_deref() == Some("1"),
        }
    }

    pub fn with_overrides(mut self, mode: TraceMode, out_path: Option<PathBuf>) -> Self {
        self.mode = mode;
        if let Some(path) = out_path {
            self.out_path = path;
        }
        self
    }

    pub fn with_capture_inputs(mut self, capture: bool) -> Self {
        self.capture_inputs = capture;
        self
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TraceMode {
    Off,
    On,
    Always,
}

#[derive(Clone, Debug)]
pub struct PackTraceInfo {
    pub pack_ref: String,
    pub resolved_digest: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TraceContext {
    pub pack_ref: String,
    pub resolved_digest: Option<String>,
    pub flow_id: String,
    pub flow_version: String,
}

pub struct TraceRecorder {
    config: TraceConfig,
    context: TraceContext,
    state: Mutex<TraceState>,
}

struct TraceState {
    buffer: VecDeque<TraceStep>,
    in_flight: Option<InFlightStep>,
    flushed: bool,
}

struct InFlightStep {
    node_id: String,
    component_id: String,
    operation: String,
    input_hash: TraceHash,
    started_at: Instant,
    validation_issues: Vec<ValidationIssue>,
    invocation_json: Option<Value>,
}

impl TraceRecorder {
    pub fn new(config: TraceConfig, context: TraceContext) -> Self {
        Self {
            config,
            context,
            state: Mutex::new(TraceState {
                buffer: VecDeque::new(),
                in_flight: None,
                flushed: false,
            }),
        }
    }

    pub fn mode(&self) -> TraceMode {
        self.config.mode
    }

    pub fn flush_success(&self) -> Result<()> {
        if self.config.mode != TraceMode::Always {
            return Ok(());
        }
        self.flush_with_steps(None)
    }

    pub fn flush_error(&self, err: &dyn std::error::Error) -> Result<()> {
        if self.config.mode == TraceMode::Off {
            return Ok(());
        }
        self.flush_with_steps(Some(err))
    }

    pub fn flush_buffer(&self) -> Result<()> {
        if self.config.mode == TraceMode::Off {
            return Ok(());
        }
        self.flush_with_steps(None)
    }

    fn flush_with_steps(&self, fallback_error: Option<&dyn std::error::Error>) -> Result<()> {
        let mut state = self.state.lock();
        if state.flushed {
            return Ok(());
        }
        if let Some(err) = fallback_error {
            let step = if let Some(in_flight) = state.in_flight.take() {
                TraceStep {
                    node_id: in_flight.node_id,
                    component_id: in_flight.component_id,
                    operation: in_flight.operation,
                    input_hash: in_flight.input_hash,
                    invocation_json: in_flight.invocation_json,
                    invocation_path: None,
                    output_hash: None,
                    state_delta_hash: None,
                    duration_ms: in_flight.started_at.elapsed().as_millis() as u64,
                    validation_issues: in_flight.validation_issues,
                    error: Some(TraceError {
                        code: "node_error".to_string(),
                        message: err.to_string(),
                        details: Value::Null,
                    }),
                }
            } else {
                TraceStep {
                    node_id: "unknown".to_string(),
                    component_id: "unknown".to_string(),
                    operation: "unknown".to_string(),
                    input_hash: hash_value(&Value::Null),
                    invocation_json: None,
                    invocation_path: None,
                    output_hash: None,
                    state_delta_hash: None,
                    duration_ms: 0,
                    validation_issues: Vec::new(),
                    error: Some(TraceError {
                        code: "flow_error".to_string(),
                        message: err.to_string(),
                        details: Value::Null,
                    }),
                }
            };
            state.buffer.push_back(step);
            while state.buffer.len() > self.config.buffer_size {
                state.buffer.pop_front();
            }
        }
        let steps = state.buffer.iter().cloned().collect::<Vec<_>>();
        state.flushed = true;
        drop(state);
        let trace = self.build_trace(steps);
        write_trace_atomic(&self.config.out_path, &trace)?;
        Ok(())
    }

    fn build_trace(&self, steps: Vec<TraceStep>) -> TraceEnvelope {
        TraceEnvelope {
            trace_version: 1,
            runner_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            git_sha: git_sha(),
            pack: TracePack {
                pack_ref: self.context.pack_ref.clone(),
                resolved_digest: self.context.resolved_digest.clone(),
            },
            flow: TraceFlow {
                id: self.context.flow_id.clone(),
                version: self.context.flow_version.clone(),
            },
            steps,
        }
    }
}

impl ExecutionObserver for TraceRecorder {
    fn on_node_start(&self, event: &NodeEvent<'_>) {
        if self.config.mode == TraceMode::Off {
            return;
        }
        let operation = event
            .node
            .operation_name()
            .or_else(|| event.node.operation_in_mapping())
            .unwrap_or("unknown")
            .to_string();
        let input_hash = hash_value(event.payload);
        let component_id = event.node.component_id().to_string();
        let mut state = self.state.lock();
        state.in_flight = Some(InFlightStep {
            node_id: event.node_id.to_string(),
            component_id: component_id.clone(),
            operation,
            input_hash,
            started_at: Instant::now(),
            validation_issues: Vec::new(),
            invocation_json: if self.config.capture_inputs {
                Some(build_invocation(event, &component_id))
            } else {
                None
            },
        });
    }

    fn on_node_end(&self, event: &NodeEvent<'_>, output: &Value) {
        if self.config.mode == TraceMode::Off {
            return;
        }
        let output_hash = hash_value(output);
        let mut state = self.state.lock();
        let step = if let Some(in_flight) = state.in_flight.take() {
            TraceStep {
                node_id: in_flight.node_id,
                component_id: in_flight.component_id,
                operation: in_flight.operation,
                input_hash: in_flight.input_hash,
                invocation_json: in_flight.invocation_json,
                invocation_path: None,
                output_hash: Some(output_hash),
                state_delta_hash: None,
                duration_ms: in_flight.started_at.elapsed().as_millis() as u64,
                validation_issues: in_flight.validation_issues,
                error: None,
            }
        } else {
            TraceStep {
                node_id: event.node_id.to_string(),
                component_id: event.node.component_id().to_string(),
                operation: event.node.operation_name().unwrap_or("unknown").to_string(),
                input_hash: hash_value(event.payload),
                invocation_json: if self.config.capture_inputs {
                    Some(build_invocation(event, event.node.component_id()))
                } else {
                    None
                },
                invocation_path: None,
                output_hash: Some(output_hash),
                state_delta_hash: None,
                duration_ms: 0,
                validation_issues: Vec::new(),
                error: None,
            }
        };
        state.buffer.push_back(step);
        while state.buffer.len() > self.config.buffer_size {
            state.buffer.pop_front();
        }
    }

    fn on_node_error(&self, event: &NodeEvent<'_>, error: &dyn std::error::Error) {
        if self.config.mode == TraceMode::Off {
            return;
        }
        let mut state = self.state.lock();
        let step = if let Some(in_flight) = state.in_flight.take() {
            TraceStep {
                node_id: in_flight.node_id,
                component_id: in_flight.component_id,
                operation: in_flight.operation,
                input_hash: in_flight.input_hash,
                invocation_json: in_flight.invocation_json,
                invocation_path: None,
                output_hash: None,
                state_delta_hash: None,
                duration_ms: in_flight.started_at.elapsed().as_millis() as u64,
                validation_issues: in_flight.validation_issues,
                error: Some(TraceError {
                    code: "node_error".to_string(),
                    message: error.to_string(),
                    details: Value::Null,
                }),
            }
        } else {
            TraceStep {
                node_id: event.node_id.to_string(),
                component_id: event.node.component_id().to_string(),
                operation: event.node.operation_name().unwrap_or("unknown").to_string(),
                input_hash: hash_value(event.payload),
                invocation_json: if self.config.capture_inputs {
                    Some(build_invocation(event, event.node.component_id()))
                } else {
                    None
                },
                invocation_path: None,
                output_hash: None,
                state_delta_hash: None,
                duration_ms: 0,
                validation_issues: Vec::new(),
                error: Some(TraceError {
                    code: "node_error".to_string(),
                    message: error.to_string(),
                    details: Value::Null,
                }),
            }
        };
        state.buffer.push_back(step);
        while state.buffer.len() > self.config.buffer_size {
            state.buffer.pop_front();
        }
        drop(state);
        if let Err(err) = self.flush_buffer() {
            tracing::warn!(error = %err, "failed to write trace");
        }
    }

    fn on_validation(&self, _event: &NodeEvent<'_>, issues: &[ValidationIssue]) {
        if self.config.mode == TraceMode::Off || issues.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        if let Some(in_flight) = state.in_flight.as_mut() {
            in_flight.validation_issues.extend_from_slice(issues);
        }
    }
}

fn hash_value(value: &Value) -> TraceHash {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    let digest = blake3::hash(&bytes).to_hex().to_string();
    TraceHash {
        algorithm: HASH_ALGORITHM.to_string(),
        value: digest,
    }
}

fn build_invocation(event: &NodeEvent<'_>, component_id: &str) -> Value {
    serde_json::json!({
        "component_id": component_id,
        "operation": event
            .node
            .operation_name()
            .or_else(|| event.node.operation_in_mapping())
            .unwrap_or("unknown")
            .to_string(),
        "payload": event.payload,
    })
}

fn write_trace_atomic(path: &Path, trace: &TraceEnvelope) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(DEFAULT_TRACE_FILE);
    let tmp = path.with_file_name(format!("{file_name}.tmp"));
    let payload = serde_json::to_vec_pretty(trace).context("serialize trace")?;
    fs::write(&tmp, payload).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn git_sha() -> Option<String> {
    env::var("GIT_SHA")
        .ok()
        .or_else(|| env::var("GITHUB_SHA").ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
