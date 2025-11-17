use std::str::FromStr;

use anyhow::{Context, Result, bail};
use greentic_types::flow::Node;
use greentic_types::{FlowKind, NodeId};
use serde_json::Value;

use super::pack::LoadedPack;

/// Executes flows using a host-provided component invoker.
pub struct FlowExecutor<'a, H: NodeInvoker> {
    pack: &'a LoadedPack,
    host: &'a H,
    max_steps: usize,
}

impl<'a, H: NodeInvoker> FlowExecutor<'a, H> {
    /// Creates a new executor bound to a loaded pack and host invoker.
    pub fn new(pack: &'a LoadedPack, host: &'a H) -> Self {
        Self {
            pack,
            host,
            max_steps: 2048,
        }
    }

    /// Overrides the default maximum node executions before aborting.
    pub fn with_max_steps(mut self, steps: usize) -> Self {
        self.max_steps = steps.max(1);
        self
    }

    /// Executes a messaging flow.
    pub fn execute_messaging(&self, flow_id: &str, input: Value) -> Result<ExecutionResult> {
        self.execute_flow(flow_id, FlowKind::Messaging, input)
    }

    /// Executes an events flow.
    pub fn execute_events(&self, flow_id: &str, input: Value) -> Result<ExecutionResult> {
        self.execute_flow(flow_id, FlowKind::Events, input)
    }

    fn execute_flow(
        &self,
        flow_id: &str,
        kind: FlowKind,
        mut payload: Value,
    ) -> Result<ExecutionResult> {
        let flow = self
            .pack
            .flows
            .get(flow_id)
            .with_context(|| format!("flow `{flow_id}` not found"))?;
        if flow.kind != kind {
            bail!(
                "flow `{flow_id}` is {:?}, cannot invoke as {:?}",
                flow.kind,
                kind
            );
        }
        let (ingress_id, _) = flow
            .ingress()
            .ok_or_else(|| FlowError::EmptyFlow(flow_id.to_string()))?;
        let mut current_id = ingress_id.clone();
        let mut steps = 0usize;

        loop {
            if steps >= self.max_steps {
                return Err(FlowError::LoopDetected(flow_id.to_string()).into());
            }
            steps += 1;
            let node = flow
                .nodes
                .get(&current_id)
                .ok_or_else(|| FlowError::MissingNode(current_id.as_str().to_string()))?;

            let node_ctx = NodeContext {
                flow_id,
                flow_kind: kind,
                node_id: current_id.as_str(),
                node,
            };
            let invocation = NodeInvocation {
                payload,
                component_id: node.component.as_ref().map(|id| id.as_str().to_string()),
                profile: node.profile.clone(),
                node_kind: node.kind.clone(),
                config: node.config.clone(),
            };
            let response = self.host.invoke(&node_ctx, invocation)?;
            if response.terminal {
                return Ok(ExecutionResult {
                    output: response.output,
                    steps,
                    last_node: current_id.as_str().to_string(),
                });
            }
            payload = response.output;
            let next_id = match resolve_route(node, response.route_label.as_deref())? {
                Some(id) => {
                    NodeId::from_str(id).map_err(|_| FlowError::MissingNode(id.to_string()))?
                }
                None => {
                    return Ok(ExecutionResult {
                        output: payload,
                        steps,
                        last_node: current_id.as_str().to_string(),
                    });
                }
            };
            current_id = next_id;
        }
    }
}

/// Result from executing a flow.
#[derive(Debug)]
pub struct ExecutionResult {
    pub output: Value,
    pub steps: usize,
    pub last_node: String,
}

/// Context supplied to host invocations.
pub struct NodeContext<'a> {
    pub flow_id: &'a str,
    pub flow_kind: FlowKind,
    pub node_id: &'a str,
    pub node: &'a Node,
}

/// Invocation payload delivered to the host.
pub struct NodeInvocation {
    pub payload: Value,
    pub component_id: Option<String>,
    pub profile: Option<String>,
    pub node_kind: String,
    pub config: Value,
}

/// Response returned by the host invoker.
#[derive(Clone, Debug)]
pub struct NodeResponse {
    pub output: Value,
    pub route_label: Option<String>,
    pub terminal: bool,
}

/// Trait implemented by runtimes capable of invoking node components.
pub trait NodeInvoker {
    fn invoke(&self, ctx: &NodeContext<'_>, invocation: NodeInvocation) -> Result<NodeResponse>;
}

#[derive(thiserror::Error, Debug)]
pub enum FlowError {
    #[error("flow `{0}` has no nodes")]
    EmptyFlow(String),
    #[error("route references missing node `{0}`")]
    MissingNode(String),
    #[error("routing table for node is not a map")]
    InvalidRouting,
    #[error("route `{0}` missing from node routing")]
    MissingRoute(String),
    #[error("flow `{0}` exceeded maximum steps (possible loop)")]
    LoopDetected(String),
}

fn resolve_route<'a>(node: &'a Node, route: Option<&'a str>) -> Result<Option<&'a str>, FlowError> {
    if node.routing.is_null() {
        return Ok(None);
    }
    match node.routing {
        Value::Object(ref map) if !map.is_empty() => {
            let label = route.unwrap_or("default");
            let next = map
                .get(label)
                .and_then(Value::as_str)
                .ok_or_else(|| FlowError::MissingRoute(label.to_string()))?;
            Ok(Some(next))
        }
        Value::Object(_) => Ok(None),
        _ => Err(FlowError::InvalidRouting),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::flow::{Flow, FlowNodes, Node as FlowNode};
    use greentic_types::pack_manifest::{PackComponentRef, PackFlowRef, PackManifest};
    use greentic_types::{ComponentId, FlowId, NodeId, PackId, SemverReq};
    use semver::Version;
    use serde_json::json;
    use std::collections::HashMap as StdHashMap;

    #[test]
    fn executes_linear_flow() {
        let flow = build_flow(vec![("start", Some("next")), ("next", None)]);
        let pack = build_loaded_pack(flow);
        let host = StubHost::from([
            (
                "start",
                NodeResponse {
                    output: json!({ "message": "stage1" }),
                    route_label: None,
                    terminal: false,
                },
            ),
            (
                "next",
                NodeResponse {
                    output: json!({ "message": "done" }),
                    route_label: None,
                    terminal: true,
                },
            ),
        ]);
        let executor = FlowExecutor::new(&pack, &host);
        let result = executor
            .execute_messaging("flow", json!({ "input": true }))
            .unwrap();
        assert_eq!(result.last_node, "next");
        assert_eq!(result.steps, 2);
        assert_eq!(result.output["message"], "done");
    }

    #[test]
    fn errors_on_missing_route() {
        let flow = build_flow(vec![("start", Some("next"))]);
        let pack = build_loaded_pack(flow);
        let host = StubHost::from([(
            "start",
            NodeResponse {
                output: json!({}),
                route_label: Some("unknown".into()),
                terminal: false,
            },
        )]);
        let executor = FlowExecutor::new(&pack, &host);
        let err = executor.execute_messaging("flow", json!({})).unwrap_err();
        assert!(
            err.to_string().contains("route `unknown`"),
            "unexpected error: {err}"
        );
    }

    fn build_flow(nodes: Vec<(&str, Option<&str>)>) -> Flow {
        let mut map: FlowNodes = FlowNodes::default();
        let component_id: ComponentId = "component.demo".parse().unwrap();
        for (idx, (node_id, next)) in nodes.into_iter().enumerate() {
            let routing = next.map(|n| json!({ "default": n })).unwrap_or(Value::Null);
            let node_key: NodeId = node_id.parse().unwrap();
            map.insert(
                node_key,
                FlowNode {
                    kind: format!("kind/{idx}"),
                    profile: None,
                    component: Some(component_id.clone()),
                    config: Value::Null,
                    routing,
                },
            );
        }
        Flow {
            kind: FlowKind::Messaging,
            id: FlowId::from_str("flow").unwrap(),
            description: None,
            nodes: map,
        }
    }

    fn build_loaded_pack(flow: Flow) -> LoadedPack {
        let manifest = PackManifest {
            id: PackId::from_str("demo.pack").unwrap(),
            version: Version::new(1, 0, 0),
            name: Some("Demo".to_string()),
            flows: vec![PackFlowRef {
                id: flow.id.clone(),
                file: "flows/flow.ygtc".to_string(),
            }],
            components: vec![PackComponentRef {
                id: ComponentId::from_str("component.demo").unwrap(),
                version_req: SemverReq::parse("^1.0").unwrap(),
                source: None,
            }],
            profiles: None,
            component_sources: None,
            connectors: None,
            kind: None,
        };
        let mut flows = StdHashMap::new();
        flows.insert(flow.id.as_str().to_string(), flow);
        LoadedPack {
            manifest,
            flows,
            components: StdHashMap::new(),
        }
    }

    struct StubHost {
        responses: StdHashMap<String, NodeResponse>,
    }

    impl StubHost {
        fn from(entries: impl IntoIterator<Item = (&'static str, NodeResponse)>) -> Self {
            Self {
                responses: entries
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
            }
        }
    }

    impl NodeInvoker for StubHost {
        fn invoke(
            &self,
            ctx: &NodeContext<'_>,
            _invocation: NodeInvocation,
        ) -> Result<NodeResponse> {
            self.responses
                .get(ctx.node_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing response for node {}", ctx.node_id))
        }
    }
}
