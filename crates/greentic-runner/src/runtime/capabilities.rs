use std::collections::HashMap;

use greentic_types::component::{ComponentCapabilities, ComponentManifest, ComponentProfileError};
use greentic_types::flow::Flow;
use greentic_types::{ComponentId, FlowKind, NodeId};

/// Per-node capability view derived from component manifests + profiles.
#[derive(Clone, Debug)]
pub struct NodeCapabilities {
    /// Node kind (opaque string from the flow).
    pub node_kind: String,
    /// Optional bound component identifier.
    pub component: Option<ComponentId>,
    /// Profile requested by the flow author.
    pub requested_profile: Option<String>,
    /// Profile selected after validating against the manifest defaults.
    pub resolved_profile: Option<String>,
    /// Flow kind this node participates in.
    pub flow_kind: FlowKind,
    /// Effective component capabilities (absent when no component is bound).
    pub capabilities: Option<ComponentCapabilities>,
}

/// Aggregates per-node capabilities for a flow using the provided component manifests.
pub fn aggregate_node_capabilities(
    flow: &Flow,
    components: &HashMap<String, ComponentManifest>,
) -> Result<HashMap<NodeId, NodeCapabilities>, CapabilityError> {
    let mut out = HashMap::new();
    for (node_id, node) in &flow.nodes {
        let mut entry = NodeCapabilities {
            node_kind: node.kind.clone(),
            component: node.component.clone(),
            requested_profile: node.profile.clone(),
            resolved_profile: None,
            flow_kind: flow.kind,
            capabilities: None,
        };

        if let Some(component_id) = node.component.as_ref() {
            let manifest = components.get(component_id.as_str()).ok_or_else(|| {
                CapabilityError::MissingComponent {
                    node_id: node_id.as_str().to_string(),
                    component: component_id.as_str().to_string(),
                }
            })?;
            if !manifest.supports_kind(flow.kind) {
                return Err(CapabilityError::UnsupportedComponent {
                    node_id: node_id.as_str().to_string(),
                    component: component_id.as_str().to_string(),
                    flow_kind: flow.kind,
                });
            }
            let resolved = manifest
                .select_profile(node.profile.as_deref())
                .map_err(|source| CapabilityError::ProfileError {
                    node_id: node_id.as_str().to_string(),
                    component: component_id.as_str().to_string(),
                    source,
                })?;
            entry.resolved_profile = resolved.map(|value| value.to_string());
            entry.capabilities = Some(manifest.capabilities.clone());
        }

        out.insert(node_id.clone(), entry);
    }
    Ok(out)
}

/// Errors emitted while aggregating capabilities.
#[derive(thiserror::Error, Debug)]
pub enum CapabilityError {
    #[error("node `{node_id}` references missing component `{component}`")]
    MissingComponent { node_id: String, component: String },
    #[error(
        "component `{component}` used by node `{node_id}` does not support `{flow_kind:?}` flows"
    )]
    UnsupportedComponent {
        node_id: String,
        component: String,
        flow_kind: FlowKind,
    },
    #[error("node `{node_id}` profile error for component `{component}`: {source}")]
    ProfileError {
        node_id: String,
        component: String,
        #[source]
        source: ComponentProfileError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::component::{
        ComponentProfiles, HostCapabilities, SecretsCapabilities, WasiCapabilities,
    };
    use greentic_types::flow::{Flow, FlowKind as Kind, FlowNodes, Node};
    use greentic_types::{ComponentId, FlowId, NodeId};
    use indexmap::IndexMap;
    use semver::Version;
    use serde_json::Value;
    use std::str::FromStr;

    #[test]
    fn aggregates_profiles_and_capabilities() {
        let manifest = demo_manifest();
        let mut flow_nodes: FlowNodes = FlowNodes::default();
        let node_id: NodeId = "start".parse().unwrap();
        let component_id = ComponentId::from_str("demo.component").unwrap();
        flow_nodes.insert(
            node_id.clone(),
            Node {
                kind: "demo/node".into(),
                profile: Some("stateless".into()),
                component: Some(component_id.clone()),
                config: Value::Null,
                routing: Value::Null,
            },
        );
        let flow = Flow {
            kind: Kind::Messaging,
            id: "flow".parse().unwrap(),
            description: None,
            nodes: flow_nodes,
        };
        let mut components = HashMap::new();
        components.insert("demo.component".into(), manifest.clone());

        let map = aggregate_node_capabilities(&flow, &components).expect("aggregate");
        let entry = map.get(&node_id).expect("node");
        assert_eq!(entry.resolved_profile.as_deref(), Some("stateless"));
        assert_eq!(
            entry
                .capabilities
                .as_ref()
                .unwrap()
                .host
                .secrets
                .as_ref()
                .unwrap()
                .required,
            vec!["SECRET_KEY".to_string()]
        );
    }

    #[test]
    fn errors_on_missing_component() {
        let node_id: NodeId = "start".parse().unwrap();
        let flow = Flow {
            kind: Kind::Messaging,
            id: FlowId::from_str("flow").unwrap(),
            description: None,
            nodes: IndexMap::from_iter([(
                node_id,
                Node {
                    kind: "demo/node".into(),
                    profile: None,
                    component: Some(ComponentId::from_str("missing.component").unwrap()),
                    config: Value::Null,
                    routing: Value::Null,
                },
            )]),
        };
        let err = aggregate_node_capabilities(&flow, &HashMap::new()).unwrap_err();
        assert!(matches!(err, CapabilityError::MissingComponent { .. }));
    }

    fn demo_manifest() -> ComponentManifest {
        ComponentManifest {
            id: ComponentId::from_str("demo.component").unwrap(),
            version: Version::new(1, 0, 0),
            supports: vec![Kind::Messaging],
            world: "greentic:demo/world@1.0.0".into(),
            profiles: ComponentProfiles {
                default: Some("stateless".into()),
                supported: vec!["stateless".into()],
            },
            capabilities: ComponentCapabilities {
                wasi: WasiCapabilities::default(),
                host: HostCapabilities {
                    secrets: Some(SecretsCapabilities {
                        required: vec!["SECRET_KEY".into()],
                    }),
                    ..HostCapabilities::default()
                },
            },
            configurators: None,
        }
    }
}
