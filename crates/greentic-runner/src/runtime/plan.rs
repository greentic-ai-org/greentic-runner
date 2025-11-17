use std::collections::HashMap;

use greentic_types::NodeId;
use greentic_types::flow::Flow;

use super::capabilities::{CapabilityError, NodeCapabilities, aggregate_node_capabilities};
use super::pack::LoadedPack;

/// Prepared flow handle bundling the parsed flow with per-node capabilities.
pub struct PreparedFlow<'a> {
    pub flow_id: &'a str,
    pub flow: &'a Flow,
    pub node_capabilities: HashMap<NodeId, NodeCapabilities>,
}

/// Builds a [`PreparedFlow`] for the requested flow identifier.
pub fn prepare_flow<'a>(
    pack: &'a LoadedPack,
    flow_id: &'a str,
) -> Result<PreparedFlow<'a>, FlowPrepError> {
    let flow = pack
        .flows
        .get(flow_id)
        .ok_or_else(|| FlowPrepError::FlowNotFound(flow_id.to_string()))?;
    let node_capabilities =
        aggregate_node_capabilities(flow, &pack.components).map_err(FlowPrepError::Capability)?;
    Ok(PreparedFlow {
        flow_id,
        flow,
        node_capabilities,
    })
}

#[derive(thiserror::Error, Debug)]
pub enum FlowPrepError {
    #[error("flow `{0}` not found in pack")]
    FlowNotFound(String),
    #[error("failed to aggregate capabilities: {0}")]
    Capability(#[from] CapabilityError),
}
