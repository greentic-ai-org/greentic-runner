use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use greentic_runner::runtime::{
    FlowExecutor, NodeContext, NodeInvocation, NodeInvoker, NodeResponse,
    aggregate_node_capabilities, load_pack, prepare_flow,
};
use greentic_types::FlowKind;
use serde_json::Value;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "greentic-runtime",
    about = "Lightweight Greentic pack executor (spec 0.4)"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// List flows declared in the pack manifest.
    List {
        /// Path to a .gtpack archive or expanded directory.
        #[arg(long)]
        pack: PathBuf,
    },
    /// Execute a flow using the in-process runtime (components are noop).
    Run {
        /// Path to a .gtpack archive or expanded directory.
        #[arg(long)]
        pack: PathBuf,
        /// Flow identifier to execute.
        #[arg(long)]
        flow: String,
        /// Flow kind (messaging|events).
        #[arg(long, value_parser = parse_flow_kind)]
        kind: FlowKind,
        /// JSON payload delivered to the ingress node (defaults to {}).
        #[arg(long = "input", default_value = "{}")]
        input_json: String,
        /// Maximum nodes to execute before aborting.
        #[arg(long, default_value_t = 2048)]
        max_steps: usize,
    },
    /// Show per-node component/profile/capability metadata for a flow.
    Capabilities {
        /// Path to a .gtpack archive or expanded directory.
        #[arg(long)]
        pack: PathBuf,
        /// Flow identifier to inspect.
        #[arg(long)]
        flow: String,
    },
}

fn parse_flow_kind(value: &str) -> Result<FlowKind, String> {
    match value.to_ascii_lowercase().as_str() {
        "messaging" => Ok(FlowKind::Messaging),
        "events" => Ok(FlowKind::Events),
        other => Err(format!("unknown flow kind `{other}`")),
    }
}

fn main() -> Result<()> {
    init_logging();
    let cli = Cli::parse();
    match cli.command {
        Commands::List { pack } => list_flows(&pack),
        Commands::Run {
            pack,
            flow,
            kind,
            input_json,
            max_steps,
        } => run_flow(&pack, &flow, kind, &input_json, max_steps),
        Commands::Capabilities { pack, flow } => show_capabilities(&pack, &flow),
    }
}

fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

fn list_flows(path: &PathBuf) -> Result<()> {
    let loaded =
        load_pack(path).with_context(|| format!("failed to load pack from {}", path.display()))?;
    println!(
        "{}@{}",
        loaded.manifest.id.as_str(),
        loaded.manifest.version
    );
    for flow in loaded.manifest.flows.iter() {
        let kind = loaded
            .flows
            .get(flow.id.as_str())
            .map(|doc| format_flow_kind(doc.kind))
            .unwrap_or("unknown");
        println!("- {} ({})", flow.id.as_str(), kind);
    }
    Ok(())
}

fn run_flow(
    path: &PathBuf,
    flow_id: &str,
    kind: FlowKind,
    input_json: &str,
    max_steps: usize,
) -> Result<()> {
    let loaded =
        load_pack(path).with_context(|| format!("failed to load pack from {}", path.display()))?;
    let prepared = prepare_flow(&loaded, flow_id)?;
    if prepared.flow.kind != kind {
        bail!(
            "flow `{flow_id}` is {:?} but run as {:?}",
            prepared.flow.kind,
            kind
        );
    }
    let payload: Value =
        serde_json::from_str(input_json).with_context(|| "input must be valid JSON")?;
    let invoker = NoopInvoker;
    let executor = FlowExecutor::new(&loaded, &invoker).with_max_steps(max_steps);
    let result = match kind {
        FlowKind::Messaging => executor.execute_messaging(flow_id, payload),
        FlowKind::Events => executor.execute_events(flow_id, payload),
    };
    match result {
        Ok(output) => {
            println!(
                "flow completed after {} steps (last node: {})",
                output.steps, output.last_node
            );
            println!("{}", serde_json::to_string_pretty(&output.output)?);
            Ok(())
        }
        Err(err) => {
            bail!("flow failed: {err}");
        }
    }
}

fn format_flow_kind(kind: FlowKind) -> &'static str {
    match kind {
        FlowKind::Messaging => "messaging",
        FlowKind::Events => "events",
    }
}

fn show_capabilities(path: &PathBuf, flow_id: &str) -> Result<()> {
    let loaded =
        load_pack(path).with_context(|| format!("failed to load pack from {}", path.display()))?;
    let flow = loaded
        .flows
        .get(flow_id)
        .with_context(|| format!("flow `{flow_id}` not found in {}", path.display()))?;

    let map = aggregate_node_capabilities(flow, &loaded.components)?;
    println!("flow {flow_id} ({})", format_flow_kind(flow.kind));
    let mut entries: Vec<_> = map.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    for (node_id, caps) in entries {
        println!("- node `{}`", node_id.as_str());
        if let Some(component) = caps.component {
            println!("  component: {}", component.as_str());
        } else {
            println!("  component: <none>");
        }
        match (
            caps.requested_profile.as_deref(),
            caps.resolved_profile.as_deref(),
        ) {
            (Some(requested), Some(resolved)) if requested != resolved => {
                println!("  profile: {} (resolved -> {})", requested, resolved);
            }
            (Some(requested), _) => println!("  profile: {}", requested),
            (None, Some(resolved)) => println!("  profile: {}", resolved),
            _ => println!("  profile: <none>"),
        }
        if let Some(capabilities) = caps.capabilities.as_ref() {
            if let Some(secrets) = capabilities.host.secrets.as_ref()
                && !secrets.required.is_empty()
            {
                println!("  secrets: {}", secrets.required.join(", "));
            }
            if let Some(env_caps) = capabilities.wasi.env.as_ref()
                && !env_caps.allow.is_empty()
            {
                println!("  env allow: {}", env_caps.allow.join(", "));
            }
        }
    }
    Ok(())
}

/// Placeholder invoker that simply forwards payloads.
struct NoopInvoker;

impl NodeInvoker for NoopInvoker {
    fn invoke(&self, _ctx: &NodeContext<'_>, invocation: NodeInvocation) -> Result<NodeResponse> {
        Ok(NodeResponse {
            output: invocation.payload,
            route_label: None,
            terminal: false,
        })
    }
}
