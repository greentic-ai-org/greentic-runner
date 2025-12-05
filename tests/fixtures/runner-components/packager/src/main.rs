use anyhow::{Context, Result, bail};
use clap::Parser;
use greentic_flow::flow_bundle::{
    FlowBundle, NodeRef, blake3_hex, canonicalize_json, extract_component_pins,
};
use greentic_flow::ir::{FlowIR, NodeIR, RouteIR};
use greentic_pack::builder::{ComponentArtifact, PackBuilder, PackMeta, Signing};
use greentic_pack::reader::{SigningPolicy, open_pack};
use semver::Version;
use serde::Deserialize;
use serde_yaml_bw as serde_yaml;
use indexmap::IndexMap;
use std::fs;
use std::path::PathBuf;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use zip::ZipArchive;
use serde_json::Map as JsonMap;

#[derive(Parser, Debug)]
#[command(about = "Build runner-components .gtpack fixture")]
struct Args {
    /// Root directory of the pack (pack.yaml + flows/ + components/)
    #[arg(long, default_value = ".")]
    root: PathBuf,
    /// Output .gtpack path
    #[arg(long, default_value = "runner-components.gtpack")]
    out: PathBuf,
    /// Optional path to write manifest.cbor for convenience
    #[arg(long)]
    manifest_out: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct PackSpec {
    #[serde(rename = "packVersion")]
    pack_version: u32,
    id: String,
    version: String,
    #[serde(default)]
    name: Option<String>,
    components: Vec<ComponentSpec>,
    flows: Vec<FlowSpec>,
    #[serde(default)]
    entry_flows: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ComponentSpec {
    id: String,
    path: String,
    #[serde(default)]
    version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FlowSpec {
    id: String,
    kind: String,
    entry: String,
    file_yaml: String,
}

#[derive(Debug, Deserialize)]
struct FlowDoc {
    id: String,
    kind: String,
    entry: String,
    nodes: std::collections::BTreeMap<String, NodeDoc>,
}

#[derive(Debug, Deserialize)]
struct NodeDoc {
    component: String,
    payload: serde_json::Value,
    routes: Vec<NodeRoute>,
}

#[derive(Debug, Deserialize)]
struct NodeRoute {
    #[serde(default)]
    to: Option<String>,
    #[serde(default)]
    out: Option<bool>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let spec_path = args.root.join("pack.yaml");
    let spec: PackSpec = {
        let text = fs::read_to_string(&spec_path)
            .with_context(|| format!("failed to read {}", spec_path.display()))?;
        serde_yaml::from_str(&text)
            .with_context(|| format!("failed to parse {}", spec_path.display()))?
    };

    if spec.flows.is_empty() {
        bail!("pack.yaml must declare at least one flow for fixture packaging");
    }

    let pack_version =
        Version::parse(&spec.version).with_context(|| format!("invalid semver {}", spec.version))?;

    let entry_flows = if spec.entry_flows.is_empty() {
        spec.flows.iter().map(|f| f.id.clone()).collect()
    } else {
        spec.entry_flows.clone()
    };

    let created_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string());

    let meta = PackMeta {
        pack_version: spec.pack_version,
        pack_id: spec.id.clone(),
        version: pack_version,
        name: spec.name.clone().unwrap_or_else(|| spec.id.clone()),
        kind: None,
        description: None,
        authors: Vec::new(),
        license: None,
        homepage: None,
        support: None,
        vendor: None,
        imports: Vec::new(),
        entry_flows,
        created_at_utc: created_at,
        events: None,
        repo: None,
        messaging: None,
        interfaces: Vec::new(),
        annotations: JsonMap::new(),
        distribution: None,
        components: Vec::new(),
    };

    let mut builder = PackBuilder::new(meta);

    for flow_spec in &spec.flows {
        let flow_path = args.root.join(&flow_spec.file_yaml);
        let yaml = fs::read_to_string(&flow_path)
            .with_context(|| format!("failed to read flow {}", flow_path.display()))?;
        let doc: FlowDoc =
            serde_yaml::from_str(&yaml).context("failed to parse flow yaml for packaging")?;
        if doc.id != flow_spec.id {
            bail!(
                "flow id mismatch: spec {} vs flow doc {}",
                flow_spec.id,
                doc.id
            );
        }
        if doc.kind != flow_spec.kind {
            bail!(
                "flow kind mismatch: spec {} vs flow doc {}",
                flow_spec.kind,
                doc.kind
            );
        }
        if doc.entry != flow_spec.entry {
            bail!(
                "flow entry mismatch: spec {} vs flow doc {}",
                flow_spec.entry,
                doc.entry
            );
        }
        let mut nodes = IndexMap::new();
        for (node_id, node) in doc.nodes {
            let routes = node
                .routes
                .into_iter()
                .map(|route| RouteIR {
                    to: route.to,
                    out: route.out.unwrap_or(false),
                })
                .collect();
            nodes.insert(
                node_id.clone(),
                NodeIR {
                    component: node.component,
                    payload_expr: node.payload,
                    routes,
                },
            );
        }

        let ir = FlowIR {
            id: doc.id.clone(),
            flow_type: doc.kind.clone(),
            start: Some(doc.entry.clone()),
            parameters: serde_json::Value::Null,
            nodes,
        };
        let flow_json = serde_json::to_value(&ir)?;
        let canonical_json = canonicalize_json(&flow_json);
        let hash_blake3 = blake3_hex(&serde_json::to_vec(&canonical_json)?);
        let entry = ir
            .start
            .clone()
            .unwrap_or_else(|| ir.nodes.keys().next().cloned().unwrap_or_default());
        let node_refs = extract_component_pins(&ir)
            .into_iter()
            .map(|(node_id, component)| NodeRef {
                node_id,
                component,
                schema_id: None,
            })
            .collect();
        let bundle = FlowBundle {
            id: ir.id.clone(),
            kind: ir.flow_type.clone(),
            entry,
            yaml: yaml.clone(),
            json: canonical_json,
            hash_blake3,
            nodes: node_refs,
        };
        builder = builder.with_flow(bundle);
    }

    for comp in &spec.components {
        let version = comp
            .version
            .clone()
            .unwrap_or_else(|| spec.version.clone());
        let semver = Version::parse(&version)
            .with_context(|| format!("invalid component version {}", version))?;
        let wasm_path = args.root.join(&comp.path);
        builder = builder.with_component(ComponentArtifact {
            name: comp.id.clone(),
            version: semver,
            wasm_path,
            schema_json: None,
            manifest_json: None,
            capabilities: None,
            world: Some("greentic:component@0.4.0".to_string()),
            hash_blake3: None,
        });
    }

    builder = builder.with_signing(Signing::Dev);

    let build = builder.build(&args.out)?;
    println!(
        "built gtpack at {} (manifest blake3 {})",
        build.out_path.display(),
        build.manifest_hash_blake3
    );

    if let Some(manifest_out) = args.manifest_out.as_ref() {
        let mut archive = ZipArchive::new(
            fs::File::open(&args.out)
                .with_context(|| format!("failed to reopen {}", args.out.display()))?,
        )?;
        let mut manifest_file = archive
            .by_name("manifest.cbor")
            .context("manifest.cbor missing from archive")?;
        let mut manifest_bytes = Vec::new();
        std::io::copy(&mut manifest_file, &mut manifest_bytes)?;
        fs::write(manifest_out, &manifest_bytes)
            .with_context(|| format!("failed to write {}", manifest_out.display()))?;
        println!("wrote manifest to {}", manifest_out.display());
    }

    // Sanity: ensure we can open the gtpack we just built.
    if let Err(err) = open_pack(&args.out, SigningPolicy::DevOk) {
        bail!("gtpack sanity check failed after build: {}", err.message);
    }

    Ok(())
}
