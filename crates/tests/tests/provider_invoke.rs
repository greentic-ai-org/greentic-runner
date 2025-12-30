use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use greentic_runner_host::pack::PackRuntime;
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine};
use greentic_runner_host::secrets::default_manager;
use greentic_runner_host::{
    HostConfig, PreopenSpec, RunnerWasiPolicy,
    storage::{DynSessionStore, state::new_state_store},
};
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, Flow, FlowComponentRef, FlowId,
    FlowKind, FlowMetadata, InputMapping, Node, NodeId, OutputMapping, PackFlowEntry, PackKind,
    PackManifest, ResourceHints, Routing, TelemetryHints,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::{Value, json};
use tempfile::TempDir;
use zip::ZipWriter;
use zip::write::FileOptions;

static WASI_POLICY: Lazy<Arc<RunnerWasiPolicy>> = Lazy::new(|| {
    Arc::new(
        RunnerWasiPolicy::new()
            .inherit_stdio(false)
            .with_preopen(PreopenSpec::new(".", "/").read_only(true)),
    )
});

#[tokio::test]
async fn provider_invoke_echoes_payload() -> Result<()> {
    let config = write_minimal_config()?;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("provider-dummy.gtpack");
    let component_path = build_dummy_component()?;
    let flows = vec![build_flow(
        "provider.echo",
        FlowKind::Job,
        json!({"echo": "/state/input/message"}),
        json!({"echoed": "/result/echo"}),
    )?];
    build_pack(&component_path, &pack_path, &flows)?;

    let pack = Arc::new(
        PackRuntime::load(
            &pack_path,
            Arc::clone(&config),
            None,
            Some(&pack_path),
            None::<DynSessionStore>,
            Some(new_state_store()),
            Arc::clone(&WASI_POLICY),
            default_manager(),
            None,
            false,
        )
        .await?,
    );
    let engine = FlowEngine::new(vec![Arc::clone(&pack)], Arc::clone(&config)).await?;
    let retry_config = config.retry_config().into();
    let ctx = FlowContext {
        tenant: config.tenant.as_str(),
        flow_id: "provider.echo",
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        retry_config,
        observer: None,
        mocks: None,
    };

    let input = json!({"message": "hello world"});
    let execution = engine.execute(ctx, input).await?;
    match execution.status {
        greentic_runner_host::runner::engine::FlowStatus::Completed => {}
        other => return Err(anyhow!("flow did not complete: {:?}", other)),
    }
    assert_eq!(
        execution.output,
        json!({"echoed": "hello world"}),
        "provider invoke should map output"
    );
    Ok(())
}

#[tokio::test]
async fn provider_invoke_supports_messaging_secrets_events() -> Result<()> {
    let _flag = EnvGuard::set("GREENTIC_PROVIDER_CORE_ONLY", "1");
    let config = write_minimal_config()?;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("provider-dummy.gtpack");
    let component_path = build_dummy_component()?;

    let flows = vec![
        build_flow(
            "provider.messaging",
            FlowKind::Messaging,
            json!({ "echo": "/state/input/message" }),
            json!({ "echoed": "/result/echo" }),
        )?,
        build_flow(
            "provider.secrets",
            FlowKind::Job,
            json!({ "echo": "/state/input/secret" }),
            json!({ "secret_echo": "/result/echo" }),
        )?,
        build_flow(
            "provider.events",
            FlowKind::Event,
            json!({ "echo": "/state/input/event" }),
            json!({ "event_echo": "/result/echo" }),
        )?,
    ];

    build_pack(&component_path, &pack_path, &flows)?;

    let pack = Arc::new(
        PackRuntime::load(
            &pack_path,
            Arc::clone(&config),
            None,
            Some(&pack_path),
            None::<DynSessionStore>,
            Some(new_state_store()),
            Arc::clone(&WASI_POLICY),
            default_manager(),
            None,
            false,
        )
        .await?,
    );
    let engine = FlowEngine::new(vec![Arc::clone(&pack)], Arc::clone(&config)).await?;
    let retry_config = config.retry_config().into();

    let cases = vec![
        (
            "provider.messaging",
            json!({"message": "hi"}),
            json!({"echoed": "hi"}),
            Some("messaging"),
        ),
        (
            "provider.secrets",
            json!({"secret": "keep-me"}),
            json!({"secret_echo": "keep-me"}),
            Some("secrets"),
        ),
        (
            "provider.events",
            json!({"event": "ping"}),
            json!({"event_echo": "ping"}),
            Some("events"),
        ),
    ];

    for (flow_id, input, expected, action) in cases {
        let ctx = FlowContext {
            tenant: config.tenant.as_str(),
            flow_id,
            node_id: None,
            tool: None,
            action,
            session_id: None,
            provider_id: None,
            retry_config,
            observer: None,
            mocks: None,
        };

        let execution = engine.execute(ctx, input).await?;
        match execution.status {
            greentic_runner_host::runner::engine::FlowStatus::Completed => {}
            other => return Err(anyhow!("flow {flow_id} did not complete: {:?}", other)),
        }
        assert_eq!(
            execution.output, expected,
            "{flow_id} should map provider-core output"
        );
    }

    Ok(())
}

fn write_minimal_config() -> Result<Arc<HostConfig>> {
    let temp = TempDir::new()?;
    let path = temp.path().join("bindings.yaml");
    let contents = r#"
tenant: demo
flow_type_bindings: {}
rate_limits: {}
retry: {}
timers: []
"#;
    std::fs::write(&path, contents)?;
    let mut cfg = HostConfig::load_from_path(&path).context("load minimal host bindings")?;
    cfg.secrets_policy = greentic_runner_host::config::SecretsPolicy::allow_all();
    Ok(Arc::new(cfg))
}

fn build_pack(component_path: &Path, pack_path: &Path, flows: &[Flow]) -> Result<()> {
    let extensions = provider_extension();
    let flow_entries = flows
        .iter()
        .map(|flow| PackFlowEntry {
            id: flow.id.clone(),
            kind: flow.kind,
            flow: flow.clone(),
            tags: Vec::new(),
            entrypoints: vec!["default".into()],
        })
        .collect::<Vec<_>>();
    let supported_kinds = flows.iter().map(|flow| flow.kind).collect::<Vec<_>>();
    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: "provider.test".parse()?,
        version: Version::parse("0.0.1")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "provider.dummy".parse()?,
            version: Version::parse("0.1.0")?,
            supports: supported_kinds,
            world: "greentic:provider-core@1.0.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities: ComponentCapabilities::default(),
            configurators: None,
            operations: Vec::new(),
            config_schema: None,
            resources: ResourceHints::default(),
            dev_flows: BTreeMap::new(),
        }],
        flows: flow_entries,
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        extensions: Some(extensions),
    };

    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = greentic_types::encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    writer.start_file("components/provider.dummy.wasm", options)?;
    let mut file = std::fs::File::open(component_path)
        .with_context(|| format!("open component {}", component_path.display()))?;
    std::io::copy(&mut file, &mut writer)?;
    writer.finish().context("finalise provider pack")?;
    Ok(())
}

fn provider_extension() -> BTreeMap<String, greentic_types::ExtensionRef> {
    let mut exts = BTreeMap::new();
    let inline = json!({
        "providers": [
            {
                "provider_id": "dummy",
                "provider_type": "example.dummy",
                "capabilities": [],
                "ops": ["echo"],
                "config_schema_ref": "schemas/config.schema.json",
                "state_schema_ref": "schemas/state.schema.json",
                "runtime": {
                    "component_ref": "provider.dummy",
                    "export": "provider-core",
                    "world": "greentic:provider-core@1.0.0"
                },
                "docs_ref": "schemas/README.md"
            }
        ]
    });
    exts.insert(
        "greentic.ext.provider".into(),
        greentic_types::ExtensionRef {
            kind: "greentic.ext.provider".into(),
            version: "1.0.0".into(),
            digest: None,
            location: None,
            inline: Some(inline),
        },
    );
    exts
}

fn build_flow(flow_id: &str, flow_kind: FlowKind, in_map: Value, out_map: Value) -> Result<Flow> {
    let node_id = NodeId::from_str("provider").context("node id")?;
    let mut nodes = HashMap::new();
    nodes.insert(
        node_id.clone(),
        Node {
            id: node_id.clone(),
            component: FlowComponentRef {
                id: "provider.invoke".parse()?,
                pack_alias: None,
                operation: None,
            },
            input: InputMapping {
                mapping: json!({
                    "provider_type": "example.dummy",
                    "op": "echo",
                    "in_map": in_map,
                    "out_map": out_map
                }),
            },
            output: OutputMapping {
                mapping: Value::Object(serde_json::Map::new()),
            },
            routing: Routing::End,
            telemetry: TelemetryHints::default(),
        },
    );

    Ok(Flow {
        schema_version: "1.0".into(),
        id: FlowId::from_str(flow_id)?,
        kind: flow_kind,
        entrypoints: BTreeMap::from([("default".to_string(), Value::String(node_id.to_string()))]),
        nodes: nodes.into_iter().collect(),
        metadata: FlowMetadata::default(),
    })
}

fn build_dummy_component() -> Result<PathBuf> {
    let root = fixture_path("tests/assets/provider-core-dummy");
    let wasm = root.join("target/wasm32-wasip2/release/provider_core_dummy.wasm");
    if !wasm.exists() {
        let offline = std::env::var("CARGO_NET_OFFLINE").ok();
        let status = Command::new("cargo")
            .args([
                "build",
                "--release",
                "--target",
                "wasm32-wasip2",
                "--manifest-path",
                root.join("Cargo.toml").to_str().expect("manifest path"),
            ])
            .envs(offline.map(|val| ("CARGO_NET_OFFLINE", val)))
            .status()
            .context("build provider-core dummy component")?;
        if !status.success() {
            return Err(anyhow!("provider-core dummy build failed with {status}"));
        }
    }
    Ok(wasm)
}

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(relative)
}

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(val) => unsafe {
                std::env::set_var(self.key, val);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}
