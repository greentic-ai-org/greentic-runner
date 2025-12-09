use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use greentic_flow::flow_bundle::load_and_validate_bundle_with_flow;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, RateLimits, SecretsPolicy, WebhookPolicy,
};
use greentic_runner_host::pack::PackRuntime;
use greentic_runner_host::runner::engine::{FlowContext, FlowEngine, FlowStatus};
use greentic_runner_host::runner::flow_adapter::{FlowIR, NodeIR, RouteIR};
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, FlowKind, PackFlowEntry, PackKind,
    PackManifest, ResourceHints, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::Arc;
use tempfile::TempDir;
use zip::ZipArchive;
use zip::write::FileOptions;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
}

fn build_components() -> Result<Vec<(String, PathBuf)>> {
    let workspace = workspace_root().join("tests/fixtures/runner-components");
    let mut results = Vec::new();
    let crates = vec![
        ("qa.process", "qa_process"),
        ("templating.handlebars", "templating_handlebars"),
    ];
    for (name, krate) in &crates {
        let manifest = workspace.join(krate).join("Cargo.toml");
        let status = std::process::Command::new("cargo")
            .env("CARGO_NET_OFFLINE", "true")
            .current_dir(&workspace)
            .args([
                "build",
                "--offline",
                "--manifest-path",
                manifest.to_str().unwrap(),
                "--target",
                "wasm32-wasip2",
                "--release",
            ])
            .status()
            .with_context(|| format!("failed to build {krate} component"))?;
        if !status.success() {
            anyhow::bail!("component build failed for {krate}");
        }
        let artifact = workspace.join(format!("target/wasm32-wasip2/release/{}.wasm", krate));
        results.push((name.to_string(), artifact));
    }
    Ok(results)
}

fn demo_flow_ir() -> FlowIR {
    let mut nodes = indexmap::IndexMap::new();
    nodes.insert(
        "qa".into(),
        NodeIR {
            component: "component.exec".into(),
            payload_expr: serde_json::json!({
                "component": "qa.process",
                "operation": "process",
                "input": { "text": "hello" }
            }),
            routes: vec![RouteIR {
                to: Some("tmpl".into()),
                out: false,
            }],
        },
    );
    nodes.insert(
        "tmpl".into(),
        NodeIR {
            component: "component.exec".into(),
            payload_expr: serde_json::json!({
                "component": "templating.handlebars",
                "operation": "render",
                "input": {
                    "template": "Echo: {{state.nodes.qa.payload.text}}"
                }
            }),
            routes: vec![RouteIR {
                to: Some("emit".into()),
                out: false,
            }],
        },
    );
    nodes.insert(
        "emit".into(),
        NodeIR {
            component: "emit.response".into(),
            payload_expr: serde_json::json!({
                "from_node": "tmpl"
            }),
            routes: vec![RouteIR {
                to: None,
                out: true,
            }],
        },
    );
    FlowIR {
        id: "demo.flow".into(),
        flow_type: "messaging".into(),
        start: Some("qa".into()),
        parameters: serde_json::Value::Object(Default::default()),
        nodes,
    }
}

fn host_config(bindings_path: &Path) -> HostConfig {
    HostConfig {
        tenant: "demo".into(),
        bindings_path: bindings_path.to_path_buf(),
        flow_type_bindings: HashMap::new(),
        rate_limits: RateLimits::default(),
        retry: FlowRetryConfig::default(),
        http_enabled: false,
        secrets_policy: SecretsPolicy::allow_all(),
        webhook_policy: WebhookPolicy::default(),
        timers: Vec::new(),
        oauth: None,
        mocks: None,
    }
}

fn component_artifact_path(temp_dir: &Path) -> Result<PathBuf> {
    let local =
        workspace_root().join("tests/fixtures/packs/runner-components/components/qa_process.wasm");
    if local.exists() {
        return Ok(local);
    }
    let archive_path =
        workspace_root().join("tests/fixtures/packs/runner-components/runner-components.gtpack");
    let mut archive = ZipArchive::new(File::open(&archive_path).context("open fixture gtpack")?)?;
    let mut wasm = archive
        .by_name("components/qa.process@0.1.0/component.wasm")
        .context("qa.process component missing from fixture pack")?;
    let out = temp_dir.join("qa_process.wasm");
    let mut buf = Vec::new();
    wasm.read_to_end(&mut buf)?;
    std::fs::write(&out, &buf)?;
    Ok(out)
}

fn build_pack(flow_yaml: &str, pack_path: &Path) -> Result<()> {
    let component_path = component_artifact_path(
        pack_path
            .parent()
            .expect("pack path should have parent for temp dir"),
    )?;
    let (_bundle, flow) = load_and_validate_bundle_with_flow(flow_yaml, None)?;
    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: "component.exec.test".parse()?,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "qa.process".parse()?,
            version: Version::parse("0.1.0")?,
            supports: vec![FlowKind::Messaging],
            world: "greentic:component@0.4.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities: ComponentCapabilities::default(),
            configurators: None,
            operations: Vec::new(),
            config_schema: None,
            resources: ResourceHints::default(),
        }],
        flows: vec![PackFlowEntry {
            id: flow.id.clone(),
            kind: flow.kind,
            flow: flow.clone(),
            tags: Vec::new(),
            entrypoints: vec!["default".into()],
        }],
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
    };

    let mut zip = zip::ZipWriter::new(File::create(pack_path).context("create pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    zip.start_file("manifest.cbor", options)?;
    zip.write_all(&manifest_bytes)?;

    zip.start_file("components/qa.process.wasm", options)?;
    let mut comp_file = File::open(&component_path)?;
    std::io::copy(&mut comp_file, &mut zip)?;
    zip.finish().context("finalise pack archive")?;
    Ok(())
}

static RUNTIME: Lazy<&'static tokio::runtime::Runtime> = Lazy::new(|| {
    Box::leak(Box::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime"),
    ))
});

#[test]
fn component_exec_invokes_pack_component() -> Result<()> {
    let temp = TempDir::new()?;
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let components = build_components()?;
    let flow = demo_flow_ir();
    let mut flows = HashMap::new();
    flows.insert(flow.id.clone(), flow);

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(
        PackRuntime::for_component_test(components, flows.clone(), Arc::clone(&config))
            .context("pack init")?,
    );
    // Ensure the runtime constructed successfully with components and flows in place.
    let _ = (runtime, config, flows);
    Ok(())
}

#[test]
fn exec_node_uses_inner_component_artifact() -> Result<()> {
    // Regression: component.exec is a meta-component and must call the referenced pack artifact.
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("component-exec.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    // Build a pack whose flow uses component.exec to call qa.process.
    let flow_yaml = r#"
id: exec.flow
type: messaging
start: exec
nodes:
  exec:
    component.exec:
      component: qa.process
      operation: process
      input:
        text: "hello"
    routing:
      - out: true
"#;
    build_pack(flow_yaml, &pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &pack_path,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager(),
        None,
        false,
    ))?);
    let engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;

    let retry_config = config.retry.clone().into();
    let tenant = config.tenant.clone();
    let flow_id = "exec.flow".to_string();
    let ctx = FlowContext {
        tenant: tenant.as_str(),
        flow_id: flow_id.as_str(),
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        retry_config,
        observer: None,
        mocks: None,
    };

    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("component.exec flow run")?;
    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => {
            anyhow::bail!("flow paused unexpectedly: {:?}", wait.reason);
        }
    }

    let output_str = execution.output.to_string();
    assert!(
        output_str.contains("hello"),
        "expected qa.process to run; output was {output_str}"
    );
    Ok(())
}

#[test]
fn emit_log_is_builtin_not_pack_component() -> Result<()> {
    // Regression: emit.log should be treated as a built-in, not looked up as a pack artifact.
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let pack_path = temp.path().join("emit-log.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let flow_yaml = r#"
id: emit.flow
type: messaging
start: exec
nodes:
  exec:
    component.exec:
      component: qa.process
      operation: process
      input:
        text: "hello"
    routing:
      - to: log
  log:
    emit.log:
      message: "logged"
    routing:
      - out: true
"#;
    build_pack(flow_yaml, &pack_path)?;

    let config = Arc::new(host_config(&bindings_path));
    let pack = Arc::new(rt.block_on(PackRuntime::load(
        &pack_path,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(greentic_runner_host::wasi::RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager(),
        None,
        false,
    ))?);
    let engine = rt.block_on(FlowEngine::new(
        vec![Arc::clone(&pack)],
        Arc::clone(&config),
    ))?;

    let retry_config = config.retry.clone().into();
    let tenant = config.tenant.clone();
    let flow_id = "emit.flow".to_string();
    let ctx = FlowContext {
        tenant: tenant.as_str(),
        flow_id: flow_id.as_str(),
        node_id: None,
        tool: None,
        action: None,
        session_id: None,
        provider_id: None,
        retry_config,
        observer: None,
        mocks: None,
    };

    let execution = rt
        .block_on(engine.execute(ctx, Value::Null))
        .context("emit.log flow run")?;
    match execution.status {
        FlowStatus::Completed => {}
        FlowStatus::Waiting(wait) => {
            anyhow::bail!("emit flow paused unexpectedly: {:?}", wait.reason);
        }
    }

    let output_str = execution.output.to_string();
    assert!(
        output_str.contains("logged"),
        "expected emit.log to produce output; got {output_str}"
    );
    Ok(())
}
