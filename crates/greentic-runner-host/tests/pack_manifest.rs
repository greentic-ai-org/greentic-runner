use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use greentic_flow::flow_bundle::load_and_validate_bundle_with_flow;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, RateLimits, SecretsPolicy, WebhookPolicy,
};
use greentic_runner_host::pack::PackRuntime;
use greentic_runner_host::wasi::RunnerWasiPolicy;
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, FlowKind, PackFlowEntry, PackKind,
    PackManifest, ResourceHints, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::json;
use std::path::Path;
use std::process::Command;
use tempfile::TempDir;
use zip::ZipWriter;
use zip::write::FileOptions;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .expect("workspace root")
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

fn build_pack(pack_path: &Path) -> Result<()> {
    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let flow_yaml =
        std::fs::read_to_string(fixtures.join("flows/demo.yaml")).context("read flow yaml")?;
    let (_bundle, flow) = load_and_validate_bundle_with_flow(&flow_yaml, None)?;

    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: "runner.components.test".parse()?,
        version: Version::parse("0.0.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![
            ComponentManifest {
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
                dev_flows: BTreeMap::new(),
            },
            ComponentManifest {
                id: "templating.handlebars".parse()?,
                version: Version::parse("0.1.0")?,
                supports: vec![FlowKind::Messaging],
                world: "greentic:component@0.4.0".into(),
                profiles: ComponentProfiles::default(),
                capabilities: ComponentCapabilities::default(),
                configurators: None,
                operations: Vec::new(),
                config_schema: None,
                resources: ResourceHints::default(),
                dev_flows: BTreeMap::new(),
            },
        ],
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
        secret_requirements: Vec::new(),
    };

    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create pack archive for test")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    for (id, artifact_path) in component_sources(&fixtures)? {
        writer.start_file(format!("components/{id}.wasm"), options)?;
        let mut file = std::fs::File::open(&artifact_path)
            .with_context(|| format!("open component {}", artifact_path.display()))?;
        std::io::copy(&mut file, &mut writer)?;
    }
    writer.finish().context("finalise test pack")?;
    Ok(())
}

fn component_sources(fixtures_root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let workspace_root = workspace_root();
    let crates_root = workspace_root.join("tests/fixtures/runner-components");
    let target_root = crates_root.join("target-test");

    let components = [
        ("qa.process", "qa_process"),
        ("templating.handlebars", "templating_handlebars"),
    ];

    let mut sources = Vec::new();

    for (id, crate_name) in components {
        let prebuilt = fixtures_root
            .join("components")
            .join(format!("{crate_name}.wasm"));
        if prebuilt.exists() {
            sources.push((id.to_string(), prebuilt));
            continue;
        }

        let crate_dir = crates_root.join(crate_name);

        let status = Command::new("cargo")
            .env("CARGO_NET_OFFLINE", "true")
            .env("CARGO_TARGET_DIR", &target_root)
            .current_dir(&crate_dir)
            .args([
                "build",
                "--offline",
                "--target",
                "wasm32-wasip2",
                "--release",
            ])
            .status()
            .with_context(|| format!("failed to build component crate {}", crate_name))?;

        if !status.success() {
            anyhow::bail!("component build failed for {}", crate_name);
        }

        let base = target_root.join("wasm32-wasip2").join("release");
        let candidates = [
            base.join(format!("{crate_name}.wasm")),
            base.join("deps").join(format!("{crate_name}.wasm")),
        ];

        let artifact = candidates
            .into_iter()
            .find(|path| path.exists())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "component artifact not found after build for {}",
                    crate_name
                )
            })?;

        sources.push((id.to_string(), artifact));
    }

    Ok(sources)
}

fn demo_exec_ctx(node_id: &str) -> greentic_runner_host::component_api::node::ExecCtx {
    greentic_runner_host::component_api::node::ExecCtx {
        tenant: greentic_runner_host::component_api::node::TenantCtx {
            tenant: "demo".into(),
            team: None,
            user: None,
            trace_id: None,
            correlation_id: None,
            deadline_unix_ms: None,
            attempt: 0,
            idempotency_key: None,
        },
        flow_id: "demo.flow".into(),
        node_id: Some(node_id.into()),
    }
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
fn gtpack_manifest_fast_path_invokes_components() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("runner-components.gtpack");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;
    build_pack(&gtpack)?;

    let config = Arc::new(host_config(&bindings_path));
    let runtime = Arc::new(rt.block_on(PackRuntime::load(
        &gtpack,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(RunnerWasiPolicy::new()),
        greentic_runner_host::secrets::default_manager(),
        None,
        false,
    ))?);

    let flows = rt.block_on(runtime.list_flows())?;
    assert_eq!(flows.len(), 1);
    assert_eq!(flows[0].id, "demo.flow");

    let flow = runtime.load_flow("demo.flow")?;
    assert!(
        flow.entrypoints.contains_key("default") || !flow.nodes.is_empty(),
        "flow should expose an entrypoint or nodes"
    );

    let ctx = demo_exec_ctx("qa");
    let result = rt.block_on(runtime.invoke_component(
        "qa.process",
        ctx,
        "process",
        None,
        serde_json::to_string(&json!({ "text": "hello" }))?,
    ))?;
    assert_eq!(result, json!({ "text": "hello" }));

    // Validate the templating component resolves and can render against simple state.
    let ctx = demo_exec_ctx("tmpl");
    let render = rt.block_on(runtime.invoke_component(
        "templating.handlebars",
        ctx,
        "render",
        None,
        serde_json::to_string(&json!({
            "template": "Echo: {{input.text}}",
            "input": { "text": "hi" }
        }))?,
    ))?;
    match render {
        serde_json::Value::String(s) => assert!(s.starts_with("Echo:")),
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(s)) = map.get("text") {
                assert!(s.starts_with("Echo:"));
            } else {
                panic!("templating output missing text: {map:?}");
            }
        }
        other => panic!("unexpected templating output: {other:?}"),
    }
    Ok(())
}
