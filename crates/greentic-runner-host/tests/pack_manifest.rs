use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use greentic_flow::flow_bundle::load_and_validate_bundle_with_flow;
use greentic_runner_host::config::{
    FlowRetryConfig, HostConfig, RateLimits, SecretsPolicy, WebhookPolicy,
};
use greentic_runner_host::pack::{ComponentResolution, PackRuntime};
use greentic_runner_host::wasi::RunnerWasiPolicy;
use greentic_types::{
    ArtifactLocationV1, ComponentCapabilities, ComponentManifest, ComponentProfiles,
    ComponentSourceEntryV1, ComponentSourceRef, ComponentSourcesV1, FlowKind, PackFlowEntry,
    PackKind, PackManifest, ResolvedComponentV1, ResourceHints, encode_pack_manifest,
};
use once_cell::sync::Lazy;
use semver::Version;
use serde_json::json;
use sha2::Digest;
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
        bootstrap: None,
        extensions: None,
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

fn build_pack_with_component_sources(
    pack_path: &Path,
    sources_payload: ComponentSourcesV1,
    include_components: bool,
) -> Result<()> {
    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let flow_yaml =
        std::fs::read_to_string(fixtures.join("flows/demo.yaml")).context("read flow yaml")?;
    let (_bundle, flow) = load_and_validate_bundle_with_flow(&flow_yaml, None)?;

    let mut manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: "runner.components.remote".parse()?,
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
        bootstrap: None,
        extensions: None,
    };
    manifest.set_component_sources_v1(sources_payload)?;

    let mut writer =
        ZipWriter::new(std::fs::File::create(pack_path).context("create pack archive for test")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    if include_components {
        for (id, artifact_path) in component_sources(&fixtures)? {
            writer.start_file(format!("components/{id}.wasm"), options)?;
            let mut file = std::fs::File::open(&artifact_path)
                .with_context(|| format!("open component {}", artifact_path.display()))?;
            std::io::copy(&mut file, &mut writer)?;
        }
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

        let mut cmd = Command::new("cargo");
        let offline = std::env::var("CARGO_NET_OFFLINE").ok();
        if let Some(val) = &offline {
            cmd.env("CARGO_NET_OFFLINE", val);
        }
        let mut args: Vec<String> = vec![
            "build".into(),
            "--target".into(),
            "wasm32-wasip2".into(),
            "--release".into(),
        ];
        if matches!(offline.as_deref(), Some("true")) {
            args.insert(1, "--offline".into());
        }
        let status = cmd
            .env("CARGO_TARGET_DIR", &target_root)
            .current_dir(&crate_dir)
            .args(args)
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

fn digest_for_bytes(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn write_component_cache(cache_root: &Path, digest: &str, bytes: &[u8]) -> Result<PathBuf> {
    let trimmed = digest.strip_prefix("sha256:").unwrap_or(digest);
    let dir = cache_root.join(trimmed);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("component.wasm");
    std::fs::write(&path, bytes)?;
    Ok(path)
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
        ComponentResolution::default(),
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

#[test]
fn gtpack_manifest_loads_remote_components_from_cache() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("runner-components-remote.gtpack");
    let cache_root = temp.path().join("dist-cache");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let mut entries = Vec::new();
    for (id, artifact_path) in component_sources(&fixtures)? {
        let bytes = std::fs::read(&artifact_path)?;
        let digest = digest_for_bytes(&bytes);
        write_component_cache(&cache_root, &digest, &bytes)?;
        let source_ref = ComponentSourceRef::Oci(format!("registry.test/{id}@{}", digest));
        entries.push(ComponentSourceEntryV1 {
            name: id,
            component_id: None,
            source: source_ref,
            resolved: ResolvedComponentV1 {
                digest,
                signature: None,
                signed_by: None,
            },
            artifact: ArtifactLocationV1::Remote,
            licensing_hint: None,
            metering_hint: None,
        });
    }

    let sources = ComponentSourcesV1::new(entries);
    build_pack_with_component_sources(&gtpack, sources, false)?;

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
        ComponentResolution {
            dist_cache_dir: Some(cache_root),
            ..ComponentResolution::default()
        },
    ))?);

    let ctx = demo_exec_ctx("qa");
    let result = rt.block_on(runtime.invoke_component(
        "qa.process",
        ctx,
        "process",
        None,
        serde_json::to_string(&json!({ "text": "hello" }))?,
    ))?;
    assert_eq!(result, json!({ "text": "hello" }));
    Ok(())
}

#[test]
fn gtpack_manifest_offline_errors_when_remote_component_missing() -> Result<()> {
    let rt = *RUNTIME;
    let temp = TempDir::new()?;
    let gtpack = temp.path().join("runner-components-offline.gtpack");
    let cache_root = temp.path().join("dist-cache");
    let bindings_path = temp.path().join("bindings.yaml");
    std::fs::write(&bindings_path, b"tenant: demo")?;

    let fixtures = workspace_root().join("tests/fixtures/packs/runner-components");
    let mut entries = Vec::new();
    for (id, artifact_path) in component_sources(&fixtures)? {
        let bytes = std::fs::read(&artifact_path)?;
        let digest = digest_for_bytes(&bytes);
        let source_ref = ComponentSourceRef::Oci(format!("registry.test/{id}@{}", digest));
        entries.push(ComponentSourceEntryV1 {
            name: id,
            component_id: None,
            source: source_ref,
            resolved: ResolvedComponentV1 {
                digest,
                signature: None,
                signed_by: None,
            },
            artifact: ArtifactLocationV1::Remote,
            licensing_hint: None,
            metering_hint: None,
        });
    }

    let sources = ComponentSourcesV1::new(entries);
    build_pack_with_component_sources(&gtpack, sources, false)?;

    let config = Arc::new(host_config(&bindings_path));
    let err = rt
        .block_on(PackRuntime::load(
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
            ComponentResolution {
                dist_offline: true,
                dist_cache_dir: Some(cache_root),
                ..ComponentResolution::default()
            },
        ))
        .err()
        .expect("pack load should fail when offline and cache is empty");

    let message = err.to_string();
    assert!(
        message.contains("greentic-dist pull"),
        "error should suggest greentic-dist pull: {message}"
    );
    Ok(())
}
