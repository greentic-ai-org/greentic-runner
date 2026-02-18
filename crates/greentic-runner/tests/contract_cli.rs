use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::io::{Write, copy};
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result};
use greentic_types::{
    ComponentCapabilities, ComponentManifest, ComponentProfiles, PackKind, PackManifest,
    ResourceHints, encode_pack_manifest,
};
use semver::Version;
use tempfile::TempDir;
use zip::ZipWriter;
use zip::write::FileOptions;

#[test]
fn contract_artifact_requires_explicit_gates() -> Result<()> {
    let temp = TempDir::new()?;
    let artifact = temp.path().join("describe.cbor");
    write_fixture_artifact("describe.valid.json", &artifact)?;

    let output = Command::new(env!("CARGO_BIN_EXE_greentic-runner"))
        .arg("contract")
        .arg("--describe")
        .arg(&artifact)
        .output()
        .context("run greentic-runner contract without gates")?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(
        "artifact-only contract inspection requires explicit --no-verify and --validate-only"
    ));
    Ok(())
}

#[test]
fn contract_artifact_prints_report_when_gated() -> Result<()> {
    let temp = TempDir::new()?;
    let artifact = temp.path().join("describe.cbor");
    write_fixture_artifact("describe.valid.json", &artifact)?;

    let output = Command::new(env!("CARGO_BIN_EXE_greentic-runner"))
        .arg("contract")
        .arg("--describe")
        .arg(&artifact)
        .arg("--no-verify")
        .arg("--validate-only")
        .arg("--print-contract")
        .output()
        .context("run greentic-runner contract in gated artifact-only mode")?;

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    let report: serde_json::Value = serde_json::from_str(&stdout)?;
    assert_eq!(report["selected_operation"], "run");
    assert_eq!(report["source"], "artifact.unverified");
    assert!(
        report["describe_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert!(
        report["schema_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    Ok(())
}

#[test]
fn contract_pack_rejects_mismatched_artifact() -> Result<()> {
    if std::env::var("GREENTIC_HEAVY_WASM").ok().as_deref() != Some("1") {
        eprintln!("skipping heavy wasm contract mismatch test (set GREENTIC_HEAVY_WASM=1)");
        return Ok(());
    }

    let temp = TempDir::new()?;
    let pack_path = temp.path().join("component-v06.gtpack");
    let mismatch_artifact = temp.path().join("mismatch.describe.cbor");
    let component_path = fixture_component_v06_path()?;

    build_component_pack_v06(&component_path, &pack_path)?;
    write_fixture_artifact("describe.mismatch.json", &mismatch_artifact)?;

    let output = Command::new(env!("CARGO_BIN_EXE_greentic-runner"))
        .arg("contract")
        .arg("--pack")
        .arg(&pack_path)
        .arg("--component")
        .arg("v06.describe")
        .arg("--describe")
        .arg(&mismatch_artifact)
        .output()
        .context("run greentic-runner contract mismatch case")?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("artifact describe does not match authoritative WASM describe"));
    Ok(())
}

fn write_fixture_artifact(fixture_name: &str, path: &Path) -> Result<()> {
    let payload_path = fixture_path(fixture_name);
    let payload_bytes =
        fs::read(&payload_path).with_context(|| format!("read {}", payload_path.display()))?;
    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes)
        .with_context(|| format!("parse {}", payload_path.display()))?;
    let bytes = serde_cbor::ser::to_vec_packed(&payload)?;
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))
}

fn build_component_pack_v06(component_path: &Path, pack_path: &Path) -> Result<()> {
    let manifest = PackManifest {
        schema_version: "1.0".into(),
        pack_id: "component.v06".parse()?,
        name: Some("component.v06".into()),
        version: Version::parse("0.1.0")?,
        kind: PackKind::Application,
        publisher: "test".into(),
        components: vec![ComponentManifest {
            id: "v06.describe".parse()?,
            version: Version::parse("0.1.0")?,
            supports: Vec::new(),
            world: "greentic:component@0.6.0".into(),
            profiles: ComponentProfiles::default(),
            capabilities: ComponentCapabilities::default(),
            configurators: None,
            operations: Vec::new(),
            config_schema: None,
            resources: ResourceHints::default(),
            dev_flows: BTreeMap::new(),
        }],
        flows: Vec::new(),
        dependencies: Vec::new(),
        capabilities: Vec::new(),
        signatures: Default::default(),
        secret_requirements: Vec::new(),
        bootstrap: None,
        extensions: None,
    };

    let mut writer = ZipWriter::new(File::create(pack_path).context("create v0.6 pack archive")?);
    let options: FileOptions<'_, ()> =
        FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let manifest_bytes = encode_pack_manifest(&manifest)?;
    writer.start_file("manifest.cbor", options)?;
    writer.write_all(&manifest_bytes)?;

    writer.start_file("components/v06.describe.wasm", options)?;
    let mut component_file =
        File::open(component_path).with_context(|| format!("open {}", component_path.display()))?;
    copy(&mut component_file, &mut writer)?;
    writer.finish().context("finalise v0.6 pack")?;
    Ok(())
}

fn fixture_component_v06_path() -> Result<PathBuf> {
    let root = workspace_root().join("tests/assets/component-v0-6-dummy");
    let wasm = root.join("target/wasm32-wasip2/release/component_v0_6_dummy.wasm");
    if !wasm.exists() {
        let offline = std::env::var("CARGO_NET_OFFLINE").ok();
        let mut cmd = Command::new("cargo");
        let mut args: Vec<String> = vec![
            "build".into(),
            "--release".into(),
            "--target".into(),
            "wasm32-wasip2".into(),
            "--manifest-path".into(),
            root.join("Cargo.toml")
                .to_str()
                .expect("manifest path")
                .into(),
        ];
        if matches!(offline.as_deref(), Some("true")) {
            args.insert(1, "--offline".into());
        }
        if let Some(val) = &offline {
            cmd.env("CARGO_NET_OFFLINE", val);
        }
        let status = cmd.args(&args).status().context("build v0.6 component")?;
        if !status.success() {
            anyhow::bail!("failed to build component-v0-6 fixture");
        }
    }
    Ok(wasm)
}

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("contract")
        .join(name)
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}
