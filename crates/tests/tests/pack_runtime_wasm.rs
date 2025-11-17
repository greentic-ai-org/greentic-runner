use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use greentic_runner_host::pack::PackRuntime;
use greentic_runner_host::{HostConfig, RunnerWasiPolicy};
use serde_json::json;

#[test]
fn pack_runtime_executes_wasm_component() -> Result<()> {
    let workspace = workspace_root();
    let bindings = workspace.join("examples/bindings/default.bindings.yaml");
    let wasm_path = workspace.join("tests/fixtures/pack-component/pack-component.wasm");
    let rt = tokio::runtime::Runtime::new()?;
    let config = Arc::new(HostConfig::load_from_path(&bindings)?);
    let pack_runtime = rt.block_on(PackRuntime::load(
        &wasm_path,
        Arc::clone(&config),
        None,
        None,
        None,
        None,
        Arc::new(RunnerWasiPolicy::default()),
        false,
    ))?;
    let input = json!({ "text": "hello" });
    let output = rt.block_on(pack_runtime.run_flow("demo.flow", input.clone()))?;
    drop(pack_runtime);
    drop(rt);
    assert_eq!(output["status"], "done");
    assert_eq!(output["response"]["text"], "hello");
    Ok(())
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .canonicalize()
        .expect("workspace root")
}
