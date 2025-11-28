use std::{fs, path::PathBuf, process::Command};

use greentic_runner::worker_integration::execute_worker_payload;
use greentic_types::{TenantCtx, TenantId};
use wasmtime::{Engine, component::Component};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("tests/fixtures/worker_component")
}

fn build_worker_component() -> PathBuf {
    let root = fixture_dir();
    let status = Command::new("cargo")
        .args([
            "build",
            "--offline",
            "--target",
            "wasm32-wasip2",
            "--release",
        ])
        .env("CARGO_NET_OFFLINE", "true")
        .current_dir(&root)
        .status()
        .expect("failed to run cargo build for worker component");
    assert!(status.success(), "worker component build failed");
    root.join("target/wasm32-wasip2/release/worker_component.wasm")
}

#[test]
fn executes_worker_component() {
    let module_path = build_worker_component();
    let component_bytes = fs::read(&module_path).expect("read wasm module");
    let mut config = wasmtime::Config::new();
    config.wasm_component_model(true);
    let engine = Engine::new(&config).expect("engine");
    let component = Component::from_binary(&engine, &component_bytes).expect("component");

    let tenant = TenantCtx::new("local".parse().unwrap(), TenantId::new("demo").unwrap());
    let response = execute_worker_payload(
        &engine,
        &component,
        &tenant,
        "demo-worker",
        Some("corr-1".into()),
        Some("sess-1".into()),
        Some("thread-1".into()),
        r#"{"hello":"world"}"#.to_string(),
    )
    .expect("execute worker");

    assert_eq!(response.worker_id, "demo-worker");
    assert_eq!(response.correlation_id.as_deref(), Some("corr-1"));
    assert_eq!(response.session_id.as_deref(), Some("sess-1"));
    assert_eq!(response.thread_id.as_deref(), Some("thread-1"));
    assert_eq!(response.messages.len(), 1);
    assert_eq!(response.messages[0].kind, "echo");
    assert_eq!(response.messages[0].payload_json, r#"{"hello":"world"}"#);
}
