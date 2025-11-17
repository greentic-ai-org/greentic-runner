use std::fs;
use std::io::Write;
use std::path::Path;

use greentic_runner::runtime::load_pack;
use tempfile::tempdir;
use zip::ZipWriter;
use zip::write::FileOptions;

#[test]
fn loads_pack_from_directory() {
    let dir = tempdir().expect("tempdir");
    write_sample_pack(dir.path());

    let pack = load_pack(dir.path()).expect("load pack");
    assert_eq!(pack.manifest.id.as_str(), "demo.pack");
    assert!(pack.flows.contains_key("demo_flow"));
}

#[test]
fn loads_pack_from_archive() {
    let dir = tempdir().expect("tempdir");
    write_sample_pack(dir.path());
    let archive_path = dir.path().join("demo.gtpack");
    build_archive(&archive_path);

    let pack = load_pack(&archive_path).expect("load pack");
    assert_eq!(pack.manifest.id.as_str(), "demo.pack");
    assert!(pack.flows.contains_key("demo_flow"));
}

fn write_sample_pack(root: &Path) {
    fs::create_dir_all(root.join("flows")).expect("flows dir");
    fs::create_dir_all(root.join("components")).expect("components dir");

    fs::write(root.join("manifest.yaml"), SAMPLE_MANIFEST).expect("manifest");
    fs::write(root.join("flows/demo_flow.ygtc"), SAMPLE_FLOW).expect("flow");
    fs::write(
        root.join("components/demo.component.manifest.yaml"),
        SAMPLE_COMPONENT,
    )
    .expect("component manifest");
}

fn build_archive(out: &Path) {
    let file = fs::File::create(out).expect("archive file");
    let mut writer = ZipWriter::new(file);
    let options: FileOptions<'static, zip::write::ExtendedFileOptions> = FileOptions::default();

    for (name, contents) in [
        ("manifest.yaml", SAMPLE_MANIFEST),
        ("flows/demo_flow.ygtc", SAMPLE_FLOW),
        ("components/demo.component.manifest.yaml", SAMPLE_COMPONENT),
    ] {
        writer
            .start_file(name, options.clone())
            .expect("start file");
        writer.write_all(contents.as_bytes()).expect("write entry");
    }

    writer.finish().expect("finish archive");
}

const SAMPLE_MANIFEST: &str = r#"
id: demo.pack
version: 1.0.0
name: Demo Pack
flows:
  - id: demo_flow
    file: flows/demo_flow.ygtc
components:
  - id: demo.component
    version_req: "^1.0"
"#;

const SAMPLE_FLOW: &str = r#"
kind: messaging
id: demo_flow
description: Demo flow
nodes:
  start:
    kind: messaging/reply
    config:
      text: "hello"
    routing: {}
"#;

const SAMPLE_COMPONENT: &str = r#"
id: demo.component
version: 1.0.0
supports:
  - messaging
world: "greentic:demo/world@1.0.0"
profiles:
  default: demo
  supported:
    - demo
capabilities:
  wasi:
    random: true
    clocks: true
  host:
    secrets:
      required: []
    telemetry:
      scope: tenant
"#;
