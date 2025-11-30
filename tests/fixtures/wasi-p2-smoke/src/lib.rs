#![no_main]

/// Minimal WASI Preview2 CLI entrypoint used for policy enforcement tests.
#[export_name = "wasi:cli/run#run"]
pub extern "C" fn run() {}
