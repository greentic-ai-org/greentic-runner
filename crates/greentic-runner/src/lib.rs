#![forbid(unsafe_code)]

pub use greentic_runner_host::{
    self as host, Activity, ActivityKind, HostBuilder, HostServer, RunnerHost, TenantHandle,
    config, http, imports, pack, routing, runner, runtime as legacy_runtime, runtime_wasmtime,
    telemetry, verify, watcher,
};

pub mod desktop {
    pub use greentic_runner_desktop::*;
}

pub mod bridge_integration;
pub mod events_integration;
pub mod gen_bindings;
pub mod runtime;
pub mod worker_integration;
