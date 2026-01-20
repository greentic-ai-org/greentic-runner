use clap::{Parser, Subcommand, ValueEnum};
mod cli;
use greentic_config::{ConfigFileFormat, ConfigLayer, ConfigResolver};
use greentic_runner_host::cache::{
    ArtifactKey, CacheConfig, CacheManager, CpuPolicy, EngineProfile,
};
use greentic_runner_host::trace::{TraceConfig, TraceMode};
use greentic_runner_host::validate::{ValidationConfig, ValidationMode};
use greentic_runner_host::{RunnerConfig, run as run_host};
use greentic_types::ComponentSourceRef;
use std::path::{Path, PathBuf};
use tokio::fs as async_fs;

use anyhow::{Context, Result, bail};
use greentic_distributor_client::dist::{DistClient, DistOptions};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Parser)]
#[command(name = "greentic-runner")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    run: RunArgs,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[command(subcommand)]
    Cache(CacheCommand),
    Replay(cli::replay::ReplayArgs),
}

#[derive(Debug, Subcommand)]
enum CacheCommand {
    Warmup(CacheWarmupArgs),
    Doctor,
    Prune(CachePruneArgs),
}

#[derive(Debug, Parser)]
struct CacheWarmupArgs {
    /// Path to a pack.lock/pack.lock.json or pack.yaml
    #[arg(long, value_name = "PATH")]
    pack: PathBuf,

    /// Warmup mode
    #[arg(long, value_enum, default_value = "disk")]
    mode: CacheWarmupMode,
}

#[derive(Debug, Parser)]
struct CachePruneArgs {
    /// Report prune result without deleting artifacts
    #[arg(long)]
    dry_run: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum CacheWarmupMode {
    Disk,
    Memory,
}

#[derive(Debug, Parser)]
struct RunArgs {
    /// Optional path to a greentic config file (toml/json). Overrides project discovery.
    #[arg(long = "config", value_name = "PATH")]
    config: Option<PathBuf>,

    /// Allow dev-only settings in the config (use with caution in prod).
    #[arg(long = "allow-dev")]
    allow_dev: bool,

    /// Print the resolved config and exit.
    #[arg(long = "config-explain")]
    config_explain: bool,

    /// Pack bindings file or directory containing *.gtbind (repeatable)
    #[arg(long = "bindings", value_name = "PATH")]
    bindings: Vec<PathBuf>,

    /// Directory containing *.gtbind files (repeatable)
    #[arg(long = "bindings-dir", value_name = "DIR")]
    bindings_dir: Vec<PathBuf>,

    /// Port to serve the HTTP server on (default 8080)
    #[arg(long, default_value = "8080")]
    port: u16,

    /// Disable the component compilation cache
    #[arg(long)]
    no_cache: bool,

    /// Emit JSON errors on failure
    #[arg(long)]
    json: bool,

    /// Trace output path (default: trace.json)
    #[arg(long = "trace-out", value_name = "PATH")]
    trace_out: Option<PathBuf>,

    /// Trace emission mode
    #[arg(long = "trace", value_enum, default_value = "on")]
    trace: TraceArg,

    /// Capture invocation inputs into trace.json (default off)
    #[arg(long = "trace-capture-inputs", value_enum, default_value = "off")]
    trace_capture_inputs: TraceCaptureArg,

    /// Invocation envelope validation mode
    #[arg(long = "validation", value_enum)]
    validation: Option<ValidationArg>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum TraceArg {
    Off,
    On,
    Always,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum TraceCaptureArg {
    Off,
    On,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ValidationArg {
    Off,
    Warn,
    Error,
}

#[greentic_types::telemetry::main(service_name = "greentic-runner")]
async fn main() {
    let cli = Cli::parse();
    let json_output = cli.run.json;
    if let Err(err) = run_with_cli(cli).await {
        if json_output {
            emit_json_error(&err);
        } else {
            tracing::error!(error = %err, "runner failed");
            eprintln!("error: {err}");
        }
        std::process::exit(1);
    }
}

async fn run_with_cli(cli: Cli) -> anyhow::Result<()> {
    if let Some(command) = cli.command {
        return match command {
            Command::Cache(cmd) => run_cache(cmd).await,
            Command::Replay(args) => cli::replay::run(args).await,
        };
    }
    let run = cli.run;
    if run.no_cache {
        // SAFETY: toggling the cache behavior is scoped to this process.
        unsafe {
            std::env::set_var("GREENTIC_NO_CACHE", "1");
        }
    }
    if run.bindings.is_empty() && run.bindings_dir.is_empty() {
        bail!("at least one --bindings path is required");
    }
    let (resolver, _) = build_resolver(run.config.as_deref(), run.allow_dev)?;
    let resolved = resolver.load()?;
    if run.config_explain {
        let report =
            greentic_config::explain(&resolved.config, &resolved.provenance, &resolved.warnings);
        println!("{}", report.text);
        return Ok(());
    }
    let bindings =
        greentic_runner_host::gtbind::collect_gtbind_paths(&run.bindings, &run.bindings_dir)?;
    let trace_out = std::env::var_os("GREENTIC_TRACE_OUT").map(PathBuf::from);
    let trace_config = TraceConfig::from_env()
        .with_overrides(
            match run.trace {
                TraceArg::Off => TraceMode::Off,
                TraceArg::On => TraceMode::On,
                TraceArg::Always => TraceMode::Always,
            },
            trace_out.or(run.trace_out.clone()),
        )
        .with_capture_inputs(matches!(run.trace_capture_inputs, TraceCaptureArg::On));
    let validation_mode = run.validation.map(|value| match value {
        ValidationArg::Off => ValidationMode::Off,
        ValidationArg::Warn => ValidationMode::Warn,
        ValidationArg::Error => ValidationMode::Error,
    });
    let validation_config = validation_mode
        .map(|mode| ValidationConfig::from_env().with_mode(mode))
        .unwrap_or_else(ValidationConfig::from_env);
    let mut cfg = RunnerConfig::from_config(resolved, bindings)?.with_port(run.port);
    cfg.trace = trace_config;
    cfg.validation = validation_config;
    run_host(cfg).await
}

fn emit_json_error(err: &anyhow::Error) {
    let chain = err
        .chain()
        .skip(1)
        .map(|source| source.to_string())
        .collect::<Vec<_>>();
    let payload = json!({
        "error": {
            "message": err.to_string(),
            "chain": chain,
        }
    });
    println!("{}", payload);
}

fn build_resolver(
    config_path: Option<&Path>,
    allow_dev: bool,
) -> anyhow::Result<(ConfigResolver, ConfigLayer)> {
    let mut resolver = ConfigResolver::new();
    if allow_dev {
        resolver = resolver.allow_dev(true);
    }
    let layer = if let Some(path) = config_path {
        let format = match path.extension().and_then(|ext| ext.to_str()) {
            Some("json") => ConfigFileFormat::Json,
            _ => ConfigFileFormat::Toml,
        };
        let contents = std::fs::read_to_string(path)?;
        let layer = match format {
            ConfigFileFormat::Toml => toml::from_str(&contents)?,
            ConfigFileFormat::Json => serde_json::from_str(&contents)?,
        };
        if let Some(parent) = path.parent() {
            resolver = resolver.with_project_root_opt(Some(parent.to_path_buf()));
        }
        layer
    } else {
        ConfigLayer::default()
    };
    resolver = resolver.with_cli_overrides(layer.clone());
    Ok((resolver, layer))
}

#[derive(Debug, Deserialize)]
struct PackLockV1 {
    schema_version: u32,
    components: Vec<PackLockComponent>,
}

#[derive(Debug, Deserialize)]
struct PackLockComponent {
    name: String,
    #[serde(default, rename = "source_ref")]
    source_ref: Option<String>,
    #[serde(default, rename = "ref")]
    legacy_ref: Option<String>,
    #[serde(default)]
    bundled_path: Option<String>,
    #[serde(default, rename = "path")]
    legacy_path: Option<String>,
    #[serde(default)]
    wasm_sha256: Option<String>,
    #[serde(default, rename = "sha256")]
    legacy_sha256: Option<String>,
    #[serde(default)]
    resolved_digest: Option<String>,
    #[serde(default)]
    digest: Option<String>,
}

impl PackLockComponent {
    fn source_ref(&self) -> Result<&str> {
        match (&self.source_ref, &self.legacy_ref) {
            (Some(primary), Some(legacy)) => {
                if primary != legacy {
                    bail!(
                        "pack.lock component {} has conflicting refs: {} vs {}",
                        self.name,
                        primary,
                        legacy
                    );
                }
                Ok(primary.as_str())
            }
            (Some(primary), None) => Ok(primary.as_str()),
            (None, Some(legacy)) => Ok(legacy.as_str()),
            (None, None) => bail!("pack.lock component {} missing source_ref", self.name),
        }
    }

    fn bundled_path(&self) -> Option<&str> {
        match (&self.bundled_path, &self.legacy_path) {
            (Some(primary), Some(legacy)) if primary == legacy => Some(primary.as_str()),
            (Some(primary), None) => Some(primary.as_str()),
            (None, Some(legacy)) => Some(legacy.as_str()),
            _ => None,
        }
    }

    fn wasm_digest(&self) -> Option<String> {
        match (&self.wasm_sha256, &self.legacy_sha256) {
            (Some(primary), Some(legacy)) if primary == legacy => Some(primary.clone()),
            (Some(primary), None) => Some(primary.clone()),
            (None, Some(legacy)) => Some(legacy.clone()),
            _ => None,
        }
    }
}

async fn run_cache(cmd: CacheCommand) -> Result<()> {
    match cmd {
        CacheCommand::Warmup(args) => warmup_cache(args).await,
        CacheCommand::Doctor => doctor_cache().await,
        CacheCommand::Prune(args) => prune_cache(args).await,
    }
}

async fn warmup_cache(args: CacheWarmupArgs) -> Result<()> {
    let (root, lock) = read_pack_lock(&args.pack).await?;
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let config = CacheConfig {
        memory_enabled: matches!(args.mode, CacheWarmupMode::Memory),
        ..CacheConfig::default()
    };
    let cache = CacheManager::new(config, profile);
    let dist_opts = DistOptions {
        allow_tags: true,
        ..DistOptions::default()
    };
    let dist_client = DistClient::new(dist_opts);

    for entry in lock.components {
        let source_ref = entry.source_ref()?;
        let wasm_digest = entry
            .wasm_digest()
            .or_else(|| entry.resolved_digest.clone())
            .or_else(|| entry.digest.clone())
            .ok_or_else(|| anyhow::anyhow!("pack.lock component {} missing digest", entry.name))?;
        let wasm_digest = normalize_digest(&wasm_digest);
        let key = ArtifactKey::new(cache.engine_profile_id().to_string(), wasm_digest);
        let bytes = resolve_component_bytes(&root, &entry, source_ref, &dist_client).await?;
        let _ = cache
            .get_component(&engine, &key, || Ok(bytes))
            .await
            .with_context(|| format!("failed to warm component {}", entry.name))?;
    }
    Ok(())
}

async fn doctor_cache() -> Result<()> {
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let cache = CacheManager::new(CacheConfig::default(), profile);
    let metrics = cache.metrics();
    let memory = cache.memory_stats();
    let disk = cache.disk_stats()?;

    println!("engine_profile_id: {}", cache.engine_profile_id());
    println!(
        "memory: entries={} bytes={} hits={} misses={}",
        memory.entries, memory.total_bytes, memory.hits, memory.misses
    );
    println!(
        "disk: artifacts={} bytes={} reads={} hits={}",
        disk.artifact_count, disk.artifact_bytes, metrics.disk_reads, metrics.disk_hits
    );
    Ok(())
}

async fn prune_cache(args: CachePruneArgs) -> Result<()> {
    let engine = wasmtime::Engine::default();
    let profile = EngineProfile::from_engine(&engine, CpuPolicy::Native, "default".to_string());
    let cache = CacheManager::new(CacheConfig::default(), profile);
    let report = cache.prune_disk(args.dry_run).await?;
    if args.dry_run {
        println!(
            "prune dry-run: would remove {} entries ({} bytes)",
            report.removed_entries, report.removed_bytes
        );
    } else {
        println!(
            "prune: removed {} entries ({} bytes)",
            report.removed_entries, report.removed_bytes
        );
    }
    Ok(())
}

async fn read_pack_lock(path: &Path) -> Result<(PathBuf, PackLockV1)> {
    let lock_path = if path.is_dir() {
        pick_lock_path(path)
            .ok_or_else(|| anyhow::anyhow!("pack.lock not found in {}", path.display()))?
    } else if is_pack_lock(path) {
        path.to_path_buf()
    } else if is_pack_yaml(path) {
        let root = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("pack.yaml has no parent directory"))?;
        pick_lock_path(root)
            .ok_or_else(|| anyhow::anyhow!("pack.lock not found in {}", root.display()))?
    } else {
        bail!("unsupported pack path {}", path.display());
    };
    let root = lock_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("pack.lock has no parent directory"))?
        .to_path_buf();
    let raw = async_fs::read_to_string(&lock_path)
        .await
        .with_context(|| format!("failed to read {}", lock_path.display()))?;
    let lock: PackLockV1 = serde_json::from_str(&raw).context("failed to parse pack.lock")?;
    if lock.schema_version != 1 {
        bail!("pack.lock schema_version must be 1");
    }
    Ok((root, lock))
}

fn pick_lock_path(root: &Path) -> Option<PathBuf> {
    let candidate = root.join("pack.lock");
    if candidate.exists() {
        return Some(candidate);
    }
    let candidate = root.join("pack.lock.json");
    candidate.exists().then_some(candidate)
}

fn is_pack_lock(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some("pack.lock") | Some("pack.lock.json")
    )
}

fn is_pack_yaml(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some("pack.yaml")
    )
}

async fn resolve_component_bytes(
    root: &Path,
    entry: &PackLockComponent,
    source_ref: &str,
    dist_client: &DistClient,
) -> Result<Vec<u8>> {
    if let Some(relative) = entry.bundled_path() {
        let path = root.join(relative);
        if path.exists() {
            return async_fs::read(&path)
                .await
                .with_context(|| format!("failed to read {}", path.display()));
        }
    }

    let source: ComponentSourceRef = source_ref
        .parse()
        .with_context(|| format!("invalid component ref {}", source_ref))?;
    if !matches!(source, ComponentSourceRef::Oci(_)) {
        bail!("unsupported component source {}", source_ref);
    }

    if let Some(digest) = entry.resolved_digest.as_deref().or(entry.digest.as_deref()) {
        let cache_path = dist_client.fetch_digest(digest).await?;
        return async_fs::read(&cache_path)
            .await
            .with_context(|| format!("failed to read {}", cache_path.display()));
    }

    let resolved = dist_client.resolve_ref(source_ref).await?;
    let cache_path = resolved
        .cache_path
        .ok_or_else(|| anyhow::anyhow!("component {} missing cache path", entry.name))?;
    async_fs::read(&cache_path)
        .await
        .with_context(|| format!("failed to read {}", cache_path.display()))
}

fn normalize_digest(digest: &str) -> String {
    if digest.starts_with("sha256:") || digest.starts_with("blake3:") {
        digest.to_string()
    } else {
        format!("sha256:{digest}")
    }
}
