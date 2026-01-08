use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use greentic_runner::desktop::{
    DevProfile, Profile, RunOptions, Runner, SigningPolicy, TenantContext,
};
use serde_json::Value;

#[derive(Debug, Parser)]
#[command(
    name = "greentic-runner-cli",
    about = "Run a Greentic pack locally using the desktop harness"
)]
struct Cli {
    /// Path to the pack (.gtpack or directory)
    #[arg(long, value_name = "PATH")]
    pack: PathBuf,

    /// Optional directory containing materialized components (`components/<id>.wasm`)
    #[arg(long, value_name = "DIR")]
    components_dir: Option<PathBuf>,

    /// JSON file mapping component id -> local wasm path (sidecar map)
    #[arg(long, value_name = "FILE")]
    components_map: Option<PathBuf>,

    /// Optional entry flow id (defaults to pack manifest)
    #[arg(long)]
    flow: Option<String>,

    /// JSON input payload (string form)
    #[arg(long, default_value = "{}")]
    input: String,

    /// Read JSON input from a file (overrides --input)
    #[arg(long, value_name = "FILE")]
    input_file: Option<PathBuf>,

    /// Override tenant id
    #[arg(long)]
    tenant: Option<String>,

    /// Override team id
    #[arg(long)]
    team: Option<String>,

    /// Override user id
    #[arg(long)]
    user: Option<String>,

    /// Override session id
    #[arg(long)]
    session: Option<String>,

    /// Write artifacts under this directory
    #[arg(long, value_name = "DIR")]
    artifacts_dir: Option<PathBuf>,

    /// Do not fetch remote components (requires cached artifacts)
    #[arg(long)]
    offline: bool,

    /// Override the component cache directory
    #[arg(long, value_name = "DIR")]
    cache_dir: Option<PathBuf>,

    /// Enforce strict signing (default is DevOk)
    #[arg(long)]
    strict_signing: bool,
}

fn parse_input(cli: &Cli) -> Result<Value> {
    if let Some(path) = &cli.input_file {
        let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        return serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse JSON from {}", path.display()));
    }
    serde_json::from_str(&cli.input).context("failed to parse JSON from --input")
}

fn parse_components_map(cli: &Cli) -> Result<HashMap<String, PathBuf>> {
    if let Some(path) = &cli.components_map {
        let bytes = fs::read(path)
            .with_context(|| format!("failed to read components map {}", path.display()))?;
        let raw: HashMap<String, String> = serde_json::from_slice(&bytes).with_context(|| {
            format!(
                "failed to parse components map JSON from {}",
                path.display()
            )
        })?;
        let mut map = HashMap::new();
        for (id, value) in raw {
            map.insert(id, PathBuf::from(value));
        }
        return Ok(map);
    }
    Ok(HashMap::new())
}

fn build_profile(cli: &Cli) -> Profile {
    let mut dev = DevProfile::default();
    if let Some(tenant) = &cli.tenant {
        dev.tenant_id = tenant.clone();
    }
    if let Some(team) = &cli.team {
        dev.team_id = team.clone();
    }
    if let Some(user) = &cli.user {
        dev.user_id = user.clone();
    }
    Profile::Dev(dev)
}

fn build_ctx(cli: &Cli) -> TenantContext {
    let mut ctx = TenantContext::default_local();
    if let Some(tenant) = &cli.tenant {
        ctx.tenant_id = Some(tenant.clone());
    }
    if let Some(team) = &cli.team {
        ctx.team_id = Some(team.clone());
    }
    if let Some(user) = &cli.user {
        ctx.user_id = Some(user.clone());
    }
    if let Some(session) = &cli.session {
        ctx.session_id = Some(session.clone());
    }
    ctx
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let input = parse_input(&cli)?;

    let signing = if cli.strict_signing {
        SigningPolicy::Strict
    } else {
        SigningPolicy::DevOk
    };

    let profile = build_profile(&cli);
    let ctx = build_ctx(&cli);
    let components_map = parse_components_map(&cli)?;

    let runner = Runner::new().configure(|opts: &mut RunOptions| {
        opts.profile = profile.clone();
        opts.entry_flow = cli.flow.clone();
        opts.input = input.clone();
        opts.ctx = ctx.clone();
        opts.artifacts_dir = cli.artifacts_dir.clone();
        opts.signing = signing;
        opts.components_dir = cli.components_dir.clone();
        opts.components_map = components_map.clone();
        opts.dist_offline = cli.offline;
        opts.dist_cache_dir = cli.cache_dir.clone();
    });

    let result = runner
        .run_pack_with(&cli.pack, |_opts| {})
        .with_context(|| format!("failed to run pack {}", cli.pack.display()))?;

    // Always emit JSON so callers can parse; stderr already carries runtime errors.
    println!(
        "{}",
        serde_json::to_string_pretty(&result)
            .map_err(|err| anyhow!("failed to serialize run result: {err}"))?
    );

    Ok(())
}
