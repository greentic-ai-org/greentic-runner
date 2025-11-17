use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail};
use greentic_types::Flow;
use greentic_types::component::ComponentManifest;
use greentic_types::pack_manifest::{PackComponentRef, PackManifest};
use zip::ZipArchive;

/// In-memory representation of a parsed pack archive/directory.
#[derive(Debug)]
pub struct LoadedPack {
    /// Pack manifest as defined by greentic-types.
    pub manifest: PackManifest,
    /// Materialized flows referenced by the manifest.
    pub flows: HashMap<String, Flow>,
    /// Embedded component manifests discovered within the pack (best effort).
    pub components: HashMap<String, ComponentManifest>,
}

/// Loads a pack either from a directory (expanded pack) or a `.gtpack` archive.
pub fn load_pack(path: impl AsRef<Path>) -> Result<LoadedPack> {
    let path = path.as_ref();
    if path.is_dir() {
        load_pack_from_dir(path)
    } else {
        load_pack_from_archive(path)
    }
}

fn load_pack_from_dir(root: &Path) -> Result<LoadedPack> {
    let manifest = read_manifest_from_dir(root)?;
    let flows = load_flows_from_dir(root, &manifest)?;
    let components = load_components_from_dir(root, &manifest)?;
    Ok(LoadedPack {
        manifest,
        flows,
        components,
    })
}

fn load_pack_from_archive(path: &Path) -> Result<LoadedPack> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("{} is not a valid gtpack archive", path.display()))?;
    let manifest = read_manifest_from_archive(&mut archive)?;
    let flows = load_flows_from_archive(&manifest, &mut archive)?;
    let components = load_components_from_archive(&manifest, &mut archive)?;
    Ok(LoadedPack {
        manifest,
        flows,
        components,
    })
}

fn read_manifest_from_dir(root: &Path) -> Result<PackManifest> {
    for candidate in ["manifest.yaml", "manifest.yml", "manifest.json"] {
        let manifest_path = root.join(candidate);
        if manifest_path.exists() {
            let bytes = fs::read(&manifest_path)
                .with_context(|| format!("failed to read manifest {}", manifest_path.display()))?;
            return parse_manifest(&bytes, extension_from_path(&manifest_path));
        }
    }
    bail!("pack manifest not found under {}", root.display());
}

fn read_manifest_from_archive(archive: &mut ZipArchive<File>) -> Result<PackManifest> {
    for candidate in ["manifest.yaml", "manifest.yml", "manifest.json"] {
        if let Ok(mut entry) = archive.by_name(candidate) {
            let mut bytes = Vec::new();
            entry
                .read_to_end(&mut bytes)
                .with_context(|| format!("failed to read {candidate} from archive"))?;
            return parse_manifest(&bytes, extension_from_str(candidate));
        }
    }
    bail!("pack manifest missing from archive");
}

fn parse_manifest(bytes: &[u8], ext: Option<&str>) -> Result<PackManifest> {
    match format_from_ext(ext) {
        FileFormat::Json => serde_json::from_slice(bytes).context("failed to parse manifest JSON"),
        FileFormat::Yaml => serde_yaml::from_slice(bytes).context("failed to parse manifest YAML"),
    }
}

fn load_flows_from_dir(root: &Path, manifest: &PackManifest) -> Result<HashMap<String, Flow>> {
    let mut flows = HashMap::new();
    for flow in &manifest.flows {
        let path = root.join(&flow.file);
        let bytes =
            fs::read(&path).with_context(|| format!("failed to read flow {}", path.display()))?;
        let parsed = parse_flow(&bytes, extension_from_path(&path))
            .with_context(|| format!("failed to parse flow {}", path.display()))?;
        flows.insert(flow.id.as_str().to_string(), parsed);
    }
    Ok(flows)
}

fn load_flows_from_archive(
    manifest: &PackManifest,
    archive: &mut ZipArchive<File>,
) -> Result<HashMap<String, Flow>> {
    let mut flows = HashMap::new();
    for flow in &manifest.flows {
        let mut entry = archive
            .by_name(&flow.file)
            .with_context(|| format!("flow {} missing from archive", flow.file))?;
        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .with_context(|| format!("failed to read flow {}", flow.file))?;
        let parsed = parse_flow(&bytes, extension_from_str(&flow.file))
            .with_context(|| format!("failed to parse flow {}", flow.file))?;
        flows.insert(flow.id.as_str().to_string(), parsed);
    }
    Ok(flows)
}

fn load_components_from_dir(
    root: &Path,
    manifest: &PackManifest,
) -> Result<HashMap<String, ComponentManifest>> {
    let mut components = HashMap::new();
    for component in &manifest.components {
        if let Some(manifest_data) = read_component_manifest_dir(root, component)? {
            components.insert(component.id.as_str().to_string(), manifest_data);
        }
    }
    Ok(components)
}

fn load_components_from_archive(
    manifest: &PackManifest,
    archive: &mut ZipArchive<File>,
) -> Result<HashMap<String, ComponentManifest>> {
    let mut components = HashMap::new();
    for component in &manifest.components {
        if let Some(data) = read_component_manifest_archive(archive, component)? {
            components.insert(component.id.as_str().to_string(), data);
        }
    }
    Ok(components)
}

fn read_component_manifest_dir(
    root: &Path,
    component: &PackComponentRef,
) -> Result<Option<ComponentManifest>> {
    for candidate in component_manifest_candidates(component) {
        let path = root.join(&candidate);
        if path.exists() {
            let bytes = fs::read(&path)
                .with_context(|| format!("failed to read component manifest {}", path.display()))?;
            let parsed = parse_component_manifest(&bytes, extension_from_path(&path))
                .with_context(|| {
                    format!("failed to parse component manifest {}", path.display())
                })?;
            return Ok(Some(parsed));
        }
    }
    Ok(None)
}

fn read_component_manifest_archive(
    archive: &mut ZipArchive<File>,
    component: &PackComponentRef,
) -> Result<Option<ComponentManifest>> {
    for candidate in component_manifest_candidates(component) {
        if let Ok(mut entry) = archive.by_name(&candidate) {
            let mut bytes = Vec::new();
            entry
                .read_to_end(&mut bytes)
                .with_context(|| format!("failed to read component manifest {candidate}"))?;
            let parsed = parse_component_manifest(&bytes, extension_from_str(&candidate))
                .with_context(|| format!("failed to parse component manifest {candidate}"))?;
            return Ok(Some(parsed));
        }
    }
    Ok(None)
}

fn parse_component_manifest(bytes: &[u8], ext: Option<&str>) -> Result<ComponentManifest> {
    match format_from_ext(ext) {
        FileFormat::Json => {
            serde_json::from_slice(bytes).context("invalid component manifest JSON")
        }
        FileFormat::Yaml => {
            serde_yaml::from_slice(bytes).context("invalid component manifest YAML")
        }
    }
}

fn parse_flow(bytes: &[u8], ext: Option<&str>) -> Result<Flow> {
    match format_from_ext(ext) {
        FileFormat::Json => serde_json::from_slice(bytes).context("invalid flow JSON"),
        FileFormat::Yaml => serde_yaml::from_slice(bytes).context("invalid flow YAML"),
    }
}

fn extension_from_path(path: &Path) -> Option<&str> {
    path.extension().and_then(OsStr::to_str)
}

fn extension_from_str(path: &str) -> Option<&str> {
    Path::new(path).extension().and_then(OsStr::to_str)
}

#[derive(Clone, Copy)]
enum FileFormat {
    Json,
    Yaml,
}

fn format_from_ext(ext: Option<&str>) -> FileFormat {
    match ext.map(|ext| ext.to_ascii_lowercase()) {
        Some(ref ext) if ext == "json" => FileFormat::Json,
        _ => FileFormat::Yaml,
    }
}

fn component_manifest_candidates(component: &PackComponentRef) -> Vec<String> {
    let base = component.id.as_str();
    let mut paths = Vec::new();
    paths.push(format!("components/{base}.component.yaml"));
    paths.push(format!("components/{base}.component.yml"));
    paths.push(format!("components/{base}.component.json"));
    paths.push(format!("components/{base}.manifest.yaml"));
    paths.push(format!("components/{base}.manifest.yml"));
    paths.push(format!("components/{base}.manifest.json"));
    paths.push(format!("components/{base}.yaml"));
    paths.push(format!("components/{base}.yml"));
    paths.push(format!("components/{base}.json"));
    paths
}
