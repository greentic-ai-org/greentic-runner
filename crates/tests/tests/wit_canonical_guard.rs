use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn repo_has_no_local_canonical_component_v060_wit() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");

    let mut stack = vec![repo_root.clone()];
    let mut offenders = Vec::new();

    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        for entry in entries.flatten() {
            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(_) => continue,
            };

            if file_type.is_dir() {
                if should_skip_dir(&path) {
                    continue;
                }
                stack.push(path);
                continue;
            }

            if path.extension() != Some(OsStr::new("wit")) {
                continue;
            }

            let Ok(contents) = fs::read_to_string(&path) else {
                continue;
            };
            if contents.contains("package greentic:component@0.6.0") {
                offenders.push(path);
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "canonical greentic:component@0.6.0 WIT must not exist in this repo:\n{}",
        offenders
            .iter()
            .map(|path| path
                .strip_prefix(&repo_root)
                .unwrap_or(path)
                .display()
                .to_string())
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn should_skip_dir(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(OsStr::to_str) else {
        return false;
    };

    if name == ".git" || name == "target" {
        return true;
    }

    path.join(".git").exists()
}
