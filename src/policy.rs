use crate::runtime::Runtime;
use crate::runtime::security::{SecurityPolicy, TrustMode};
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
struct LoomPolicyFile {
    version: Option<u32>,
    allow_all: Option<bool>,
    trust_mode: Option<String>,
    read_paths: Option<Vec<String>>,
    write_paths: Option<Vec<String>>,
    import_paths: Option<Vec<String>>,
    watch_paths: Option<Vec<String>>,
    network_hosts: Option<Vec<String>>,
    deny_globs: Option<Vec<String>>,
}

pub fn parse_trust_mode(raw: &str) -> Result<TrustMode, String> {
    match raw.to_ascii_lowercase().as_str() {
        "trusted" => Ok(TrustMode::Trusted),
        "restricted" => Ok(TrustMode::Restricted),
        _ => Err(format!(
            "Invalid trust mode '{}'; expected 'trusted' or 'restricted'",
            raw
        )),
    }
}

fn has_glob_meta(raw: &str) -> bool {
    raw.chars()
        .any(|ch| matches!(ch, '*' | '?' | '[' | ']' | '{' | '}'))
}

fn normalize_glob_path_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn canonicalize_with_existing_ancestor_lossy(path: &Path) -> PathBuf {
    let mut unresolved_tail: Vec<PathBuf> = Vec::new();
    let mut cursor = path.to_path_buf();

    while !cursor.exists() {
        let Some(name) = cursor.file_name() else {
            return path.to_path_buf();
        };
        unresolved_tail.push(PathBuf::from(name));
        let Some(parent) = cursor.parent() else {
            return path.to_path_buf();
        };
        cursor = parent.to_path_buf();
    }

    let Ok(mut canonical) = std::fs::canonicalize(&cursor) else {
        return path.to_path_buf();
    };

    for segment in unresolved_tail.iter().rev() {
        canonical.push(segment);
    }

    canonical
}

fn resolve_capability_paths(
    raw_paths: Option<Vec<String>>,
    base_dir: &Path,
) -> (Vec<PathBuf>, Vec<String>) {
    let mut literal_paths = Vec::new();
    let mut glob_paths = Vec::new();

    for raw in raw_paths.unwrap_or_default() {
        if raw.trim() == "*" {
            literal_paths.push(current_filesystem_root());
            continue;
        }

        let as_path = PathBuf::from(&raw);
        if has_glob_meta(&raw) {
            let glob_path = if as_path.is_absolute() {
                as_path
            } else {
                base_dir.join(as_path)
            };
            let canonical_glob = canonicalize_with_existing_ancestor_lossy(&glob_path);
            glob_paths.push(normalize_glob_path_string(&canonical_glob));
        } else if as_path.is_absolute() {
            literal_paths.push(as_path);
        } else {
            literal_paths.push(base_dir.join(as_path));
        }
    }

    (literal_paths, glob_paths)
}

fn current_filesystem_root() -> PathBuf {
    std::env::current_dir()
        .ok()
        .and_then(|cwd| cwd.ancestors().last().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from(std::path::MAIN_SEPARATOR.to_string()))
}

pub fn apply_runtime_policy(
    runtime: &mut Runtime,
    policy_file_path: Option<&Path>,
    trust_mode_override: Option<TrustMode>,
) -> Result<(), String> {
    let mut trust_mode_from_policy: Option<TrustMode> = None;

    if let Some(path) = policy_file_path {
        let policy_text = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read policy file '{}': {}", path.display(), e))?;
        let raw: LoomPolicyFile = serde_json::from_str(&policy_text)
            .map_err(|e| format!("Invalid policy JSON '{}': {}", path.display(), e))?;

        let version = raw.version.ok_or_else(|| {
            format!(
                "Missing required policy field 'version' in '{}'; expected version 1",
                path.display()
            )
        })?;
        if version != 1 {
            return Err(format!(
                "Unsupported policy version {} in '{}'; expected version 1",
                version,
                path.display()
            ));
        }

        let LoomPolicyFile {
            version: _,
            allow_all,
            trust_mode,
            read_paths,
            write_paths,
            import_paths,
            watch_paths,
            network_hosts,
            deny_globs,
        } = raw;

        let policy_dir = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let mut policy = SecurityPolicy::restricted();
        let allow_all_enabled = allow_all.unwrap_or(false);
        let all_paths = vec![current_filesystem_root()];

        if allow_all_enabled || read_paths.is_some() {
            if let Some(paths) = read_paths {
                let (read_literal_paths, read_globs) =
                    resolve_capability_paths(Some(paths), &policy_dir);
                policy = policy.with_read_paths(read_literal_paths);
                policy = policy
                    .with_read_path_globs(read_globs)
                    .map_err(|e| e.to_string())?;
            } else {
                policy = policy.with_read_paths(all_paths.clone());
            }
        }
        if allow_all_enabled || write_paths.is_some() {
            if let Some(paths) = write_paths {
                let (write_literal_paths, write_globs) =
                    resolve_capability_paths(Some(paths), &policy_dir);
                policy = policy.with_write_paths(write_literal_paths);
                policy = policy
                    .with_write_path_globs(write_globs)
                    .map_err(|e| e.to_string())?;
            } else {
                policy = policy.with_write_paths(all_paths.clone());
            }
        }
        if allow_all_enabled || import_paths.is_some() {
            if let Some(paths) = import_paths {
                let (import_literal_paths, import_globs) =
                    resolve_capability_paths(Some(paths), &policy_dir);
                policy = policy.with_import_paths(import_literal_paths);
                policy = policy
                    .with_import_path_globs(import_globs)
                    .map_err(|e| e.to_string())?;
            } else {
                policy = policy.with_import_paths(all_paths.clone());
            }
        }
        if allow_all_enabled || watch_paths.is_some() {
            if let Some(paths) = watch_paths {
                let (watch_literal_paths, watch_globs) =
                    resolve_capability_paths(Some(paths), &policy_dir);
                policy = policy.with_watch_paths(watch_literal_paths);
                policy = policy
                    .with_watch_path_globs(watch_globs)
                    .map_err(|e| e.to_string())?;
            } else {
                policy = policy.with_watch_paths(all_paths.clone());
            }
        }
        if allow_all_enabled || network_hosts.is_some() {
            if let Some(hosts) = network_hosts {
                policy = policy.with_network_hosts(hosts);
            } else {
                policy = policy.with_network_hosts(vec!["*".to_string()]);
            }
        }
        if let Some(deny_globs) = deny_globs {
            policy = policy
                .with_deny_globs(deny_globs)
                .map_err(|e| e.to_string())?;
        }
        runtime
            .set_security_policy(policy)
            .map_err(|e| format!("Invalid policy '{}': {}", path.display(), e))?;

        if let Some(mode) = trust_mode {
            trust_mode_from_policy = Some(parse_trust_mode(&mode)?);
        }
    }

    if let Some(mode) = trust_mode_override.or(trust_mode_from_policy) {
        runtime.set_trust_mode(mode);
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/policy_tests.rs"]
mod policy_tests;
