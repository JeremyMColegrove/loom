#[cfg(test)]
mod tests {
    use super::{
        canonicalize_with_existing_ancestor_lossy, current_filesystem_root, resolve_capability_paths,
    };

    #[test]
    fn wildcard_paths_resolve_to_filesystem_root() {
        let base = std::env::temp_dir();
        let (resolved, globs) = resolve_capability_paths(Some(vec!["*".to_string()]), &base);
        let expected_root = current_filesystem_root();
        assert_eq!(resolved, vec![expected_root]);
        assert!(globs.is_empty());
    }

    #[test]
    fn watch_path_globs_and_literals_are_split() {
        let base = std::path::Path::new("/tmp/loom-policy");
        let (literals, globs) = resolve_capability_paths(
            Some(vec!["./inbox".to_string(), "./inbox_*".to_string()]),
            base,
        );
        let expected_glob = canonicalize_with_existing_ancestor_lossy(&base.join("inbox_*"));

        assert_eq!(literals, vec![base.join("inbox")]);
        assert_eq!(globs, vec![expected_glob.to_string_lossy().replace('\\', "/")]);
    }
}
