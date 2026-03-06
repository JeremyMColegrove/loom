#[cfg(test)]
mod tests {
    use super::{Capability, SecurityPolicy, TrustMode};
    use crate::runtime::Runtime;
    use crate::runtime::security::AuditOperation;
    use tempfile::tempdir;

    #[test]
    fn watch_path_glob_allows_matching_paths_only() {
        let script_dir = tempdir().expect("script dir");
        let allowed = script_dir.path().join("inbox_orders");
        let denied = script_dir.path().join("inbox.me");
        std::fs::create_dir_all(&allowed).expect("create allowed path");
        std::fs::create_dir_all(&denied).expect("create denied path");
        let canonical_script_dir = std::fs::canonicalize(script_dir.path())
            .expect("script dir should canonicalize");

        let policy = SecurityPolicy::restricted()
            .with_watch_paths(vec![])
            .with_watch_path_globs(vec![format!("{}/inbox_*", canonical_script_dir.display())])
            .expect("valid watch glob");

        let mut runtime = Runtime::new().with_script_dir(
            script_dir
                .path()
                .to_str()
                .expect("script dir should be valid utf-8"),
        );
        runtime.set_security_policy(policy).expect("policy set");
        runtime.set_trust_mode(TrustMode::Trusted);

        runtime
            .authorize_watch_path(allowed.to_str().expect("allowed path utf-8"))
            .expect("matching glob should allow watch path");
        let err = runtime
            .authorize_watch_path(denied.to_str().expect("denied path utf-8"))
            .expect_err("non-matching glob should deny watch path");
        assert!(err.to_string().contains("Unauthorized Watch"));
    }

    #[test]
    fn watch_path_literals_still_allow_child_paths() {
        let script_dir = tempdir().expect("script dir");
        let root = script_dir.path().join("inbox");
        let child = root.join("orders");
        std::fs::create_dir_all(&child).expect("create child path");

        let policy = SecurityPolicy::restricted().with_watch_paths(vec![root.clone()]);

        let mut runtime = Runtime::new().with_script_dir(
            script_dir
                .path()
                .to_str()
                .expect("script dir should be valid utf-8"),
        );
        runtime.set_security_policy(policy).expect("policy set");
        runtime.set_trust_mode(TrustMode::Trusted);

        runtime
            .authorize_watch_path(child.to_str().expect("child path utf-8"))
            .expect("literal watch path should preserve starts_with behavior");
    }

    #[test]
    fn read_path_glob_allows_matching_paths_only() {
        let script_dir = tempdir().expect("script dir");
        let allowed = script_dir.path().join("inbox_orders").join("a.txt");
        let denied = script_dir.path().join("inbox.me").join("a.txt");
        std::fs::create_dir_all(allowed.parent().expect("allowed parent")).expect("mkdir");
        std::fs::create_dir_all(denied.parent().expect("denied parent")).expect("mkdir");
        std::fs::write(&allowed, "ok").expect("write allowed");
        std::fs::write(&denied, "no").expect("write denied");
        let canonical_script_dir = std::fs::canonicalize(script_dir.path())
            .expect("script dir should canonicalize");

        let policy = SecurityPolicy::restricted()
            .with_read_paths(vec![])
            .with_read_path_globs(vec![format!("{}/inbox_*", canonical_script_dir.display())])
            .expect("valid read glob");

        let mut runtime = Runtime::new();
        runtime.set_security_policy(policy).expect("policy set");
        runtime.set_trust_mode(TrustMode::Trusted);

        runtime
            .authorize_existing_path(
                Capability::Read,
                AuditOperation::Read,
                allowed.to_str().expect("allowed utf-8"),
            )
            .expect("matching read glob should allow");
        runtime
            .authorize_existing_path(
                Capability::Read,
                AuditOperation::Read,
                denied.to_str().expect("denied utf-8"),
            )
            .expect_err("non-matching read glob should deny");
    }

    #[test]
    fn write_and_import_path_globs_are_enforced() {
        let script_dir = tempdir().expect("script dir");
        let write_allowed = script_dir.path().join("out_orders").join("new.txt");
        let write_denied = script_dir.path().join("out.me").join("new.txt");
        let import_allowed = script_dir.path().join("lib_orders").join("mod.loom");
        let import_denied = script_dir.path().join("lib.me").join("mod.loom");
        std::fs::create_dir_all(import_allowed.parent().expect("import allowed parent"))
            .expect("mkdir");
        std::fs::create_dir_all(import_denied.parent().expect("import denied parent"))
            .expect("mkdir");
        std::fs::write(&import_allowed, "x() => 1").expect("write import allowed");
        std::fs::write(&import_denied, "x() => 2").expect("write import denied");
        let canonical_script_dir = std::fs::canonicalize(script_dir.path())
            .expect("script dir should canonicalize");

        let policy = SecurityPolicy::restricted()
            .with_write_paths(vec![])
            .with_write_path_globs(vec![format!("{}/out_*", canonical_script_dir.display())])
            .expect("valid write glob")
            .with_import_paths(vec![])
            .with_import_path_globs(vec![format!("{}/lib_*", canonical_script_dir.display())])
            .expect("valid import glob");

        let mut runtime = Runtime::new();
        runtime.set_security_policy(policy).expect("policy set");
        runtime.set_trust_mode(TrustMode::Trusted);

        runtime
            .authorize_new_path(
                Capability::Write,
                AuditOperation::Write,
                write_allowed.to_str().expect("write allowed utf-8"),
            )
            .expect("matching write glob should allow");
        runtime
            .authorize_new_path(
                Capability::Write,
                AuditOperation::Write,
                write_denied.to_str().expect("write denied utf-8"),
            )
            .expect_err("non-matching write glob should deny");

        runtime
            .authorize_import_path(import_allowed.to_str().expect("import allowed utf-8"))
            .expect("matching import glob should allow");
        runtime
            .authorize_import_path(import_denied.to_str().expect("import denied utf-8"))
            .expect_err("non-matching import glob should deny");
    }
}
