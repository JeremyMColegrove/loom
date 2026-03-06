use crate::runtime::Runtime;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustMode {
    Trusted,
    Restricted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOutcome {
    Allowed,
    Denied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOperation {
    Read,
    Write,
    Move,
    Import,
    Watch,
    Network,
}

#[derive(Debug, Clone)]
pub struct AuditEvent {
    pub operation: AuditOperation,
    pub outcome: AuditOutcome,
    pub path: String,
    pub target: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Capability {
    Read,
    Write,
    Import,
    Watch,
}

#[derive(Debug, Clone)]
pub struct SecurityPolicy {
    pub read_paths: Vec<PathBuf>,
    pub write_paths: Vec<PathBuf>,
    pub import_paths: Vec<PathBuf>,
    pub watch_paths: Vec<PathBuf>,
    pub read_path_globs: Vec<String>,
    pub write_path_globs: Vec<String>,
    pub import_path_globs: Vec<String>,
    pub watch_path_globs: Vec<String>,
    pub network_hosts: Vec<String>,
    pub deny_globs: Vec<String>,
    pub allow_all: bool,
    read_allow_set: Option<GlobSet>,
    write_allow_set: Option<GlobSet>,
    import_allow_set: Option<GlobSet>,
    watch_allow_set: Option<GlobSet>,
    deny_set: Option<GlobSet>,
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self::allow_all()
    }
}

impl SecurityPolicy {
    pub fn allow_all() -> Self {
        Self {
            read_paths: vec![],
            write_paths: vec![],
            import_paths: vec![],
            watch_paths: vec![],
            read_path_globs: vec![],
            write_path_globs: vec![],
            import_path_globs: vec![],
            watch_path_globs: vec![],
            network_hosts: vec![],
            deny_globs: vec![],
            allow_all: true,
            read_allow_set: None,
            write_allow_set: None,
            import_allow_set: None,
            watch_allow_set: None,
            deny_set: None,
        }
    }

    pub fn restricted() -> Self {
        Self {
            read_paths: vec![],
            write_paths: vec![],
            import_paths: vec![],
            watch_paths: vec![],
            read_path_globs: vec![],
            write_path_globs: vec![],
            import_path_globs: vec![],
            watch_path_globs: vec![],
            network_hosts: vec![],
            deny_globs: vec![],
            allow_all: false,
            read_allow_set: None,
            write_allow_set: None,
            import_allow_set: None,
            watch_allow_set: None,
            deny_set: None,
        }
    }

    pub fn with_read_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.read_paths = paths;
        self.allow_all = false;
        self
    }

    pub fn with_read_path_globs(mut self, globs: Vec<String>) -> RuntimeResult<Self> {
        self.read_path_globs = globs;
        self.allow_all = false;
        self.rebuild_read_allow_set()?;
        Ok(self)
    }

    pub fn with_write_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.write_paths = paths;
        self.allow_all = false;
        self
    }

    pub fn with_write_path_globs(mut self, globs: Vec<String>) -> RuntimeResult<Self> {
        self.write_path_globs = globs;
        self.allow_all = false;
        self.rebuild_write_allow_set()?;
        Ok(self)
    }

    pub fn with_import_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.import_paths = paths;
        self.allow_all = false;
        self
    }

    pub fn with_import_path_globs(mut self, globs: Vec<String>) -> RuntimeResult<Self> {
        self.import_path_globs = globs;
        self.allow_all = false;
        self.rebuild_import_allow_set()?;
        Ok(self)
    }

    pub fn with_watch_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.watch_paths = paths;
        self.allow_all = false;
        self
    }

    pub fn with_watch_path_globs(mut self, globs: Vec<String>) -> RuntimeResult<Self> {
        self.watch_path_globs = globs;
        self.allow_all = false;
        self.rebuild_watch_allow_set()?;
        Ok(self)
    }

    pub fn with_deny_globs(mut self, globs: Vec<String>) -> RuntimeResult<Self> {
        self.deny_globs = globs;
        self.rebuild_deny_set()?;
        Ok(self)
    }

    pub fn with_network_hosts(mut self, hosts: Vec<String>) -> Self {
        self.network_hosts = hosts;
        self
    }

    pub fn try_finalize(mut self) -> RuntimeResult<Self> {
        self.rebuild_read_allow_set()?;
        self.rebuild_write_allow_set()?;
        self.rebuild_import_allow_set()?;
        self.rebuild_watch_allow_set()?;
        self.rebuild_deny_set()?;
        Ok(self)
    }

    fn rebuild_allow_set(globs: &[String], label: &str) -> RuntimeResult<Option<GlobSet>> {
        if globs.is_empty() {
            return Ok(None);
        }

        let mut builder = GlobSetBuilder::new();
        for raw in globs {
            let glob = Glob::new(raw).map_err(|e| {
                RuntimeError::message(format!("Invalid {} glob '{}': {}", label, raw, e))
            })?;
            builder.add(glob);
        }
        Ok(Some(builder.build().map_err(|e| {
            RuntimeError::message(format!("Failed to compile {} globs: {}", label, e))
        })?))
    }

    fn rebuild_read_allow_set(&mut self) -> RuntimeResult<()> {
        self.read_allow_set = Self::rebuild_allow_set(&self.read_path_globs, "read_paths")?;
        Ok(())
    }

    fn rebuild_write_allow_set(&mut self) -> RuntimeResult<()> {
        self.write_allow_set = Self::rebuild_allow_set(&self.write_path_globs, "write_paths")?;
        Ok(())
    }

    fn rebuild_import_allow_set(&mut self) -> RuntimeResult<()> {
        self.import_allow_set = Self::rebuild_allow_set(&self.import_path_globs, "import_paths")?;
        Ok(())
    }

    fn rebuild_watch_allow_set(&mut self) -> RuntimeResult<()> {
        self.watch_allow_set = Self::rebuild_allow_set(&self.watch_path_globs, "watch_paths")?;
        Ok(())
    }

    fn rebuild_deny_set(&mut self) -> RuntimeResult<()> {
        if self.deny_globs.is_empty() {
            self.deny_set = None;
            return Ok(());
        }

        let mut builder = GlobSetBuilder::new();
        for raw in &self.deny_globs {
            let glob = Glob::new(raw).map_err(|e| {
                RuntimeError::message(format!("Invalid deny_glob '{}': {}", raw, e))
            })?;
            builder.add(glob);
        }
        self.deny_set =
            Some(builder.build().map_err(|e| {
                RuntimeError::message(format!("Failed to compile deny_globs: {}", e))
            })?);
        Ok(())
    }
}

impl Runtime {
    pub fn with_security_policy(mut self, policy: SecurityPolicy) -> RuntimeResult<Self> {
        self.set_security_policy(policy)?;
        Ok(self)
    }

    pub fn set_security_policy(&mut self, policy: SecurityPolicy) -> RuntimeResult<()> {
        self.security_policy = policy.try_finalize()?;
        Ok(())
    }

    pub fn with_trust_mode(mut self, mode: TrustMode) -> Self {
        self.trust_mode = mode;
        self
    }

    pub fn set_trust_mode(&mut self, mode: TrustMode) {
        self.trust_mode = mode;
    }

    pub fn audit_log(&self) -> &[AuditEvent] {
        &self.audit_log
    }

    pub(crate) fn authorize_existing_path(
        &mut self,
        capability: Capability,
        op: AuditOperation,
        raw_path: &str,
    ) -> RuntimeResult<PathBuf> {
        let path = self.resolve_user_path(raw_path);
        let canonical = std::fs::canonicalize(&path).map_err(|e| {
            RuntimeError::message(format!(
                "Failed to resolve path '{}': {}",
                path.display(),
                e
            ))
        })?;
        self.authorize_canonical(capability, op, canonical, None)
    }

    pub(crate) fn authorize_new_path(
        &mut self,
        capability: Capability,
        op: AuditOperation,
        raw_path: &str,
    ) -> RuntimeResult<PathBuf> {
        let path = self.resolve_user_path(raw_path);
        let canonical = self.canonicalize_with_existing_ancestor(&path)?;
        self.authorize_canonical(capability, op, canonical, None)
    }

    pub(crate) fn authorize_move_paths(
        &mut self,
        raw_src: &str,
        raw_dest: &str,
    ) -> RuntimeResult<(PathBuf, PathBuf)> {
        if self.trust_mode == TrustMode::Restricted {
            let err = RuntimeError::restricted_operation("move");
            self.push_audit_event(
                AuditOperation::Move,
                AuditOutcome::Denied,
                raw_src.to_string(),
                Some(raw_dest.to_string()),
                Some(err.to_string()),
            );
            return Err(err);
        }

        let src = self.authorize_existing_path(Capability::Read, AuditOperation::Read, raw_src)?;
        let dest = self.authorize_new_path(Capability::Write, AuditOperation::Write, raw_dest)?;
        self.push_audit_event(
            AuditOperation::Move,
            AuditOutcome::Allowed,
            src.to_string_lossy().to_string(),
            Some(dest.to_string_lossy().to_string()),
            None,
        );
        Ok((src, dest))
    }

    pub(crate) fn authorize_watch_path(&mut self, raw_path: &str) -> RuntimeResult<PathBuf> {
        if self.trust_mode == TrustMode::Restricted {
            let err = RuntimeError::restricted_operation("watch");
            self.push_audit_event(
                AuditOperation::Watch,
                AuditOutcome::Denied,
                raw_path.to_string(),
                None,
                Some(err.to_string()),
            );
            return Err(err);
        }
        self.authorize_existing_path(Capability::Watch, AuditOperation::Watch, raw_path)
    }

    pub(crate) fn authorize_import_path(&mut self, raw_path: &str) -> RuntimeResult<PathBuf> {
        if self.trust_mode == TrustMode::Restricted {
            let err = RuntimeError::restricted_operation("import");
            self.push_audit_event(
                AuditOperation::Import,
                AuditOutcome::Denied,
                raw_path.to_string(),
                None,
                Some(err.to_string()),
            );
            return Err(err);
        }
        self.authorize_existing_path(Capability::Import, AuditOperation::Import, raw_path)
    }

    pub(crate) fn authorize_network_url(&mut self, raw_url: &str) -> RuntimeResult<()> {
        if self.trust_mode == TrustMode::Restricted {
            let err = RuntimeError::restricted_operation("network");
            self.push_audit_event(
                AuditOperation::Network,
                AuditOutcome::Denied,
                raw_url.to_string(),
                None,
                Some(err.to_string()),
            );
            return Err(err);
        }

        let parsed = reqwest::Url::parse(raw_url)
            .map_err(|e| RuntimeError::message(format!("Invalid URL '{}': {}", raw_url, e)))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| RuntimeError::message(format!("URL has no host: '{}'", raw_url)))?
            .to_ascii_lowercase();
        let host_with_port = match parsed.port_or_known_default() {
            Some(port) => format!("{}:{}", host, port),
            None => host.clone(),
        };

        let allowed = if self.security_policy.allow_all
            && self.security_policy.network_hosts.is_empty()
        {
            true
        } else if self.security_policy.network_hosts.is_empty() {
            false
        } else {
            self.security_policy
                .network_hosts
                .iter()
                .map(|h| h.trim().to_ascii_lowercase())
                .any(|allowed_host| {
                    allowed_host == "*" || allowed_host == host || allowed_host == host_with_port
                })
        };

        if !allowed {
            let err = RuntimeError::unauthorized_access("Network", host_with_port.clone());
            self.push_audit_event(
                AuditOperation::Network,
                AuditOutcome::Denied,
                raw_url.to_string(),
                Some(host_with_port),
                Some(err.to_string()),
            );
            return Err(err);
        }

        self.push_audit_event(
            AuditOperation::Network,
            AuditOutcome::Allowed,
            raw_url.to_string(),
            Some(host_with_port),
            None,
        );
        Ok(())
    }

    fn authorize_canonical(
        &mut self,
        capability: Capability,
        op: AuditOperation,
        canonical_path: PathBuf,
        target: Option<String>,
    ) -> RuntimeResult<PathBuf> {
        let display_path = canonical_path.to_string_lossy().to_string();

        if self.trust_mode == TrustMode::Restricted && matches!(capability, Capability::Write) {
            let err = RuntimeError::restricted_operation("write");
            self.push_audit_event(
                op,
                AuditOutcome::Denied,
                display_path,
                target,
                Some(err.to_string()),
            );
            return Err(err);
        }

        if self.path_is_denied(&canonical_path) {
            let err = RuntimeError::denied_by_deny_globs(display_path.clone());
            self.push_audit_event(
                op,
                AuditOutcome::Denied,
                display_path,
                target,
                Some(err.to_string()),
            );
            return Err(err);
        }

        if !self.security_policy.allow_all {
            let allowlist = self.allowlist_for(capability)?;
            let mut allowed = allowlist
                .iter()
                .any(|root| canonical_path.starts_with(root));
            if !allowed {
                allowed = self.capability_glob_allows(capability, &canonical_path);
            }
            if !allowed {
                let err = RuntimeError::unauthorized_access(
                    format!("{:?}", capability),
                    display_path.clone(),
                );
                self.push_audit_event(
                    op,
                    AuditOutcome::Denied,
                    display_path,
                    target,
                    Some(err.to_string()),
                );
                return Err(err);
            }
        }

        self.push_audit_event(op, AuditOutcome::Allowed, display_path, target, None);
        Ok(canonical_path)
    }

    fn allowlist_for(&self, capability: Capability) -> RuntimeResult<Vec<PathBuf>> {
        let paths = match capability {
            Capability::Read => &self.security_policy.read_paths,
            Capability::Write => &self.security_policy.write_paths,
            Capability::Import => &self.security_policy.import_paths,
            Capability::Watch => &self.security_policy.watch_paths,
        };

        let mut out = Vec::with_capacity(paths.len());
        for raw in paths {
            out.push(self.canonicalize_with_existing_ancestor(raw)?);
        }
        Ok(out)
    }

    fn path_matches_glob_set(path: &Path, allow_set: Option<&GlobSet>) -> bool {
        let Some(allow_set) = allow_set else {
            return false;
        };
        let normalized = normalize_for_glob(path);
        allow_set.is_match(&normalized)
    }

    fn capability_glob_allows(&self, capability: Capability, path: &Path) -> bool {
        match capability {
            Capability::Read => {
                Self::path_matches_glob_set(path, self.security_policy.read_allow_set.as_ref())
            }
            Capability::Write => {
                Self::path_matches_glob_set(path, self.security_policy.write_allow_set.as_ref())
            }
            Capability::Import => {
                Self::path_matches_glob_set(path, self.security_policy.import_allow_set.as_ref())
            }
            Capability::Watch => {
                Self::path_matches_glob_set(path, self.security_policy.watch_allow_set.as_ref())
            }
        }
    }

    fn path_is_denied(&self, path: &Path) -> bool {
        let Some(deny_set) = &self.security_policy.deny_set else {
            return false;
        };
        let normalized = normalize_for_glob(path);
        deny_set.is_match(&normalized)
    }

    fn push_audit_event(
        &mut self,
        operation: AuditOperation,
        outcome: AuditOutcome,
        path: String,
        target: Option<String>,
        reason: Option<String>,
    ) {
        self.audit_log.push(AuditEvent {
            operation,
            outcome,
            path,
            target,
            reason,
        });
    }

    pub(crate) fn resolve_user_path(&self, raw_path: &str) -> PathBuf {
        let path = PathBuf::from(raw_path);
        if path.is_absolute() {
            return path;
        }
        if let Some(dir) = &self.script_dir {
            return PathBuf::from(dir).join(path);
        }
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }

    pub(crate) fn canonicalize_with_existing_ancestor(
        &self,
        path: &Path,
    ) -> RuntimeResult<PathBuf> {
        let mut unresolved_tail: Vec<PathBuf> = Vec::new();
        let mut cursor = path.to_path_buf();

        while !cursor.exists() {
            let name = cursor.file_name().ok_or_else(|| {
                RuntimeError::message(format!("Cannot resolve path '{}'", path.display()))
            })?;
            unresolved_tail.push(PathBuf::from(name));
            cursor = cursor
                .parent()
                .ok_or_else(|| {
                    RuntimeError::message(format!("Cannot resolve path '{}'", path.display()))
                })?
                .to_path_buf();
        }

        let mut canonical = std::fs::canonicalize(&cursor).map_err(|e| {
            RuntimeError::message(format!(
                "Failed to canonicalize '{}': {}",
                cursor.display(),
                e
            ))
        })?;

        for segment in unresolved_tail.iter().rev() {
            canonical.push(segment);
        }

        Ok(canonical)
    }
}

fn normalize_for_glob(path: &Path) -> String {
    let mut parts = Vec::new();
    for comp in path.components() {
        match comp {
            Component::Prefix(p) => parts.push(p.as_os_str().to_string_lossy().to_string()),
            Component::RootDir => parts.push(String::new()),
            Component::CurDir => parts.push(".".to_string()),
            Component::ParentDir => parts.push("..".to_string()),
            Component::Normal(s) => parts.push(s.to_string_lossy().to_string()),
        }
    }

    if parts.is_empty() {
        return path.to_string_lossy().to_string();
    }

    if parts.first().is_some_and(|p| p.is_empty()) {
        format!("/{}", parts[1..].join("/"))
    } else {
        parts.join("/")
    }
}

#[cfg(test)]
#[path = "../../tests/unit/runtime_security_tests.rs"]
mod runtime_security_tests;
