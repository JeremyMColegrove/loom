use log::debug;
use std::path::Path;

use crate::ast::PipeOp;
use crate::runtime::Runtime;
use crate::runtime::atomic::AtomicContext;
use crate::runtime::env::Value;
use crate::runtime::error::{RuntimeError, RuntimeResult};
use crate::runtime::security::{AuditOperation, Capability};

impl Runtime {
    pub(crate) fn write_or_move_path(
        &mut self,
        op: &PipeOp,
        raw_target: &str,
        pipe_val: &Value,
    ) -> RuntimeResult<Value> {
        if matches!(op, PipeOp::Move) {
            return self.move_file(raw_target, pipe_val, op);
        }

        // If the source is a file path and the target looks like a directory, move the file
        if pipe_val.as_path().is_some() && self.is_directory_target(raw_target) {
            return self.move_file(raw_target, pipe_val, op);
        }

        let payload = match pipe_val {
            Value::Path(src) => self.read_text_path(src)?,
            _ => self.serialize_for_path_output(pipe_val),
        };

        let target_path =
            self.authorize_new_path(Capability::Write, AuditOperation::Write, raw_target)?;
        let target = target_path.to_string_lossy().to_string();
        self.snapshot_if_atomic(&target)?;
        match op {
            PipeOp::Safe => {
                if payload.is_empty() {
                    return Ok(Value::Path(target));
                }

                debug!("appending output to {}", target);
                let mut content = payload;
                if !content.ends_with('\n') {
                    content.push('\n');
                }
                self.append_path(&target, &content)?;
            }
            PipeOp::Force => {
                debug!("overwriting output at {}", target);
                self.write_path(&target, &payload)?;
            }
            PipeOp::Move => unreachable!(),
        }

        Ok(Value::Path(target))
    }

    pub(crate) fn serialize_for_path_output(&self, value: &Value) -> String {
        self.serialize_csv_if_possible(value)
            .unwrap_or_else(|| value.as_string())
    }

    pub(crate) fn serialize_csv_if_possible(&self, value: &Value) -> Option<String> {
        match value {
            Value::Record(map) => {
                if let Some(Value::List(rows)) = map.get("rows") {
                    let preferred_headers = map.get("headers").and_then(|h| match h {
                        Value::List(items) => {
                            Some(items.iter().map(|v| v.as_string()).collect::<Vec<_>>())
                        }
                        _ => None,
                    });
                    return self.serialize_records_as_csv(rows, preferred_headers.as_deref());
                }
                None
            }
            Value::List(rows) => self.serialize_records_as_csv(rows, None),
            _ => None,
        }
    }

    pub(crate) fn serialize_records_as_csv(
        &self,
        rows: &[Value],
        preferred_headers: Option<&[String]>,
    ) -> Option<String> {
        if rows.is_empty() {
            return Some(String::new());
        }
        if !rows.iter().all(|r| matches!(r, Value::Record(_))) {
            return None;
        }

        let mut headers: Vec<String> = preferred_headers.map(|h| h.to_vec()).unwrap_or_default();
        let mut seen = std::collections::HashSet::new();
        for h in &headers {
            seen.insert(h.to_ascii_lowercase());
        }
        for row in rows {
            let Value::Record(map) = row else { continue };
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort_by_key(|k| k.to_ascii_lowercase());
            for key in keys {
                let folded = key.to_ascii_lowercase();
                if !seen.contains(&folded) {
                    seen.insert(folded);
                    headers.push(key);
                }
            }
        }

        if headers.is_empty() {
            return Some(String::new());
        }

        let mut out = String::new();
        out.push_str(
            &headers
                .iter()
                .map(|h| csv_escape(h))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push('\n');
        for row in rows {
            let Value::Record(map) = row else { continue };
            let line = headers
                .iter()
                .map(|h| {
                    map.iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case(h))
                        .map(|(_, v)| csv_escape(&v.as_string()))
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(",");
            out.push_str(&line);
            out.push('\n');
        }

        Some(out)
    }

    pub(crate) fn is_directory_target(&self, target: &str) -> bool {
        target.ends_with('/') || Path::new(target).is_dir()
    }

    pub(crate) fn move_file(
        &mut self,
        raw_target: &str,
        pipe_val: &Value,
        op: &PipeOp,
    ) -> RuntimeResult<Value> {
        let src_path = match pipe_val.as_path() {
            Some(p) => p.to_string(),
            None => {
                return Err(RuntimeError::message(
                    "Move targets require a file path source",
                ));
            }
        };

        let src = Path::new(&src_path);
        let file_name = src
            .file_name()
            .ok_or_else(|| format!("Source path has no file name: '{}'", src_path))?;

        let mut target_path = self.resolve_user_path(raw_target);

        if self.is_directory_target(raw_target) {
            self.create_dir_all_checked(&target_path)?;
            target_path = target_path.join(file_name);
        } else if let Some(parent) = target_path.parent() {
            self.create_dir_all_checked(parent)?;
        }

        let dest = target_path.to_string_lossy().to_string();
        let (src_checked, dest_checked) =
            self.authorize_move_paths(&src_path, &dest).map_err(|e| {
                RuntimeError::message(format!(
                    "Failed to move '{}' to '{}': {}",
                    src_path, dest, e
                ))
            })?;
        let dest_checked_s = dest_checked.to_string_lossy().to_string();

        self.snapshot_if_atomic(&src_path)?;
        self.snapshot_if_atomic(&dest_checked_s)?;

        if matches!(op, PipeOp::Force) && dest_checked.exists() {
            std::fs::remove_file(&dest_checked)
                .map_err(|e| format!("Failed to replace '{}': {}", dest_checked_s, e))?;
        }

        std::fs::rename(src_checked, &dest_checked).map_err(|e| {
            format!(
                "Failed to move '{}' to '{}': {}",
                src_path, dest_checked_s, e
            )
        })?;

        Ok(Value::Path(dest_checked_s))
    }

    pub(crate) fn begin_atomic(&mut self) -> RuntimeResult<()> {
        let base = if let Some(dir) = &self.script_dir {
            Path::new(dir).to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|e| format!("Failed to resolve current directory: {}", e))?
        };
        if self.atomic_context.is_none() {
            self.atomic_context = Some(
                AtomicContext::new(base)
                    .map_err(|e| format!("Failed to initialize atomic journal: {}", e))?,
            );
        }
        let txn = self
            .atomic_context
            .as_ref()
            .ok_or_else(|| "Atomic context unavailable".to_string())?
            .begin()
            .map_err(|e| format!("Failed to begin atomic transaction: {}", e))?;
        self.atomic_txn = Some(txn);
        self.atomic_active = true;
        Ok(())
    }

    pub(crate) fn snapshot_if_atomic(&mut self, path: &str) -> RuntimeResult<()> {
        if !self.atomic_active {
            return Ok(());
        }
        if let Some(txn) = self.atomic_txn.as_mut() {
            txn.snapshot_path(path)
                .map_err(|e| format!("Failed to snapshot '{}' for atomic rollback: {}", path, e))?;
        }
        Ok(())
    }

    pub(crate) fn commit_atomic(&mut self) -> RuntimeResult<()> {
        if let Some(txn) = self.atomic_txn.take() {
            txn.commit()
                .map_err(|e| format!("Failed to commit atomic transaction: {}", e))?;
        }
        self.atomic_active = false;
        Ok(())
    }

    pub(crate) fn rollback_atomic(&mut self) -> RuntimeResult<()> {
        if let Some(txn) = self.atomic_txn.take() {
            txn.rollback()
                .map_err(|e| format!("Failed to roll back atomic transaction: {}", e))?;
        }
        self.atomic_active = false;
        Ok(())
    }

    pub(crate) fn read_text_path(&mut self, raw_path: &str) -> RuntimeResult<String> {
        let path =
            self.authorize_existing_path(Capability::Read, AuditOperation::Read, raw_path)?;
        let meta = std::fs::metadata(&path).map_err(|e| {
            RuntimeError::message(format!("Failed to stat '{}': {}", path.display(), e))
        })?;
        self.ensure_file_size_within_limit(&path.to_string_lossy(), meta.len())?;
        std::fs::read_to_string(&path).map_err(|e| {
            RuntimeError::message(format!("Failed to read '{}': {}", path.display(), e))
        })
    }

    #[allow(dead_code)]
    pub(crate) fn read_bytes_path(&mut self, raw_path: &str) -> RuntimeResult<Vec<u8>> {
        let path =
            self.authorize_existing_path(Capability::Read, AuditOperation::Read, raw_path)?;
        let meta = std::fs::metadata(&path).map_err(|e| {
            RuntimeError::message(format!("Failed to stat '{}': {}", path.display(), e))
        })?;
        self.ensure_file_size_within_limit(&path.to_string_lossy(), meta.len())?;
        std::fs::read(&path).map_err(|e| {
            RuntimeError::message(format!("Failed to read '{}': {}", path.display(), e))
        })
    }

    pub(crate) fn write_path(&mut self, raw_path: &str, content: &str) -> RuntimeResult<()> {
        let path = self.authorize_new_path(Capability::Write, AuditOperation::Write, raw_path)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                RuntimeError::message(format!(
                    "Failed to create directory '{}': {}",
                    parent.display(),
                    e
                ))
            })?;
        }
        std::fs::write(&path, content).map_err(|e| {
            RuntimeError::message(format!("Failed to write '{}': {}", path.display(), e))
        })
    }

    pub(crate) fn append_path(&mut self, raw_path: &str, content: &str) -> RuntimeResult<()> {
        let path = self.authorize_new_path(Capability::Write, AuditOperation::Write, raw_path)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                RuntimeError::message(format!(
                    "Failed to create directory '{}': {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        use std::io::{Read, Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                RuntimeError::message(format!("Failed to open '{}': {}", path.display(), e))
            })?;

        let mut needs_newline = false;
        if let Ok(meta) = file.metadata()
            && meta.len() > 0
            && file.seek(SeekFrom::End(-1)).is_ok()
        {
            let mut buf = [0; 1];
            if file.read_exact(&mut buf).is_ok() && buf[0] != b'\n' {
                needs_newline = true;
            }
        }
        if needs_newline {
            file.write_all(b"\n").map_err(|e| {
                RuntimeError::message(format!("Failed to append '{}': {}", path.display(), e))
            })?;
        }
        file.write_all(content.as_bytes()).map_err(|e| {
            RuntimeError::message(format!("Failed to append '{}': {}", path.display(), e))
        })?;
        Ok(())
    }

    pub(crate) fn create_dir_all_checked(&mut self, raw_path: &Path) -> RuntimeResult<()> {
        let path = self.authorize_new_path(
            Capability::Write,
            AuditOperation::Write,
            &raw_path.to_string_lossy(),
        )?;
        std::fs::create_dir_all(&path).map_err(|e| {
            RuntimeError::message(format!(
                "Failed to create directory '{}': {}",
                path.display(),
                e
            ))
        })
    }
}
fn csv_escape(value: &str) -> String {
    let needs_quotes =
        value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r');
    if !needs_quotes {
        return value.to_string();
    }
    format!("\"{}\"", value.replace('"', "\"\""))
}
