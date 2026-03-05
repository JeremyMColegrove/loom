use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct AtomicContext {
    journal_dir: PathBuf,
}

#[derive(Debug)]
pub struct AtomicTransaction {
    txn_dir: PathBuf,
    manifest_path: PathBuf,
    manifest: JournalManifest,
    snapshotted: HashSet<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct JournalManifest {
    entries: Vec<JournalEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JournalEntry {
    path: String,
    existed: bool,
    snapshot_file: Option<String>,
}

impl AtomicContext {
    pub fn new<P: AsRef<Path>>(base: P) -> io::Result<Self> {
        let journal_dir = base.as_ref().join(".loom_journal");
        fs::create_dir_all(&journal_dir)?;
        Ok(Self { journal_dir })
    }

    pub fn begin(&self) -> io::Result<AtomicTransaction> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let txn_dir = self.journal_dir.join(format!("txn-{}", nanos));
        fs::create_dir_all(&txn_dir)?;
        let manifest_path = txn_dir.join("manifest.json");
        let manifest = JournalManifest::default();
        fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;

        Ok(AtomicTransaction {
            txn_dir,
            manifest_path,
            manifest,
            snapshotted: HashSet::new(),
        })
    }
}

impl AtomicTransaction {
    pub fn snapshot_path(&mut self, path: &str) -> io::Result<()> {
        if self.snapshotted.contains(path) {
            return Ok(());
        }
        self.snapshotted.insert(path.to_string());

        let path_obj = Path::new(path);
        if path_obj.exists() && path_obj.is_file() {
            let idx = self.manifest.entries.len();
            let snapshot_name = format!("snapshot-{}.bin", idx);
            let snapshot_path = self.txn_dir.join(&snapshot_name);
            let bytes = fs::read(path_obj)?;
            fs::write(snapshot_path, bytes)?;
            self.manifest.entries.push(JournalEntry {
                path: path.to_string(),
                existed: true,
                snapshot_file: Some(snapshot_name),
            });
        } else {
            self.manifest.entries.push(JournalEntry {
                path: path.to_string(),
                existed: false,
                snapshot_file: None,
            });
        }

        self.persist_manifest()
    }

    pub fn commit(self) -> io::Result<()> {
        fs::remove_dir_all(self.txn_dir)
    }

    pub fn rollback(self) -> io::Result<()> {
        for entry in self.manifest.entries.iter().rev() {
            if entry.existed {
                if let Some(snapshot) = &entry.snapshot_file {
                    let snapshot_path = self.txn_dir.join(snapshot);
                    let bytes = fs::read(snapshot_path)?;
                    if let Some(parent) = Path::new(&entry.path).parent() {
                        fs::create_dir_all(parent)?;
                    }
                    fs::write(&entry.path, bytes)?;
                }
            } else {
                let target = Path::new(&entry.path);
                if target.exists() && target.is_file() {
                    fs::remove_file(target)?;
                }
            }
        }

        fs::remove_dir_all(self.txn_dir)
    }

    pub(crate) fn persist_manifest(&self) -> io::Result<()> {
        let bytes = serde_json::to_vec_pretty(&self.manifest)?;
        fs::write(&self.manifest_path, bytes)
    }
}


use crate::runtime::Runtime;
use crate::ast::PipeOp;
use crate::runtime::env::Value;

impl Runtime {
    pub(crate) fn write_or_move_path(&mut self, op: &PipeOp, raw_target: &str, pipe_val: &Value) -> Result<Value, String> {
        if matches!(op, PipeOp::Move) {
            return self.move_file(raw_target, pipe_val, op);
        }

        // If the source is a file path and the target looks like a directory, move the file
        if pipe_val.as_path().is_some() && self.is_directory_target(raw_target) {
            return self.move_file(raw_target, pipe_val, op);
        }

        let payload = match pipe_val {
            Value::Path(src) => std::fs::read_to_string(src)
                .map_err(|e| format!("Failed to read '{}': {}", src, e))?,
            _ => self.serialize_for_path_output(pipe_val),
        };



        self.snapshot_if_atomic(raw_target)?;
        match op {
            PipeOp::Safe => {
                if payload.is_empty() {
                    return Ok(Value::Path(raw_target.to_string()));
                }
                
                println!("  📁 Appending to: {}", raw_target);
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open(raw_target)
                    .map_err(|e| format!("Failed to open '{}': {}", raw_target, e))?;
                use std::io::{Read, Seek, SeekFrom, Write};
                
                let mut needs_newline = false;
                if let Ok(meta) = file.metadata() {
                    if meta.len() > 0 {
                        if file.seek(SeekFrom::End(-1)).is_ok() {
                            let mut buf = [0; 1];
                            if file.read_exact(&mut buf).is_ok() && buf[0] != b'\n' {
                                needs_newline = true;
                            }
                        }
                    }
                }
                
                if needs_newline {
                    let _ = file.write_all(b"\n");
                }
                
                let mut bytes = payload.into_bytes();
                if !bytes.ends_with(b"\n") {
                    bytes.push(b'\n');
                }
                
                file.write_all(&bytes)
                    .map_err(|e| format!("Failed to append '{}': {}", raw_target, e))?;
            }
            PipeOp::Force => {
                println!("  📁 Overwriting: {}", raw_target);
                std::fs::write(raw_target, payload)
                    .map_err(|e| format!("Failed to write '{}': {}", raw_target, e))?;
            }
            PipeOp::Move => unreachable!(),
        }

        Ok(Value::Path(raw_target.to_string()))
    }

    pub(crate) fn serialize_for_path_output(&self, value: &Value) -> String {
        self.serialize_csv_if_possible(value)
            .unwrap_or_else(|| value.as_string())
    }

    pub(crate) fn serialize_csv_if_possible(&self, value: &Value) -> Option<String> {
        match value {
            Value::Record(map) => {
                if let Some(Value::List(rows)) = map.get("rows") {
                    let preferred_headers = map.get("headers")
                        .and_then(|h| match h {
                            Value::List(items) => Some(items.iter()
                                .map(|v| v.as_string())
                                .collect::<Vec<_>>()),
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

    pub(crate) fn serialize_records_as_csv(&self, rows: &[Value], preferred_headers: Option<&[String]>) -> Option<String> {
        if rows.is_empty() {
            return Some(String::new());
        }
        if !rows.iter().all(|r| matches!(r, Value::Record(_))) {
            return None;
        }

        let mut headers: Vec<String> = preferred_headers
            .map(|h| h.to_vec())
            .unwrap_or_default();
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
        out.push_str(&headers.iter().map(|h| csv_escape(h)).collect::<Vec<_>>().join(","));
        out.push('\n');
        for row in rows {
            let Value::Record(map) = row else { continue };
            let line = headers.iter()
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

    pub(crate) fn move_file(&mut self, raw_target: &str, pipe_val: &Value, op: &PipeOp) -> Result<Value, String> {
        let src_path = match pipe_val.as_path() {
            Some(p) => p.to_string(),
            None => return Err("Move targets require a file path source".to_string()),
        };

        let src = Path::new(&src_path);
        let file_name = src.file_name()
            .ok_or_else(|| format!("Source path has no file name: '{}'", src_path))?;

        let mut target_path = std::path::PathBuf::from(raw_target);
        if !target_path.is_absolute() {
            if let Some(dir) = &self.script_dir {
                target_path = std::path::PathBuf::from(dir).join(target_path);
            }
        }

        if self.is_directory_target(raw_target) {
            std::fs::create_dir_all(&target_path)
                .map_err(|e| format!("Failed to create directory '{}': {}", target_path.display(), e))?;
            target_path = target_path.join(file_name);
        } else {
            if let Some(parent) = target_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("Failed to create directory '{}': {}", parent.display(), e))?;
            }
        }

        let dest = target_path.to_string_lossy().to_string();

        self.snapshot_if_atomic(&src_path)?;
        self.snapshot_if_atomic(&dest)?;

        if matches!(op, PipeOp::Force) && target_path.exists() {
            std::fs::remove_file(&target_path)
                .map_err(|e| format!("Failed to replace '{}': {}", dest, e))?;
        }

        std::fs::rename(src, &target_path)
            .map_err(|e| format!("Failed to move '{}' to '{}': {}", src_path, dest, e))?;

        Ok(Value::Path(dest))
    }

    pub(crate) fn begin_atomic(&mut self) -> Result<(), String> {
        let base = if let Some(dir) = &self.script_dir {
            Path::new(dir).to_path_buf()
        } else {
            std::env::current_dir().map_err(|e| format!("Failed to resolve current directory: {}", e))?
        };
        if self.atomic_context.is_none() {
            self.atomic_context = Some(
                AtomicContext::new(base).map_err(|e| format!("Failed to initialize atomic journal: {}", e))?
            );
        }
        let txn = self.atomic_context
            .as_ref()
            .ok_or_else(|| "Atomic context unavailable".to_string())?
            .begin()
            .map_err(|e| format!("Failed to begin atomic transaction: {}", e))?;
        self.atomic_txn = Some(txn);
        self.atomic_active = true;
        Ok(())
    }

    pub(crate) fn snapshot_if_atomic(&mut self, path: &str) -> Result<(), String> {
        if !self.atomic_active {
            return Ok(());
        }
        if let Some(txn) = self.atomic_txn.as_mut() {
            txn.snapshot_path(path)
                .map_err(|e| format!("Failed to snapshot '{}' for atomic rollback: {}", path, e))?;
        }
        Ok(())
    }

    pub(crate) fn commit_atomic(&mut self) -> Result<(), String> {
        if let Some(txn) = self.atomic_txn.take() {
            txn.commit()
                .map_err(|e| format!("Failed to commit atomic transaction: {}", e))?;
        }
        self.atomic_active = false;
        Ok(())
    }

    pub(crate) fn rollback_atomic(&mut self) -> Result<(), String> {
        if let Some(txn) = self.atomic_txn.take() {
            txn.rollback()
                .map_err(|e| format!("Failed to roll back atomic transaction: {}", e))?;
        }
        self.atomic_active = false;
        Ok(())
    }
}
fn csv_escape(value: &str) -> String {
    let needs_quotes = value.contains(',')
        || value.contains('"')
        || value.contains('\n')
        || value.contains('\r');
    if !needs_quotes {
        return value.to_string();
    }
    format!("\"{}\"", value.replace('"', "\"\""))
}

