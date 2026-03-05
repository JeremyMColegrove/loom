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

    fn persist_manifest(&self) -> io::Result<()> {
        let bytes = serde_json::to_vec_pretty(&self.manifest)?;
        fs::write(&self.manifest_path, bytes)
    }
}
