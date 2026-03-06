use super::*;
use notify::event::{CreateKind, ModifyKind};
use std::path::PathBuf;

#[test]
fn flatten_rename_event_maps_to_deleted_then_created() {
    let event = Event {
        kind: EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
        paths: vec![PathBuf::from("/tmp/old.txt"), PathBuf::from("/tmp/new.txt")],
        attrs: Default::default(),
    };

    let flattened = Runtime::flatten_notify_event(event);
    assert_eq!(flattened.len(), 2);
    assert_eq!(flattened[0], ("/tmp/old.txt".to_string(), "deleted"));
    assert_eq!(flattened[1], ("/tmp/new.txt".to_string(), "created"));
}

#[test]
fn flatten_rename_to_event_maps_to_created() {
    let event = Event {
        kind: EventKind::Modify(ModifyKind::Name(RenameMode::To)),
        paths: vec![PathBuf::from("/tmp/new-only.txt")],
        attrs: Default::default(),
    };

    let flattened = Runtime::flatten_notify_event(event);
    assert_eq!(
        flattened,
        vec![("/tmp/new-only.txt".to_string(), "created")]
    );
}

#[test]
fn flatten_rename_any_event_maps_to_renamed() {
    let event = Event {
        kind: EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
        paths: vec![PathBuf::from("/tmp/unknown-rename.txt")],
        attrs: Default::default(),
    };

    let flattened = Runtime::flatten_notify_event(event);
    assert_eq!(
        flattened,
        vec![("/tmp/unknown-rename.txt".to_string(), "renamed")]
    );
}

#[test]
fn normalize_renamed_uses_path_existence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let existing = dir.path().join("present.txt");
    std::fs::write(&existing, "x").expect("write");
    let missing = dir.path().join("missing.txt");

    assert_eq!(
        Runtime::normalize_event_type(existing.to_string_lossy().as_ref(), "renamed"),
        "created"
    );
    assert_eq!(
        Runtime::normalize_event_type(missing.to_string_lossy().as_ref(), "renamed"),
        "deleted"
    );
}

#[test]
fn normalize_created_or_modified_to_deleted_when_missing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("missing.txt");

    assert_eq!(
        Runtime::normalize_event_type(missing.to_string_lossy().as_ref(), "created"),
        "deleted"
    );
    assert_eq!(
        Runtime::normalize_event_type(missing.to_string_lossy().as_ref(), "modified"),
        "deleted"
    );
}

#[test]
fn watcher_storm_keeps_created_when_followed_by_modified() {
    let mut pending = std::collections::HashMap::new();

    for _ in 0..2_000 {
        let create_event = Event {
            kind: EventKind::Create(CreateKind::Any),
            paths: vec![
                PathBuf::from("/tmp/file-a.txt"),
                PathBuf::from("/tmp/file-b.txt"),
            ],
            attrs: Default::default(),
        };
        Runtime::coalesce_pending_events(&mut pending, Runtime::flatten_notify_event(create_event));

        let modify_event = Event {
            kind: EventKind::Modify(ModifyKind::Any),
            paths: vec![
                PathBuf::from("/tmp/file-a.txt"),
                PathBuf::from("/tmp/file-b.txt"),
            ],
            attrs: Default::default(),
        };
        Runtime::coalesce_pending_events(&mut pending, Runtime::flatten_notify_event(modify_event));
    }

    assert_eq!(pending.len(), 2);
    assert_eq!(pending.get("/tmp/file-a.txt"), Some(&"created".to_string()));
    assert_eq!(pending.get("/tmp/file-b.txt"), Some(&"created".to_string()));
}
