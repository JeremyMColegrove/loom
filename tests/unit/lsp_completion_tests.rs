use super::*;
use std::path::Path;

#[test]
fn std_out_is_suggested_for_std_import_prefix() {
    let labels = collect_std_import_candidates(Path::new("/"), "std.o");
    assert!(labels.iter().any(|label| label == "std.out"));
}

#[test]
fn std_http_is_suggested_for_std_import_prefix() {
    let labels = collect_std_import_candidates(Path::new("/"), "std.h");
    assert!(labels.iter().any(|label| label == "std.http"));
}

#[test]
fn secret_is_suggested_in_directive_completion() {
    let items = completion_items_for_trigger(Some("@"));
    assert!(items.iter().any(|item| item.label == "secret"));
}

#[test]
fn signature_context_tracks_active_param_on_single_line() {
    let text = "@map(\"a\", \"b\")";
    let line_text = "@map(\"a\", \"";
    let character = line_text.encode_utf16().count() as u32;
    let ctx = get_signature_context(text, 0, character);
    assert_eq!(ctx, Some(("@map".to_string(), 1)));
}

#[test]
fn signature_context_tracks_active_param_across_unicode_previous_line() {
    let text = "@map(\n  \"😀\", \n  \"b\"\n)";
    let line_text = "  \"";
    let character = line_text.encode_utf16().count() as u32;
    let ctx = get_signature_context(text, 2, character);
    assert_eq!(ctx, Some(("@map".to_string(), 1)));
}
