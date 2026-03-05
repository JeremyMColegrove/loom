use crate::lsp::catalog::{BUILTIN_FUNCTIONS, DIRECTIVES, KEYWORDS, MEMBER_FIELDS};
use crate::lsp::text::{byte_idx_to_utf16_col, utf16_col_to_byte_idx, utf16_col_to_char_idx};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use tower_lsp::lsp_types::{
    CompletionItem, CompletionItemKind, CompletionTextEdit, Documentation, MarkupContent,
    MarkupKind, Position, Range, TextEdit, Url,
};

#[allow(dead_code)]
enum CompletionContext {
    Directive,
    Source,
    PipeDestination,
    MemberAccess,
    StringLiteral,
    General,
}

#[allow(dead_code)]
fn get_context_at_position(text: &str, line: u32, character: u32) -> CompletionContext {
    let lines: Vec<&str> = text.lines().collect();
    let line_idx = line as usize;
    if line_idx >= lines.len() {
        return CompletionContext::General;
    }
    let line_text = lines[line_idx];
    let col = utf16_col_to_byte_idx(line_text, character);
    let before_cursor = &line_text[..col];
    let trimmed = before_cursor.trim_start();

    let quote_count = before_cursor.chars().filter(|c| *c == '"').count();
    if quote_count % 2 == 1 {
        return CompletionContext::StringLiteral;
    }

    if trimmed.ends_with('@') {
        return CompletionContext::Directive;
    }

    if let Some(at_pos) = before_cursor.rfind('@') {
        let after_at = &before_cursor[at_pos + 1..];
        if after_at
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '_')
        {
            return CompletionContext::Directive;
        }
    }

    if trimmed.ends_with('.') || (trimmed.contains('.') && !trimmed.ends_with(' ')) {
        return CompletionContext::MemberAccess;
    }

    if trimmed.contains(">>")
        && let Some(last_pipe) = trimmed.rfind(">>")
    {
        let after_pipe = trimmed[last_pipe + 2..].trim();
        if after_pipe.is_empty() || after_pipe.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return CompletionContext::PipeDestination;
        }
    }

    if trimmed.is_empty()
        || trimmed
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '@')
    {
        return CompletionContext::Source;
    }

    CompletionContext::General
}

/// Returns `(prefix, content_start_column)` where `content_start_column` is the
/// 0-based column of the first character after the opening `"` of the import path.
pub(crate) fn extract_import_prefix(
    text: &str,
    line: u32,
    character: u32,
) -> Option<(String, u32)> {
    let lines: Vec<&str> = text.lines().collect();
    let line_text = lines.get(line as usize)?;
    let col = utf16_col_to_byte_idx(line_text, character);
    let before_cursor = &line_text[..col];

    let import_pos = before_cursor.rfind("@import")?;
    let after_import = &before_cursor[import_pos + "@import".len()..];
    let quote_pos = after_import.find('"')?;
    let prefix_start = import_pos + "@import".len() + quote_pos + 1;
    let prefix = &before_cursor[prefix_start..];
    if prefix.contains('"') {
        return None;
    }
    Some((
        prefix.to_string(),
        byte_idx_to_utf16_col(line_text, prefix_start),
    ))
}

pub(crate) fn get_signature_context(
    text: &str,
    line: u32,
    character: u32,
) -> Option<(String, u32)> {
    let lines: Vec<&str> = text.lines().collect();
    let line_idx = line as usize;
    if line_idx >= lines.len() {
        return None;
    }

    let mut depth = 0;
    let mut param_index = 0;
    let mut current_idx = {
        let line_text = lines[line_idx];
        utf16_col_to_char_idx(line_text, character)
    };
    let mut current_line = line_idx;

    while current_line < lines.len() {
        let line_text = lines[current_line];
        let chars: Vec<char> = line_text.chars().collect();
        let end = current_idx.min(chars.len());

        for i in (0..end).rev() {
            let c = chars[i];
            match c {
                ')' => depth += 1,
                '(' => {
                    depth -= 1;
                    if depth < 0 {
                        let before_paren: String = chars[..i].iter().collect();
                        let before_paren = before_paren.trim_end();

                        let before_chars: Vec<char> = before_paren.chars().collect();
                        let mut name_start = before_chars.len();
                        while name_start > 0 {
                            let pc = before_chars[name_start - 1];
                            if pc.is_alphanumeric() || pc == '_' || pc == '.' || pc == '@' {
                                name_start -= 1;
                            } else {
                                break;
                            }
                        }

                        let name: String = before_chars[name_start..].iter().collect();
                        if !name.is_empty() {
                            return Some((name, param_index));
                        }
                        return None;
                    }
                }
                ',' => {
                    if depth == 0 {
                        param_index += 1;
                    }
                }
                _ => {}
            }
        }

        if current_line == 0 {
            break;
        }
        current_line -= 1;
        current_idx = lines[current_line].len();
    }

    None
}

fn collect_local_import_candidates(base_dir: &Path, prefix: &str) -> Vec<String> {
    let (rel_dir, name_prefix) = match prefix.rsplit_once('/') {
        Some((d, p)) => (d, p),
        None => ("", prefix),
    };

    let search_dir = if rel_dir.is_empty() {
        base_dir.to_path_buf()
    } else {
        base_dir.join(rel_dir)
    };

    let mut out = Vec::new();
    let entries = match std::fs::read_dir(&search_dir) {
        Ok(e) => e,
        Err(_) => return out,
    };

    for entry in entries.flatten() {
        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.starts_with(name_prefix) {
            continue;
        }
        let entry_path = entry.path();
        if entry_path.is_dir() {
            let rel = if rel_dir.is_empty() {
                format!("{}/", file_name)
            } else {
                format!("{}/{}/", rel_dir, file_name)
            };
            out.push(rel);
            continue;
        }
        let is_loom = entry_path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e == "loom")
            .unwrap_or(false);
        if !is_loom {
            continue;
        }
        let stem = entry_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        let rel = if rel_dir.is_empty() {
            stem.to_string()
        } else {
            format!("{}/{}", rel_dir, stem)
        };
        out.push(rel);
    }
    out.sort();
    out.dedup();
    out
}

fn collect_std_modules_from_dir(std_dir: &Path, rel: PathBuf, out: &mut BTreeSet<String>) {
    let entries = match std::fs::read_dir(std_dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if path.is_dir() {
            let mut next_rel = rel.clone();
            next_rel.push(name);
            collect_std_modules_from_dir(&path, next_rel, out);
            continue;
        }
        let is_loom = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e == "loom")
            .unwrap_or(false);
        if !is_loom {
            continue;
        }
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let mut parts = vec!["std".to_string()];
        for p in rel.components() {
            parts.push(p.as_os_str().to_string_lossy().to_string());
        }
        if stem != "mod" {
            parts.push(stem.to_string());
        }
        out.insert(parts.join("."));
    }
}

fn collect_std_import_candidates(base_dir: &Path, prefix: &str) -> Vec<String> {
    let mut modules = BTreeSet::new();
    modules.insert("std.csv".to_string());

    for ancestor in base_dir.ancestors() {
        let std_dir = ancestor.join("std");
        if std_dir.is_dir() {
            collect_std_modules_from_dir(&std_dir, PathBuf::new(), &mut modules);
        }
    }

    modules
        .into_iter()
        .filter(|m| m.starts_with(prefix))
        .collect()
}

/// Returns `(prefix, content_start_column)` where `content_start_column` is the
/// 0-based column of the first character after the opening `"` of the string.
pub(crate) fn extract_string_literal_prefix(
    text: &str,
    line: u32,
    character: u32,
) -> Option<(String, u32)> {
    let lines: Vec<&str> = text.lines().collect();
    let line_text = lines.get(line as usize)?;
    let col = utf16_col_to_byte_idx(line_text, character);
    let before_cursor = &line_text[..col];

    let mut quote_indices = Vec::new();
    let mut in_escape = false;
    for (i, c) in before_cursor.char_indices() {
        if in_escape {
            in_escape = false;
            continue;
        }
        if c == '\\' {
            in_escape = true;
        } else if c == '"' {
            quote_indices.push(i);
        }
    }

    if quote_indices.len() % 2 == 1 {
        let last_quote = *quote_indices.last()?;
        let content_start = (last_quote + 1) as u32;
        Some((
            before_cursor[last_quote + 1..].to_string(),
            byte_idx_to_utf16_col(line_text, content_start as usize),
        ))
    } else {
        None
    }
}

pub(crate) fn file_completion_items(
    uri: &Url,
    prefix: &str,
    line: u32,
    content_start_col: u32,
    cursor_col: u32,
) -> Vec<CompletionItem> {
    let file_path = match uri.to_file_path() {
        Ok(path) => path,
        Err(_) => return vec![],
    };
    let base_dir = match file_path.parent() {
        Some(dir) => dir,
        None => return vec![],
    };

    let (rel_dir, name_prefix) = match prefix.rsplit_once('/') {
        Some((d, p)) => (d, p),
        None => ("", prefix),
    };

    let search_dir = if rel_dir.is_empty() {
        if prefix.starts_with('/') {
            Path::new("/").to_path_buf()
        } else {
            base_dir.to_path_buf()
        }
    } else if rel_dir.starts_with('/') {
        Path::new(rel_dir).to_path_buf()
    } else {
        base_dir.join(rel_dir)
    };

    let replace_range = Range {
        start: Position {
            line,
            character: content_start_col,
        },
        end: Position {
            line,
            character: cursor_col,
        },
    };

    let mut out = Vec::new();
    let entries = match std::fs::read_dir(&search_dir) {
        Ok(e) => e,
        Err(_) => return out,
    };

    for entry in entries.flatten() {
        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.starts_with(name_prefix) {
            continue;
        }
        let entry_path = entry.path();

        let rel = if rel_dir.is_empty() {
            file_name.clone()
        } else {
            format!("{}/{}", rel_dir, file_name)
        };

        if entry_path.is_dir() {
            out.push(CompletionItem {
                label: format!("{}/", file_name),
                kind: Some(CompletionItemKind::FOLDER),
                filter_text: Some(format!("{}/", rel)),
                text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                    range: replace_range,
                    new_text: format!("{}/", rel),
                })),
                ..Default::default()
            });
        } else {
            out.push(CompletionItem {
                label: file_name,
                kind: Some(CompletionItemKind::FILE),
                filter_text: Some(rel.clone()),
                text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                    range: replace_range,
                    new_text: rel,
                })),
                ..Default::default()
            });
        }
    }

    out.sort_by(|a, b| a.label.cmp(&b.label));
    out
}

pub(crate) fn import_completion_items(
    uri: &Url,
    prefix: &str,
    line: u32,
    content_start_col: u32,
    cursor_col: u32,
) -> Vec<CompletionItem> {
    let file_path = match uri.to_file_path() {
        Ok(path) => path,
        Err(_) => return vec![],
    };
    let base_dir = match file_path.parent() {
        Some(dir) => dir,
        None => return vec![],
    };

    let mut labels = collect_local_import_candidates(base_dir, prefix);
    if prefix.is_empty() || prefix.starts_with("std") {
        labels.extend(collect_std_import_candidates(base_dir, prefix));
    }
    labels.sort();
    labels.dedup();

    let replace_range = Range {
        start: Position {
            line,
            character: content_start_col,
        },
        end: Position {
            line,
            character: cursor_col,
        },
    };

    labels
        .into_iter()
        .map(|label| CompletionItem {
            kind: Some(if label.ends_with('/') {
                CompletionItemKind::FOLDER
            } else {
                CompletionItemKind::MODULE
            }),
            filter_text: Some(label.clone()),
            text_edit: Some(CompletionTextEdit::Edit(TextEdit {
                range: replace_range,
                new_text: label.clone(),
            })),
            label,
            ..Default::default()
        })
        .collect()
}

pub(crate) fn completion_items_for_trigger(trigger: Option<&str>) -> Vec<CompletionItem> {
    let mut items = Vec::new();

    match trigger {
        Some("@") => {
            for dir in DIRECTIVES {
                items.push(CompletionItem {
                    label: dir.name.to_string(),
                    kind: Some(CompletionItemKind::KEYWORD),
                    detail: Some(dir.signature.to_string()),
                    documentation: Some(Documentation::MarkupContent(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: dir.description.to_string(),
                    })),
                    insert_text: Some(dir.name.to_string()),
                    ..Default::default()
                });
            }
        }
        Some(".") => {
            for (field, desc) in MEMBER_FIELDS {
                items.push(CompletionItem {
                    label: field.to_string(),
                    kind: Some(CompletionItemKind::FIELD),
                    detail: Some(desc.to_string()),
                    ..Default::default()
                });
            }
        }
        _ => {
            for dir in DIRECTIVES {
                items.push(CompletionItem {
                    label: dir.name.to_string(),
                    kind: Some(CompletionItemKind::KEYWORD),
                    detail: Some(dir.signature.to_string()),
                    documentation: Some(Documentation::MarkupContent(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: dir.description.to_string(),
                    })),
                    insert_text: Some(dir.name.to_string()),
                    ..Default::default()
                });
            }
            for (name, desc) in KEYWORDS {
                items.push(CompletionItem {
                    label: name.to_string(),
                    kind: Some(CompletionItemKind::KEYWORD),
                    detail: Some(desc.to_string()),
                    ..Default::default()
                });
            }
            for (name, sig, desc) in BUILTIN_FUNCTIONS {
                items.push(CompletionItem {
                    label: name.to_string(),
                    kind: Some(CompletionItemKind::FUNCTION),
                    detail: Some(sig.to_string()),
                    documentation: Some(Documentation::MarkupContent(MarkupContent {
                        kind: MarkupKind::Markdown,
                        value: desc.to_string(),
                    })),
                    ..Default::default()
                });
            }
        }
    }

    items
}
