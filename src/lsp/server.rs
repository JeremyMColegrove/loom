use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use tokio::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};
use crate::ast::{Expression, FlowOrBranch, Program, Source, Span, Statement};
use crate::formatter::Formatter;
use crate::parser::parse;

#[derive(Debug)]
pub struct Backend {
    client: Client,
    documents: RwLock<HashMap<String, String>>,
}

/// Built-in directive documentation for hover and completion
struct DirectiveInfo {
    name: &'static str,
    signature: &'static str,
    description: &'static str,
}

const DIRECTIVES: &[DirectiveInfo] = &[
    DirectiveInfo { name: "watch",     signature: "@watch(path)",              description: "Watches a file or directory for changes. Returns an event record with `file`, `path`, and `type` fields." },
    DirectiveInfo { name: "atomic",    signature: "@atomic",                   description: "Wraps subsequent operations in a transaction. If any step fails, all changes are rolled back." },
    DirectiveInfo { name: "chunk",     signature: "@chunk(size, source)",      description: "Splits the input into chunks of the given size (e.g. `\"5mb\"`). Returns chunk records." },
    DirectiveInfo { name: "csv.parse", signature: "@csv.parse(data)",          description: "Parses CSV data into records. Returns a record with `source`, `valid`, and `rows` fields." },
    DirectiveInfo { name: "log",       signature: "@log",                      description: "Logs the current pipe value to stdout. Passes the value through unchanged." },
    DirectiveInfo { name: "read",      signature: "@read(path)",               description: "Reads the contents of a file and returns it as a string." },
    DirectiveInfo { name: "write",     signature: "@write(path)",              description: "Writes the current pipe value to a file at the given path." },
    DirectiveInfo { name: "import",    signature: "@import \"path\" [as alias]", description: "Imports functions and definitions from another Loom file." },
];

const KEYWORDS: &[(&str, &str)] = &[
    ("on_fail", "Error handler block. Catches errors from the preceding pipe flow."),
    ("as",      "Binds the result of a directive or on_fail to a named variable."),
    ("true",    "Boolean literal true."),
    ("false",   "Boolean literal false."),
];

const BUILTIN_FUNCTIONS: &[(&str, &str, &str)] = &[
    ("filter", "filter(predicate)", "Filters items using a lambda predicate. E.g. `filter(r >> r.valid)`"),
    ("map",    "map(transform)",    "Transforms each item using a lambda. E.g. `map(r >> r.name)`"),
    ("print",  "print(value)",      "Prints a value to stdout."),
    ("concat", "concat(a, b, ...)", "Concatenates values into a single string."),
    ("exists", "exists(path)",      "Returns true if the file at path exists."),
];

fn get_word_at_position(text: &str, line: u32, character: u32) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let line_idx = line as usize;
    if line_idx >= lines.len() {
        return String::new();
    }
    let line_text = lines[line_idx];
    let col = character as usize;
    
    // Walk backwards to find start of word (including @ and .)
    let chars: Vec<char> = line_text.chars().collect();
    let mut start = col;
    while start > 0 {
        let c = chars[start - 1];
        if c.is_alphanumeric() || c == '_' || c == '@' || c == '.' {
            start -= 1;
        } else {
            break;
        }
    }
    let mut end = col;
    while end < chars.len() {
        let c = chars[end];
        if c.is_alphanumeric() || c == '_' || c == '.' {
            end += 1;
        } else {
            break;
        }
    }
    chars[start..end].iter().collect()
}

#[allow(dead_code)]
fn get_context_at_position(text: &str, line: u32, character: u32) -> CompletionContext {
    let lines: Vec<&str> = text.lines().collect();
    let line_idx = line as usize;
    if line_idx >= lines.len() {
        return CompletionContext::General;
    }
    let line_text = lines[line_idx];
    let col = character as usize;
    let before_cursor = &line_text[..col.min(line_text.len())];
    let trimmed = before_cursor.trim_start();

    // Check if we're inside a string literal
    let quote_count = before_cursor.chars().filter(|c| *c == '"').count();
    if quote_count % 2 == 1 {
        return CompletionContext::StringLiteral;
    }

    // Check if directly after @
    if trimmed.ends_with('@') {
        return CompletionContext::Directive;
    }

    // Check if typing a directive name after @
    if let Some(at_pos) = before_cursor.rfind('@') {
        let after_at = &before_cursor[at_pos + 1..];
        if after_at.chars().all(|c| c.is_alphanumeric() || c == '.' || c == '_') {
            return CompletionContext::Directive;
        }
    }

    // Check if after a dot (member access)
    if trimmed.ends_with('.') || (trimmed.contains('.') && !trimmed.ends_with(' ')) {
        return CompletionContext::MemberAccess;
    }

    // Check if after >> (pipe destination)
    if trimmed.contains(">>") {
        let last_pipe = trimmed.rfind(">>").unwrap();
        let after_pipe = trimmed[last_pipe + 2..].trim();
        if after_pipe.is_empty() || after_pipe.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return CompletionContext::PipeDestination;
        }
    }

    // Check if at start of line (source position)
    if trimmed.is_empty() || trimmed.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '@') {
        return CompletionContext::Source;
    }

    CompletionContext::General
}

#[allow(dead_code)]
enum CompletionContext {
    Directive,
    Source,
    PipeDestination,
    MemberAccess,
    StringLiteral,
    General,
}

#[derive(Clone, Copy, Debug)]
struct SymbolAtPos<'a> {
    name: &'a str,
}

fn lsp_range_from_span(span: Span) -> Range {
    Range {
        start: Position {
            line: span.start.line.saturating_sub(1) as u32,
            character: span.start.col.saturating_sub(1) as u32,
        },
        end: Position {
            line: span.end.line.saturating_sub(1) as u32,
            character: span.end.col.saturating_sub(1) as u32,
        },
    }
}

fn find_expression_symbol_at_pos<'a>(expr: &'a Expression, pos: Position) -> Option<SymbolAtPos<'a>> {
    match expr {
        Expression::FunctionCall(call) => {
            if call.span.contains_zero_based(pos.line, pos.character) {
                Some(SymbolAtPos {
                    name: &call.name,
                })
            } else {
                None
            }
        }
        Expression::Lambda(lambda) => find_expression_symbol_at_pos(&lambda.body, pos),
        Expression::BinaryOp(left, _, right) => {
            find_expression_symbol_at_pos(left, pos).or_else(|| find_expression_symbol_at_pos(right, pos))
        }
        Expression::UnaryOp(_, inner) => find_expression_symbol_at_pos(inner, pos),
        _ => None,
    }
}

fn find_flow_or_branch_symbol_at_pos<'a>(body: &'a FlowOrBranch, pos: Position) -> Option<SymbolAtPos<'a>> {
    match body {
        FlowOrBranch::Flow(flow) => find_pipe_flow_symbol_at_pos(flow, pos),
        FlowOrBranch::Branch(branch) => find_branch_symbol_at_pos(branch, pos),
    }
}

fn find_branch_symbol_at_pos<'a>(branch: &'a crate::ast::Branch, pos: Position) -> Option<SymbolAtPos<'a>> {
    if !branch.span.contains_zero_based(pos.line, pos.character) {
        return None;
    }
    for item in &branch.items {
        if let crate::ast::BranchItem::Flow(flow) = item {
            if let Some(found) = find_pipe_flow_symbol_at_pos(flow, pos) {
                return Some(found);
            }
        }
    }
    None
}

fn find_pipe_flow_symbol_at_pos<'a>(flow: &'a crate::ast::PipeFlow, pos: Position) -> Option<SymbolAtPos<'a>> {
    if !flow.span.contains_zero_based(pos.line, pos.character) {
        return None;
    }

    if let Some(found) = find_source_symbol_at_pos(&flow.source, pos) {
        return Some(found);
    }

    for (_, dest) in &flow.operations {
        if let Some(found) = find_destination_symbol_at_pos(dest, pos) {
            return Some(found);
        }
    }

    if let Some(on_fail) = &flow.on_fail {
        if on_fail.span.contains_zero_based(pos.line, pos.character) {
            if let Some(found) = find_flow_or_branch_symbol_at_pos(&on_fail.handler, pos) {
                return Some(found);
            }
        }
    }

    None
}

fn find_source_symbol_at_pos<'a>(source: &'a Source, pos: Position) -> Option<SymbolAtPos<'a>> {
    match source {
        Source::Directive(dir) if dir.span.contains_zero_based(pos.line, pos.character) => Some(SymbolAtPos {
            name: &dir.name,
        }),
        Source::FunctionCall(call) if call.span.contains_zero_based(pos.line, pos.character) => {
            Some(SymbolAtPos {
                name: &call.name,
            })
        }
        Source::Expression(expr) => find_expression_symbol_at_pos(expr, pos),
        _ => None,
    }
}

fn find_destination_symbol_at_pos<'a>(dest: &'a crate::ast::Destination, pos: Position) -> Option<SymbolAtPos<'a>> {
    match dest {
        crate::ast::Destination::Directive(dir)
            if dir.span.contains_zero_based(pos.line, pos.character) =>
        {
            Some(SymbolAtPos {
                name: &dir.name,
            })
        }
        crate::ast::Destination::FunctionCall(call)
            if call.span.contains_zero_based(pos.line, pos.character) =>
        {
            Some(SymbolAtPos {
                name: &call.name,
            })
        }
        crate::ast::Destination::Branch(branch) => find_branch_symbol_at_pos(branch, pos),
        crate::ast::Destination::Expression(expr) => find_expression_symbol_at_pos(expr, pos),
        _ => None,
    }
}

fn find_symbol_at_position<'a>(program: &'a Program, pos: Position) -> Option<SymbolAtPos<'a>> {
    for stmt in &program.statements {
        match stmt {
            Statement::Import(imp) if imp.span.contains_zero_based(pos.line, pos.character) => {
                return Some(SymbolAtPos {
                    name: &imp.path,
                });
            }
            Statement::Function(func) if func.span.contains_zero_based(pos.line, pos.character) => {
                if let Some(found) = find_flow_or_branch_symbol_at_pos(&func.body, pos) {
                    return Some(found);
                }
                return Some(SymbolAtPos {
                    name: &func.name,
                });
            }
            Statement::Pipe(flow) if flow.span.contains_zero_based(pos.line, pos.character) => {
                if let Some(found) = find_pipe_flow_symbol_at_pos(flow, pos) {
                    return Some(found);
                }
            }
            _ => {}
        }
    }
    None
}

/// Returns `(prefix, content_start_column)` where `content_start_column` is the
/// 0-based column of the first character after the opening `"` of the import path.
fn extract_import_prefix(text: &str, line: u32, character: u32) -> Option<(String, u32)> {
    let lines: Vec<&str> = text.lines().collect();
    let line_text = lines.get(line as usize)?;
    let col = (character as usize).min(line_text.len());
    let before_cursor = &line_text[..col];

    let import_pos = before_cursor.rfind("@import")?;
    let after_import = &before_cursor[import_pos + "@import".len()..];
    let quote_pos = after_import.find('"')?;
    let prefix_start = import_pos + "@import".len() + quote_pos + 1;
    let prefix = &before_cursor[prefix_start..];
    if prefix.contains('"') {
        return None;
    }
    Some((prefix.to_string(), prefix_start as u32))
}

fn get_signature_context(text: &str, line: u32, character: u32) -> Option<(String, u32)> {
    let lines: Vec<&str> = text.lines().collect();
    let line_idx = line as usize;
    if line_idx >= lines.len() {
        return None;
    }

    let mut depth = 0;
    let mut param_index = 0;
    let mut current_idx = character as usize;
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
                        // Found the start of the current function call
                        let before_paren = &line_text[..i].trim_end();
                        
                        // Extract the function/directive name before the parenthesis
                        let mut name_start = before_paren.len();
                        let before_chars: Vec<char> = before_paren.chars().collect();
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
        let stem = entry_path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
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
fn extract_string_literal_prefix(text: &str, line: u32, character: u32) -> Option<(String, u32)> {
    let lines: Vec<&str> = text.lines().collect();
    let line_text = lines.get(line as usize)?;
    let col = (character as usize).min(line_text.len());
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
        let last_quote = *quote_indices.last().unwrap();
        let content_start = (last_quote + 1) as u32;
        Some((before_cursor[last_quote + 1..].to_string(), content_start))
    } else {
        None
    }
}

fn file_completion_items(
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
        start: Position { line, character: content_start_col },
        end: Position { line, character: cursor_col },
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

fn import_completion_items(
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
        start: Position { line, character: content_start_col },
        end: Position { line, character: cursor_col },
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

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                definition_provider: Some(OneOf::Left(true)),
                completion_provider: Some(CompletionOptions {
                    resolve_provider: Some(false),
                    trigger_characters: Some(vec!["@".to_string(), ".".to_string(), "/".to_string(), "\"".to_string()]),
                    ..Default::default()
                }),
                signature_help_provider: Some(SignatureHelpOptions {
                    trigger_characters: Some(vec!["(".to_string(), ",".to_string()]),
                    retrigger_characters: None,
                    work_done_progress_options: WorkDoneProgressOptions::default(),
                }),
                document_symbol_provider: Some(OneOf::Left(true)),
                document_formatting_provider: Some(OneOf::Left(true)),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "Loom LSP initialized!")
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        self.documents
            .write()
            .await
            .insert(params.text_document.uri.to_string(), params.text_document.text.clone());
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        if let Some(change) = params.content_changes.first() {
            let text = &change.text;
            self.documents
                .write()
                .await
                .insert(uri.to_string(), text.clone());
            let parse_result = parse(text);
            
            let mut diagnostics = Vec::new();
            match parse_result {
                Ok(program) => {
                    diagnostics.extend(validate_program(&program, text));
                }
                Err(errors) => {
                    for err in errors {
                        let pos = Position {
                            line: err.line.saturating_sub(1) as u32,
                            character: err.col.saturating_sub(1) as u32,
                        };
                        diagnostics.push(Diagnostic {
                            range: Range { start: pos, end: pos },
                            severity: Some(DiagnosticSeverity::ERROR),
                            code: None,
                            code_description: None,
                            source: Some("loom".to_string()),
                            message: err.message,
                            related_information: None,
                            tags: None,
                            data: None,
                        });
                    }
                }
            }

            self.client
                .publish_diagnostics(uri, diagnostics, None)
                .await;
        }
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        self.documents
            .write()
            .await
            .remove(&params.text_document.uri.to_string());
    }

    async fn formatting(&self, params: DocumentFormattingParams) -> Result<Option<Vec<TextEdit>>> {
        let uri = params.text_document.uri;
        let text = match self.documents.read().await.get(&uri.to_string()).cloned() {
            Some(t) => t,
            None => return Ok(None),
        };

        let program = match parse(&text) {
            Ok(program) => program,
            Err(_) => return Ok(None),
        };
        let formatted = Formatter::format(&program);
        let edit = TextEdit {
            range: full_document_range(&text),
            new_text: formatted,
        };
        Ok(Some(vec![edit]))
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let position = params.text_document_position_params.position;
        let uri = params.text_document_position_params.text_document.uri;

        if let Some(text) = self.documents.read().await.get(&uri.to_string()) {
            let mut word = if let Ok(program) = parse(text) {
                if let Some(symbol) = find_symbol_at_position(&program, position) {
                    symbol.name.to_string()
                } else {
                    get_word_at_position(text, position.line, position.character)
                }
            } else {
                get_word_at_position(text, position.line, position.character)
            };
            if word.starts_with('@') {
                word = word[1..].to_string();
            }

            for dir in DIRECTIVES {
                if word == dir.name {
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!("**@{}**\n\n```loom\n{}\n```\n\n{}", dir.name, dir.signature, dir.description),
                        }),
                        range: None,
                    }));
                }
            }

            for (name, sig, desc) in BUILTIN_FUNCTIONS {
                if word == *name {
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!("**{}**\n\n```loom\n{}\n```\n\n{}", name, sig, desc),
                        }),
                        range: None,
                    }));
                }
            }

            for (name, desc) in KEYWORDS {
                if word == *name {
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!("**{}** (keyword)\n\n{}", name, desc),
                        }),
                        range: None,
                    }));
                }
            }
        }

        Ok(None)
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let position = params.text_document_position_params.position;
        let uri = params.text_document_position_params.text_document.uri;

        if let Some(text) = self.documents.read().await.get(&uri.to_string()) {
            let doc_path = uri.to_file_path().unwrap_or_default();
            let base_dir = doc_path.parent().unwrap_or_else(|| std::path::Path::new(""));

            let parsed = parse(text).ok();
            let word = parsed
                .as_ref()
                .and_then(|program| find_symbol_at_position(program, position).map(|s| s.name.to_string()))
                .unwrap_or_else(|| get_word_at_position(text, position.line, position.character));
            
            // Check if word is of the form `module.function`
            if word.contains('.') {
                let parts: Vec<&str> = word.split('.').collect();
                if parts.len() == 2 {
                    let module_name = parts[0];
                    let mut resolved_module = module_name.to_string();

                    // Resolve aliases via AST
                    if let Some(program) = parsed {
                        for stmt in program.statements {
                            if let crate::ast::Statement::Import(imp) = stmt {
                                if imp.alias.as_deref() == Some(module_name) {
                                    resolved_module = imp.path;
                                    break;
                                } else if imp.alias.is_none() {
                                    let stem = imp.path.rsplit('/').next().unwrap_or(&imp.path);
                                    let stem = stem.split('.').last().unwrap_or(&imp.path);
                                    if stem == module_name {
                                        resolved_module = imp.path;
                                    break;
                                }
                            }
                        }
                    }
                    }
                    
                    let module_path = resolved_module.replace('.', "/");
                    let target_file = format!("{}.loom", module_path);
                    
                    // Search for the file locally or in std
                    let mut possible_paths = vec![
                        base_dir.join(&target_file),
                        base_dir.join(&module_path).join("mod.loom")
                    ];

                    for ancestor in base_dir.ancestors() {
                        possible_paths.push(ancestor.join("std").join(&target_file));
                        possible_paths.push(ancestor.join("std").join(&module_path).join("mod.loom"));
                        possible_paths.push(ancestor.join(&target_file));
                        possible_paths.push(ancestor.join(&module_path).join("mod.loom"));
                    }

                    for path in possible_paths {
                        if path.is_file() {
                            if let Ok(target_uri) = Url::from_file_path(&path) {
                                return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                                    uri: target_uri,
                                    range: Range::default(),
                                })));
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let position = params.text_document_position.position;
        let uri = params.text_document_position.text_document.uri;
        let mut items: Vec<CompletionItem> = Vec::new();
        let doc_text = self.documents.read().await.get(&uri.to_string()).cloned();

        if let Some(text) = &doc_text {
            if let Some((prefix, start_col)) = extract_import_prefix(text, position.line, position.character) {
                return Ok(Some(CompletionResponse::Array(import_completion_items(
                    &uri, &prefix, position.line, start_col, position.character,
                ))));
            }
            if let Some((prefix, start_col)) = extract_string_literal_prefix(text, position.line, position.character) {
                return Ok(Some(CompletionResponse::Array(file_completion_items(
                    &uri, &prefix, position.line, start_col, position.character,
                ))));
            }
        }

        // We don't have document text in this simple server implementation,
        // so provide completions based on position context hints from the trigger character.
        let trigger = params.context
            .as_ref()
            .and_then(|ctx| ctx.trigger_character.as_deref());

        match trigger {
            Some("@") => {
                // Directive completions
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
                // Member access completions — common record fields
                let member_fields = vec![
                    ("file", "The file path from a watch event or chunk"),
                    ("path", "The full path of the resource"),
                    ("type", "The type of event (created, modified, deleted)"),
                    ("valid", "Whether the record passed validation"),
                    ("data", "The data content"),
                    ("size", "The size of the chunk or file"),
                    ("source", "The source of the data"),
                    ("rows", "Parsed rows from CSV data"),
                    ("length", "Length of a string value"),
                    ("name", "Name of the resource"),
                ];
                for (field, desc) in member_fields {
                    items.push(CompletionItem {
                        label: field.to_string(),
                        kind: Some(CompletionItemKind::FIELD),
                        detail: Some(desc.to_string()),
                        ..Default::default()
                    });
                }
            }
            _ => {
                // General completions: directives + keywords + functions
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

        Ok(Some(CompletionResponse::Array(items)))
    }

    async fn signature_help(&self, params: SignatureHelpParams) -> Result<Option<SignatureHelp>> {
        let position = params.text_document_position_params.position;
        let uri = params.text_document_position_params.text_document.uri;

        if let Some(text) = self.documents.read().await.get(&uri.to_string()) {
            if let Some((name, param_index)) = get_signature_context(text, position.line, position.character) {
                let mut stripped_name = name.clone();
                if stripped_name.starts_with('@') {
                    stripped_name = stripped_name[1..].to_string();
                }

                // Search directives
                for dir in DIRECTIVES {
                    if dir.name == stripped_name {
                        let sig = SignatureInformation {
                            label: dir.signature.to_string(),
                            documentation: Some(Documentation::MarkupContent(MarkupContent {
                                kind: MarkupKind::Markdown,
                                value: dir.description.to_string(),
                            })),
                            parameters: None, // Could parse arguments out of `dir.signature` for specific parameter highlighting
                            active_parameter: Some(param_index),
                        };
                        return Ok(Some(SignatureHelp {
                            signatures: vec![sig],
                            active_signature: Some(0),
                            active_parameter: Some(param_index),
                        }));
                    }
                }

                // Search builtin functions
                for (func_name, sig, desc) in BUILTIN_FUNCTIONS {
                    if *func_name == stripped_name {
                        let sig_info = SignatureInformation {
                            label: sig.to_string(),
                            documentation: Some(Documentation::MarkupContent(MarkupContent {
                                kind: MarkupKind::Markdown,
                                value: desc.to_string(),
                            })),
                            parameters: None,
                            active_parameter: Some(param_index),
                        };
                        return Ok(Some(SignatureHelp {
                            signatures: vec![sig_info],
                            active_signature: Some(0),
                            active_parameter: Some(param_index),
                        }));
                    }
                }
            }
        }

        Ok(None)
    }

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        let uri = params.text_document.uri;
        let text = match self.documents.read().await.get(&uri.to_string()).cloned() {
            Some(t) => t,
            None => return Ok(None),
        };

        let program = match crate::parser::parse(&text) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        let mut symbols: Vec<DocumentSymbol> = Vec::new();

        for stmt in &program.statements {
            match stmt {
                crate::ast::Statement::Comment(_) => {}
                crate::ast::Statement::Import(imp) => {
                    let label = if let Some(alias) = &imp.alias {
                        format!("@import \"{}\" as {}", imp.path, alias)
                    } else {
                        format!("@import \"{}\"", imp.path)
                    };
                    let range = lsp_range_from_span(imp.span);
                    #[allow(deprecated)]
                    symbols.push(DocumentSymbol {
                        name: label,
                        detail: Some(imp.path.clone()),
                        kind: SymbolKind::MODULE,
                        tags: None,
                        deprecated: None,
                        range,
                        selection_range: range,
                        children: None,
                    });
                }
                crate::ast::Statement::Function(func) => {
                    let params_str = func.parameters.join(", ");
                    let label = format!("{}({})", func.name, params_str);
                    let range = lsp_range_from_span(func.span);
                    #[allow(deprecated)]
                    symbols.push(DocumentSymbol {
                        name: label,
                        detail: Some("function".to_string()),
                        kind: SymbolKind::FUNCTION,
                        tags: None,
                        deprecated: None,
                        range,
                        selection_range: range,
                        children: None,
                    });
                }
                crate::ast::Statement::Pipe(flow) => {
                    let label = match &flow.source {
                        crate::ast::Source::Directive(dir) => {
                            if let Some(alias) = &dir.alias {
                                format!("@{} as {}", dir.name, alias)
                            } else {
                                format!("@{}", dir.name)
                            }
                        }
                        crate::ast::Source::FunctionCall(call) => call.name.clone(),
                        crate::ast::Source::Expression(expr) => {
                            format!("{:?}", expr).chars().take(40).collect()
                        }
                    };
                    let range = lsp_range_from_span(flow.span);
                    #[allow(deprecated)]
                    symbols.push(DocumentSymbol {
                        name: label,
                        detail: Some("pipeline".to_string()),
                        kind: SymbolKind::EVENT,
                        tags: None,
                        deprecated: None,
                        range,
                        selection_range: range,
                        children: None,
                    });
                }
            }
        }

        Ok(Some(DocumentSymbolResponse::Nested(symbols)))
    }
}

fn full_document_range(text: &str) -> Range {
    let mut line: u32 = 0;
    let mut character: u32 = 0;

    for ch in text.chars() {
        if ch == '\n' {
            line += 1;
            character = 0;
        } else {
            character += 1;
        }
    }

    Range {
        start: Position {
            line: 0,
            character: 0,
        },
        end: Position { line, character },
    }
}

fn validate_program(program: &crate::ast::Program, text: &str) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let mut defined_funcs = std::collections::HashSet::new();

    for stmt in &program.statements {
        if let crate::ast::Statement::Function(func) = stmt {
            defined_funcs.insert(func.name.clone());
        }
    }

    for stmt in &program.statements {
        if let crate::ast::Statement::Pipe(flow) = stmt {
            validate_pipe_flow(flow, text, &defined_funcs, &mut diagnostics);
        } else if let crate::ast::Statement::Function(func) = stmt {
            match &func.body {
                crate::ast::FlowOrBranch::Flow(flow) => validate_pipe_flow(flow, text, &defined_funcs, &mut diagnostics),
                crate::ast::FlowOrBranch::Branch(branch) => {
                    for item in &branch.items {
                        if let crate::ast::BranchItem::Flow(f) = item {
                            validate_pipe_flow(f, text, &defined_funcs, &mut diagnostics);
                        }
                    }
                }
            }
        }
    }

    diagnostics
}

fn validate_pipe_flow(flow: &crate::ast::PipeFlow, text: &str, defined_funcs: &std::collections::HashSet<String>, diagnostics: &mut Vec<Diagnostic>) {
    match &flow.source {
        crate::ast::Source::Directive(dir) => {
            if !DIRECTIVES.iter().any(|d| d.name == dir.name) {
                let token = format!("@{}", dir.name);
                diagnostics.push(Diagnostic {
                    range: lsp_range_from_span(dir.span),
                    severity: Some(DiagnosticSeverity::ERROR),
                    message: format!("Unknown directive: {}", token),
                    source: Some("loom".to_string()),
                    ..Default::default()
                });
            }
        }
        _ => {}
    }

    for (_, dest) in &flow.operations {
        match dest {
            crate::ast::Destination::Directive(dir) => {
                if !DIRECTIVES.iter().any(|d| d.name == dir.name) {
                    let token = format!("@{}", dir.name);
                    diagnostics.push(Diagnostic {
                        range: lsp_range_from_span(dir.span),
                        severity: Some(DiagnosticSeverity::ERROR),
                        message: format!("Unknown directive: {}", token),
                        source: Some("loom".to_string()),
                        ..Default::default()
                    });
                }
            }
            crate::ast::Destination::FunctionCall(call) => {
                if !call.name.contains('.') && !defined_funcs.contains(&call.name) && !BUILTIN_FUNCTIONS.iter().any(|(name, _, _)| *name == &call.name) {
                    diagnostics.push(Diagnostic {
                        range: lsp_range_from_span(call.span),
                        severity: Some(DiagnosticSeverity::WARNING),
                        message: format!("Unknown function: {}", call.name),
                        source: Some("loom".to_string()),
                        ..Default::default()
                    });
                }
            }
            crate::ast::Destination::Branch(branch) => {
                for item in &branch.items {
                    if let crate::ast::BranchItem::Flow(f) = item {
                        validate_pipe_flow(f, text, defined_funcs, diagnostics);
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(fail) = &flow.on_fail {
        match fail.handler.as_ref() {
            crate::ast::FlowOrBranch::Flow(f) => validate_pipe_flow(f, text, defined_funcs, diagnostics),
            crate::ast::FlowOrBranch::Branch(b) => {
                for item in &b.items {
                    if let crate::ast::BranchItem::Flow(f) = item {
                        validate_pipe_flow(f, text, defined_funcs, diagnostics);
                    }
                }
            }
        }
    }
}

pub async fn run_server() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(|client| Backend {
        client,
        documents: RwLock::new(HashMap::new()),
    });
    Server::new(stdin, stdout, socket).serve(service).await;
}
