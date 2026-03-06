use crate::formatter::Formatter;
use crate::lsp::catalog::{BUILTIN_FUNCTION_DOCS, DIRECTIVES, KEYWORDS};
use crate::lsp::completion::{
    completion_items_for_trigger, extract_import_prefix, extract_string_literal_prefix,
    file_completion_items, get_signature_context, import_completion_items,
};
use crate::lsp::diagnostics::{full_document_range, validate_program};
use crate::lsp::symbols::{document_symbols, find_symbol_at_position};
use crate::lsp::text::get_word_at_position;
use crate::parser::parse;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};

#[derive(Debug)]
pub struct Backend {
    client: Client,
    documents: RwLock<HashMap<String, String>>,
}

impl Backend {
    async fn publish_diagnostics_for(&self, uri: Url, text: &str) {
        let parse_result = parse(text);

        let mut diagnostics = Vec::new();
        match parse_result {
            Ok(program) => diagnostics.extend(validate_program(&program)),
            Err(errors) => {
                for err in errors {
                    let pos = Position {
                        line: err.line.saturating_sub(1) as u32,
                        character: err.col.saturating_sub(1) as u32,
                    };
                    diagnostics.push(Diagnostic {
                        range: Range {
                            start: pos,
                            end: pos,
                        },
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
                    trigger_characters: Some(vec![
                        "@".to_string(),
                        ".".to_string(),
                        "/".to_string(),
                        "\"".to_string(),
                    ]),
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
        let uri = params.text_document.uri;
        let text = params.text_document.text;

        self.documents
            .write()
            .await
            .insert(uri.to_string(), text.clone());

        self.publish_diagnostics_for(uri, &text).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri;
        if let Some(change) = params.content_changes.first() {
            let text = &change.text;
            self.documents
                .write()
                .await
                .insert(uri.to_string(), text.clone());
            self.publish_diagnostics_for(uri, text).await;
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
            word = word.strip_prefix('@').unwrap_or(&word).to_string();

            for dir in DIRECTIVES {
                if word == dir.name {
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!(
                                "**@{}**\n\n```loom\n{}\n```\n\n{}",
                                dir.name, dir.signature, dir.description
                            ),
                        }),
                        range: None,
                    }));
                }
            }

            for func in BUILTIN_FUNCTION_DOCS {
                if word == func.name {
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!(
                                "**{}**\n\n```loom\n{}\n```\n\n{}",
                                func.name, func.signature, func.description
                            ),
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
            let base_dir = doc_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new(""));

            let parsed = parse(text).ok();
            let word = parsed
                .as_ref()
                .and_then(|program| {
                    find_symbol_at_position(program, position).map(|s| s.name.to_string())
                })
                .unwrap_or_else(|| get_word_at_position(text, position.line, position.character));

            if word.contains('.') {
                let parts: Vec<&str> = word.split('.').collect();
                if parts.len() == 2 {
                    let module_name = parts[0];
                    let mut resolved_module = module_name.to_string();

                    if let Some(program) = parsed {
                        for stmt in program.statements {
                            if let crate::ast::Statement::Import(imp) = stmt {
                                if imp.alias.as_deref() == Some(module_name) {
                                    resolved_module = imp.path;
                                    break;
                                } else if imp.alias.is_none() {
                                    let stem = imp.path.rsplit('/').next().unwrap_or(&imp.path);
                                    let stem = stem.split('.').next_back().unwrap_or(&imp.path);
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

                    let mut possible_paths = vec![
                        base_dir.join(&target_file),
                        base_dir.join(&module_path).join("mod.loom"),
                    ];

                    for ancestor in base_dir.ancestors() {
                        possible_paths.push(ancestor.join("std").join(&target_file));
                        possible_paths
                            .push(ancestor.join("std").join(&module_path).join("mod.loom"));
                        possible_paths.push(ancestor.join(&target_file));
                        possible_paths.push(ancestor.join(&module_path).join("mod.loom"));
                    }

                    for path in possible_paths {
                        if path.is_file()
                            && let Ok(target_uri) = Url::from_file_path(&path)
                        {
                            return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                                uri: target_uri,
                                range: Range::default(),
                            })));
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
        let doc_text = self.documents.read().await.get(&uri.to_string()).cloned();

        if let Some(text) = &doc_text {
            if let Some((prefix, start_col)) =
                extract_import_prefix(text, position.line, position.character)
            {
                let uri = uri.clone();
                let line = position.line;
                let cursor_col = position.character;
                let labels = tokio::task::spawn_blocking(move || {
                    import_completion_items(&uri, &prefix, line, start_col, cursor_col)
                })
                .await
                .unwrap_or_default();
                return Ok(Some(CompletionResponse::Array(labels)));
            }
            if let Some((prefix, start_col)) =
                extract_string_literal_prefix(text, position.line, position.character)
            {
                let uri = uri.clone();
                let line = position.line;
                let cursor_col = position.character;
                let labels = tokio::task::spawn_blocking(move || {
                    file_completion_items(&uri, &prefix, line, start_col, cursor_col)
                })
                .await
                .unwrap_or_default();
                return Ok(Some(CompletionResponse::Array(labels)));
            }
        }

        let trigger = params
            .context
            .as_ref()
            .and_then(|ctx| ctx.trigger_character.as_deref());

        Ok(Some(CompletionResponse::Array(
            completion_items_for_trigger(trigger),
        )))
    }

    async fn signature_help(&self, params: SignatureHelpParams) -> Result<Option<SignatureHelp>> {
        let position = params.text_document_position_params.position;
        let uri = params.text_document_position_params.text_document.uri;

        if let Some(text) = self.documents.read().await.get(&uri.to_string())
            && let Some((name, param_index)) =
                get_signature_context(text, position.line, position.character)
        {
            let mut stripped_name = name.clone();
            stripped_name = stripped_name
                .strip_prefix('@')
                .unwrap_or(&stripped_name)
                .to_string();

            for dir in DIRECTIVES {
                if dir.name == stripped_name {
                    let sig = SignatureInformation {
                        label: dir.signature.to_string(),
                        documentation: Some(Documentation::MarkupContent(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: dir.description.to_string(),
                        })),
                        parameters: None,
                        active_parameter: Some(param_index),
                    };
                    return Ok(Some(SignatureHelp {
                        signatures: vec![sig],
                        active_signature: Some(0),
                        active_parameter: Some(param_index),
                    }));
                }
            }

            for func in BUILTIN_FUNCTION_DOCS {
                if func.name == stripped_name {
                    let sig_info = SignatureInformation {
                        label: func.signature.to_string(),
                        documentation: Some(Documentation::MarkupContent(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: func.description.to_string(),
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

        let program = match parse(&text) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        Ok(Some(DocumentSymbolResponse::Nested(document_symbols(
            &program,
        ))))
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
