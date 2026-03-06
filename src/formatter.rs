use crate::ast::*;

pub struct Formatter {
    indent_level: usize,
    max_width: usize,
    output: String,
}

impl Default for Formatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Formatter {
    const DEFAULT_MAX_WIDTH: usize = 100;
    const CONTINUATION_INDENT_LEVELS: usize = 1;

    pub fn new() -> Self {
        Self::new_with_max_width(Self::DEFAULT_MAX_WIDTH)
    }

    pub fn new_with_max_width(max_width: usize) -> Self {
        Self {
            indent_level: 0,
            max_width: max_width.max(1),
            output: String::new(),
        }
    }

    pub fn format(program: &Program) -> String {
        let mut f = Self::new();
        f.format_program(program);
        f.output
    }

    pub fn format_with_max_width(program: &Program, max_width: usize) -> String {
        let mut f = Self::new_with_max_width(max_width);
        f.format_program(program);
        f.output
    }

    fn push(&mut self, s: &str) {
        self.output.push_str(s);
    }

    fn push_indent(&mut self) {
        for _ in 0..self.indent_level {
            self.output.push_str("    ");
        }
    }

    fn push_newline(&mut self) {
        self.output.push('\n');
    }

    fn current_line_len(&self) -> usize {
        self.output
            .rsplit_once('\n')
            .map_or(self.output.len(), |(_, tail)| tail.len())
    }

    fn push_continuation_indent(&mut self) {
        for _ in 0..(self.indent_level + Self::CONTINUATION_INDENT_LEVELS) {
            self.output.push_str("    ");
        }
    }

    fn format_source_to_string(&self, source: &Source) -> String {
        let mut formatter = Self {
            indent_level: self.indent_level,
            max_width: self.max_width,
            output: String::new(),
        };
        formatter.format_source(source);
        formatter.output
    }

    fn format_destination_to_string(&self, dest: &Destination) -> String {
        let mut formatter = Self {
            indent_level: self.indent_level,
            max_width: self.max_width,
            output: String::new(),
        };
        formatter.format_destination(dest);
        formatter.output
    }

    fn format_comments(&mut self, comments: &[String]) {
        for comment in comments {
            self.push(comment.trim());
            self.push_newline();
            self.push_indent();
        }
    }

    pub fn format_program(&mut self, program: &Program) {
        for stmt in program.statements.iter() {
            self.format_statement(stmt);
        }
    }

    fn format_statement(&mut self, stmt: &Statement) {
        match stmt {
            Statement::Import(import_stmt) => {
                self.push_indent();
                self.format_comments(&import_stmt.comments);
                self.push("@import \"");
                self.push(&import_stmt.path);
                self.push("\"");
                if let Some(alias) = &import_stmt.alias {
                    self.push(" as ");
                    self.push(alias);
                }
                self.push_newline();
            }
            Statement::Pipe(flow) => {
                self.push_indent();
                self.format_comments(&flow.comments);
                self.format_pipe_flow(flow, false);
                self.push_newline();
            }
            Statement::Function(func) => {
                self.push_indent();
                self.format_comments(&func.comments);
                self.push(&func.name);
                self.push("(");
                for (i, param) in func.parameters.iter().enumerate() {
                    self.push(param);
                    if i < func.parameters.len() - 1 {
                        self.push(", ");
                    }
                }
                self.push(") => ");

                let is_branch = matches!(&func.body, FlowOrBranch::Branch(_));
                if is_branch {
                    self.push("[");
                    self.push_newline();
                    self.indent_level += 1;
                }

                self.format_flow_or_branch(&func.body, true);

                if is_branch {
                    self.indent_level -= 1;
                    self.push_indent();
                    self.push("]");
                    self.push_newline();
                }
            }
            Statement::Comment(comment) => {
                self.push_indent();
                self.push(comment.trim());
                self.push_newline();
            }
        }
    }

    fn format_flow_or_branch(&mut self, body: &FlowOrBranch, inline: bool) {
        match body {
            FlowOrBranch::Flow(flow) => {
                if !inline {
                    self.push_indent();
                }
                self.format_pipe_flow(flow, inline);
                self.push_newline();
            }
            FlowOrBranch::Branch(branch) => {
                for (i, item) in branch.items.iter().enumerate() {
                    self.push_indent();
                    match item {
                        BranchItem::Flow(flow) => {
                            self.format_comments(&flow.comments);
                            self.format_pipe_flow(flow, true);
                            if branch.items[i + 1..]
                                .iter()
                                .any(|next| matches!(next, BranchItem::Flow(_)))
                            {
                                self.push(",");
                            }
                        }
                        BranchItem::Comment(comment) => {
                            self.push(comment.trim());
                        }
                    }
                    if i < branch.items.len() - 1 {
                        self.push_newline();
                    }
                }
                self.push_newline();
            }
        }
    }

    fn format_pipe_flow(&mut self, flow: &PipeFlow, is_inside_branch: bool) {
        self.push(&self.format_source_to_string(&flow.source));

        for (op, dest) in &flow.operations {
            let op_text = match op {
                PipeOp::Safe => ">>",
                PipeOp::Force => ">>>",
                PipeOp::Move => "->",
            };
            let dest_text = self.format_destination_to_string(dest);
            let segment_first_line_len =
                op_text.len() + 1 + dest_text.lines().next().map_or(0, str::len);
            let should_wrap = !dest_text.contains('\n')
                && self.current_line_len() + 1 + segment_first_line_len > self.max_width;

            if should_wrap {
                self.push_newline();
                self.push_continuation_indent();
                self.push(op_text);
                self.push(" ");
                self.push(&dest_text);
            } else {
                self.push(" ");
                self.push(op_text);
                self.push(" ");
                self.push(&dest_text);
            }
        }

        if let Some(on_fail) = &flow.on_fail {
            self.push(" on_fail ");
            if let Some(alias) = &on_fail.alias {
                self.push("as ");
                self.push(alias);
                self.push(" ");
            }

            let is_branch = matches!(on_fail.handler.as_ref(), FlowOrBranch::Branch(_));
            if is_branch {
                self.push(">> [");
                self.push_newline();
                self.indent_level += 1;
            } else {
                self.push(">> ");
            }

            self.format_flow_or_branch(on_fail.handler.as_ref(), !is_branch);

            if is_branch {
                self.indent_level -= 1;
                self.push_indent();
                self.push("]");
                if !is_inside_branch {
                    self.push_newline();
                }
            }
        }
    }

    fn format_source(&mut self, source: &Source) {
        match source {
            Source::Directive(dir) => self.format_directive_flow(dir),
            Source::FunctionCall(call) => self.format_function_call(call),
            Source::Expression(expr) => self.format_expression(expr),
        }
    }

    fn format_destination(&mut self, dest: &Destination) {
        match dest {
            Destination::Branch(branch) => {
                self.push("[");
                self.push_newline();
                self.indent_level += 1;

                self.format_flow_or_branch(&FlowOrBranch::Branch(branch.clone()), true);

                self.indent_level -= 1;
                self.push_indent();
                self.push("]");
            }
            Destination::Directive(dir) => self.format_directive_flow(dir),
            Destination::FunctionCall(call) => self.format_function_call(call),
            Destination::Expression(expr) => self.format_expression(expr),
        }
    }

    fn format_directive_flow(&mut self, dir: &DirectiveFlow) {
        self.push("@");
        self.push(&dir.name);
        if !dir.arguments.is_empty() || !dir.named_arguments.is_empty() {
            self.push("(");
            let mut first = true;
            for arg in &dir.arguments {
                if !first {
                    self.push(", ");
                }
                self.format_expression(arg);
                first = false;
            }
            for named in &dir.named_arguments {
                if !first {
                    self.push(", ");
                }
                self.push(&named.name);
                self.push(": ");
                self.format_expression(&named.value);
                first = false;
            }
            self.push(")");
        }
        if let Some(alias) = &dir.alias {
            self.push(" as ");
            self.push(alias);
        }
    }

    fn format_function_call(&mut self, call: &FunctionCall) {
        self.push(&call.name);
        if !call.arguments.is_empty() || !call.named_arguments.is_empty() {
            self.push("(");
            let mut first = true;
            for arg in &call.arguments {
                if !first {
                    self.push(", ");
                }
                self.format_expression(arg);
                first = false;
            }
            for named in &call.named_arguments {
                if !first {
                    self.push(", ");
                }
                self.push(&named.name);
                self.push(": ");
                self.format_expression(&named.value);
                first = false;
            }
            self.push(")");
        }
        if let Some(alias) = &call.alias {
            self.push(" as ");
            self.push(alias);
        }
    }

    fn format_secret_call(&mut self, secret: &SecretCall) {
        self.push("@secret(");
        let mut first = true;
        for arg in &secret.arguments {
            if !first {
                self.push(", ");
            }
            self.format_expression(arg);
            first = false;
        }
        for named in &secret.named_arguments {
            if !first {
                self.push(", ");
            }
            self.push(&named.name);
            self.push(": ");
            self.format_expression(&named.value);
            first = false;
        }
        self.push(")");
    }

    fn format_expression(&mut self, expr: &Expression) {
        match expr {
            Expression::Literal(lit) => self.format_literal(lit),
            Expression::Identifier(id) => self.push(id),
            Expression::ObjectLiteral(entries) => {
                self.push("{");
                for (idx, (key, value)) in entries.iter().enumerate() {
                    match key {
                        ObjectKey::Identifier(name) => self.push(name),
                        ObjectKey::Path(path) => {
                            self.push("\"");
                            self.push(path);
                            self.push("\"");
                        }
                        ObjectKey::String(text) => {
                            self.push("\\\"");
                            self.push(&escape_string_literal_contents(text));
                            self.push("\"");
                        }
                    }
                    self.push(": ");
                    self.format_expression(value);
                    if idx + 1 < entries.len() {
                        self.push(", ");
                    }
                }
                self.push("}");
            }
            Expression::BinaryOp(left, op, right) => {
                self.format_expression(left);
                self.push(" ");
                self.push(op);
                self.push(" ");
                self.format_expression(right);
            }
            Expression::UnaryOp(op, inner) => {
                self.push(op);
                self.format_expression(inner);
            }
            Expression::Lambda(lambda) => {
                self.push(&lambda.param);
                self.push(" >> ");
                self.format_expression(&lambda.body);
            }
            Expression::FunctionCall(call) => self.format_function_call(call),
            Expression::SecretCall(secret) => self.format_secret_call(secret),
            Expression::MemberAccess(parts) => {
                self.push(&parts.join("."));
            }
        }
    }

    fn format_literal(&mut self, lit: &Literal) {
        match lit {
            Literal::Path(path) => {
                self.push("\"");
                self.push(path);
                self.push("\"");
            }
            Literal::String(s) => {
                self.push("\\\"");
                self.push(&escape_string_literal_contents(s));
                self.push("\"");
            }
            Literal::Number(n) => {
                self.push(&n.to_string());
            }
            Literal::Boolean(b) => {
                if *b {
                    self.push("true");
                } else {
                    self.push("false");
                }
            }
        }
    }
}

fn escape_string_literal_contents(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}
