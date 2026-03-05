# Loom

Loom is a custom language built for defining clean data pipelines and file processing workflows. 

If you have ever written a script to watch a directory, parse a CSV, filter out invalid rows, and move the processed files based on the result, you probably know how quickly the code can become hard to read. Loom solves this by treating everything as a data pipeline. Data moves from left to right, making it incredibly easy to see exactly what is happening at a glance.

## What is it for?

Loom is perfect for automation, stream handling, and background processing tasks:
- Watching directories for new files and kicking off workflows.
- Reading and parsing file formats like CSV.
- Filtering and transforming data streams.
- Moving, renaming, or archiving processed files without writing deep boilerplate.

## Basic Concepts

Loom is built around a few core ideas that keep pipelines robust.

**File-First Philosophy**
Loom treats the filesystem as a first-class citizen. You can read a file simply by streaming its path: `"input.txt" >>`. To append data to a file at the end of a pipeline, just stream the output into it: `>> "output.txt"`. There is no need for deep boilerplate or manually opening file handles.

**Pipes (`>>`)**
The pipe operator is the foundation of Loom. It takes the output of the left side and feeds it into the right side for processing.

**Directives (`@`)**
Directives are built-in operations that handle external systems, data sources, or side effects. For example, `@watch` monitors a directory for file changes, and `@read` streams a file's contents into the pipeline.

**Branching (`[]`)**
Sometimes you need to send the exact same data to multiple places. Loom allows you to split a single pipeline into concurrent branches using brackets. For example, you can capture invalid rows into an error log, and send valid rows to a database, all from the same pipeline source.

**Error Handling**
Things often go wrong in file processing. Loom lets you attach `on_fail` handlers to your pipelines to intercept errors, log custom alerts, and safely quarantine bad files without crashing the whole process.

## Advantages

- **Readability**: The syntax matches the architecture of your data flow. Because data processes linearly, you do not have to jump around nested loops or callbacks to understand the core logic.
- **Robustness**: With explicit error handling and features like the `@atomic` directive, you can build scripts that recover gracefully from unexpected data or file access errors.
- **Integrated Tooling**: Loom ships with a built-in Language Server (LSP). If you configure your editor to use it, you get autocomplete, hover hints, and instant syntax validation as you type.

## Installation and Usage

The easiest way to get started with Loom is to install the official VS Code extension.

1. Open VS Code.
2. Go to the Extensions view (`Ctrl+Shift+X` or `Cmd+Shift+X`).
3. Search for **Loom Language** (published by `loom`).
4. Click **Install**.

The extension comes with syntax highlighting and a built-in Language Server (LSP) that provides autocomplete, hover hints, and instant syntax validation as you type. It also allows you to run your Loom scripts directly from the editor!

### Building from Source

If you prefer to run Loom from your terminal or want to contribute to the language itself, you can build it from source. Loom is written in Rust. Make sure you have Rust and Cargo installed, then clone the repository and build the project:

```bash
cargo build --release
```

To run a Loom script directly:

```bash
./target/release/loom my_script.loom
```

Loom runs in strict mode by default. Use `--no-strict` to opt out:

```bash
./target/release/loom --no-strict my_script.loom
```

To start the built-in Language Server for your code editor (if not using the official extension):

```bash
./target/release/loom --lsp
```

## Contributing


Contributions to the codebase are always welcome. Whether you want to add a new core directive, improve the parser rules, or just hunt down bugs in the runtime environment, your help is appreciated. 

To get started:
1. Fork and clone the repository.
2. Read through the `src/` directory to familiarize yourself with the parser, AST, or runtime.
3. Write standard Rust tests for any new behavior or fixes.
4. Open a pull request with a straightforward description of your changes.

If you are modifying the grammar or language semantics, please ensure that all the existing test suites in the `tests/` directory continue to pass.
