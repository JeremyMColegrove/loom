# Loom VS Code Extension

This extension provides:
- `.loom` language registration
- syntax highlighting
- LSP integration (`loom --lsp`)
- `Loom: Run Current File` command

## 1) Build the Loom binary

From repository root:

```bash
cargo build --release
```

## One command (recommended)

From `vscode-loom/`:

```bash
npm run build:vsix
```

This command will:
- build Loom in release mode
- install/update extension npm dependencies
- stage the built Loom binary into `bin/<platform>/`
- compile and package the extension into a `.vsix`

## 2) Stage binary into extension bundle

From `vscode-loom/`:

```bash
npm run stage-binary
```

You can stage for a specific platform by passing a second arg:

```bash
bash ./scripts/stage-binary.sh ../target/release/loom darwin-arm64
bash ./scripts/stage-binary.sh ../target/release/loom linux-x64
bash ./scripts/stage-binary.sh ../target/release/loom win32-x64
```

Binaries are expected at:
- `bin/darwin-arm64/loom`
- `bin/darwin-x64/loom`
- `bin/linux-x64/loom`
- `bin/win32-x64/loom.exe`

## 3) Build extension

```bash
npm install
npm run compile
```

## 4) Run in Extension Development Host

Open `vscode-loom/` in VS Code and press `F5`.

## 5) Package VSIX

```bash
npm run package
```

## Runtime behavior

Binary resolution order:
1. `loom.server.path` setting (absolute path)
2. bundled binary for current `platform-arch`
3. `loom` from system `PATH`

## Commands

- `Loom: Run Current File`

## Settings

- `loom.server.path`: explicit binary path override
- `loom.server.extraArgs`: extra args passed to `loom --lsp`
