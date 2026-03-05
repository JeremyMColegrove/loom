import * as fs from "node:fs";
import * as path from "node:path";
import * as vscode from "vscode";
import {
  LanguageClient,
  LanguageClientOptions,
  ServerOptions
} from "vscode-languageclient/node";

let client: LanguageClient | undefined;
let runTerminal: vscode.Terminal | undefined;

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  const output = vscode.window.createOutputChannel("Loom");
  context.subscriptions.push(output);

  const loomBinary = resolveLoomBinary(context.extensionPath);
  output.appendLine(`Using Loom binary: ${loomBinary}`);

  const extraArgs = vscode.workspace
    .getConfiguration("loom")
    .get<string[]>("server.extraArgs", []);

  const serverOptions: ServerOptions = {
    command: loomBinary,
    args: ["--lsp", ...extraArgs],
    options: {
      cwd: workspaceFolderPath() ?? context.extensionPath
    }
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "loom" }],
    outputChannel: output
  };

  client = new LanguageClient("loomLsp", "Loom Language Server", serverOptions, clientOptions);
  context.subscriptions.push(client);
  await client.start();

  const runCommand = vscode.commands.registerCommand("loom.runCurrentFile", async () => {
    const editor = vscode.window.activeTextEditor;
    if (!editor || !isLoomDocument(editor.document)) {
      void vscode.window.showErrorMessage("Open a .loom file to run it.");
      return;
    }

    const bin = resolveLoomBinary(context.extensionPath);
    if (editor.document.isDirty) {
      await editor.document.save();
    }

    const filePath = editor.document.uri.fsPath;
    const fileDir = path.dirname(filePath);

    const terminal = getOrCreateRunTerminal(fileDir);

    terminal.show(true);
    terminal.sendText(changeDirectoryCommand(fileDir));
    terminal.sendText(`${shellEscape(bin)} ${shellEscape(filePath)}`);
  });

  context.subscriptions.push(runCommand);

  const runStatus = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
  runStatus.command = "loom.runCurrentFile";
  runStatus.text = "$(play) Run Loom";
  runStatus.tooltip = "Run the current .loom file";
  context.subscriptions.push(runStatus);

  const updateRunStatusVisibility = (): void => {
    const editor = vscode.window.activeTextEditor;
    if (editor && isLoomDocument(editor.document)) {
      runStatus.show();
    } else {
      runStatus.hide();
    }
  };

  context.subscriptions.push(
    vscode.window.onDidChangeActiveTextEditor(updateRunStatusVisibility),
    vscode.window.onDidChangeVisibleTextEditors(updateRunStatusVisibility),
    vscode.workspace.onDidOpenTextDocument(updateRunStatusVisibility),
    vscode.workspace.onDidCloseTextDocument(updateRunStatusVisibility),
    vscode.window.onDidCloseTerminal((closedTerminal) => {
      if (closedTerminal === runTerminal) {
        runTerminal = undefined;
      }
    })
  );
  updateRunStatusVisibility();
}

export async function deactivate(): Promise<void> {
  if (client) {
    await client.stop();
    client = undefined;
  }
  runTerminal = undefined;
}

function resolveLoomBinary(extensionPath: string): string {
  const configured = vscode.workspace
    .getConfiguration("loom")
    .get<string>("server.path", "")
    .trim();

  if (configured && isExecutable(configured)) {
    return configured;
  }

  const platform = process.platform;
  const arch = process.arch;
  const ext = platform === "win32" ? ".exe" : "";
  const bundled = path.join(extensionPath, "bin", `${platform}-${arch}`, `loom${ext}`);
  if (isExecutable(bundled)) {
    return bundled;
  }

  return "loom";
}

function isExecutable(filePath: string): boolean {
  if (!fs.existsSync(filePath)) {
    return false;
  }

  try {
    fs.accessSync(filePath, process.platform === "win32" ? fs.constants.F_OK : fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function workspaceFolderPath(): string | undefined {
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
}

function isLoomDocument(document: vscode.TextDocument): boolean {
  return document.languageId === "loom" || document.uri.fsPath.toLowerCase().endsWith(".loom");
}

function shellEscape(value: string): string {
  if (process.platform === "win32") {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

function getOrCreateRunTerminal(cwd: string): vscode.Terminal {
  if (runTerminal) {
    return runTerminal;
  }

  runTerminal =
    vscode.window.terminals.find((terminal) => terminal.name === "Loom Run") ??
    vscode.window.createTerminal({
      name: "Loom Run",
      cwd
    });
  return runTerminal;
}

function changeDirectoryCommand(directory: string): string {
  if (process.platform === "win32") {
    return `cd /d ${shellEscape(directory)}`;
  }
  return `cd ${shellEscape(directory)}`;
}
