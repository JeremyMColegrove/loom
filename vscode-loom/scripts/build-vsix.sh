#!/usr/bin/env bash
set -euo pipefail

EXT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "${EXT_DIR}/.." && pwd)"
RELEASE_BIN="${REPO_DIR}/target/release/loom"

target_platform_dir() {
  local target_id
  target_id="$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m)"
  case "${target_id}" in
    darwin-arm64|darwin-aarch64) echo "darwin-arm64" ;;
    darwin-x86_64|darwin-x64) echo "darwin-x64" ;;
    linux-x86_64|linux-x64) echo "linux-x64" ;;
    mingw*-x86_64|msys*-x86_64|windows-x64|win32-x64) echo "win32-x64" ;;
    *)
      echo "Unsupported target id: ${target_id}" >&2
      exit 1
      ;;
  esac
}

sha256_file() {
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    echo "No SHA-256 tool found (need shasum or sha256sum)" >&2
    exit 1
  fi
}

echo "==> Building Loom release binary"
(
  cd "${REPO_DIR}"
  cargo build --release
)

if [[ ! -x "${RELEASE_BIN}" ]]; then
  echo "Release binary missing: ${RELEASE_BIN}" >&2
  exit 1
fi

echo "==> Installing extension dependencies"
(
  cd "${EXT_DIR}"
  npm install
)

echo "==> Staging bundled Loom binary"
(
  cd "${EXT_DIR}"
  bash ./scripts/stage-binary.sh "${RELEASE_BIN}"
)

PLATFORM_DIR="$(target_platform_dir)"
STAGED_BIN="${EXT_DIR}/bin/${PLATFORM_DIR}/loom"
if [[ "${PLATFORM_DIR}" == "win32-x64" ]]; then
  STAGED_BIN="${EXT_DIR}/bin/${PLATFORM_DIR}/loom.exe"
fi

if [[ ! -f "${STAGED_BIN}" ]]; then
  echo "Staged binary missing: ${STAGED_BIN}" >&2
  exit 1
fi

RELEASE_SHA="$(sha256_file "${RELEASE_BIN}")"
STAGED_SHA="$(sha256_file "${STAGED_BIN}")"
echo "Release SHA256: ${RELEASE_SHA}"
echo "Staged  SHA256: ${STAGED_SHA}"
if [[ "${RELEASE_SHA}" != "${STAGED_SHA}" ]]; then
  echo "Staged binary does not match freshly built release binary" >&2
  exit 1
fi

echo "==> Compiling and packaging VS Code extension"
(
  cd "${EXT_DIR}"
  npm run compile
)

echo "==> Packaging VS Code extension"
(
  cd "${EXT_DIR}"
  npm run package
)

VSIX_PATH="${EXT_DIR}/loom-language-0.1.0.vsix"
if [[ ! -f "${VSIX_PATH}" ]]; then
  echo "VSIX not found after packaging: ${VSIX_PATH}" >&2
  exit 1
fi

echo "==> Verifying VSIX contains the staged Loom binary"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT
unzip -qq "${VSIX_PATH}" "extension/bin/${PLATFORM_DIR}/*" -d "${TMP_DIR}"
VSIX_BIN="${TMP_DIR}/extension/bin/${PLATFORM_DIR}/$(basename "${STAGED_BIN}")"
if [[ ! -f "${VSIX_BIN}" ]]; then
  echo "Bundled binary missing from VSIX: ${VSIX_BIN}" >&2
  exit 1
fi
VSIX_SHA="$(sha256_file "${VSIX_BIN}")"
echo "VSIX    SHA256: ${VSIX_SHA}"
if [[ "${VSIX_SHA}" != "${RELEASE_SHA}" ]]; then
  echo "VSIX binary does not match freshly built release binary" >&2
  exit 1
fi

echo "==> Done"
echo "VSIX: ${VSIX_PATH}"
