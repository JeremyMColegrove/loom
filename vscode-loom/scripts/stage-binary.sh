#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "${ROOT_DIR}/.." && pwd)"

SOURCE_BIN="${1:-${REPO_DIR}/target/release/loom}"
TARGET_ID="${2:-$(uname -s | tr '[:upper:]' '[:lower:]')-$(uname -m)}"

case "${TARGET_ID}" in
  darwin-arm64|darwin-aarch64)
    PLATFORM_DIR="darwin-arm64"
    OUT_NAME="loom"
    ;;
  darwin-x86_64|darwin-x64)
    PLATFORM_DIR="darwin-x64"
    OUT_NAME="loom"
    ;;
  linux-x86_64|linux-x64)
    PLATFORM_DIR="linux-x64"
    OUT_NAME="loom"
    ;;
  mingw*-x86_64|msys*-x86_64|windows-x64|win32-x64)
    PLATFORM_DIR="win32-x64"
    OUT_NAME="loom.exe"
    ;;
  *)
    echo "Unsupported target id: ${TARGET_ID}"
    echo "Supported: darwin-arm64, darwin-x64, linux-x64, win32-x64"
    exit 1
    ;;
esac

if [[ ! -f "${SOURCE_BIN}" ]]; then
  echo "Source binary not found: ${SOURCE_BIN}"
  echo "Build first, e.g. from repo root: cargo build --release"
  exit 1
fi

mkdir -p "${ROOT_DIR}/bin/${PLATFORM_DIR}"
cp "${SOURCE_BIN}" "${ROOT_DIR}/bin/${PLATFORM_DIR}/${OUT_NAME}"

if [[ "${PLATFORM_DIR}" != "win32-x64" ]]; then
  chmod +x "${ROOT_DIR}/bin/${PLATFORM_DIR}/${OUT_NAME}"
fi

echo "Staged ${SOURCE_BIN} -> ${ROOT_DIR}/bin/${PLATFORM_DIR}/${OUT_NAME}"
