#!/usr/bin/env bash
set -euo pipefail

ITERATIONS="${FLAKE_ITERATIONS:-5}"
MAX_FLAKY_FAILURES="${MAX_FLAKY_FAILURES:-0}"
FAILURES=0

for i in $(seq 1 "$ITERATIONS"); do
  echo "[flake-check] iteration $i/$ITERATIONS"
  if ! cargo test --test runtime_integration --test fault_injection --test compatibility_semantics -- --nocapture; then
    FAILURES=$((FAILURES + 1))
  fi
done

echo "[flake-check] failures=$FAILURES max_allowed=$MAX_FLAKY_FAILURES"
if [ "$FAILURES" -gt "$MAX_FLAKY_FAILURES" ]; then
  echo "Flakiness threshold failed"
  exit 1
fi
