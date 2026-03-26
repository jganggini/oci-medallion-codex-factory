#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID="${1:-sample-project}"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  echo "No se encontro Python 3 en PATH." >&2
  exit 1
fi

"$PYTHON_BIN" "$REPO_ROOT/scripts/init_workspace.py" --repo-root "$REPO_ROOT" --project-id "$PROJECT_ID"
"$PYTHON_BIN" "$REPO_ROOT/scripts/validate_factory.py" --repo-root "$REPO_ROOT"
echo "Workspace inicializado y validado para $PROJECT_ID"
