#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID="${1:-sample-project}"

cd "$REPO_ROOT"
docker compose up -d dev-base oci-runner dataflow-local >/dev/null
./scripts/docker_repo_python.sh scripts/init_workspace.py --repo-root . --project-id "$PROJECT_ID"
./scripts/docker_repo_python.sh scripts/validate_factory.py --repo-root .
echo "Workspace inicializado y validado para $PROJECT_ID usando Docker"
