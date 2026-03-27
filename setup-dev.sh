#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ID="${1:-sample-project}"
CODEX_TEMPLATE="$REPO_ROOT/.codex/config.template.toml"
CODEX_CONFIG="$REPO_ROOT/.codex/config.toml"

resolve_codex_python_launcher() {
  if command -v python3 >/dev/null 2>&1; then
    CODEX_PYTHON_COMMAND="python3"
    CODEX_BRIDGE_PREFIX_ARGS='".codex/factory_mcp_bridge.py"'
    return 0
  fi

  if command -v python >/dev/null 2>&1; then
    CODEX_PYTHON_COMMAND="python"
    CODEX_BRIDGE_PREFIX_ARGS='".codex/factory_mcp_bridge.py"'
    return 0
  fi

  if command -v py >/dev/null 2>&1; then
    CODEX_PYTHON_COMMAND="py"
    CODEX_BRIDGE_PREFIX_ARGS='"-3", ".codex/factory_mcp_bridge.py"'
    return 0
  fi

  echo "No se encontro un launcher Python en host (python3, python o py). Los runtimes siguen siendo Docker-first, pero Codex necesita ese launcher local para levantar el bridge MCP del factory." >&2
  exit 1
}

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker es un prerrequisito. Instala Docker Desktop o Docker Engine con Docker Compose antes de ejecutar setup-dev.sh." >&2
  exit 1
fi

if ! docker version >/dev/null 2>&1; then
  echo "Docker esta instalado pero no responde. Asegurate de que el daemon o Docker Desktop este corriendo antes de ejecutar setup-dev.sh." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "No se encontro \`docker compose\`. Instala el plugin de Docker Compose antes de ejecutar setup-dev.sh." >&2
  exit 1
fi

if [[ ! -f "$CODEX_TEMPLATE" ]]; then
  echo "No se encontro la plantilla MCP en $CODEX_TEMPLATE" >&2
  exit 1
fi

resolve_codex_python_launcher
template_content="$(cat "$CODEX_TEMPLATE")"
template_content="${template_content//__CODEX_PYTHON_COMMAND__/$CODEX_PYTHON_COMMAND}"
template_content="${template_content//__CODEX_BRIDGE_PREFIX_ARGS__/$CODEX_BRIDGE_PREFIX_ARGS}"
printf '%s' "$template_content" > "$CODEX_CONFIG"

cd "$REPO_ROOT"
docker compose up -d --build dev-base oci-runner dataflow-local >/dev/null
./scripts/docker_repo_python.sh scripts/init_workspace.py --repo-root . --project-id "$PROJECT_ID"
./scripts/docker_repo_python.sh scripts/validate_factory.py --repo-root .
echo "Runtime Docker levantado, workspace inicializado y MCP local sincronizado en .codex/config.toml para $PROJECT_ID"
echo "Launcher MCP detectado para Codex: $CODEX_PYTHON_COMMAND"
echo "Si Codex, Cursor o VS Code ya estaban abiertos, recarga el proyecto para que tomen la configuracion local del factory."
