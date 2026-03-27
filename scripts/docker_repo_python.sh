#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Uso: scripts/docker_repo_python.sh <script_path> [args...]" >&2
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PATH_OPTIONS=(
  --source-dir
  --dependency-root
  --from-json-file
  --archive-source-file
  --lineage-file
  --from-outbox-file
  --wallet-dir
  --sql-file
  --sql-dir
  --source-file
  --contract-file
  --result-path
  --working-directory
  --config-source-file
)

contains_path_option() {
  local needle="$1"
  local item
  for item in "${PATH_OPTIONS[@]}"; do
    if [[ "$item" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

to_container_repo_path() {
  local raw_path="$1"
  local normalized_path="${raw_path//\\//}"
  local candidate

  if [[ "$normalized_path" == /workspace* ]]; then
    printf '%s\n' "$normalized_path"
    return 0
  fi

  if [[ "$normalized_path" == /* ]]; then
    candidate="$normalized_path"
  elif [[ "$normalized_path" =~ ^[A-Za-z]:/ ]]; then
    echo "La ruta '$raw_path' esta fuera del repo. Primero copiala con scripts/docker_stage_assets.sh o usa una ruta dentro de workspace/ o .local/." >&2
    exit 1
  else
    normalized_path="${normalized_path#./}"
    if [[ "$normalized_path" == ../* || "$normalized_path" == */../* || "$normalized_path" == *"/.." ]]; then
      echo "La ruta '$raw_path' sale del repo. Usa una ruta relativa dentro del repo." >&2
      exit 1
    fi
    candidate="$REPO_ROOT/$normalized_path"
  fi

  if [[ "$candidate" == "$REPO_ROOT" ]]; then
    printf '/workspace\n'
    return 0
  fi

  case "$candidate" in
    "$REPO_ROOT"/*)
      printf '/workspace/%s\n' "${candidate#"$REPO_ROOT"/}"
      ;;
    *)
      echo "La ruta '$raw_path' esta fuera del repo. Primero copiala con scripts/docker_stage_assets.sh o usa una ruta dentro de workspace/ o .local/." >&2
      exit 1
      ;;
  esac
}

SCRIPT_PATH="$(to_container_repo_path "$1")"
shift

CONTAINER_ARGS=()

while (($#)); do
  token="$1"
  shift

  if [[ "$token" == "--repo-root" ]]; then
    if (($#)) && [[ "$1" != --* ]]; then
      shift
    fi
    CONTAINER_ARGS+=(--repo-root /workspace)
    continue
  fi

  if contains_path_option "$token"; then
    if [[ $# -eq 0 ]]; then
      echo "Falta el valor para $token" >&2
      exit 1
    fi
    CONTAINER_ARGS+=("$token" "$(to_container_repo_path "$1")")
    shift
    continue
  fi

  CONTAINER_ARGS+=("$token")
done

cd "$REPO_ROOT"
docker compose run --rm -e "HOST_REPO_ROOT=$REPO_ROOT" oci-runner python "$SCRIPT_PATH" "${CONTAINER_ARGS[@]}"
