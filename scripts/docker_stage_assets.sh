#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PATH_OPTIONS=(
  --sql-source
  --scripts-source
  --data-source
  --docs-source
  --references-source
  --ddl-source
  --samples-source
  --exports-source
  --mappings-source
  --notes-source
  --dataflow-jar-source
  --oci-config-source
  --oci-key-source
  --wallet-source
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

resolve_stage_path() {
  local raw_path="$1"
  if [[ -e "$raw_path" ]]; then
    if [[ -d "$raw_path" ]]; then
      (cd "$raw_path" && pwd)
    else
      local dir_name
      dir_name="$(cd "$(dirname "$raw_path")" && pwd)"
      printf '%s/%s\n' "$dir_name" "$(basename "$raw_path")"
    fi
    return 0
  fi

  local candidate="$REPO_ROOT/$raw_path"
  if [[ -e "$candidate" ]]; then
    if [[ -d "$candidate" ]]; then
      (cd "$candidate" && pwd)
    else
      local dir_name
      dir_name="$(cd "$(dirname "$candidate")" && pwd)"
      printf '%s/%s\n' "$dir_name" "$(basename "$candidate")"
    fi
    return 0
  fi

  echo "No existe la ruta fuente: $raw_path" >&2
  exit 1
}

to_container_repo_path() {
  local resolved_path="$1"
  local relative_path="${resolved_path#$REPO_ROOT/}"
  printf '/workspace/%s\n' "${relative_path//\\//}"
}

mount_args=()
script_args=(--repo-root /workspace)
mount_index=0

while (($#)); do
  token="$1"
  shift

  if [[ "$token" == "--repo-root" ]]; then
    shift
    continue
  fi

  if contains_path_option "$token"; then
    if [[ $# -eq 0 ]]; then
      echo "Falta el valor para $token" >&2
      exit 1
    fi

    resolved_path="$(resolve_stage_path "$1")"
    shift

    if [[ "$resolved_path" == "$REPO_ROOT"* ]]; then
      container_path="$(to_container_repo_path "$resolved_path")"
    else
      container_mount="/mnt/stage/$mount_index"
      if [[ -d "$resolved_path" ]]; then
        mount_source="$resolved_path"
        container_path="$container_mount"
      else
        mount_source="$(cd "$(dirname "$resolved_path")" && pwd)"
        container_path="$container_mount/$(basename "$resolved_path")"
      fi
      mount_args+=(-v "${mount_source}:${container_mount}:ro")
      mount_index=$((mount_index + 1))
    fi

    script_args+=("$token" "$container_path")
    continue
  fi

  script_args+=("$token")
done

cd "$REPO_ROOT"
docker compose run --rm -e "HOST_REPO_ROOT=$REPO_ROOT" "${mount_args[@]}" oci-runner python scripts/stage_local_assets.py "${script_args[@]}"
