#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

JOB_PATH=""
PROCESS_DATE=""
PROJECT_ID="sample-project"
ENVIRONMENT="dev"
BUILD_IMAGE="false"
declare -a JARS=()
declare -a SPARK_CONF=()
declare -a JOB_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --job-path)
      JOB_PATH="$2"
      shift 2
      ;;
    --process-date)
      PROCESS_DATE="$2"
      shift 2
      ;;
    --project-id)
      PROJECT_ID="$2"
      shift 2
      ;;
    --environment)
      ENVIRONMENT="$2"
      shift 2
      ;;
    --jar-path)
      JARS+=("$2")
      shift 2
      ;;
    --spark-conf)
      SPARK_CONF+=("$2")
      shift 2
      ;;
    --job-arg)
      JOB_ARGS+=("$2")
      shift 2
      ;;
    --build-image)
      BUILD_IMAGE="true"
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$JOB_PATH" ]]; then
  echo "--job-path is required" >&2
  exit 1
fi

if [[ "$BUILD_IMAGE" == "true" ]]; then
  docker compose -f "$REPO_ROOT/docker-compose.yml" build dataflow-local
fi

declare -a CMD=("spark-submit")

if [[ ${#JARS[@]} -gt 0 ]]; then
  jar_list=""
  for item in "${JARS[@]}"; do
    if [[ "$item" = /* ]]; then
      jar_path="$item"
    else
      jar_path="$REPO_ROOT/$item"
    fi
    if [[ -n "$jar_list" ]]; then
      jar_list="$jar_list,$jar_path"
    else
      jar_list="$jar_path"
    fi
  done
  CMD+=("--jars" "$jar_list")
fi

for conf in "${SPARK_CONF[@]}"; do
  CMD+=("--conf" "$conf")
done

if [[ "$JOB_PATH" = /* ]]; then
  JOB_FULL_PATH="$JOB_PATH"
else
  JOB_FULL_PATH="$REPO_ROOT/$JOB_PATH"
fi

CMD+=("$JOB_FULL_PATH")

if [[ -n "$PROCESS_DATE" ]]; then
  CMD+=("--process-date" "$PROCESS_DATE")
fi
CMD+=("--project-id" "$PROJECT_ID" "--environment" "$ENVIRONMENT")

if [[ ${#JOB_ARGS[@]} -gt 0 ]]; then
  CMD+=("${JOB_ARGS[@]}")
fi

docker compose -f "$REPO_ROOT/docker-compose.yml" run --rm dataflow-local "${CMD[@]}"
