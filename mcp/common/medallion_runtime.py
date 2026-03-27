from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .runtime import append_jsonl, ensure_directory, read_json, sanitize_name, utc_timestamp, write_json


DEFAULT_CONTROL_SCHEMA = "MDL_CTL"
DEFAULT_CONTROL_USER = "MDL_CTL"
DEFAULT_LINEAGE_NAMESPACE = "oci-medallion"
DEFAULT_LINEAGE_PROVIDER = "oci-medallion-codex-factory"
MAX_CONTROL_PATH_LENGTH = 240
SHORT_RECORD_PREFIX_LENGTH = 96

STANDARD_RUNTIME_ARG_NAMES = (
    "project_id",
    "workflow_id",
    "run_id",
    "parent_run_id",
    "entity_name",
    "source_type",
    "layer",
    "slice_key",
    "business_date",
    "batch_id",
    "watermark_low",
    "watermark_high",
    "reprocess_request_id",
    "quality_profile",
    "source_asset_ref",
    "target_asset_ref",
    "service_run_ref",
    "control_database_name",
    "lineage_namespace",
    "lineage_provider_key",
    "lineage_enabled",
)


def parse_bool_string(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in ("true", "1", "yes", "y"):
        return True
    if normalized in ("false", "0", "no", "n"):
        return False
    raise ValueError(f"Valor booleano invalido: {value}")


def current_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def add_standard_runtime_args(parser: argparse.ArgumentParser, *, include_control_database: bool = True) -> None:
    parser.add_argument("--project-id")
    parser.add_argument("--workflow-id")
    parser.add_argument("--run-id")
    parser.add_argument("--parent-run-id")
    parser.add_argument("--entity-name")
    parser.add_argument("--source-type", default="object_storage")
    parser.add_argument("--layer")
    parser.add_argument("--slice-key")
    parser.add_argument("--business-date")
    parser.add_argument("--batch-id")
    parser.add_argument("--watermark-low")
    parser.add_argument("--watermark-high")
    parser.add_argument("--reprocess-request-id")
    parser.add_argument("--quality-profile")
    parser.add_argument("--source-asset-ref")
    parser.add_argument("--target-asset-ref")
    parser.add_argument("--service-run-ref")
    if include_control_database:
        parser.add_argument("--control-database-name")
    parser.add_argument("--lineage-namespace")
    parser.add_argument("--lineage-provider-key")
    parser.add_argument("--lineage-enabled", default="true")


def _coalesce_slice_key(payload: dict[str, Any]) -> str | None:
    if payload.get("slice_key"):
        return str(payload["slice_key"])

    parts: list[str] = []
    if payload.get("entity_name"):
        parts.append(f"entity={payload['entity_name']}")
    if payload.get("business_date"):
        parts.append(f"business_date={payload['business_date']}")
    if payload.get("batch_id"):
        parts.append(f"batch_id={payload['batch_id']}")
    if not parts:
        return None
    return "/".join(parts)


def runtime_payload_from_args(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for name in STANDARD_RUNTIME_ARG_NAMES:
        if not hasattr(args, name):
            continue
        value = getattr(args, name)
        if value in (None, ""):
            continue
        payload[name] = value

    payload["lineage_enabled"] = parse_bool_string(str(payload.get("lineage_enabled", "true")), default=True)
    payload.setdefault("lineage_namespace", DEFAULT_LINEAGE_NAMESPACE)
    payload.setdefault("lineage_provider_key", DEFAULT_LINEAGE_PROVIDER)

    slice_key = _coalesce_slice_key(payload)
    if slice_key:
        payload["slice_key"] = slice_key
    return payload


def sanitized_runtime_id(value: str | None, fallback: str) -> str:
    if value:
        return sanitize_name(value)
    return sanitize_name(fallback)


def control_plane_root(context: Any, database_name: str) -> Path:
    return ensure_directory(context.service_root("autonomous_database") / sanitize_name(database_name) / "control_plane")


def _short_record_key(record_id: str, *, fallback: str = "record") -> str:
    sanitized = sanitize_name(record_id)
    trimmed = sanitized[:SHORT_RECORD_PREFIX_LENGTH].rstrip("-_.")
    if not trimmed:
        trimmed = fallback
    digest = hashlib.sha1(record_id.encode("utf-8")).hexdigest()[:12]
    return f"{digest}-{trimmed}"


def _record_path(root: Path, collection: str, record_id: str, *, suffix: str = ".json") -> Path:
    collection_root = ensure_directory(root / collection)
    sanitized = sanitize_name(record_id)
    candidate = collection_root / f"{sanitized}{suffix}"
    if len(str(candidate)) <= MAX_CONTROL_PATH_LENGTH:
        return candidate

    shortened = _short_record_key(record_id, fallback=collection.rstrip("s") or "record")
    candidate = collection_root / f"{shortened}{suffix}"
    if len(str(candidate)) <= MAX_CONTROL_PATH_LENGTH:
        return candidate

    digest = hashlib.sha1(record_id.encode("utf-8")).hexdigest()[:16]
    return collection_root / f"{digest}{suffix}"


def _merge_payload(path: Path, payload: dict[str, Any]) -> Path:
    existing = read_json(path, default={})
    now = utc_timestamp()
    if "created_at_utc" not in existing:
        existing["created_at_utc"] = now
    for key, value in payload.items():
        if value is not None:
            existing[key] = value
    existing["updated_at_utc"] = now
    write_json(path, existing)
    return path


def _control_operation(root: Path, operation: str, payload: dict[str, Any]) -> None:
    append_jsonl(root / "operations.log.jsonl", {"operation": operation, **payload})


def ensure_control_plane_manifest(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> Path:
    root = control_plane_root(context, database_name)
    existing = read_json(root / "control-plane.manifest.json", default={})
    payload = {
        "database_name": database_name,
        "schema_name": existing.get("schema_name", DEFAULT_CONTROL_SCHEMA),
        "control_user": existing.get("control_user", DEFAULT_CONTROL_USER),
        "default_source_type": existing.get("default_source_type", runtime_payload.get("source_type", "object_storage")),
        "lineage_namespace": existing.get("lineage_namespace", runtime_payload.get("lineage_namespace", DEFAULT_LINEAGE_NAMESPACE)),
        "lineage_provider_key": existing.get("lineage_provider_key", runtime_payload.get("lineage_provider_key", DEFAULT_LINEAGE_PROVIDER)),
        "reprocess_granularity": existing.get("reprocess_granularity", "run+slice"),
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(root / "control-plane.manifest.json", payload)
    _control_operation(root, "ensure_control_plane_manifest", payload)
    return path


def register_workflow_definition(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> Path | None:
    workflow_id = runtime_payload.get("workflow_id")
    if not workflow_id:
        return None

    root = control_plane_root(context, database_name)
    payload = {
        "workflow_id": workflow_id,
        "project_id": runtime_payload.get("project_id"),
        "entity_name": runtime_payload.get("entity_name"),
        "source_type": runtime_payload.get("source_type"),
        "source_asset_ref": runtime_payload.get("source_asset_ref"),
        "target_asset_ref": runtime_payload.get("target_asset_ref"),
        "default_layer": runtime_payload.get("layer"),
        "quality_profile": runtime_payload.get("quality_profile"),
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(_record_path(root, "workflows", workflow_id), payload)
    _control_operation(root, "register_workflow_definition", payload)
    return path


def register_entity_definition(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> Path | None:
    entity_name = runtime_payload.get("entity_name")
    if not entity_name:
        return None

    root = control_plane_root(context, database_name)
    payload = {
        "entity_name": entity_name,
        "project_id": runtime_payload.get("project_id"),
        "source_type": runtime_payload.get("source_type"),
        "layer": runtime_payload.get("layer"),
        "partition_pattern": "source_system={source_system}/entity={entity}/business_date={business_date}/batch_id={batch_id}",
        "technical_columns": {
            "bronze": [
                "_source_system",
                "_source_uri",
                "_ingest_ts",
                "_workflow_id",
                "_run_id",
                "_slice_key",
            ],
            "silver": [
                "_workflow_id",
                "_run_id",
                "_slice_key",
                "_source_uri",
                "_record_hash",
                "_ingest_ts",
                "_dq_status",
            ],
            "gold": [
                "_published_run_id",
                "_valid_from",
                "_valid_to",
                "_is_current",
            ],
        },
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(_record_path(root, "entities", entity_name), payload)
    _control_operation(root, "register_entity_definition", payload)
    return path


def register_run_state(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    status: str,
    extra: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
) -> Path | None:
    run_id = runtime_payload.get("run_id")
    if not run_id:
        return None

    root = control_plane_root(context, database_name)
    payload = {
        "run_id": run_id,
        "workflow_id": runtime_payload.get("workflow_id"),
        "parent_run_id": runtime_payload.get("parent_run_id"),
        "project_id": runtime_payload.get("project_id"),
        "entity_name": runtime_payload.get("entity_name"),
        "source_type": runtime_payload.get("source_type"),
        "layer": runtime_payload.get("layer"),
        "slice_key": runtime_payload.get("slice_key"),
        "business_date": runtime_payload.get("business_date"),
        "batch_id": runtime_payload.get("batch_id"),
        "watermark_low": runtime_payload.get("watermark_low"),
        "watermark_high": runtime_payload.get("watermark_high"),
        "reprocess_request_id": runtime_payload.get("reprocess_request_id"),
        "source_asset_ref": runtime_payload.get("source_asset_ref"),
        "target_asset_ref": runtime_payload.get("target_asset_ref"),
        "service_run_ref": runtime_payload.get("service_run_ref"),
        "quality_profile": runtime_payload.get("quality_profile"),
        "status": status,
        "metrics": metrics or {},
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(_record_path(root, "runs", run_id), payload)
    _control_operation(root, "register_run_state", payload)
    return path


def register_step_state(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    service_name: str,
    command_name: str,
    status: str,
    extra: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
) -> Path | None:
    run_id = runtime_payload.get("run_id")
    if not run_id:
        return None

    root = control_plane_root(context, database_name)
    step_id = sanitized_runtime_id(
        runtime_payload.get("service_run_ref"),
        f"{run_id}-{service_name}-{command_name}",
    )
    payload = {
        "step_id": step_id,
        "run_id": run_id,
        "workflow_id": runtime_payload.get("workflow_id"),
        "service_name": service_name,
        "command_name": command_name,
        "layer": runtime_payload.get("layer"),
        "slice_key": runtime_payload.get("slice_key"),
        "status": status,
        "service_run_ref": runtime_payload.get("service_run_ref"),
        "rows_in": (metrics or {}).get("rows_in"),
        "rows_out": (metrics or {}).get("rows_out"),
        "rows_rejected": (metrics or {}).get("rows_rejected"),
        "metrics": metrics or {},
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(_record_path(root, "steps", step_id), payload)
    _control_operation(root, "register_step_state", payload)
    return path


def register_slice_state(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    status: str,
    extra: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
) -> Path | None:
    run_id = runtime_payload.get("run_id")
    slice_key = runtime_payload.get("slice_key")
    if not run_id or not slice_key:
        return None

    root = control_plane_root(context, database_name)
    slice_id = sanitize_name(f"{run_id}-{slice_key}")
    payload = {
        "slice_id": slice_id,
        "run_id": run_id,
        "workflow_id": runtime_payload.get("workflow_id"),
        "entity_name": runtime_payload.get("entity_name"),
        "layer": runtime_payload.get("layer"),
        "slice_key": slice_key,
        "business_date": runtime_payload.get("business_date"),
        "batch_id": runtime_payload.get("batch_id"),
        "source_asset_ref": runtime_payload.get("source_asset_ref"),
        "target_asset_ref": runtime_payload.get("target_asset_ref"),
        "status": status,
        "rows_in": (metrics or {}).get("rows_in"),
        "rows_out": (metrics or {}).get("rows_out"),
        "rows_rejected": (metrics or {}).get("rows_rejected"),
        "checksum_ref": (extra or {}).get("checksum_ref"),
        "profile_ref": (extra or {}).get("profile_ref"),
        "metrics": metrics or {},
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(_record_path(root, "slices", slice_id), payload)
    _control_operation(root, "register_slice_state", payload)
    return path


def register_checkpoint(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    checkpoint_type: str,
    checkpoint_value: str,
    status: str = "ready",
    extra: dict[str, Any] | None = None,
) -> Path:
    root = control_plane_root(context, database_name)
    entity_name = runtime_payload.get("entity_name") or "shared"
    layer = runtime_payload.get("layer") or "shared"
    slice_key = runtime_payload.get("slice_key") or "global"
    checkpoint_id = sanitize_name(f"{entity_name}-{layer}-{slice_key}-{checkpoint_type}")
    payload = {
        "checkpoint_id": checkpoint_id,
        "workflow_id": runtime_payload.get("workflow_id"),
        "run_id": runtime_payload.get("run_id"),
        "entity_name": entity_name,
        "layer": layer,
        "slice_key": slice_key,
        "checkpoint_type": checkpoint_type,
        "checkpoint_value": checkpoint_value,
        "watermark_low": runtime_payload.get("watermark_low"),
        "watermark_high": runtime_payload.get("watermark_high"),
        "status": status,
        "last_run_id": runtime_payload.get("run_id"),
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(_record_path(root, "checkpoints", checkpoint_id), payload)
    _control_operation(root, "register_checkpoint", payload)
    return path


def register_reprocess_request(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    requested_reason: str,
    requested_by: str,
    extra: dict[str, Any] | None = None,
) -> Path:
    root = control_plane_root(context, database_name)
    request_id = runtime_payload.get("reprocess_request_id") or f"{utc_timestamp()}-reprocess"
    payload = {
        "reprocess_request_id": request_id,
        "workflow_id": runtime_payload.get("workflow_id"),
        "parent_run_id": runtime_payload.get("parent_run_id"),
        "entity_name": runtime_payload.get("entity_name"),
        "layer": runtime_payload.get("layer"),
        "slice_key": runtime_payload.get("slice_key"),
        "business_date": runtime_payload.get("business_date"),
        "batch_id": runtime_payload.get("batch_id"),
        "requested_reason": requested_reason,
        "requested_by": requested_by,
        "request_scope": "run+slice",
        "status": "requested",
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(_record_path(root, "reprocess_requests", request_id), payload)
    _control_operation(root, "register_reprocess_request", payload)
    return path


def register_quality_result(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    contract_name: str,
    status: str,
    summary: dict[str, Any],
    report_ref: str,
    extra: dict[str, Any] | None = None,
) -> Path:
    root = control_plane_root(context, database_name)
    run_id = runtime_payload.get("run_id") or "adhoc"
    slice_key = runtime_payload.get("slice_key") or "global"
    result_id = sanitize_name(f"{run_id}-{slice_key}-{contract_name}")
    payload = {
        "quality_result_id": result_id,
        "run_id": runtime_payload.get("run_id"),
        "workflow_id": runtime_payload.get("workflow_id"),
        "slice_key": runtime_payload.get("slice_key"),
        "contract_name": contract_name,
        "status": status,
        "summary": summary,
        "failed_checks": summary.get("by_status", {}).get("failed", 0),
        "report_ref": report_ref,
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(_record_path(root, "quality_results", result_id), payload)
    _control_operation(root, "register_quality_result", payload)
    return path


def build_openlineage_event(
    runtime_payload: dict[str, Any],
    event_type: str,
    job_name: str,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
    run_facets: dict[str, Any] | None = None,
    job_facets: dict[str, Any] | None = None,
    event_facets: dict[str, Any] | None = None,
) -> dict[str, Any]:
    namespace = runtime_payload.get("lineage_namespace") or DEFAULT_LINEAGE_NAMESPACE
    run_id = runtime_payload.get("run_id") or f"adhoc-{utc_timestamp()}"
    inputs = inputs or []
    outputs = outputs or []
    return {
        "eventType": event_type,
        "eventTime": current_utc_iso(),
        "producer": DEFAULT_LINEAGE_PROVIDER,
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
        "run": {
            "runId": run_id,
            "facets": run_facets or {},
        },
        "job": {
            "namespace": namespace,
            "name": job_name,
            "facets": job_facets or {},
        },
        "inputs": [{"namespace": namespace, "name": item} for item in inputs],
        "outputs": [{"namespace": namespace, "name": item} for item in outputs],
        "eventFacets": event_facets or {},
    }


def queue_lineage_event(
    context: Any,
    database_name: str,
    runtime_payload: dict[str, Any],
    event_type: str,
    lineage_payload: dict[str, Any],
    status: str = "pending",
    extra: dict[str, Any] | None = None,
) -> Path:
    root = control_plane_root(context, database_name)
    event_id = sanitize_name(
        f"{utc_timestamp()}-{runtime_payload.get('run_id') or 'adhoc'}-{runtime_payload.get('slice_key') or 'global'}-{event_type}"
    )
    payload = {
        "lineage_event_id": event_id,
        "run_id": runtime_payload.get("run_id"),
        "workflow_id": runtime_payload.get("workflow_id"),
        "slice_key": runtime_payload.get("slice_key"),
        "event_type": event_type,
        "provider_key": runtime_payload.get("lineage_provider_key", DEFAULT_LINEAGE_PROVIDER),
        "namespace_name": runtime_payload.get("lineage_namespace", DEFAULT_LINEAGE_NAMESPACE),
        "source_asset_ref": runtime_payload.get("source_asset_ref"),
        "target_asset_ref": runtime_payload.get("target_asset_ref"),
        "status": status,
        "lineage_payload": lineage_payload,
    }
    if extra:
        payload.update(extra)
    path = _merge_payload(_record_path(root, "lineage_outbox", event_id), payload)
    _control_operation(root, "queue_lineage_event", payload)
    return path


def record_control_runtime(
    context: Any,
    runtime_payload: dict[str, Any],
    service_name: str,
    command_name: str,
    status: str,
    *,
    database_name: str | None = None,
    metrics: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, str]:
    control_database_name = database_name or runtime_payload.get("control_database_name")
    if not control_database_name:
        return {}

    result: dict[str, str] = {}
    result["manifest_path"] = str(
        ensure_control_plane_manifest(context, control_database_name, runtime_payload, extra={"last_service": service_name})
    )
    workflow_path = register_workflow_definition(context, control_database_name, runtime_payload, extra=extra)
    if workflow_path is not None:
        result["workflow_path"] = str(workflow_path)
    entity_path = register_entity_definition(context, control_database_name, runtime_payload, extra=extra)
    if entity_path is not None:
        result["entity_path"] = str(entity_path)
    run_path = register_run_state(context, control_database_name, runtime_payload, status, extra=extra, metrics=metrics)
    if run_path is not None:
        result["run_path"] = str(run_path)
    step_path = register_step_state(
        context,
        control_database_name,
        runtime_payload,
        service_name,
        command_name,
        status,
        extra=extra,
        metrics=metrics,
    )
    if step_path is not None:
        result["step_path"] = str(step_path)
    slice_path = register_slice_state(context, control_database_name, runtime_payload, status, extra=extra, metrics=metrics)
    if slice_path is not None:
        result["slice_path"] = str(slice_path)
    return result
