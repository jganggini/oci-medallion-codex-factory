from __future__ import annotations

import shutil
import zipfile
from pathlib import Path
from typing import Any

from .runtime import MirrorContext, append_jsonl, copy_file, ensure_directory, sanitize_name, utc_timestamp, write_json


def create_bucket_manifest(context: MirrorContext, bucket_name: str, metadata: dict[str, Any]) -> Path:
    bucket_root = context.bucket_root(bucket_name)
    manifest = {
        "bucket_name": bucket_name,
        "environment": context.environment,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    manifest_path = bucket_root / "bucket.manifest.json"
    write_json(manifest_path, manifest)
    append_jsonl(bucket_root / "operations.log.jsonl", {"operation": "create_bucket", **manifest})
    context.report("buckets", "create-bucket", manifest)
    return manifest_path


def upload_object_to_bucket(context: MirrorContext, bucket_name: str, source_file: Path, object_name: str | None) -> Path:
    bucket_root = context.bucket_root(bucket_name)
    target_name = sanitize_name(object_name or source_file.name)
    destination = bucket_root / "objects" / target_name
    copy_file(source_file, destination)
    payload = {
        "bucket_name": bucket_name,
        "source_file": str(source_file),
        "object_name": target_name,
        "stored_at": str(destination),
        "uploaded_at_utc": utc_timestamp(),
    }
    append_jsonl(bucket_root / "operations.log.jsonl", {"operation": "upload_object", **payload})
    context.report("buckets", "upload-object", payload)
    return destination


def create_data_flow_application(context: MirrorContext, app_name: str, source_dir: Path, main_file: str, extra: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_flow") / sanitize_name(app_name))
    app_root = ensure_directory(service_root / "application")
    archive_path = service_root / "archive.zip"

    if archive_path.exists():
        archive_path.unlink()

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in sorted(source_dir.rglob("*")):
            if item.is_file():
                archive.write(item, item.relative_to(source_dir))

    payload = {
        "application_name": app_name,
        "source_dir": str(source_dir),
        "main_file": main_file,
        "archive_path": str(archive_path),
        "extra": extra,
        "created_at_utc": utc_timestamp(),
    }
    write_json(app_root / "application.manifest.json", payload)
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "create_application", **payload})
    context.report("data_flow", "create-application", payload)
    return archive_path


def run_data_flow_application(context: MirrorContext, app_name: str, parameters: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_flow") / sanitize_name(app_name))
    run_id = f"localrun-{utc_timestamp().lower()}"
    run_payload = {
        "application_name": app_name,
        "run_id": run_id,
        "state": "SUCCEEDED",
        "parameters": parameters,
        "run_at_utc": utc_timestamp(),
    }
    run_path = service_root / "runs" / f"{run_id}.json"
    write_json(run_path, run_payload)
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "run_application", **run_payload})
    context.report("data_flow", "run-application", run_payload)
    return run_path


def create_di_workspace_metadata(context: MirrorContext, workspace_name: str, extra: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    payload = {
        "workspace_name": workspace_name,
        "environment": context.environment,
        "created_at_utc": utc_timestamp(),
        "extra": extra,
    }
    manifest_path = service_root / "workspace.manifest.json"
    write_json(manifest_path, payload)
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "create_workspace_metadata", **payload})
    context.report("data_integration", "create-workspace", payload)
    return manifest_path


def create_di_folder(context: MirrorContext, workspace_name: str, folder_name: str) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    folder_path = service_root / "folders" / f"{sanitize_name(folder_name)}.json"
    payload = {
        "workspace_name": workspace_name,
        "folder_name": folder_name,
        "created_at_utc": utc_timestamp(),
    }
    write_json(folder_path, payload)
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "create_folder", **payload})
    context.report("data_integration", "create-folder", payload)
    return folder_path


def create_di_dataflow_task(context: MirrorContext, workspace_name: str, task_name: str, application_name: str, extra: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    task_path = service_root / "tasks" / f"{sanitize_name(task_name)}.json"
    payload = {
        "workspace_name": workspace_name,
        "task_name": task_name,
        "application_name": application_name,
        "created_at_utc": utc_timestamp(),
        "extra": extra,
    }
    write_json(task_path, payload)
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "create_task_from_dataflow", **payload})
    context.report("data_integration", "create-task-from-dataflow", payload)
    return task_path


def create_di_pipeline(context: MirrorContext, workspace_name: str, pipeline_name: str, tasks: list[str]) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    pipeline_path = service_root / "pipelines" / f"{sanitize_name(pipeline_name)}.json"
    payload = {
        "workspace_name": workspace_name,
        "pipeline_name": pipeline_name,
        "tasks": tasks,
        "created_at_utc": utc_timestamp(),
    }
    write_json(pipeline_path, payload)
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "create_pipeline", **payload})
    context.report("data_integration", "create-pipeline", payload)
    return pipeline_path


def create_adb_definition(context: MirrorContext, adb_name: str, db_user: str, load_strategy: str, wallet_dir: Path | None) -> Path:
    service_root = ensure_directory(context.service_root("autonomous_database") / sanitize_name(adb_name))
    payload = {
        "database_name": adb_name,
        "database_user": db_user,
        "load_strategy": load_strategy,
        "wallet_dir": str(wallet_dir) if wallet_dir else None,
        "wallet_present": wallet_dir.exists() if wallet_dir else False,
        "created_at_utc": utc_timestamp(),
    }
    manifest_path = service_root / "database.manifest.json"
    write_json(manifest_path, payload)
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "create_autonomous_database", **payload})
    context.report("autonomous_database", "create-adb-definition", payload)
    return manifest_path


def write_adb_bootstrap(context: MirrorContext, adb_name: str, db_user: str, sql_text: str) -> Path:
    service_root = ensure_directory(context.service_root("autonomous_database") / sanitize_name(adb_name))
    bootstrap_dir = ensure_directory(service_root / "bootstrap")
    script_path = bootstrap_dir / f"{sanitize_name(db_user)}-bootstrap.sql"
    script_path.write_text(sql_text, encoding="utf-8")
    payload = {
        "database_name": adb_name,
        "database_user": db_user,
        "script_path": str(script_path),
        "created_at_utc": utc_timestamp(),
    }
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "bootstrap_schema", **payload})
    context.report("autonomous_database", "bootstrap-schema", payload)
    return script_path


def register_adb_load(context: MirrorContext, adb_name: str, object_name: str, source_file: Path) -> Path:
    service_root = ensure_directory(context.service_root("autonomous_database") / sanitize_name(adb_name))
    load_dir = ensure_directory(service_root / "loads")
    destination = load_dir / source_file.name
    shutil.copy2(source_file, destination)
    payload = {
        "database_name": adb_name,
        "object_name": object_name,
        "source_file": str(source_file),
        "mirrored_file": str(destination),
        "registered_at_utc": utc_timestamp(),
    }
    receipt = load_dir / f"{sanitize_name(object_name)}-{utc_timestamp()}.json"
    write_json(receipt, payload)
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "load_gold_objects", **payload})
    context.report("autonomous_database", "load-gold-objects", payload)
    return receipt
