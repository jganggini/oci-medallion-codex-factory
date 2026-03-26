from __future__ import annotations

import shutil
import zipfile
from pathlib import Path
from typing import Any

from .runtime import MirrorContext, append_jsonl, copy_file, ensure_directory, read_json, sanitize_name, utc_timestamp, write_json


def _record_operation(service_root: Path, context: MirrorContext, service_name: str, operation: str, payload: dict[str, Any]) -> None:
    append_jsonl(service_root / "operations.log.jsonl", {"operation": operation, **payload})
    context.report(service_name, operation.replace("_", "-"), payload)


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
    _record_operation(bucket_root, context, "buckets", "create_bucket", manifest)
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
    _record_operation(bucket_root, context, "buckets", "upload_object", payload)
    return destination


def write_data_flow_application(
    context: MirrorContext,
    app_name: str,
    source_dir: Path | None,
    main_file: str,
    extra: dict[str, Any],
    json_source_file: Path | None = None,
    archive_source_file: Path | None = None,
    operation: str = "create_application",
) -> dict[str, Path | None]:
    service_root = ensure_directory(context.service_root("data_flow") / sanitize_name(app_name))
    app_root = ensure_directory(service_root / "application")

    archive_path = service_root / "archive.zip"
    mirrored_archive: Path | None = archive_path if archive_path.exists() else None
    if archive_source_file is not None:
        mirrored_archive = copy_file(archive_source_file, archive_path)
    elif source_dir is not None:
        if archive_path.exists():
            archive_path.unlink()
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for item in sorted(source_dir.rglob("*")):
                if item.is_file():
                    archive.write(item, item.relative_to(source_dir))
        mirrored_archive = archive_path

    application_json_path = app_root / "application.json"
    mirrored_json: Path | None = application_json_path if application_json_path.exists() else None
    if json_source_file is not None:
        mirrored_json = copy_file(json_source_file, application_json_path)

    payload = {
        "application_name": app_name,
        "source_dir": str(source_dir) if source_dir else None,
        "main_file": main_file,
        "archive_path": str(mirrored_archive) if mirrored_archive else None,
        "archive_source_file": str(archive_source_file) if archive_source_file else None,
        "application_json_path": str(mirrored_json) if mirrored_json else None,
        "application_json_source_file": str(json_source_file) if json_source_file else None,
        "extra": extra,
        "created_at_utc": utc_timestamp(),
    }
    manifest_path = app_root / "application.manifest.json"
    write_json(manifest_path, payload)
    _record_operation(service_root, context, "data_flow", operation, payload)
    return {
        "manifest_path": manifest_path,
        "archive_path": mirrored_archive,
        "application_json_path": mirrored_json,
    }


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
    _record_operation(service_root, context, "data_flow", "run_application", run_payload)
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
    _record_operation(service_root, context, "data_integration", "create_workspace", payload)
    return manifest_path


def create_di_project(context: MirrorContext, workspace_name: str, project_name: str, extra: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    project_path = service_root / "projects" / f"{sanitize_name(project_name)}.json"
    payload = {
        "workspace_name": workspace_name,
        "project_name": project_name,
        "created_at_utc": utc_timestamp(),
        "extra": extra,
    }
    write_json(project_path, payload)
    _record_operation(service_root, context, "data_integration", "create_project", payload)
    return project_path


def create_di_folder(context: MirrorContext, workspace_name: str, folder_name: str, extra: dict[str, Any] | None = None) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    folder_path = service_root / "folders" / f"{sanitize_name(folder_name)}.json"
    payload = {
        "workspace_name": workspace_name,
        "folder_name": folder_name,
        "created_at_utc": utc_timestamp(),
        "extra": extra or {},
    }
    write_json(folder_path, payload)
    _record_operation(service_root, context, "data_integration", "create_folder", payload)
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
    _record_operation(service_root, context, "data_integration", "create_task_from_dataflow", payload)
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
    _record_operation(service_root, context, "data_integration", "create_pipeline", payload)
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
    _record_operation(service_root, context, "autonomous_database", "create_autonomous_database", payload)
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
    _record_operation(service_root, context, "autonomous_database", "bootstrap_schema", payload)
    return script_path


def register_adb_user(context: MirrorContext, adb_name: str, db_user: str, sql_text: str, metadata: dict[str, Any]) -> dict[str, Path]:
    service_root = ensure_directory(context.service_root("autonomous_database") / sanitize_name(adb_name))
    user_root = ensure_directory(service_root / "users" / sanitize_name(db_user))
    receipts_root = ensure_directory(user_root / "receipts")
    script_path = user_root / "create-user.sql"
    script_path.write_text(sql_text, encoding="utf-8")

    payload = {
        "database_name": adb_name,
        "database_user": db_user,
        "script_path": str(script_path),
        "created_at_utc": utc_timestamp(),
        **metadata,
    }
    receipt_path = receipts_root / f"{utc_timestamp()}-create-user.json"
    write_json(receipt_path, payload)
    _record_operation(service_root, context, "autonomous_database", "create_database_user", payload)
    return {"script_path": script_path, "receipt_path": receipt_path}


def register_adb_sql_execution(
    context: MirrorContext,
    adb_name: str,
    operation: str,
    source_files: list[Path],
    metadata: dict[str, Any],
    rendered_sql: str | None = None,
) -> Path:
    service_root = ensure_directory(context.service_root("autonomous_database") / sanitize_name(adb_name))
    run_id = f"{utc_timestamp()}-{sanitize_name(operation)}"
    run_root = ensure_directory(service_root / "sql_runs" / run_id)
    sources_root = ensure_directory(run_root / "sources")

    mirrored_files: list[str] = []
    for index, source_file in enumerate(source_files, start=1):
        target = sources_root / f"{index:03d}-{sanitize_name(source_file.name)}"
        copy_file(source_file, target)
        mirrored_files.append(str(target))

    rendered_sql_path: str | None = None
    if rendered_sql is not None:
        rendered_path = run_root / "rendered.sql"
        rendered_path.write_text(rendered_sql, encoding="utf-8")
        rendered_sql_path = str(rendered_path)

    payload = {
        "database_name": adb_name,
        "source_files": [str(item) for item in source_files],
        "mirrored_files": mirrored_files,
        "rendered_sql_path": rendered_sql_path,
        "created_at_utc": utc_timestamp(),
        **metadata,
    }
    receipt_path = run_root / "execution.json"
    write_json(receipt_path, payload)
    _record_operation(service_root, context, "autonomous_database", operation, payload)
    return receipt_path


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
    _record_operation(service_root, context, "autonomous_database", "load_gold_objects", payload)
    return receipt


def collect_data_flow_run_report(context: MirrorContext, app_name: str, payload: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_flow") / sanitize_name(app_name))
    reports_root = ensure_directory(service_root / "reports")
    report_path = reports_root / f"{utc_timestamp()}-run-report.json"
    report_payload = {
        "application_name": app_name,
        "created_at_utc": utc_timestamp(),
        **payload,
    }
    write_json(report_path, report_payload)
    _record_operation(service_root, context, "data_flow", "collect_run_report", report_payload)
    return report_path


def collect_di_task_run_report(
    context: MirrorContext,
    workspace_name: str,
    task_name: str | None,
    pipeline_name: str | None,
    payload: dict[str, Any],
) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    reports_root = ensure_directory(service_root / "reports")
    report_path = reports_root / f"{utc_timestamp()}-task-run-report.json"
    report_payload = {
        "workspace_name": workspace_name,
        "task_name": task_name,
        "pipeline_name": pipeline_name,
        "created_at_utc": utc_timestamp(),
        **payload,
    }
    write_json(report_path, report_payload)
    _record_operation(service_root, context, "data_integration", "collect_task_run_report", report_payload)
    return report_path


def sync_bucket_manifest(context: MirrorContext, bucket_name: str, metadata: dict[str, Any]) -> Path:
    bucket_root = context.bucket_root(bucket_name)
    manifest_path = bucket_root / "bucket.manifest.json"
    existing = read_json(manifest_path, default={})
    payload = {
        **existing,
        "bucket_name": bucket_name,
        "environment": context.environment,
        "updated_at_utc": utc_timestamp(),
        "metadata": {
            **existing.get("metadata", {}),
            **metadata,
        },
    }
    if "created_at_utc" not in payload:
        payload["created_at_utc"] = utc_timestamp()
    write_json(manifest_path, payload)
    _record_operation(bucket_root, context, "buckets", "sync_bucket_manifest", payload)
    return manifest_path


def create_data_catalog_manifest(context: MirrorContext, catalog_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    payload = {
        "catalog_name": catalog_name,
        "environment": context.environment,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    manifest_path = service_root / "catalog.manifest.json"
    write_json(manifest_path, payload)
    _record_operation(service_root, context, "data_catalog", "create_catalog", payload)
    return manifest_path


def create_data_catalog_private_endpoint(context: MirrorContext, endpoint_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "private_endpoints") / f"{sanitize_name(endpoint_name)}.json"
    payload = {
        "private_endpoint_name": endpoint_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "create_private_endpoint", payload)
    return path


def register_data_catalog_asset(context: MirrorContext, asset_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "assets") / f"{sanitize_name(asset_name)}.json"
    payload = {
        "asset_name": asset_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "create_data_asset", payload)
    return path


def create_data_catalog_connection(context: MirrorContext, connection_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "connections") / f"{sanitize_name(connection_name)}.json"
    payload = {
        "connection_name": connection_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "create_connection", payload)
    return path


def create_data_catalog_job_definition(context: MirrorContext, job_definition_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "job_definitions") / f"{sanitize_name(job_definition_name)}.json"
    payload = {
        "job_definition_name": job_definition_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "create_harvest_job_definition", payload)
    return path


def run_data_catalog_job(context: MirrorContext, job_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "jobs") / f"{utc_timestamp()}-{sanitize_name(job_name)}.json"
    payload = {
        "job_name": job_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "run_harvest_job", payload)
    return path


def sync_di_lineage(context: MirrorContext, workspace_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "lineage_syncs") / f"{utc_timestamp()}-{sanitize_name(workspace_name)}.json"
    payload = {
        "workspace_name": workspace_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "sync_di_lineage", payload)
    return path


def import_openlineage_payload(context: MirrorContext, lineage_name: str, payload: dict[str, Any], metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "lineage" / "imports") / f"{utc_timestamp()}-{sanitize_name(lineage_name)}.json"
    body = {
        "lineage_name": lineage_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
        "payload": payload,
    }
    write_json(path, body)
    _record_operation(service_root, context, "data_catalog", "import_openlineage", body)
    return path


def collect_data_catalog_lineage_report(context: MirrorContext, report_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "lineage" / "reports") / f"{utc_timestamp()}-{sanitize_name(report_name)}.json"
    payload = {
        "report_name": report_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "collect_lineage_report", payload)
    return path
