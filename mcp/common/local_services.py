from __future__ import annotations

import hashlib
import shutil
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

from .runtime import MirrorContext, append_jsonl, copy_file, docker_mount_source, ensure_directory, read_json, sanitize_name, utc_timestamp, write_json


def _record_operation(service_root: Path, context: MirrorContext, service_name: str, operation: str, payload: dict[str, Any]) -> None:
    append_jsonl(service_root / "operations.log.jsonl", {"operation": operation, **payload})
    context.report(service_name, operation.replace("_", "-"), payload)


def _json_children(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    results: list[dict[str, Any]] = []
    for item in sorted(root.rglob("*.json")):
        if item.name.endswith(".manifest.json"):
            continue
        payload = read_json(item, default={})
        payload["_path"] = str(item)
        results.append(payload)
    return results


def _write_service_manifest(service_root: Path, manifest_name: str, payload: dict[str, Any]) -> Path:
    manifest_path = service_root / manifest_name
    write_json(manifest_path, payload)
    return manifest_path


def create_iam_compartment(context: MirrorContext, compartment_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("iam")
    path = ensure_directory(service_root / "compartments") / f"{sanitize_name(compartment_name)}.json"
    payload = {
        "compartment_name": compartment_name,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "iam", "create_compartment", payload)
    export_iam_manifest(context)
    return path


def create_iam_group(context: MirrorContext, group_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("iam")
    path = ensure_directory(service_root / "groups") / f"{sanitize_name(group_name)}.json"
    payload = {
        "group_name": group_name,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "iam", "create_group", payload)
    export_iam_manifest(context)
    return path


def create_iam_dynamic_group(
    context: MirrorContext,
    dynamic_group_name: str,
    matching_rule: str,
    metadata: dict[str, Any],
) -> Path:
    service_root = context.service_root("iam")
    path = ensure_directory(service_root / "dynamic_groups") / f"{sanitize_name(dynamic_group_name)}.json"
    payload = {
        "dynamic_group_name": dynamic_group_name,
        "matching_rule": matching_rule,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "iam", "create_dynamic_group", payload)
    export_iam_manifest(context)
    return path


def create_iam_policy(context: MirrorContext, policy_name: str, statements: list[str], metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("iam")
    path = ensure_directory(service_root / "policies") / f"{sanitize_name(policy_name)}.json"
    payload = {
        "policy_name": policy_name,
        "statements": statements,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "iam", "create_policy", payload)
    export_iam_manifest(context)
    return path


def export_iam_manifest(context: MirrorContext) -> Path:
    service_root = context.service_root("iam")
    payload = {
        "environment": context.environment,
        "created_at_utc": utc_timestamp(),
        "compartments": _json_children(service_root / "compartments"),
        "groups": _json_children(service_root / "groups"),
        "dynamic_groups": _json_children(service_root / "dynamic_groups"),
        "policies": _json_children(service_root / "policies"),
    }
    return _write_service_manifest(service_root, "iam.manifest.json", payload)


def create_network_vcn(context: MirrorContext, vcn_name: str, cidr_blocks: list[str], metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("network")
    path = ensure_directory(service_root / "vcns") / f"{sanitize_name(vcn_name)}.json"
    payload = {
        "vcn_name": vcn_name,
        "cidr_blocks": cidr_blocks,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "network", "create_vcn", payload)
    export_network_manifest(context)
    return path


def create_network_subnet(context: MirrorContext, subnet_name: str, cidr_block: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("network")
    path = ensure_directory(service_root / "subnets") / f"{sanitize_name(subnet_name)}.json"
    payload = {
        "subnet_name": subnet_name,
        "cidr_block": cidr_block,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "network", "create_subnet", payload)
    export_network_manifest(context)
    return path


def create_network_nsg(context: MirrorContext, nsg_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("network")
    path = ensure_directory(service_root / "nsgs") / f"{sanitize_name(nsg_name)}.json"
    payload = {
        "nsg_name": nsg_name,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "network", "create_nsg", payload)
    export_network_manifest(context)
    return path


def update_network_nsg(context: MirrorContext, nsg_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("network")
    path = ensure_directory(service_root / "nsgs") / f"{sanitize_name(nsg_name)}.json"
    existing = read_json(path, default={})
    payload = {
        **existing,
        "nsg_name": nsg_name,
        "updated_at_utc": utc_timestamp(),
        "extra": {
            **existing.get("extra", {}),
            **metadata,
        },
    }
    if "created_at_utc" not in payload:
        payload["created_at_utc"] = utc_timestamp()
    write_json(path, payload)
    _record_operation(service_root, context, "network", "update_nsg", payload)
    export_network_manifest(context)
    return path


def create_network_route_table(context: MirrorContext, route_table_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("network")
    path = ensure_directory(service_root / "route_tables") / f"{sanitize_name(route_table_name)}.json"
    payload = {
        "route_table_name": route_table_name,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "network", "create_route_table", payload)
    export_network_manifest(context)
    return path


def create_network_service_gateway(context: MirrorContext, service_gateway_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("network")
    path = ensure_directory(service_root / "service_gateways") / f"{sanitize_name(service_gateway_name)}.json"
    payload = {
        "service_gateway_name": service_gateway_name,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "network", "create_service_gateway", payload)
    export_network_manifest(context)
    return path


def update_network_route_table(context: MirrorContext, route_table_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("network")
    path = ensure_directory(service_root / "route_tables") / f"{sanitize_name(route_table_name)}.json"
    existing = read_json(path, default={})
    payload = {
        **existing,
        "route_table_name": route_table_name,
        "updated_at_utc": utc_timestamp(),
        "extra": {
            **existing.get("extra", {}),
            **metadata,
        },
    }
    if "created_at_utc" not in payload:
        payload["created_at_utc"] = utc_timestamp()
    write_json(path, payload)
    _record_operation(service_root, context, "network", "update_route_table", payload)
    export_network_manifest(context)
    return path


def export_network_manifest(context: MirrorContext) -> Path:
    service_root = context.service_root("network")
    payload = {
        "environment": context.environment,
        "created_at_utc": utc_timestamp(),
        "vcns": _json_children(service_root / "vcns"),
        "subnets": _json_children(service_root / "subnets"),
        "nsgs": _json_children(service_root / "nsgs"),
        "service_gateways": _json_children(service_root / "service_gateways"),
        "route_tables": _json_children(service_root / "route_tables"),
    }
    return _write_service_manifest(service_root, "network.manifest.json", payload)


def _vault_root(context: MirrorContext, vault_name: str) -> Path:
    return ensure_directory(context.service_root("vault") / "vaults" / sanitize_name(vault_name))


def create_vault_definition(context: MirrorContext, vault_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("vault")
    path = _vault_root(context, vault_name) / "vault.manifest.json"
    payload = {
        "vault_name": vault_name,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "vault", "create_vault", payload)
    export_vault_manifest(context)
    return path


def create_vault_secret(context: MirrorContext, vault_name: str, secret_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("vault")
    path = ensure_directory(_vault_root(context, vault_name) / "secrets") / f"{sanitize_name(secret_name)}.json"
    payload = {
        "vault_name": vault_name,
        "secret_name": secret_name,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "vault", "create_secret", payload)
    export_vault_manifest(context)
    return path


def rotate_vault_secret_reference(context: MirrorContext, vault_name: str, secret_name: str, metadata: dict[str, Any]) -> Path:
    service_root = context.service_root("vault")
    rotation_dir = ensure_directory(_vault_root(context, vault_name) / "rotations" / sanitize_name(secret_name))
    path = rotation_dir / f"{utc_timestamp()}-rotation.json"
    payload = {
        "vault_name": vault_name,
        "secret_name": secret_name,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "vault", "rotate_secret_reference", payload)
    export_vault_manifest(context)
    return path


def export_vault_manifest(context: MirrorContext) -> Path:
    service_root = context.service_root("vault")
    vaults: list[dict[str, Any]] = []
    vaults_root = service_root / "vaults"
    if vaults_root.exists():
        for vault_root in sorted(item for item in vaults_root.iterdir() if item.is_dir()):
            vault_manifest = read_json(vault_root / "vault.manifest.json", default={})
            entry = {
                "vault_name": vault_manifest.get("vault_name", vault_root.name),
                "vault_manifest": vault_manifest,
                "secrets": _json_children(vault_root / "secrets"),
                "rotations": _json_children(vault_root / "rotations"),
            }
            vaults.append(entry)
    payload = {
        "environment": context.environment,
        "created_at_utc": utc_timestamp(),
        "vaults": vaults,
    }
    return _write_service_manifest(service_root, "vault.manifest.json", payload)


def _resource_manager_root(context: MirrorContext) -> Path:
    return ensure_directory(context.service_root("reports") / "resource_manager")


def create_resource_manager_stack(
    context: MirrorContext,
    stack_name: str,
    metadata: dict[str, Any],
    config_source_file: Path | None = None,
) -> Path:
    service_root = _resource_manager_root(context)
    stack_root = ensure_directory(service_root / "stacks" / sanitize_name(stack_name))
    config_copy: str | None = None
    if config_source_file is not None:
        copied = copy_file(config_source_file, stack_root / "config" / config_source_file.name)
        config_copy = str(copied)
    payload = {
        "stack_name": stack_name,
        "created_at_utc": utc_timestamp(),
        "config_source_file": str(config_source_file) if config_source_file else None,
        "config_copy": config_copy,
        "extra": metadata,
    }
    path = stack_root / "stack.manifest.json"
    write_json(path, payload)
    _record_operation(service_root, context, "resource_manager", "create_stack", payload)
    return path


def register_resource_manager_job(context: MirrorContext, stack_name: str, job_type: str, metadata: dict[str, Any]) -> Path:
    service_root = _resource_manager_root(context)
    job_root = ensure_directory(service_root / "stacks" / sanitize_name(stack_name) / "jobs")
    path = job_root / f"{utc_timestamp()}-{sanitize_name(job_type)}.json"
    payload = {
        "stack_name": stack_name,
        "job_type": job_type,
        "created_at_utc": utc_timestamp(),
        "extra": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "resource_manager", job_type, payload)
    return path


def export_stack_report(context: MirrorContext, stack_name: str | None) -> Path:
    service_root = _resource_manager_root(context)
    stacks_root = service_root / "stacks"
    stacks: list[dict[str, Any]] = []

    def collect_stack(root: Path) -> dict[str, Any]:
        manifest_path = root / "stack.manifest.json"
        return {
            "stack_name": root.name,
            "stack_manifest": read_json(manifest_path, default={}),
            "jobs": _json_children(root / "jobs"),
        }

    if stack_name:
        stack_root = stacks_root / sanitize_name(stack_name)
        stacks = [collect_stack(stack_root)] if stack_root.exists() else []
        report_name = sanitize_name(stack_name)
    else:
        if stacks_root.exists():
            stacks = [collect_stack(item) for item in sorted(stacks_root.iterdir()) if item.is_dir()]
        report_name = "all-stacks"

    report_path = service_root / f"{report_name}.report.json"
    write_json(
        report_path,
        {
            "environment": context.environment,
            "created_at_utc": utc_timestamp(),
            "stacks": stacks,
        },
    )
    return report_path


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


def _bucket_object_destination(context: MirrorContext, bucket_root: Path, source_file: Path, object_name: str | None) -> tuple[Path, str]:
    raw_object_name = (object_name or source_file.name).replace("\\", "/")
    object_parts = [sanitize_name(part) for part in PurePosixPath(raw_object_name).parts if part not in ("", ".", "/")]
    relative_path = Path(*object_parts) if object_parts else Path(sanitize_name(source_file.name))
    destination = bucket_root / "objects" / relative_path
    effective_destination = docker_mount_source(destination, context.repo_root)
    if len(str(effective_destination)) > 240:
        hashed_name = f"{hashlib.sha1(raw_object_name.encode('utf-8')).hexdigest()[:12]}-{sanitize_name(source_file.name)}"
        relative_path = Path(hashed_name)
        destination = bucket_root / "objects" / relative_path
    return destination, str(relative_path).replace("\\", "/")


def upload_object_to_bucket(context: MirrorContext, bucket_name: str, source_file: Path, object_name: str | None) -> Path:
    bucket_root = context.bucket_root(bucket_name)
    destination, mirrored_object_name = _bucket_object_destination(context, bucket_root, source_file, object_name)
    copy_file(source_file, destination)
    payload = {
        "bucket_name": bucket_name,
        "source_file": str(source_file),
        "object_name": object_name or source_file.name,
        "mirrored_object_name": mirrored_object_name,
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


def create_di_application(context: MirrorContext, workspace_name: str, application_name: str, extra: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    application_path = service_root / "applications" / f"{sanitize_name(application_name)}.json"
    payload = {
        "workspace_name": workspace_name,
        "application_name": application_name,
        "created_at_utc": utc_timestamp(),
        "extra": extra,
    }
    write_json(application_path, payload)
    _record_operation(service_root, context, "data_integration", "create_application_from_template", payload)
    return application_path


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


def create_di_pipeline(
    context: MirrorContext,
    workspace_name: str,
    pipeline_name: str,
    tasks: list[str],
    extra: dict[str, Any] | None = None,
) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    pipeline_path = service_root / "pipelines" / f"{sanitize_name(pipeline_name)}.json"
    payload = {
        "workspace_name": workspace_name,
        "pipeline_name": pipeline_name,
        "tasks": tasks,
        "created_at_utc": utc_timestamp(),
    }
    if extra:
        payload["extra"] = extra
    write_json(pipeline_path, payload)
    _record_operation(service_root, context, "data_integration", "create_pipeline", payload)
    return pipeline_path


def create_adb_definition(
    context: MirrorContext,
    adb_name: str,
    db_user: str,
    load_strategy: str,
    wallet_dir: Path | None,
    metadata: dict[str, Any] | None = None,
) -> Path:
    service_root = ensure_directory(context.service_root("autonomous_database") / sanitize_name(adb_name))
    manifest_path = service_root / "database.manifest.json"
    existing = read_json(manifest_path, default={})
    payload = {
        **existing,
        "database_name": adb_name,
        "database_user": db_user,
        "load_strategy": load_strategy,
        "wallet_dir": str(wallet_dir) if wallet_dir else None,
        "wallet_present": wallet_dir.exists() if wallet_dir else False,
        "created_at_utc": existing.get("created_at_utc", utc_timestamp()),
    }
    if metadata:
        payload.update(metadata)
    write_json(manifest_path, payload)
    _record_operation(service_root, context, "autonomous_database", "create_autonomous_database", payload)
    return manifest_path


def register_adb_wallet_metadata(context: MirrorContext, adb_name: str, wallet_dir: Path, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("autonomous_database") / sanitize_name(adb_name))
    wallet_root = ensure_directory(service_root / "wallet")
    wallet_manifest = wallet_root / "wallet.manifest.json"
    wallet_files = [str(item) for item in sorted(wallet_dir.rglob("*")) if item.is_file()]
    payload = {
        "database_name": adb_name,
        "wallet_dir": str(wallet_dir),
        "wallet_present": wallet_dir.exists(),
        "wallet_files": wallet_files,
        "created_at_utc": utc_timestamp(),
        **metadata,
    }
    write_json(wallet_manifest, payload)
    _record_operation(service_root, context, "autonomous_database", "download_wallet_metadata", payload)
    create_adb_definition(
        context,
        adb_name,
        metadata.get("database_user", "app_gold"),
        metadata.get("load_strategy", "single-writer-batch"),
        wallet_dir,
        metadata={
            "wallet_dir": str(wallet_dir),
            "wallet_present": wallet_dir.exists(),
            "wallet_manifest_path": str(wallet_manifest),
            "autonomous_database_id": metadata.get("autonomous_database_id"),
            "resource_state": metadata.get("resource_state"),
        },
    )
    return wallet_manifest


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


def register_adb_load(
    context: MirrorContext,
    adb_name: str,
    object_name: str,
    source_file: Path | None = None,
    metadata: dict[str, Any] | None = None,
    rendered_sql: str | None = None,
) -> Path:
    service_root = ensure_directory(context.service_root("autonomous_database") / sanitize_name(adb_name))
    load_dir = ensure_directory(service_root / "loads")
    destination: Path | None = None
    if source_file is not None:
        destination = load_dir / source_file.name
        shutil.copy2(source_file, destination)
    payload = {
        "database_name": adb_name,
        "object_name": object_name,
        "source_file": str(source_file) if source_file else None,
        "mirrored_file": str(destination) if destination else None,
        "registered_at_utc": utc_timestamp(),
    }
    if metadata:
        payload.update(metadata)
    receipt = load_dir / f"{sanitize_name(object_name)}-{utc_timestamp()}.json"
    write_json(receipt, payload)
    if rendered_sql is not None:
        rendered_sql_path = load_dir / f"{sanitize_name(object_name)}-{utc_timestamp()}-load.sql"
        rendered_sql_path.write_text(rendered_sql, encoding="utf-8")
        payload["rendered_sql_path"] = str(rendered_sql_path)
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


def create_data_flow_private_endpoint(context: MirrorContext, endpoint_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_flow"))
    path = ensure_directory(service_root / "private_endpoints") / f"{sanitize_name(endpoint_name)}.json"
    payload = {
        "private_endpoint_name": endpoint_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_flow", "create_private_endpoint", payload)
    return path


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


def write_di_published_objects_report(
    context: MirrorContext,
    workspace_name: str,
    application_name: str,
    payload: dict[str, Any],
) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    reports_root = ensure_directory(service_root / "reports")
    report_path = reports_root / f"{utc_timestamp()}-published-objects.json"
    report_payload = {
        "workspace_name": workspace_name,
        "application_name": application_name,
        "created_at_utc": utc_timestamp(),
        **payload,
    }
    write_json(report_path, report_payload)
    _record_operation(service_root, context, "data_integration", "list_published_objects", report_payload)
    return report_path


def register_di_task_run(
    context: MirrorContext,
    workspace_name: str,
    task_run_name: str,
    task_run_key: str | None,
    payload: dict[str, Any],
    *,
    operation: str,
) -> Path:
    service_root = ensure_directory(context.service_root("data_integration") / sanitize_name(workspace_name))
    task_runs_root = ensure_directory(service_root / "task_runs")
    receipt_name = sanitize_name(task_run_key or task_run_name or utc_timestamp())
    receipt_path = task_runs_root / f"{receipt_name}.json"
    existing = read_json(receipt_path, default={})
    report_payload = {
        **existing,
        "workspace_name": workspace_name,
        "task_run_name": task_run_name,
        "task_run_key": task_run_key,
        "updated_at_utc": utc_timestamp(),
        **payload,
    }
    if "created_at_utc" not in report_payload:
        report_payload["created_at_utc"] = utc_timestamp()
    write_json(receipt_path, report_payload)
    _record_operation(service_root, context, "data_integration", operation, report_payload)
    return receipt_path


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


def create_data_catalog_job(context: MirrorContext, job_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "jobs") / f"{sanitize_name(job_name)}.json"
    payload = {
        "job_name": job_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "create_job", payload)
    return path


def create_data_catalog_pattern(context: MirrorContext, pattern_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "patterns") / f"{sanitize_name(pattern_name)}.json"
    payload = {
        "pattern_name": pattern_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "create_pattern", payload)
    return path


def attach_data_catalog_patterns(context: MirrorContext, asset_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "pattern_attachments") / f"{utc_timestamp()}-{sanitize_name(asset_name)}.json"
    payload = {
        "asset_name": asset_name,
        "created_at_utc": utc_timestamp(),
        "metadata": metadata,
    }
    write_json(path, payload)
    _record_operation(service_root, context, "data_catalog", "attach_data_selector_patterns", payload)
    return path


def run_data_catalog_job(context: MirrorContext, job_name: str, metadata: dict[str, Any]) -> Path:
    service_root = ensure_directory(context.service_root("data_catalog"))
    path = ensure_directory(service_root / "job_executions") / f"{utc_timestamp()}-{sanitize_name(job_name)}.json"
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
