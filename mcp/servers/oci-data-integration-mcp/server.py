from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import (
    create_di_application,
    collect_di_task_run_report,
    create_di_dataflow_task,
    create_di_folder,
    create_di_pipeline,
    create_di_project,
    create_di_workspace_metadata,
    register_di_task_run,
    write_di_published_objects_report,
)
from mcp.common.medallion_runtime import (
    add_standard_runtime_args,
    build_openlineage_event,
    queue_lineage_event,
    record_control_runtime,
    runtime_payload_from_args,
)
from mcp.common.oci_cli import OciExecutionContext, ensure_service_compartment_id, execute_oci, parse_oci_result_data
from mcp.common.runtime import MirrorContext


def parse_list(items: list[str]) -> list[str]:
    return [item for item in items if item]


def default_identifier(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value.upper())


def parse_bool_string(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in ("true", "1", "yes", "y"):
        return True
    if normalized in ("false", "0", "no", "n"):
        return False
    raise ValueError(f"Valor booleano invalido: {value}")


def build_registry_metadata(args: argparse.Namespace) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    aggregator_key = args.aggregator_key or args.folder_key
    if aggregator_key:
        metadata["aggregatorKey"] = aggregator_key
    favorite = parse_bool_string(args.favorite)
    if favorite is not None:
        metadata["isFavorite"] = favorite
    labels = parse_list(args.label)
    if labels:
        metadata["labels"] = labels
    if args.registry_version is not None:
        metadata["registryVersion"] = int(args.registry_version)
    return metadata


def build_parent_ref(args: argparse.Namespace) -> str | None:
    if args.parent_ref:
        return args.parent_ref
    if args.folder_key and args.aggregator_key:
        return json.dumps({"parent": args.folder_key, "rootDocId": args.aggregator_key}, ensure_ascii=True)
    parent = args.folder_key or args.aggregator_key
    if parent:
        return json.dumps({"parent": parent, "rootDocId": parent}, ensure_ascii=True)
    return None


def build_dataflow_application(args: argparse.Namespace) -> str:
    payload: dict[str, str] = {}
    if args.application_id:
        payload["applicationId"] = args.application_id
    if args.application_compartment_id:
        ensure_service_compartment_id(args.application_compartment_id, "--application-compartment-id")
        payload["compartmentId"] = args.application_compartment_id
    if not payload and args.application_name:
        payload["name"] = args.application_name
    return json.dumps(payload, ensure_ascii=True)


def build_source_application_info(args: argparse.Namespace) -> str:
    source_application_key = args.template_application_key or args.application_key
    if not source_application_key:
        raise SystemExit("--template-application-key o --application-key es requerido para create-application-from-template")
    payload = {
        "applicationKey": source_application_key,
        "copyType": args.copy_type or "DISCONNECTED",
    }
    return json.dumps(payload, ensure_ascii=True)


def parse_binding(binding: str) -> tuple[str, Any]:
    if "=" not in binding:
        raise SystemExit(f"Binding invalido '{binding}'. Usa el formato CLAVE=valor")
    key, value = binding.split("=", 1)
    normalized_key = key.strip()
    if not normalized_key:
        raise SystemExit(f"Binding invalido '{binding}'. La clave no puede estar vacia")
    return normalized_key, value


def build_config_provider(args: argparse.Namespace) -> str | None:
    if not args.config_binding:
        return None
    bindings: dict[str, dict[str, Any]] = {}
    for raw_binding in args.config_binding:
        key, value = parse_binding(raw_binding)
        bindings[key] = {"simpleValue": value}
    return json.dumps({"bindings": bindings}, ensure_ascii=True)


def build_task_run_registry_metadata(args: argparse.Namespace) -> dict[str, Any]:
    metadata = build_registry_metadata(args)
    aggregator_key = metadata.get("aggregatorKey") or args.published_object_key or args.task_key
    if aggregator_key:
        metadata["aggregatorKey"] = aggregator_key
    if "registryVersion" not in metadata:
        metadata["registryVersion"] = 1
    return metadata


def add_wait_options(command: list[str], wait_for_state: list[str], max_wait_seconds: int | None, wait_interval_seconds: int | None) -> None:
    for state in wait_for_state:
        command.extend(["--wait-for-state", state])
    if max_wait_seconds is not None:
        command.extend(["--max-wait-seconds", str(max_wait_seconds)])
    if wait_interval_seconds is not None:
        command.extend(["--wait-interval-seconds", str(wait_interval_seconds)])


def extract_work_request_resource_identifier(oci_data: dict[str, Any], entity_type: str) -> str | None:
    resources = oci_data.get("resources")
    if not isinstance(resources, list):
        return None
    expected = entity_type.strip().lower()
    for item in resources:
        if not isinstance(item, dict):
            continue
        if str(item.get("entity-type", "")).strip().lower() != expected:
            continue
        identifier = item.get("identifier")
        if identifier:
            return str(identifier)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-data-integration-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument(
        "--command",
        required=True,
        choices=(
            "create-workspace",
            "create-project",
            "create-folder",
            "create-task-from-dataflow",
            "create-application-from-template",
            "list-published-objects",
            "create-task-run",
            "get-task-run",
            "create-pipeline",
            "collect-task-run-report",
        ),
    )
    parser.add_argument("--workspace-name", required=True)
    parser.add_argument("--compartment-id")
    parser.add_argument("--workspace-id")
    parser.add_argument("--is-private-network", default="false")
    parser.add_argument("--subnet-id")
    parser.add_argument("--vcn-id")
    parser.add_argument("--description")
    parser.add_argument("--identifier")
    parser.add_argument("--project-name")
    parser.add_argument("--folder-name")
    parser.add_argument("--folder-key")
    parser.add_argument("--aggregator-key")
    parser.add_argument("--registry-version")
    parser.add_argument("--parent-ref")
    parser.add_argument("--task-name")
    parser.add_argument("--task-key")
    parser.add_argument("--application-name")
    parser.add_argument("--application-id")
    parser.add_argument("--application-key")
    parser.add_argument("--template-application-key")
    parser.add_argument("--copy-type", default="DISCONNECTED")
    parser.add_argument("--published-object-key")
    parser.add_argument("--task-run-name")
    parser.add_argument("--application-compartment-id")
    parser.add_argument("--pipeline-name")
    parser.add_argument("--task", action="append", default=[])
    parser.add_argument("--config-binding", action="append", default=[])
    parser.add_argument("--label", action="append", default=[])
    parser.add_argument("--favorite")
    parser.add_argument("--wait-for-state", action="append", default=[])
    parser.add_argument("--max-wait-seconds", type=int)
    parser.add_argument("--wait-interval-seconds", type=int)
    parser.add_argument("--task-run-key")
    parser.add_argument("--state", default="SUCCESS")
    parser.add_argument("--bytes-read", type=int)
    parser.add_argument("--bytes-written", type=int)
    parser.add_argument("--rows-in", type=int)
    parser.add_argument("--rows-out", type=int)
    parser.add_argument("--rows-rejected", type=int)
    add_standard_runtime_args(parser)
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    runtime_payload = runtime_payload_from_args(args)
    registry_metadata = build_registry_metadata(args)

    if args.runtime == "oci":
        execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
        if args.command == "create-workspace":
            if not args.compartment_id:
                raise SystemExit("--compartment-id es requerido en runtime oci para create-workspace")
            ensure_service_compartment_id(args.compartment_id)
            command = [
                "data-integration",
                "workspace",
                "create",
                "--display-name",
                args.workspace_name,
                "--compartment-id",
                args.compartment_id,
                "--is-private-network",
                args.is_private_network,
            ]
            if args.description:
                command.extend(["--description", args.description])
            if args.subnet_id:
                command.extend(["--subnet-id", args.subnet_id])
            if args.vcn_id:
                command.extend(["--vcn-id", args.vcn_id])
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_integration", context, "create-workspace", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            workspace_id = extract_work_request_resource_identifier(oci_data, "disworkspace") or oci_data.get("id")
            work_request_id = None
            raw_id = oci_data.get("id")
            if isinstance(raw_id, str) and raw_id.startswith("ocid1.coreservicesworkrequest."):
                work_request_id = raw_id
            manifest = create_di_workspace_metadata(
                context,
                args.workspace_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "description": args.description,
                    "compartment_id": args.compartment_id,
                    "subnet_id": args.subnet_id,
                    "vcn_id": args.vcn_id,
                    "workspace_id": workspace_id,
                    "work_request_id": work_request_id,
                    "lifecycle_state": oci_data.get("lifecycle-state") or oci_data.get("status"),
                    "wait_for_state": args.wait_for_state,
                    "max_wait_seconds": args.max_wait_seconds,
                    "wait_interval_seconds": args.wait_interval_seconds,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_integration",
                "create_workspace",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={
                    "workspace_name": args.workspace_name,
                    "workspace_id": workspace_id,
                    "work_request_id": work_request_id,
                    "manifest_path": str(manifest),
                },
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "manifest_path": str(manifest),
                        "workspace_id": workspace_id,
                        "work_request_id": work_request_id,
                        "lifecycle_state": oci_data.get("lifecycle-state") or oci_data.get("status"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-project":
            if not args.workspace_id or not args.project_name:
                raise SystemExit("--workspace-id y --project-name son requeridos en runtime oci para create-project")
            command = [
                "data-integration",
                "project",
                "create",
                "--workspace-id",
                args.workspace_id,
                "--name",
                args.project_name,
                "--identifier",
                args.identifier or default_identifier(args.project_name),
            ]
            if args.description:
                command.extend(["--description", args.description])
            if registry_metadata:
                command.extend(["--registry-metadata", json.dumps(registry_metadata, ensure_ascii=True)])
            result = execute_oci(execution, "data_integration", context, "create-project", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            project_manifest = create_di_project(
                context,
                args.workspace_name,
                args.project_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "project_key": oci_data.get("key"),
                    "identifier": args.identifier or default_identifier(args.project_name),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "description": args.description,
                    "registry_metadata": registry_metadata,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_integration",
                "create_project",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"workspace_name": args.workspace_name, "project_manifest": str(project_manifest), "project_key": oci_data.get("key")},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "project_manifest": str(project_manifest),
                        "project_key": oci_data.get("key"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-folder":
            if not args.workspace_id or not args.folder_name:
                raise SystemExit("--workspace-id y --folder-name son requeridos en runtime oci para create-folder")
            command = [
                "data-integration",
                "folder",
                "create",
                "--workspace-id",
                args.workspace_id,
                "--name",
                args.folder_name,
                "--identifier",
                args.identifier or default_identifier(args.folder_name),
            ]
            if args.description:
                command.extend(["--description", args.description])
            if registry_metadata:
                command.extend(["--registry-metadata", json.dumps(registry_metadata, ensure_ascii=True)])
            result = execute_oci(execution, "data_integration", context, "create-folder", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            folder_manifest = create_di_folder(
                context,
                args.workspace_name,
                args.folder_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "folder_key": oci_data.get("key"),
                    "identifier": args.identifier or default_identifier(args.folder_name),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "description": args.description,
                    "registry_metadata": registry_metadata,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_integration",
                "create_folder",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"workspace_name": args.workspace_name, "folder_manifest": str(folder_manifest), "folder_key": oci_data.get("key")},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "folder_manifest": str(folder_manifest),
                        "folder_key": oci_data.get("key"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-application-from-template":
            if not args.workspace_id or not args.application_name:
                raise SystemExit("--workspace-id y --application-name son requeridos en runtime oci para create-application-from-template")
            command = [
                "data-integration",
                "dis-application",
                "create",
                "--workspace-id",
                args.workspace_id,
                "--name",
                args.application_name,
                "--identifier",
                args.identifier or default_identifier(args.application_name),
                "--model-type",
                "INTEGRATION_APPLICATION",
                "--source-application-info",
                build_source_application_info(args),
            ]
            if args.compartment_id:
                ensure_service_compartment_id(args.compartment_id)
                command.extend(["--compartment-id", args.compartment_id])
            if args.description:
                command.extend(["--description", args.description])
            if registry_metadata:
                command.extend(["--registry-metadata", json.dumps(registry_metadata, ensure_ascii=True)])
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_integration", context, "create-application-from-template", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            application_manifest = create_di_application(
                context,
                args.workspace_name,
                args.application_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "application_id": oci_data.get("id"),
                    "application_key": oci_data.get("key"),
                    "identifier": args.identifier or default_identifier(args.application_name),
                    "description": args.description,
                    "source_application_key": args.template_application_key or args.application_key,
                    "copy_type": args.copy_type,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "registry_metadata": registry_metadata,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_integration",
                "create_application_from_template",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={
                    "workspace_name": args.workspace_name,
                    "application_manifest": str(application_manifest),
                    "application_key": oci_data.get("key"),
                    "application_id": oci_data.get("id"),
                },
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "application_manifest": str(application_manifest),
                        "application_key": oci_data.get("key"),
                        "application_id": oci_data.get("id"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-task-from-dataflow":
            if not args.workspace_id or not args.task_name or not args.application_id:
                raise SystemExit("--workspace-id, --task-name y --application-id son requeridos en runtime oci para create-task-from-dataflow")
            command = [
                "data-integration",
                "task",
                "create-task-from-dataflow-task",
                "--workspace-id",
                args.workspace_id,
                "--name",
                args.task_name,
                "--identifier",
                args.identifier or default_identifier(args.task_name),
            ]
            if args.description:
                command.extend(["--description", args.description])
            if registry_metadata:
                command.extend(["--registry-metadata", json.dumps(registry_metadata, ensure_ascii=True)])
            parent_ref = build_parent_ref(args)
            if parent_ref:
                command.extend(["--parent-ref", parent_ref])
            command.extend(["--dataflow-application", build_dataflow_application(args)])
            result = execute_oci(execution, "data_integration", context, "create-task-from-dataflow", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            task_manifest = create_di_dataflow_task(
                context,
                args.workspace_name,
                args.task_name,
                args.application_name or args.application_id,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "identifier": args.identifier or default_identifier(args.task_name),
                    "task_key": oci_data.get("key") or args.task_key,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "description": args.description,
                    "registry_metadata": registry_metadata,
                    "parent_ref": json.loads(parent_ref) if parent_ref else None,
                    "application_id": args.application_id,
                    "application_compartment_id": args.application_compartment_id,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_integration",
                "create_task_from_dataflow",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"workspace_name": args.workspace_name, "task_manifest": str(task_manifest), "task_key": oci_data.get("key") or args.task_key},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "task_manifest": str(task_manifest),
                        "task_key": oci_data.get("key") or args.task_key,
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "list-published-objects":
            if not args.workspace_id or not args.application_key:
                raise SystemExit("--workspace-id y --application-key son requeridos en runtime oci para list-published-objects")
            command = [
                "data-integration",
                "application",
                "list-published-objects",
                "--workspace-id",
                args.workspace_id,
                "--application-key",
                args.application_key,
                "--all",
            ]
            result = execute_oci(execution, "data_integration", context, "list-published-objects", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            items = oci_data.get("items") if isinstance(oci_data.get("items"), list) else []
            report_path = write_di_published_objects_report(
                context,
                args.workspace_name,
                args.application_name or args.application_key,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "application_key": args.application_key,
                    "published_object_count": len(items),
                    "items": items,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_integration",
                "list_published_objects",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={
                    "workspace_name": args.workspace_name,
                    "application_key": args.application_key,
                    "published_objects_report": str(report_path),
                    "published_object_count": len(items),
                },
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "report_path": str(report_path),
                        "published_object_count": len(items),
                        "items": items,
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-task-run":
            if not args.workspace_id or not args.application_key:
                raise SystemExit("--workspace-id y --application-key son requeridos en runtime oci para create-task-run")
            if not args.published_object_key and not args.task_key and not args.aggregator_key:
                raise SystemExit("--published-object-key, --task-key o --aggregator-key es requerido en runtime oci para create-task-run")
            task_run_name = args.task_run_name or args.task_name or "di-task-run"
            task_run_identifier = args.identifier or default_identifier(task_run_name)
            task_run_registry_metadata = build_task_run_registry_metadata(args)
            command = [
                "data-integration",
                "task-run",
                "create",
                "--workspace-id",
                args.workspace_id,
                "--application-key",
                args.application_key,
                "--name",
                task_run_name,
                "--identifier",
                task_run_identifier,
                "--registry-metadata",
                json.dumps(task_run_registry_metadata, ensure_ascii=True),
            ]
            config_provider = build_config_provider(args)
            if config_provider:
                command.extend(["--config-provider", config_provider])
            if args.description:
                command.extend(["--description", args.description])
            result = execute_oci(execution, "data_integration", context, "create-task-run", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            task_run_receipt = register_di_task_run(
                context,
                args.workspace_name,
                str(oci_data.get("name") or task_run_name),
                str(oci_data.get("key") or ""),
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "application_key": args.application_key,
                    "identifier": task_run_identifier,
                    "task_key": oci_data.get("task-key") or args.task_key or args.published_object_key,
                    "status": oci_data.get("status"),
                    "description": args.description,
                    "config_provider": json.loads(config_provider) if config_provider else None,
                    "registry_metadata": task_run_registry_metadata,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
                operation="create_task_run",
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_integration",
                "create_task_run",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={
                    "workspace_name": args.workspace_name,
                    "task_run_path": str(task_run_receipt),
                    "task_run_key": oci_data.get("key"),
                    "application_key": args.application_key,
                },
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "task_run_path": str(task_run_receipt),
                        "task_run_key": oci_data.get("key"),
                        "task_run_name": oci_data.get("name") or task_run_name,
                        "task_status": oci_data.get("status"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "get-task-run":
            if not args.workspace_id or not args.application_key or not args.task_run_key:
                raise SystemExit("--workspace-id, --application-key y --task-run-key son requeridos en runtime oci para get-task-run")
            command = [
                "data-integration",
                "task-run",
                "get",
                "--workspace-id",
                args.workspace_id,
                "--application-key",
                args.application_key,
                "--task-run-key",
                args.task_run_key,
            ]
            result = execute_oci(execution, "data_integration", context, "get-task-run", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            task_run_receipt = register_di_task_run(
                context,
                args.workspace_name,
                str(oci_data.get("name") or args.task_run_key),
                args.task_run_key,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "application_key": args.application_key,
                    "task_key": oci_data.get("task-key"),
                    "task_type": oci_data.get("task-type"),
                    "status": oci_data.get("status"),
                    "error_message": oci_data.get("error-message"),
                    "outputs": oci_data.get("outputs"),
                    "config_provider": oci_data.get("config-provider"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
                operation="get_task_run",
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_integration",
                "get_task_run",
                str(oci_data.get("status", "observed")).lower(),
                extra={
                    "workspace_name": args.workspace_name,
                    "task_run_path": str(task_run_receipt),
                    "task_run_key": args.task_run_key,
                    "application_key": args.application_key,
                },
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "task_run_path": str(task_run_receipt),
                        "task_run_key": args.task_run_key,
                        "task_status": oci_data.get("status"),
                        "task_key": oci_data.get("task-key"),
                        "task_type": oci_data.get("task-type"),
                        "error_message": oci_data.get("error-message"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-pipeline":
            if not args.workspace_id or not args.pipeline_name:
                raise SystemExit("--workspace-id y --pipeline-name son requeridos en runtime oci para create-pipeline")
            pipeline_identifier = args.identifier or default_identifier(args.pipeline_name)
            pipeline_registry_metadata = registry_metadata or {
                "aggregatorKey": args.aggregator_key or args.folder_key or "PROJECT",
                "registryVersion": int(args.registry_version) if args.registry_version is not None else 1,
            }
            command = [
                "data-integration",
                "pipeline",
                "create",
                "--workspace-id",
                args.workspace_id,
                "--name",
                args.pipeline_name,
                "--identifier",
                pipeline_identifier,
                "--registry-metadata",
                json.dumps(pipeline_registry_metadata, ensure_ascii=True),
            ]
            parent_ref = build_parent_ref(args)
            if parent_ref:
                command.extend(["--parent-ref", parent_ref])
            if args.description:
                command.extend(["--description", args.description])
            result = execute_oci(execution, "data_integration", context, "create-pipeline", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            pipeline_manifest = create_di_pipeline(
                context,
                args.workspace_name,
                args.pipeline_name,
                parse_list(args.task),
                extra={
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "pipeline_key": oci_data.get("key"),
                    "identifier": pipeline_identifier,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "description": args.description,
                    "registry_metadata": pipeline_registry_metadata,
                    "parent_ref": json.loads(parent_ref) if parent_ref else None,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_integration",
                "create_pipeline",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"workspace_name": args.workspace_name, "pipeline_manifest": str(pipeline_manifest), "pipeline_key": oci_data.get("key")},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "pipeline_manifest": str(pipeline_manifest),
                        "pipeline_key": oci_data.get("key"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

    if args.command == "create-workspace":
        result = create_di_workspace_metadata(
            context,
            args.workspace_name,
            {
                "description": args.description,
                "compartment_id": args.compartment_id,
                "subnet_id": args.subnet_id,
                "vcn_id": args.vcn_id,
                "wait_for_state": args.wait_for_state,
                "max_wait_seconds": args.max_wait_seconds,
                "wait_interval_seconds": args.wait_interval_seconds,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_integration",
            "create_workspace",
            "mirrored",
            extra={"workspace_name": args.workspace_name, "manifest_path": str(result)},
        )
        print(json.dumps({"status": "ok", "manifest_path": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-project":
        if not args.project_name:
            raise SystemExit("--project-name es requerido para create-project")
        result = create_di_project(
            context,
            args.workspace_name,
            args.project_name,
            {
                "workspace_id": args.workspace_id,
                "identifier": args.identifier or default_identifier(args.project_name),
                "description": args.description,
                "registry_metadata": registry_metadata,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_integration",
            "create_project",
            "mirrored",
            extra={"workspace_name": args.workspace_name, "project_manifest": str(result)},
        )
        print(json.dumps({"status": "ok", "project_manifest": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-folder":
        if not args.folder_name:
            raise SystemExit("--folder-name es requerido para create-folder")
        result = create_di_folder(
            context,
            args.workspace_name,
            args.folder_name,
            {
                "workspace_id": args.workspace_id,
                "identifier": args.identifier or default_identifier(args.folder_name),
                "description": args.description,
                "registry_metadata": registry_metadata,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_integration",
            "create_folder",
            "mirrored",
            extra={"workspace_name": args.workspace_name, "folder_manifest": str(result)},
        )
        print(json.dumps({"status": "ok", "folder_manifest": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-application-from-template":
        if not args.application_name:
            raise SystemExit("--application-name es requerido para create-application-from-template")
        result = create_di_application(
            context,
            args.workspace_name,
            args.application_name,
            {
                "workspace_id": args.workspace_id,
                "identifier": args.identifier or default_identifier(args.application_name),
                "description": args.description,
                "source_application_key": args.template_application_key or args.application_key,
                "copy_type": args.copy_type,
                "registry_metadata": registry_metadata,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_integration",
            "create_application_from_template",
            "mirrored",
            extra={"workspace_name": args.workspace_name, "application_manifest": str(result)},
        )
        print(json.dumps({"status": "ok", "application_manifest": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-task-from-dataflow":
        if not args.task_name or not (args.application_name or args.application_id):
            raise SystemExit("--task-name y --application-name/--application-id son requeridos para create-task-from-dataflow")
        parent_ref = build_parent_ref(args)
        result = create_di_dataflow_task(
            context,
            args.workspace_name,
            args.task_name,
            args.application_name or args.application_id,
            {
                "workspace_id": args.workspace_id,
                "identifier": args.identifier or default_identifier(args.task_name),
                "task_key": args.task_key,
                "description": args.description,
                "registry_metadata": registry_metadata,
                "parent_ref": json.loads(parent_ref) if parent_ref else None,
                "application_id": args.application_id,
                "application_compartment_id": args.application_compartment_id,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_integration",
            "create_task_from_dataflow",
            "mirrored",
            extra={"workspace_name": args.workspace_name, "task_manifest": str(result)},
        )
        print(json.dumps({"status": "ok", "task_manifest": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "list-published-objects":
        result = write_di_published_objects_report(
            context,
            args.workspace_name,
            args.application_name or args.application_key or "application",
            {
                "workspace_id": args.workspace_id,
                "application_key": args.application_key,
                "published_object_count": 0,
                "items": [],
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_integration",
            "list_published_objects",
            "mirrored",
            extra={"workspace_name": args.workspace_name, "report_path": str(result)},
        )
        print(json.dumps({"status": "ok", "report_path": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-task-run":
        task_run_name = args.task_run_name or args.task_name or "di-task-run"
        task_run_key = args.task_run_key or default_identifier(task_run_name)
        result = register_di_task_run(
            context,
            args.workspace_name,
            task_run_name,
            task_run_key,
            {
                "workspace_id": args.workspace_id,
                "application_key": args.application_key,
                "task_key": args.task_key or args.published_object_key,
                "status": "NOT_STARTED",
                "config_provider": json.loads(build_config_provider(args)) if build_config_provider(args) else None,
                "registry_metadata": build_task_run_registry_metadata(args),
            },
            operation="create_task_run",
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_integration",
            "create_task_run",
            "mirrored",
            extra={"workspace_name": args.workspace_name, "task_run_path": str(result), "task_run_key": task_run_key},
        )
        print(json.dumps({"status": "ok", "task_run_path": str(result), "task_run_key": task_run_key, "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "get-task-run":
        result = register_di_task_run(
            context,
            args.workspace_name,
            args.task_run_name or args.task_run_key or "di-task-run",
            args.task_run_key,
            {
                "workspace_id": args.workspace_id,
                "application_key": args.application_key,
                "status": args.state,
            },
            operation="get_task_run",
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_integration",
            "get_task_run",
            args.state.lower(),
            extra={"workspace_name": args.workspace_name, "task_run_path": str(result), "task_run_key": args.task_run_key},
        )
        print(json.dumps({"status": "ok", "task_run_path": str(result), "task_run_key": args.task_run_key, "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "collect-task-run-report":
        metrics = {
            "bytes_read": args.bytes_read,
            "bytes_written": args.bytes_written,
            "rows_in": args.rows_in,
            "rows_out": args.rows_out,
            "rows_rejected": args.rows_rejected,
        }
        result = collect_di_task_run_report(
            context,
            args.workspace_name,
            args.task_name,
            args.pipeline_name,
            {
                "task_run_key": args.task_run_key,
                "state": args.state,
                "service_run_ref": runtime_payload.get("service_run_ref"),
                "workflow_id": runtime_payload.get("workflow_id"),
                "run_id": runtime_payload.get("run_id"),
                "slice_key": runtime_payload.get("slice_key"),
                "metrics": metrics,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_integration",
            "collect_task_run_report",
            args.state.lower(),
            metrics=metrics,
            extra={"workspace_name": args.workspace_name, "report_path": str(result), "task_run_key": args.task_run_key},
        )
        lineage_path = None
        if runtime_payload.get("lineage_enabled") and runtime_payload.get("control_database_name"):
            job_name = args.pipeline_name or args.task_name or args.workspace_name
            lineage_payload = build_openlineage_event(
                runtime_payload,
                "COMPLETE" if args.state.upper() in ("SUCCESS", "SUCCEEDED") else "FAIL",
                job_name,
                inputs=[runtime_payload["source_asset_ref"]] if runtime_payload.get("source_asset_ref") else [],
                outputs=[runtime_payload["target_asset_ref"]] if runtime_payload.get("target_asset_ref") else [],
                event_facets={"dataIntegration": {"state": args.state, "metrics": metrics}},
            )
            lineage_path = queue_lineage_event(
                context,
                runtime_payload["control_database_name"],
                runtime_payload,
                "data_integration_task_run",
                lineage_payload,
            )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "report_path": str(result),
                    "control_paths": control_paths,
                    "lineage_outbox_path": str(lineage_path) if lineage_path else None,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if not args.pipeline_name:
        raise SystemExit("--pipeline-name es requerido para create-pipeline")

    result = create_di_pipeline(context, args.workspace_name, args.pipeline_name, parse_list(args.task))
    control_paths = record_control_runtime(
        context,
        runtime_payload,
        "data_integration",
        "create_pipeline",
        "mirrored",
        extra={"workspace_name": args.workspace_name, "pipeline_manifest": str(result)},
    )
    print(json.dumps({"status": "ok", "pipeline_manifest": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
