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
    create_di_dataflow_task,
    create_di_folder,
    create_di_pipeline,
    create_di_project,
    create_di_workspace_metadata,
)
from mcp.common.oci_cli import OciExecutionContext, execute_oci
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


def build_registry_metadata(args: argparse.Namespace, default_aggregator_key: str | None = None) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    aggregator_key = args.aggregator_key or args.folder_key or default_aggregator_key
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
    parent = args.aggregator_key or args.folder_key
    if parent and args.workspace_id:
        return json.dumps({"parent": parent, "rootDocId": args.workspace_id}, ensure_ascii=True)
    return None


def build_dataflow_application(args: argparse.Namespace) -> str:
    payload: dict[str, str] = {}
    if args.application_id:
        payload["applicationId"] = args.application_id
    if args.application_compartment_id:
        payload["compartmentId"] = args.application_compartment_id
    if not payload and args.application_name:
        payload["name"] = args.application_name
    return json.dumps(payload, ensure_ascii=True)


def add_wait_options(command: list[str], wait_for_state: list[str], max_wait_seconds: int | None, wait_interval_seconds: int | None) -> None:
    for state in wait_for_state:
        command.extend(["--wait-for-state", state])
    if max_wait_seconds is not None:
        command.extend(["--max-wait-seconds", str(max_wait_seconds)])
    if wait_interval_seconds is not None:
        command.extend(["--wait-interval-seconds", str(wait_interval_seconds)])


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
        choices=("create-workspace", "create-project", "create-folder", "create-task-from-dataflow", "create-pipeline"),
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
    parser.add_argument("--registry-version", default="1")
    parser.add_argument("--parent-ref")
    parser.add_argument("--task-name")
    parser.add_argument("--task-key")
    parser.add_argument("--application-name")
    parser.add_argument("--application-id")
    parser.add_argument("--application-compartment-id")
    parser.add_argument("--pipeline-name")
    parser.add_argument("--task", action="append", default=[])
    parser.add_argument("--label", action="append", default=[])
    parser.add_argument("--favorite")
    parser.add_argument("--wait-for-state", action="append", default=[])
    parser.add_argument("--max-wait-seconds", type=int)
    parser.add_argument("--wait-interval-seconds", type=int)
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    registry_metadata = build_registry_metadata(args, default_aggregator_key="PROJECT" if args.command == "create-project" else None)

    if args.runtime == "oci":
        execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
        if args.command == "create-workspace":
            if not args.compartment_id:
                raise SystemExit("--compartment-id es requerido en runtime oci para create-workspace")
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
                    "wait_for_state": args.wait_for_state,
                    "max_wait_seconds": args.max_wait_seconds,
                    "wait_interval_seconds": args.wait_interval_seconds,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            print(json.dumps({"status": "ok", "runtime": "oci", "manifest_path": str(manifest)}, indent=2, ensure_ascii=True))
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
            project_manifest = create_di_project(
                context,
                args.workspace_name,
                args.project_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "identifier": args.identifier or default_identifier(args.project_name),
                    "description": args.description,
                    "registry_metadata": registry_metadata,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            print(json.dumps({"status": "ok", "runtime": "oci", "project_manifest": str(project_manifest)}, indent=2, ensure_ascii=True))
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
            folder_manifest = create_di_folder(
                context,
                args.workspace_name,
                args.folder_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "workspace_id": args.workspace_id,
                    "identifier": args.identifier or default_identifier(args.folder_name),
                    "description": args.description,
                    "registry_metadata": registry_metadata,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            print(json.dumps({"status": "ok", "runtime": "oci", "folder_manifest": str(folder_manifest)}, indent=2, ensure_ascii=True))
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
            if args.task_key:
                command.extend(["--key", args.task_key])
            if args.description:
                command.extend(["--description", args.description])
            if registry_metadata:
                command.extend(["--registry-metadata", json.dumps(registry_metadata, ensure_ascii=True)])
            parent_ref = build_parent_ref(args)
            if parent_ref:
                command.extend(["--parent-ref", parent_ref])
            command.extend(["--dataflow-application", build_dataflow_application(args)])
            result = execute_oci(execution, "data_integration", context, "create-task-from-dataflow", command, args.oci_mode)
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
                    "task_key": args.task_key,
                    "description": args.description,
                    "registry_metadata": registry_metadata,
                    "parent_ref": json.loads(parent_ref) if parent_ref else None,
                    "application_id": args.application_id,
                    "application_compartment_id": args.application_compartment_id,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            print(json.dumps({"status": "ok", "runtime": "oci", "task_manifest": str(task_manifest)}, indent=2, ensure_ascii=True))
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
        print(json.dumps({"status": "ok", "manifest_path": str(result)}, indent=2, ensure_ascii=True))
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
        print(json.dumps({"status": "ok", "project_manifest": str(result)}, indent=2, ensure_ascii=True))
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
        print(json.dumps({"status": "ok", "folder_manifest": str(result)}, indent=2, ensure_ascii=True))
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
        print(json.dumps({"status": "ok", "task_manifest": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if not args.pipeline_name:
        raise SystemExit("--pipeline-name es requerido para create-pipeline")

    result = create_di_pipeline(context, args.workspace_name, args.pipeline_name, parse_list(args.task))
    print(json.dumps({"status": "ok", "pipeline_manifest": str(result)}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
