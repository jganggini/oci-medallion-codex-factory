from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import create_di_dataflow_task, create_di_folder, create_di_pipeline, create_di_workspace_metadata
from mcp.common.oci_cli import OciExecutionContext, execute_oci
from mcp.common.runtime import MirrorContext


def parse_list(items: list[str]) -> list[str]:
    return [item for item in items if item]


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-data-integration-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument("--command", required=True, choices=("create-workspace", "create-folder", "create-task-from-dataflow", "create-pipeline"))
    parser.add_argument("--workspace-name", required=True)
    parser.add_argument("--compartment-id")
    parser.add_argument("--workspace-id")
    parser.add_argument("--is-private-network", default="false")
    parser.add_argument("--subnet-id")
    parser.add_argument("--vcn-id")
    parser.add_argument("--folder-name")
    parser.add_argument("--folder-key")
    parser.add_argument("--task-name")
    parser.add_argument("--application-name")
    parser.add_argument("--application-id")
    parser.add_argument("--pipeline-name")
    parser.add_argument("--task", action="append", default=[])
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)

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
            if args.subnet_id:
                command.extend(["--subnet-id", args.subnet_id])
            if args.vcn_id:
                command.extend(["--vcn-id", args.vcn_id])
            result = execute_oci(execution, "data_integration", context, "create-workspace", command, args.oci_mode)
            manifest = create_di_workspace_metadata(
                context,
                args.workspace_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "compartment_id": args.compartment_id,
                    "subnet_id": args.subnet_id,
                    "vcn_id": args.vcn_id,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            print(json.dumps({"status": "ok", "runtime": "oci", "manifest_path": str(manifest)}, indent=2, ensure_ascii=True))
            return 0

        if args.command == "create-task-from-dataflow":
            if not args.workspace_id or not args.task_name or not args.application_id or not args.folder_key:
                raise SystemExit("--workspace-id, --task-name, --application-id y --folder-key son requeridos en runtime oci para create-task-from-dataflow")
            identifier = "".join(ch if ch.isalnum() else "_" for ch in args.task_name.upper())
            registry_metadata = json.dumps({"aggregatorKey": args.folder_key})
            dataflow_application = json.dumps({"applicationId": args.application_id, "name": args.application_name or args.task_name})
            command = [
                "data-integration",
                "task",
                "create-task-from-dataflow-task",
                "--workspace-id",
                args.workspace_id,
                "--name",
                args.task_name,
                "--identifier",
                identifier,
                "--registry-metadata",
                registry_metadata,
                "--dataflow-application",
                dataflow_application,
            ]
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
                    "folder_key": args.folder_key,
                    "application_id": args.application_id,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            print(json.dumps({"status": "ok", "runtime": "oci", "task_manifest": str(task_manifest)}, indent=2, ensure_ascii=True))
            return 0

    if args.command == "create-workspace":
        result = create_di_workspace_metadata(context, args.workspace_name, {})
        print(json.dumps({"status": "ok", "manifest_path": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-folder":
        if not args.folder_name:
            raise SystemExit("--folder-name es requerido para create-folder")
        result = create_di_folder(context, args.workspace_name, args.folder_name)
        print(json.dumps({"status": "ok", "folder_manifest": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-task-from-dataflow":
        if not args.task_name or not args.application_name:
            raise SystemExit("--task-name y --application-name son requeridos para create-task-from-dataflow")
        result = create_di_dataflow_task(context, args.workspace_name, args.task_name, args.application_name, {})
        print(json.dumps({"status": "ok", "task_manifest": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if not args.pipeline_name:
        raise SystemExit("--pipeline-name es requerido para create-pipeline")

    result = create_di_pipeline(context, args.workspace_name, args.pipeline_name, parse_list(args.task))
    print(json.dumps({"status": "ok", "pipeline_manifest": str(result)}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
