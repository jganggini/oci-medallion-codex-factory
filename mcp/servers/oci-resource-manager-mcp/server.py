from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import create_resource_manager_stack, export_stack_report, register_resource_manager_job
from mcp.common.runtime import MirrorContext


COMMAND_ALIASES = {
    "create-stack": "create_stack",
    "create_stack": "create_stack",
    "plan-stack": "plan_stack",
    "plan_stack": "plan_stack",
    "apply-stack": "apply_stack",
    "apply_stack": "apply_stack",
    "export-stack-report": "export_stack_report",
    "export_stack_report": "export_stack_report",
}


def resolve_optional_file(path_value: str | None, label: str) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value).resolve()
    if not path.exists():
        raise FileNotFoundError(f"No existe {label}: {path}")
    return path


def parse_variables(items: list[str]) -> dict[str, str]:
    payload: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Variable invalida: {item}. Usa key=value.")
        key, value = item.split("=", 1)
        payload[key] = value
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-resource-manager-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--command", required=True, choices=tuple(COMMAND_ALIASES.keys()))
    parser.add_argument("--stack-name")
    parser.add_argument("--compartment-id")
    parser.add_argument("--working-directory")
    parser.add_argument("--description")
    parser.add_argument("--config-source-file")
    parser.add_argument("--variable", action="append", default=[])
    parser.add_argument("--job-id")
    args = parser.parse_args()

    canonical_command = COMMAND_ALIASES[args.command]
    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    config_source_file = resolve_optional_file(args.config_source_file, "el archivo de configuracion del stack")
    variables = parse_variables(args.variable)

    if canonical_command == "export_stack_report":
        result = export_stack_report(context, args.stack_name)
        print(json.dumps({"status": "ok", "command": canonical_command, "report_path": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if not args.stack_name:
        raise SystemExit("--stack-name es requerido para create_stack, plan_stack y apply_stack")

    if canonical_command == "create_stack":
        result = create_resource_manager_stack(
            context,
            args.stack_name,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "compartment_id": args.compartment_id,
                "working_directory": args.working_directory,
                "description": args.description,
                "variables": variables,
            },
            config_source_file=config_source_file,
        )
        print(json.dumps({"status": "ok", "command": canonical_command, "manifest_path": str(result)}, indent=2, ensure_ascii=True))
        return 0

    result = register_resource_manager_job(
        context,
        args.stack_name,
        canonical_command,
        {
            "runtime": args.runtime,
            "oci_mode": args.oci_mode if args.runtime == "oci" else None,
            "compartment_id": args.compartment_id,
            "job_id": args.job_id,
            "working_directory": args.working_directory,
            "description": args.description,
            "variables": variables,
        },
    )
    print(json.dumps({"status": "ok", "command": canonical_command, "job_receipt": str(result)}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
