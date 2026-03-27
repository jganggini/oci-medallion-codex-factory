from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import (
    create_iam_compartment,
    create_iam_dynamic_group,
    create_iam_group,
    create_iam_policy,
    export_iam_manifest,
)
from mcp.common.oci_cli import OciExecutionContext, execute_oci, parse_oci_result_data
from mcp.common.runtime import MirrorContext


COMMAND_ALIASES = {
    "create-compartment": "create_compartment",
    "create_compartment": "create_compartment",
    "create-group": "create_group",
    "create_group": "create_group",
    "create-dynamic-group": "create_dynamic_group",
    "create_dynamic_group": "create_dynamic_group",
    "create-policy": "create_policy",
    "create_policy": "create_policy",
    "export-iam-manifest": "export_iam_manifest",
    "export_iam_manifest": "export_iam_manifest",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-iam-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument("--command", required=True, choices=tuple(COMMAND_ALIASES.keys()))
    parser.add_argument("--compartment-name")
    parser.add_argument("--parent-compartment-id")
    parser.add_argument("--compartment-id")
    parser.add_argument("--group-name")
    parser.add_argument("--dynamic-group-name")
    parser.add_argument("--matching-rule")
    parser.add_argument("--policy-name")
    parser.add_argument("--description")
    parser.add_argument("--statement", action="append", default=[])
    args = parser.parse_args()

    canonical_command = COMMAND_ALIASES[args.command]
    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)

    if canonical_command == "export_iam_manifest":
        result = export_iam_manifest(context)
        print(json.dumps({"status": "ok", "command": canonical_command, "manifest_path": str(result)}, indent=2, ensure_ascii=True))
        return 0

    oci_result: dict[str, object] = {}
    if args.runtime == "oci":
        execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
        if canonical_command == "create_compartment":
            if not args.compartment_name or not args.parent_compartment_id:
                raise SystemExit("--compartment-name y --parent-compartment-id son requeridos para create_compartment en runtime oci")
            command = [
                "iam",
                "compartment",
                "create",
                "--compartment-id",
                args.parent_compartment_id,
                "--name",
                args.compartment_name,
            ]
            if args.description:
                command.extend(["--description", args.description])
            oci_result = execute_oci(execution, "iam", context, "create_compartment", command, args.oci_mode)
        elif canonical_command == "create_group":
            if not args.group_name or not args.compartment_id:
                raise SystemExit("--group-name y --compartment-id son requeridos para create_group en runtime oci")
            command = ["iam", "group", "create", "--name", args.group_name, "--compartment-id", args.compartment_id]
            if args.description:
                command.extend(["--description", args.description])
            oci_result = execute_oci(execution, "iam", context, "create_group", command, args.oci_mode)
        elif canonical_command == "create_dynamic_group":
            if not args.dynamic_group_name or not args.matching_rule:
                raise SystemExit("--dynamic-group-name y --matching-rule son requeridos para create_dynamic_group en runtime oci")
            command = [
                "iam",
                "dynamic-group",
                "create",
                "--name",
                args.dynamic_group_name,
                "--matching-rule",
                args.matching_rule,
            ]
            if args.description:
                command.extend(["--description", args.description])
            oci_result = execute_oci(execution, "iam", context, "create_dynamic_group", command, args.oci_mode)
        else:
            if not args.policy_name or not args.compartment_id or not args.statement:
                raise SystemExit("--policy-name, --compartment-id y --statement son requeridos para create_policy en runtime oci")
            command = [
                "iam",
                "policy",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--name",
                args.policy_name,
                "--statements",
                json.dumps(args.statement, ensure_ascii=True),
            ]
            if args.description:
                command.extend(["--description", args.description])
            oci_result = execute_oci(execution, "iam", context, "create_policy", command, args.oci_mode)

    oci_data = parse_oci_result_data(oci_result) if oci_result else {}

    if canonical_command == "create_compartment":
        if not args.compartment_name:
            raise SystemExit("--compartment-name es requerido para create_compartment")
        result = create_iam_compartment(
            context,
            args.compartment_name,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "parent_compartment_id": args.parent_compartment_id,
                "description": args.description,
                "resource_id": oci_data.get("id"),
                "lifecycle_state": oci_data.get("lifecycle-state"),
                "plan_path": oci_result.get("plan_path"),
                "result_path": oci_result.get("result_path"),
            },
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "command": canonical_command,
                    "manifest_path": str(result),
                    "compartment_id": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if canonical_command == "create_group":
        if not args.group_name:
            raise SystemExit("--group-name es requerido para create_group")
        result = create_iam_group(
            context,
            args.group_name,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "compartment_id": args.compartment_id,
                "description": args.description,
                "resource_id": oci_data.get("id"),
                "lifecycle_state": oci_data.get("lifecycle-state"),
                "plan_path": oci_result.get("plan_path"),
                "result_path": oci_result.get("result_path"),
            },
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "command": canonical_command,
                    "manifest_path": str(result),
                    "group_id": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if canonical_command == "create_dynamic_group":
        if not args.dynamic_group_name or not args.matching_rule:
            raise SystemExit("--dynamic-group-name y --matching-rule son requeridos para create_dynamic_group")
        result = create_iam_dynamic_group(
            context,
            args.dynamic_group_name,
            args.matching_rule,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "description": args.description,
                "resource_id": oci_data.get("id"),
                "lifecycle_state": oci_data.get("lifecycle-state"),
                "plan_path": oci_result.get("plan_path"),
                "result_path": oci_result.get("result_path"),
            },
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "command": canonical_command,
                    "manifest_path": str(result),
                    "dynamic_group_id": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if not args.policy_name or not args.statement:
        raise SystemExit("--policy-name y al menos un --statement son requeridos para create_policy")
    result = create_iam_policy(
        context,
        args.policy_name,
        args.statement,
        {
            "runtime": args.runtime,
            "oci_mode": args.oci_mode if args.runtime == "oci" else None,
            "compartment_id": args.compartment_id,
            "description": args.description,
            "resource_id": oci_data.get("id"),
            "lifecycle_state": oci_data.get("lifecycle-state"),
            "plan_path": oci_result.get("plan_path"),
            "result_path": oci_result.get("result_path"),
        },
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "command": canonical_command,
                "manifest_path": str(result),
                "policy_id": oci_data.get("id"),
                "lifecycle_state": oci_data.get("lifecycle-state"),
            },
            indent=2,
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
