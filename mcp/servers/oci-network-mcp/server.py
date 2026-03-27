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
    create_network_nsg,
    create_network_route_table,
    create_network_service_gateway,
    create_network_subnet,
    create_network_vcn,
    export_network_manifest,
    update_network_nsg,
    update_network_route_table,
)
from mcp.common.oci_cli import OciExecutionContext, ensure_service_compartment_id, execute_oci, parse_oci_result_data
from mcp.common.runtime import MirrorContext


COMMAND_ALIASES = {
    "create-vcn": "create_vcn",
    "create_vcn": "create_vcn",
    "create-subnet": "create_subnet",
    "create_subnet": "create_subnet",
    "create-nsg": "create_nsg",
    "create_nsg": "create_nsg",
    "create-route-table": "create_route_table",
    "create_route_table": "create_route_table",
    "create-service-gateway": "create_service_gateway",
    "create_service_gateway": "create_service_gateway",
    "add-nsg-rules": "add_nsg_rules",
    "add_nsg_rules": "add_nsg_rules",
    "update-route-table": "update_route_table",
    "update_route_table": "update_route_table",
    "export-network-manifest": "export_network_manifest",
    "export_network_manifest": "export_network_manifest",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-network-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument("--command", required=True, choices=tuple(COMMAND_ALIASES.keys()))
    parser.add_argument("--compartment-id")
    parser.add_argument("--vcn-name")
    parser.add_argument("--vcn-id")
    parser.add_argument("--subnet-name")
    parser.add_argument("--nsg-name")
    parser.add_argument("--service-gateway-name")
    parser.add_argument("--route-table-name")
    parser.add_argument("--cidr-block", action="append", default=[])
    parser.add_argument("--dns-label")
    parser.add_argument("--route-table-id")
    parser.add_argument("--nsg-id", action="append", default=[])
    parser.add_argument("--service-id", action="append", default=[])
    parser.add_argument("--prohibit-public-ip-on-vnic", default="true")
    parser.add_argument("--description")
    parser.add_argument("--route-rule-json", action="append", default=[])
    parser.add_argument("--security-rules-json")
    args = parser.parse_args()

    canonical_command = COMMAND_ALIASES[args.command]
    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)

    if canonical_command == "export_network_manifest":
        result = export_network_manifest(context)
        print(json.dumps({"status": "ok", "command": canonical_command, "manifest_path": str(result)}, indent=2, ensure_ascii=True))
        return 0

    oci_result: dict[str, object] = {}
    if args.runtime == "oci":
        execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
        if canonical_command == "create_vcn":
            if not args.compartment_id or not args.vcn_name or not args.cidr_block:
                raise SystemExit("--compartment-id, --vcn-name y al menos un --cidr-block son requeridos para create_vcn en runtime oci")
            ensure_service_compartment_id(args.compartment_id)
            command = [
                "network",
                "vcn",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--display-name",
                args.vcn_name,
                "--cidr-blocks",
                json.dumps(args.cidr_block, ensure_ascii=True),
            ]
            if args.dns_label:
                command.extend(["--dns-label", args.dns_label])
            oci_result = execute_oci(execution, "network", context, "create_vcn", command, args.oci_mode)
        elif canonical_command == "create_subnet":
            if not args.compartment_id or not args.vcn_id or not args.subnet_name or not args.cidr_block:
                raise SystemExit("--compartment-id, --vcn-id, --subnet-name y --cidr-block son requeridos para create_subnet en runtime oci")
            ensure_service_compartment_id(args.compartment_id)
            command = [
                "network",
                "subnet",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--vcn-id",
                args.vcn_id,
                "--display-name",
                args.subnet_name,
                "--cidr-block",
                args.cidr_block[0],
                "--prohibit-public-ip-on-vnic",
                args.prohibit_public_ip_on_vnic,
            ]
            if args.dns_label:
                command.extend(["--dns-label", args.dns_label])
            if args.route_table_id:
                command.extend(["--route-table-id", args.route_table_id])
            oci_result = execute_oci(execution, "network", context, "create_subnet", command, args.oci_mode)
        elif canonical_command == "create_nsg":
            if not args.compartment_id or not args.vcn_id or not args.nsg_name:
                raise SystemExit("--compartment-id, --vcn-id y --nsg-name son requeridos para create_nsg en runtime oci")
            ensure_service_compartment_id(args.compartment_id)
            command = [
                "network",
                "nsg",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--vcn-id",
                args.vcn_id,
                "--display-name",
                args.nsg_name,
            ]
            oci_result = execute_oci(execution, "network", context, "create_nsg", command, args.oci_mode)
        elif canonical_command == "add_nsg_rules":
            if not args.nsg_id:
                raise SystemExit("--nsg-id es requerido para add_nsg_rules en runtime oci")
            if not args.security_rules_json:
                raise SystemExit("--security-rules-json es requerido para add_nsg_rules en runtime oci")
            command = [
                "network",
                "nsg",
                "rules",
                "add",
                "--nsg-id",
                args.nsg_id[0],
                "--security-rules",
                json.dumps(json.loads(args.security_rules_json), ensure_ascii=True),
            ]
            oci_result = execute_oci(execution, "network", context, "add_nsg_rules", command, args.oci_mode)
        elif canonical_command == "create_service_gateway":
            if not args.compartment_id or not args.vcn_id or not args.service_gateway_name or not args.service_id:
                raise SystemExit(
                    "--compartment-id, --vcn-id, --service-gateway-name y al menos un --service-id son requeridos para create_service_gateway en runtime oci"
                )
            ensure_service_compartment_id(args.compartment_id)
            command = [
                "network",
                "service-gateway",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--vcn-id",
                args.vcn_id,
                "--display-name",
                args.service_gateway_name,
                "--services",
                json.dumps([{"serviceId": service_id} for service_id in args.service_id], ensure_ascii=True),
            ]
            oci_result = execute_oci(execution, "network", context, "create_service_gateway", command, args.oci_mode)
        elif canonical_command == "update_route_table":
            if not args.route_table_id:
                raise SystemExit("--route-table-id es requerido para update_route_table en runtime oci")
            if not args.route_rule_json:
                raise SystemExit("--route-rule-json es requerido al menos una vez para update_route_table en runtime oci")
            command = [
                "network",
                "route-table",
                "update",
                "--rt-id",
                args.route_table_id,
                "--route-rules",
                json.dumps([json.loads(item) for item in args.route_rule_json], ensure_ascii=True),
                "--force",
            ]
            oci_result = execute_oci(execution, "network", context, "update_route_table", command, args.oci_mode)
        else:
            if not args.compartment_id or not args.vcn_id or not args.route_table_name:
                raise SystemExit("--compartment-id, --vcn-id y --route-table-name son requeridos para create_route_table en runtime oci")
            ensure_service_compartment_id(args.compartment_id)
            command = [
                "network",
                "route-table",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--vcn-id",
                args.vcn_id,
                "--display-name",
                args.route_table_name,
                "--route-rules",
                json.dumps([json.loads(item) for item in args.route_rule_json], ensure_ascii=True) if args.route_rule_json else "[]",
            ]
            oci_result = execute_oci(execution, "network", context, "create_route_table", command, args.oci_mode)

    oci_data = parse_oci_result_data(oci_result) if oci_result else {}

    if canonical_command == "create_vcn":
        if not args.vcn_name or not args.cidr_block:
            raise SystemExit("--vcn-name y al menos un --cidr-block son requeridos para create_vcn")
        result = create_network_vcn(
            context,
            args.vcn_name,
            args.cidr_block,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "compartment_id": args.compartment_id,
                "dns_label": args.dns_label,
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
                    "vcn_id": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if canonical_command == "create_subnet":
        if not args.subnet_name or not args.cidr_block:
            raise SystemExit("--subnet-name y --cidr-block son requeridos para create_subnet")
        result = create_network_subnet(
            context,
            args.subnet_name,
            args.cidr_block[0],
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "compartment_id": args.compartment_id,
                "vcn_name": args.vcn_name,
                "vcn_id": args.vcn_id,
                "dns_label": args.dns_label,
                "route_table_id": args.route_table_id,
                "nsg_ids": args.nsg_id,
                "prohibit_public_ip_on_vnic": args.prohibit_public_ip_on_vnic,
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
                    "subnet_id": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if canonical_command == "create_nsg":
        if not args.nsg_name:
            raise SystemExit("--nsg-name es requerido para create_nsg")
        result = create_network_nsg(
            context,
            args.nsg_name,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "compartment_id": args.compartment_id,
                "vcn_name": args.vcn_name,
                "vcn_id": args.vcn_id,
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
                    "nsg_id": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if canonical_command == "add_nsg_rules":
        nsg_name = args.nsg_name or (args.nsg_id[0] if args.nsg_id else None)
        if not nsg_name:
            raise SystemExit("--nsg-name o --nsg-id es requerido para add_nsg_rules")
        if not args.security_rules_json:
            raise SystemExit("--security-rules-json es requerido para add_nsg_rules")
        security_rules = json.loads(args.security_rules_json)
        result = update_network_nsg(
            context,
            nsg_name,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "compartment_id": args.compartment_id,
                "vcn_name": args.vcn_name,
                "vcn_id": args.vcn_id,
                "nsg_id": args.nsg_id[0] if args.nsg_id else None,
                "security_rules": security_rules,
                "description": args.description,
                "resource_id": args.nsg_id[0] if args.nsg_id else None,
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
                    "nsg_id": args.nsg_id[0] if args.nsg_id else None,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if canonical_command == "create_service_gateway":
        if not args.service_gateway_name or not args.service_id:
            raise SystemExit("--service-gateway-name y al menos un --service-id son requeridos para create_service_gateway")
        result = create_network_service_gateway(
            context,
            args.service_gateway_name,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "compartment_id": args.compartment_id,
                "vcn_name": args.vcn_name,
                "vcn_id": args.vcn_id,
                "service_ids": args.service_id,
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
                    "service_gateway_id": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if canonical_command == "update_route_table":
        route_table_name = args.route_table_name or args.route_table_id
        if not route_table_name:
            raise SystemExit("--route-table-name o --route-table-id es requerido para update_route_table")
        result = update_network_route_table(
            context,
            route_table_name,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "compartment_id": args.compartment_id,
                "vcn_name": args.vcn_name,
                "vcn_id": args.vcn_id,
                "route_table_id": args.route_table_id,
                "route_rules_json": [json.loads(item) for item in args.route_rule_json] if args.route_rule_json else [],
                "description": args.description,
                "resource_id": oci_data.get("id") or args.route_table_id,
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
                    "route_table_id": oci_data.get("id") or args.route_table_id,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if not args.route_table_name:
        raise SystemExit("--route-table-name es requerido para create_route_table")
    result = create_network_route_table(
        context,
        args.route_table_name,
        {
            "runtime": args.runtime,
            "oci_mode": args.oci_mode if args.runtime == "oci" else None,
            "compartment_id": args.compartment_id,
            "vcn_name": args.vcn_name,
            "vcn_id": args.vcn_id,
            "route_rules_json": [json.loads(item) for item in args.route_rule_json] if args.route_rule_json else [],
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
                "route_table_id": oci_data.get("id"),
                "lifecycle_state": oci_data.get("lifecycle-state"),
            },
            indent=2,
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
