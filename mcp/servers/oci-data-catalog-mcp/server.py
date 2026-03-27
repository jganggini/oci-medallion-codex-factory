from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path
from typing import Any


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import (
    attach_data_catalog_patterns,
    collect_data_catalog_lineage_report,
    create_data_catalog_connection,
    create_data_catalog_job,
    create_data_catalog_job_definition,
    create_data_catalog_manifest,
    create_data_catalog_pattern,
    create_data_catalog_private_endpoint,
    import_openlineage_payload,
    register_data_catalog_asset,
    run_data_catalog_job,
    sync_di_lineage,
)
from mcp.common.medallion_runtime import add_standard_runtime_args, record_control_runtime, runtime_payload_from_args
from mcp.common.oci_cli import OciExecutionContext, ensure_service_compartment_id, execute_oci, parse_oci_result_data
from mcp.common.runtime import MirrorContext, sanitize_name


def parse_json_object(raw_value: str | None, label: str) -> dict[str, Any]:
    if not raw_value:
        return {}
    payload = json.loads(raw_value)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} debe ser un objeto JSON.")
    return payload


def ensure_optional_file(path_value: str | None, label: str) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value).resolve()
    if not path.exists():
        raise FileNotFoundError(f"No existe {label}: {path}")
    return path


def add_wait_options(command: list[str], wait_for_state: list[str], max_wait_seconds: int | None, wait_interval_seconds: int | None) -> None:
    for state in wait_for_state:
        command.extend(["--wait-for-state", state])
    if max_wait_seconds is not None:
        command.extend(["--max-wait-seconds", str(max_wait_seconds)])
    if wait_interval_seconds is not None:
        command.extend(["--wait-interval-seconds", str(wait_interval_seconds)])


def extract_work_request_resource_identifier(oci_data: dict[str, Any], *entity_types: str) -> str | None:
    expected = {item.strip().lower() for item in entity_types if item}
    resources = oci_data.get("resources")
    if not isinstance(resources, list):
        return None
    for item in resources:
        if not isinstance(item, dict):
            continue
        if str(item.get("entity-type", "")).strip().lower() not in expected:
            continue
        identifier = item.get("identifier")
        if identifier:
            return str(identifier)
    return None


def load_lineage_payload(args: argparse.Namespace) -> tuple[str, dict[str, Any], dict[str, Any]]:
    from_json_file = ensure_optional_file(args.from_json_file, "el JSON de entrada")
    lineage_file = ensure_optional_file(args.lineage_file, "el payload OpenLineage")
    outbox_file = ensure_optional_file(args.from_outbox_file, "el evento de outbox")

    if lineage_file is not None:
        payload = json.loads(lineage_file.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("El payload de lineage debe ser un objeto JSON.")
        return lineage_file.stem, payload, {"source_path": str(lineage_file), "source_kind": "lineage_file"}

    if outbox_file is not None:
        wrapped = json.loads(outbox_file.read_text(encoding="utf-8"))
        if not isinstance(wrapped, dict):
            raise ValueError("El evento de outbox debe ser un objeto JSON.")
        payload = wrapped.get("lineage_payload")
        if not isinstance(payload, dict):
            raise ValueError("El outbox debe incluir lineage_payload.")
        return wrapped.get("lineage_event_id", outbox_file.stem), payload, {"source_path": str(outbox_file), "source_kind": "outbox"}

    if from_json_file is not None:
        payload = json.loads(from_json_file.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("El archivo JSON debe ser un objeto.")
        return from_json_file.stem, payload, {"source_path": str(from_json_file), "source_kind": "json_file"}

    raise ValueError("Debes informar --lineage-file, --from-outbox-file o --from-json-file para import-openlineage.")


def summarize_lineage(context: MirrorContext, control_database_name: str | None) -> dict[str, Any]:
    service_root = context.service_root("data_catalog")
    imports_dir = service_root / "lineage" / "imports"
    report: dict[str, Any] = {
        "imports": [],
        "outbox": [],
    }

    if imports_dir.exists():
        for item in sorted(imports_dir.glob("*.json")):
            try:
                payload = json.loads(item.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError):
                continue
            report["imports"].append(
                {
                    "path": str(item),
                    "lineage_name": payload.get("lineage_name"),
                    "source_kind": payload.get("metadata", {}).get("source_kind"),
                }
            )

    if control_database_name:
        outbox_dir = context.service_root("autonomous_database") / sanitize_name(control_database_name) / "control_plane" / "lineage_outbox"
        if outbox_dir.exists():
            for item in sorted(outbox_dir.glob("*.json")):
                try:
                    payload = json.loads(item.read_text(encoding="utf-8"))
                except (FileNotFoundError, json.JSONDecodeError):
                    continue
                report["outbox"].append(
                    {
                        "path": str(item),
                        "event_type": payload.get("event_type"),
                        "status": payload.get("status"),
                        "run_id": payload.get("run_id"),
                        "slice_key": payload.get("slice_key"),
                    }
                )
    report["summary"] = {
        "import_count": len(report["imports"]),
        "outbox_count": len(report["outbox"]),
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-data-catalog-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument(
        "--command",
        required=True,
        choices=(
            "create-catalog",
            "create-private-endpoint",
            "create-data-asset",
            "create-connection",
            "create-harvest-job-definition",
            "create-job",
            "create-pattern",
            "attach-data-selector-patterns",
            "run-harvest-job",
            "sync-di-lineage",
            "import-openlineage",
            "collect-lineage-report",
        ),
    )
    parser.add_argument("--catalog-name", default="data-catalog-medallion")
    parser.add_argument("--catalog-id")
    parser.add_argument("--compartment-id")
    parser.add_argument("--private-endpoint-name")
    parser.add_argument("--private-endpoint-id")
    parser.add_argument("--subnet-id")
    parser.add_argument("--vcn-id")
    parser.add_argument("--dns-zone", action="append", default=[])
    parser.add_argument("--asset-name")
    parser.add_argument("--asset-type-key")
    parser.add_argument("--asset-properties-json")
    parser.add_argument("--data-asset-key")
    parser.add_argument("--connection-name")
    parser.add_argument("--connection-type-key")
    parser.add_argument("--connection-key")
    parser.add_argument("--connection-properties-json")
    parser.add_argument("--job-name")
    parser.add_argument("--job-key")
    parser.add_argument("--job-definition-key")
    parser.add_argument("--job-type", default="HARVEST")
    parser.add_argument("--job-properties-json")
    parser.add_argument("--pattern-name")
    parser.add_argument("--pattern-key", action="append", default=[])
    parser.add_argument("--pattern-expression")
    parser.add_argument("--pattern-file-path-prefix")
    parser.add_argument("--pattern-description")
    parser.add_argument("--workspace-name")
    parser.add_argument("--lineage-file")
    parser.add_argument("--from-outbox-file")
    parser.add_argument("--from-json-file")
    parser.add_argument("--lineage-name")
    parser.add_argument("--wait-for-state", action="append", default=[])
    parser.add_argument("--max-wait-seconds", type=int)
    parser.add_argument("--wait-interval-seconds", type=int)
    add_standard_runtime_args(parser)
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    runtime_payload = runtime_payload_from_args(args)
    asset_properties = parse_json_object(args.asset_properties_json, "asset-properties-json")
    connection_properties = parse_json_object(args.connection_properties_json, "connection-properties-json")
    job_properties = parse_json_object(args.job_properties_json, "job-properties-json")
    from_json_file = ensure_optional_file(args.from_json_file, "el JSON de entrada")

    if args.runtime == "oci":
        extra_mounts = tuple({path.parent for path in (from_json_file,) if path is not None})
        execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile, extra_mounts=extra_mounts)

        if args.command == "create-catalog":
            if not args.compartment_id:
                raise SystemExit("--compartment-id es requerido para create-catalog en runtime oci")
            ensure_service_compartment_id(args.compartment_id)
            command = [
                "data-catalog",
                "catalog",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--display-name",
                args.catalog_name,
            ]
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_catalog", context, "create-catalog", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            catalog_id = extract_work_request_resource_identifier(oci_data, "catalog", "datacatalog") or oci_data.get("id")
            manifest = create_data_catalog_manifest(
                context,
                args.catalog_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "compartment_id": args.compartment_id,
                    "catalog_id": catalog_id,
                    "work_request_id": oci_data.get("id") if str(oci_data.get("id", "")).startswith("ocid1.coreservicesworkrequest.") else None,
                    "lifecycle_state": oci_data.get("lifecycle-state") or oci_data.get("status"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                "create_catalog",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"catalog_manifest": str(manifest), "catalog_id": catalog_id},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "catalog_manifest": str(manifest),
                        "catalog_id": catalog_id,
                        "lifecycle_state": oci_data.get("lifecycle-state") or oci_data.get("status"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-private-endpoint":
            if not args.catalog_id or not args.subnet_id or not args.private_endpoint_name:
                raise SystemExit("--catalog-id, --subnet-id y --private-endpoint-name son requeridos para create-private-endpoint en runtime oci")
            command = [
                "data-catalog",
                "catalog-private-endpoint",
                "create",
                "--catalog-id",
                args.catalog_id,
                "--display-name",
                args.private_endpoint_name,
                "--subnet-id",
                args.subnet_id,
            ]
            if args.dns_zone:
                command.extend(["--dns-zones", json.dumps(args.dns_zone, ensure_ascii=True)])
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_catalog", context, "create-private-endpoint", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            manifest = create_data_catalog_private_endpoint(
                context,
                args.private_endpoint_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "catalog_id": args.catalog_id,
                    "subnet_id": args.subnet_id,
                    "vcn_id": args.vcn_id,
                    "private_endpoint_id": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "dns_zones": args.dns_zone,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                "create_private_endpoint",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"private_endpoint_manifest": str(manifest), "private_endpoint_id": oci_data.get("id")},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "private_endpoint_manifest": str(manifest),
                        "private_endpoint_id": oci_data.get("id"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-data-asset":
            if not args.catalog_id or not args.asset_name or not args.asset_type_key:
                raise SystemExit("--catalog-id, --asset-name y --asset-type-key son requeridos para create-data-asset en runtime oci")
            command = ["data-catalog", "data-asset", "create", "--catalog-id", args.catalog_id]
            if from_json_file is not None:
                command.extend(["--from-json", f"file://{execution.host_to_container_path(from_json_file)}"])
            else:
                command.extend(["--display-name", args.asset_name, "--type-key", args.asset_type_key])
                if asset_properties:
                    command.extend(["--properties", json.dumps(asset_properties, ensure_ascii=True)])
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_catalog", context, "create-data-asset", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            manifest = register_data_catalog_asset(
                context,
                args.asset_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "catalog_id": args.catalog_id,
                    "data_asset_key": oci_data.get("key"),
                    "asset_type_key": args.asset_type_key,
                    "asset_properties": asset_properties,
                    "private_endpoint_id": args.private_endpoint_id,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                "create_data_asset",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"asset_manifest": str(manifest), "data_asset_key": oci_data.get("key")},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "asset_manifest": str(manifest),
                        "data_asset_key": oci_data.get("key"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-connection":
            if not args.catalog_id or not args.connection_name or not args.connection_type_key or not args.data_asset_key:
                raise SystemExit("--catalog-id, --connection-name, --connection-type-key y --data-asset-key son requeridos para create-connection en runtime oci")
            command = ["data-catalog", "connection", "create", "--catalog-id", args.catalog_id]
            if from_json_file is not None:
                command.extend(["--from-json", f"file://{execution.host_to_container_path(from_json_file)}"])
            else:
                command.extend(
                    [
                        "--display-name",
                        args.connection_name,
                        "--type-key",
                        args.connection_type_key,
                        "--data-asset-key",
                        args.data_asset_key,
                    ]
                )
                if connection_properties:
                    command.extend(["--properties", json.dumps(connection_properties, ensure_ascii=True)])
                if args.private_endpoint_id:
                    command.extend(["--catalog-private-endpoint-id", args.private_endpoint_id])
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_catalog", context, "create-connection", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            manifest = create_data_catalog_connection(
                context,
                args.connection_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "catalog_id": args.catalog_id,
                    "data_asset_key": args.data_asset_key,
                    "connection_key": oci_data.get("key"),
                    "connection_type_key": args.connection_type_key,
                    "connection_properties": connection_properties,
                    "private_endpoint_id": args.private_endpoint_id,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                "create_connection",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"connection_manifest": str(manifest), "connection_key": oci_data.get("key")},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "connection_manifest": str(manifest),
                        "connection_key": oci_data.get("key"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-harvest-job-definition":
            if not args.catalog_id or not args.job_name:
                raise SystemExit("--catalog-id y --job-name son requeridos para create-harvest-job-definition en runtime oci")
            command = ["data-catalog", "job-definition", "create", "--catalog-id", args.catalog_id]
            if from_json_file is not None:
                command.extend(["--from-json", f"file://{execution.host_to_container_path(from_json_file)}"])
            else:
                command.extend(["--display-name", args.job_name, "--job-type", args.job_type])
                if args.connection_key:
                    command.extend(["--connection-key", args.connection_key])
                if args.data_asset_key:
                    command.extend(["--data-asset-key", args.data_asset_key])
                if job_properties:
                    command.extend(["--properties", json.dumps(job_properties, ensure_ascii=True)])
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_catalog", context, "create-harvest-job-definition", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            manifest = create_data_catalog_job_definition(
                context,
                args.job_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "catalog_id": args.catalog_id,
                    "job_definition_key": oci_data.get("key"),
                    "job_type": args.job_type,
                    "connection_key": args.connection_key,
                    "data_asset_key": args.data_asset_key,
                    "job_properties": job_properties,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                "create_harvest_job_definition",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"job_definition_manifest": str(manifest), "job_definition_key": oci_data.get("key")},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "job_definition_manifest": str(manifest),
                        "job_definition_key": oci_data.get("key"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-job":
            if not args.catalog_id or not args.job_name or not args.job_definition_key:
                raise SystemExit("--catalog-id, --job-name y --job-definition-key son requeridos para create-job en runtime oci")
            command = ["data-catalog", "job", "create", "--catalog-id", args.catalog_id]
            if from_json_file is not None:
                command.extend(["--from-json", f"file://{execution.host_to_container_path(from_json_file)}"])
            else:
                command.extend(
                    [
                        "--display-name",
                        args.job_name,
                        "--job-definition-key",
                        args.job_definition_key,
                    ]
                )
                if args.connection_key:
                    command.extend(["--connection-key", args.connection_key])
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_catalog", context, "create-job", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            manifest = create_data_catalog_job(
                context,
                args.job_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "catalog_id": args.catalog_id,
                    "job_key": oci_data.get("key"),
                    "job_definition_key": args.job_definition_key,
                    "connection_key": args.connection_key,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                "create_job",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"job_manifest": str(manifest), "job_key": oci_data.get("key")},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "job_manifest": str(manifest),
                        "job_key": oci_data.get("key"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "create-pattern":
            if not args.catalog_id or not args.pattern_name:
                raise SystemExit("--catalog-id y --pattern-name son requeridos para create-pattern en runtime oci")
            if not from_json_file and not args.pattern_expression and not args.pattern_file_path_prefix:
                raise SystemExit("--pattern-expression o --pattern-file-path-prefix son requeridos para create-pattern en runtime oci")
            command = ["data-catalog", "pattern", "create", "--catalog-id", args.catalog_id]
            if from_json_file is not None:
                command.extend(["--from-json", f"file://{execution.host_to_container_path(from_json_file)}"])
            else:
                command.extend(["--display-name", args.pattern_name])
                if args.pattern_description:
                    command.extend(["--description", args.pattern_description])
                if args.pattern_expression:
                    command.extend(["--expression", args.pattern_expression])
                if args.pattern_file_path_prefix:
                    command.extend(["--file-path-prefix", args.pattern_file_path_prefix])
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_catalog", context, "create-pattern", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            manifest = create_data_catalog_pattern(
                context,
                args.pattern_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "catalog_id": args.catalog_id,
                    "pattern_key": oci_data.get("key"),
                    "pattern_expression": args.pattern_expression,
                    "pattern_file_path_prefix": args.pattern_file_path_prefix,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                "create_pattern",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"pattern_manifest": str(manifest), "pattern_key": oci_data.get("key")},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "pattern_manifest": str(manifest),
                        "pattern_key": oci_data.get("key"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "attach-data-selector-patterns":
            if not args.catalog_id or not args.data_asset_key or not args.pattern_key:
                raise SystemExit("--catalog-id, --data-asset-key y al menos un --pattern-key son requeridos para attach-data-selector-patterns en runtime oci")
            command = [
                "data-catalog",
                "data-asset",
                "add-data-selector-patterns",
                "--catalog-id",
                args.catalog_id,
                "--data-asset-key",
                args.data_asset_key,
                "--items",
                json.dumps(args.pattern_key, ensure_ascii=True),
            ]
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_catalog", context, "attach-data-selector-patterns", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            manifest = attach_data_catalog_patterns(
                context,
                args.asset_name or args.data_asset_key,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "catalog_id": args.catalog_id,
                    "data_asset_key": args.data_asset_key,
                    "pattern_keys": args.pattern_key,
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                "attach_data_selector_patterns",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"attachment_manifest": str(manifest), "data_asset_key": args.data_asset_key, "pattern_keys": args.pattern_key},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "attachment_manifest": str(manifest),
                        "data_asset_key": args.data_asset_key,
                        "pattern_keys": args.pattern_key,
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command in ("run-harvest-job", "sync-di-lineage"):
            if not args.catalog_id or not (args.job_key or args.job_definition_key):
                raise SystemExit("--catalog-id y --job-key/--job-definition-key son requeridos para run-harvest-job o sync-di-lineage en runtime oci")
            job_key = args.job_key
            created_job_manifest: Path | None = None
            if not job_key and args.job_definition_key:
                derived_job_name = args.job_name or f"{args.workspace_name or args.command}-job"
                create_job_command = [
                    "data-catalog",
                    "job",
                    "create",
                    "--catalog-id",
                    args.catalog_id,
                    "--display-name",
                    derived_job_name,
                    "--job-definition-key",
                    args.job_definition_key,
                ]
                if args.connection_key:
                    create_job_command.extend(["--connection-key", args.connection_key])
                create_job_result = execute_oci(execution, "data_catalog", context, "create-job", create_job_command, args.oci_mode)
                create_job_oci_data = parse_oci_result_data(create_job_result)
                job_key = create_job_oci_data.get("key")
                created_job_manifest = create_data_catalog_job(
                    context,
                    derived_job_name,
                    {
                        "runtime": "oci",
                        "oci_mode": args.oci_mode,
                        "catalog_id": args.catalog_id,
                        "job_key": job_key,
                        "job_definition_key": args.job_definition_key,
                        "connection_key": args.connection_key,
                        "lifecycle_state": create_job_oci_data.get("lifecycle-state"),
                        "plan_path": create_job_result.get("plan_path"),
                        "result_path": create_job_result.get("result_path"),
                        "created_for_command": args.command,
                    },
                )
            if not job_key:
                raise SystemExit("No se pudo resolver --job-key para ejecutar el harvest.")
            command = ["data-catalog", "job-execution", "create", "--catalog-id", args.catalog_id, "--job-key", job_key]
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_catalog", context, args.command, command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            job_execution_id = oci_data.get("id") or oci_data.get("key")
            job_execution_state = oci_data.get("lifecycle-state")
            job_execution_error_code = oci_data.get("error-code")
            job_execution_error_message = oci_data.get("error-message")
            if args.oci_mode == "apply" and job_execution_id:
                status_result = execute_oci(
                    execution,
                    "data_catalog",
                    context,
                    f"{args.command}-status",
                    [
                        "data-catalog",
                        "job-execution",
                        "get",
                        "--catalog-id",
                        args.catalog_id,
                        "--job-key",
                        job_key,
                        "--job-execution-key",
                        str(job_execution_id),
                    ],
                    "apply",
                )
                status_data = parse_oci_result_data(status_result)
                job_execution_state = status_data.get("lifecycle-state") or job_execution_state
                job_execution_error_code = status_data.get("error-code") or job_execution_error_code
                job_execution_error_message = status_data.get("error-message") or job_execution_error_message
            manifest = run_data_catalog_job(
                context,
                args.job_name or args.workspace_name or args.command,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "catalog_id": args.catalog_id,
                    "job_execution_id": job_execution_id,
                    "job_key": job_key,
                    "job_definition_key": args.job_definition_key,
                    "workspace_name": args.workspace_name,
                    "created_job_manifest": str(created_job_manifest) if created_job_manifest else None,
                    "lifecycle_state": job_execution_state,
                    "error_code": job_execution_error_code,
                    "error_message": job_execution_error_message,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            if str(job_execution_state).upper() == "FAILED":
                raise RuntimeError(
                    json.dumps(
                        {
                            "message": "El job execution de Data Catalog termino en FAILED",
                            "job_key": job_key,
                            "job_execution_id": job_execution_id,
                            "error_code": job_execution_error_code,
                            "error_message": job_execution_error_message,
                            "job_manifest": str(manifest),
                        },
                        ensure_ascii=True,
                    )
                )
            if args.command == "sync-di-lineage" and args.workspace_name:
                sync_manifest = sync_di_lineage(
                    context,
                    args.workspace_name,
                    {
                        "runtime": "oci",
                        "oci_mode": args.oci_mode,
                        "catalog_id": args.catalog_id,
                        "job_key": args.job_key,
                        "job_definition_key": args.job_definition_key,
                    },
                )
            else:
                sync_manifest = None
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                args.command.replace("-", "_"),
                "applied" if args.oci_mode == "apply" else "planned",
                extra={
                    "job_manifest": str(manifest),
                    "job_execution_id": job_execution_id,
                    "sync_manifest": str(sync_manifest) if sync_manifest else None,
                },
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "job_manifest": str(manifest),
                        "job_key": job_key,
                        "job_execution_id": job_execution_id,
                        "lifecycle_state": job_execution_state,
                        "sync_manifest": str(sync_manifest) if sync_manifest else None,
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if args.command == "import-openlineage":
            if not args.catalog_id or not args.data_asset_key:
                raise SystemExit("--catalog-id y --data-asset-key son requeridos para import-openlineage en runtime oci")
            lineage_name, payload, metadata = load_lineage_payload(args)
            payload_base64 = base64.b64encode(
                json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
            ).decode("ascii")
            command = [
                "data-catalog",
                "data-asset",
                "import-lineage",
                "--catalog-id",
                args.catalog_id,
                "--data-asset-key",
                args.data_asset_key,
                "--lineage-payload",
                payload_base64,
            ]
            result = execute_oci(execution, "data_catalog", context, "import-openlineage", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            manifest = import_openlineage_payload(
                context,
                args.lineage_name or lineage_name,
                payload,
                {
                    **metadata,
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "catalog_id": args.catalog_id,
                    "data_asset_key": args.data_asset_key,
                    "import_status": oci_data.get("lifecycle-state") or oci_data.get("status"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_catalog",
                "import_openlineage",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"import_manifest": str(manifest), "lineage_name": args.lineage_name or lineage_name},
            )
            print(json.dumps({"status": "ok", "import_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
            return 0

    if args.command == "create-catalog":
        manifest = create_data_catalog_manifest(context, args.catalog_name, {"compartment_id": args.compartment_id})
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "create_catalog", "mirrored", extra={"catalog_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "catalog_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-private-endpoint":
        if not args.private_endpoint_name:
            raise SystemExit("--private-endpoint-name es requerido para create-private-endpoint")
        manifest = create_data_catalog_private_endpoint(
            context,
            args.private_endpoint_name,
            {
                "catalog_id": args.catalog_id,
                "subnet_id": args.subnet_id,
                "vcn_id": args.vcn_id,
                "dns_zones": args.dns_zone,
            },
        )
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "create_private_endpoint", "mirrored", extra={"private_endpoint_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "private_endpoint_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-data-asset":
        if not args.asset_name:
            raise SystemExit("--asset-name es requerido para create-data-asset")
        manifest = register_data_catalog_asset(
            context,
            args.asset_name,
            {
                "catalog_id": args.catalog_id,
                "asset_type_key": args.asset_type_key,
                "asset_properties": asset_properties,
                "private_endpoint_id": args.private_endpoint_id,
            },
        )
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "create_data_asset", "mirrored", extra={"asset_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "asset_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-connection":
        if not args.connection_name:
            raise SystemExit("--connection-name es requerido para create-connection")
        manifest = create_data_catalog_connection(
            context,
            args.connection_name,
            {
                "catalog_id": args.catalog_id,
                "connection_type_key": args.connection_type_key,
                "data_asset_key": args.data_asset_key,
                "connection_properties": connection_properties,
                "private_endpoint_id": args.private_endpoint_id,
            },
        )
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "create_connection", "mirrored", extra={"connection_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "connection_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-harvest-job-definition":
        if not args.job_name:
            raise SystemExit("--job-name es requerido para create-harvest-job-definition")
        manifest = create_data_catalog_job_definition(
            context,
            args.job_name,
            {
                "catalog_id": args.catalog_id,
                "job_type": args.job_type,
                "connection_key": args.connection_key,
                "data_asset_key": args.data_asset_key,
                "job_properties": job_properties,
            },
        )
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "create_harvest_job_definition", "mirrored", extra={"job_definition_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "job_definition_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-job":
        if not args.job_name:
            raise SystemExit("--job-name es requerido para create-job")
        manifest = create_data_catalog_job(
            context,
            args.job_name,
            {
                "catalog_id": args.catalog_id,
                "job_definition_key": args.job_definition_key,
                "connection_key": args.connection_key,
            },
        )
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "create_job", "mirrored", extra={"job_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "job_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-pattern":
        if not args.pattern_name:
            raise SystemExit("--pattern-name es requerido para create-pattern")
        manifest = create_data_catalog_pattern(
            context,
            args.pattern_name,
            {
                "catalog_id": args.catalog_id,
                "pattern_expression": args.pattern_expression,
                "pattern_file_path_prefix": args.pattern_file_path_prefix,
            },
        )
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "create_pattern", "mirrored", extra={"pattern_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "pattern_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "attach-data-selector-patterns":
        if not args.pattern_key:
            raise SystemExit("Debes informar al menos un --pattern-key para attach-data-selector-patterns")
        manifest = attach_data_catalog_patterns(
            context,
            args.asset_name or args.data_asset_key or "data-asset",
            {
                "catalog_id": args.catalog_id,
                "data_asset_key": args.data_asset_key,
                "pattern_keys": args.pattern_key,
            },
        )
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "attach_data_selector_patterns", "mirrored", extra={"attachment_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "attachment_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "run-harvest-job":
        manifest = run_data_catalog_job(
            context,
            args.job_name or args.job_key or "harvest-job",
            {
                "catalog_id": args.catalog_id,
                "job_key": args.job_key,
                "job_definition_key": args.job_definition_key,
            },
        )
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "run_harvest_job", "mirrored", extra={"job_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "job_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "sync-di-lineage":
        if not args.workspace_name:
            raise SystemExit("--workspace-name es requerido para sync-di-lineage")
        manifest = sync_di_lineage(
            context,
            args.workspace_name,
            {
                "catalog_id": args.catalog_id,
                "job_key": args.job_key,
                "job_definition_key": args.job_definition_key,
            },
        )
        control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "sync_di_lineage", "mirrored", extra={"sync_manifest": str(manifest)})
        print(json.dumps({"status": "ok", "sync_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "import-openlineage":
        lineage_name, payload, metadata = load_lineage_payload(args)
        manifest = import_openlineage_payload(
            context,
            args.lineage_name or lineage_name,
            payload,
            {
                **metadata,
                "catalog_id": args.catalog_id,
                "data_asset_key": args.data_asset_key,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_catalog",
            "import_openlineage",
            "mirrored",
            extra={"import_manifest": str(manifest), "lineage_name": args.lineage_name or lineage_name},
        )
        print(json.dumps({"status": "ok", "import_manifest": str(manifest), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    report_name = args.lineage_name or args.workspace_name or args.catalog_name
    payload = summarize_lineage(context, runtime_payload.get("control_database_name"))
    manifest = collect_data_catalog_lineage_report(context, report_name, payload)
    control_paths = record_control_runtime(context, runtime_payload, "data_catalog", "collect_lineage_report", "mirrored", extra={"report_manifest": str(manifest)})
    print(json.dumps({"status": "ok", "report_manifest": str(manifest), "summary": payload["summary"], "control_paths": control_paths}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
