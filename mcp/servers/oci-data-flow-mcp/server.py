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

from mcp.common.dataflow_packager import package_dependency_archive, validate_dependency_archive
from mcp.common.local_services import create_data_flow_private_endpoint, collect_data_flow_run_report, run_data_flow_application, write_data_flow_application
from mcp.common.medallion_runtime import (
    add_standard_runtime_args,
    build_openlineage_event,
    queue_lineage_event,
    record_control_runtime,
    runtime_payload_from_args,
)
from mcp.common.oci_cli import OciExecutionContext, ensure_service_compartment_id, execute_oci, parse_oci_result_data
from mcp.common.runtime import MirrorContext


def parse_parameters(items: list[str]) -> dict[str, str]:
    params: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Parametro invalido: {item}. Usa key=value.")
        key, value = item.split("=", 1)
        params[key] = value
    return params


def to_parameter_array(parameters: dict[str, str]) -> list[dict[str, str]]:
    return [{"name": key, "value": value} for key, value in parameters.items()]


def normalize_number(value: float | None) -> int | float | None:
    if value is None:
        return None
    if float(value).is_integer():
        return int(value)
    return value


def parse_shape_config(raw_json: str | None, ocpus: float | None, memory_gbs: float | None) -> dict[str, int | float] | None:
    if raw_json:
        payload = json.loads(raw_json)
        if not isinstance(payload, dict):
            raise ValueError("El shape config debe ser un objeto JSON.")
        return payload

    payload: dict[str, int | float] = {}
    normalized_ocpus = normalize_number(ocpus)
    normalized_memory = normalize_number(memory_gbs)
    if normalized_ocpus is not None:
        payload["ocpus"] = normalized_ocpus
    if normalized_memory is not None:
        payload["memoryInGBs"] = normalized_memory
    return payload or None


def default_flex_shape_config(shape: str | None, payload: dict[str, int | float] | None) -> dict[str, int | float] | None:
    if payload is not None:
        return payload
    if shape and shape.upper().endswith(".FLEX"):
        # OCI Data Flow rejects Flex shapes without an explicit shape config.
        return {"ocpus": 1, "memoryInGBs": 16}
    return payload


def add_wait_options(command: list[str], wait_for_state: list[str], max_wait_seconds: int | None, wait_interval_seconds: int | None) -> None:
    for state in wait_for_state:
        command.extend(["--wait-for-state", state])
    if max_wait_seconds is not None:
        command.extend(["--max-wait-seconds", str(max_wait_seconds)])
    if wait_interval_seconds is not None:
        command.extend(["--wait-interval-seconds", str(wait_interval_seconds)])


def ensure_optional_directory(path_value: str | None, label: str) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value).resolve()
    if not path.exists():
        raise FileNotFoundError(f"No existe el {label}: {path}")
    return path


def ensure_optional_file(path_value: str | None, label: str) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value).resolve()
    if not path.exists():
        raise FileNotFoundError(f"No existe el {label}: {path}")
    return path


def package_dependencies_if_requested(
    context: MirrorContext,
    args: argparse.Namespace,
    dependency_root: Path | None,
    packager_image: str | None,
) -> dict[str, Any] | None:
    if dependency_root is None:
        return None
    return package_dependency_archive(
        context=context,
        application_name=args.application_name,
        dependency_root=dependency_root,
        python_version=args.python_version,
        image=packager_image,
        archive_name=args.archive_name,
    )


def mirror_application(
    context: MirrorContext,
    args: argparse.Namespace,
    source_dir: Path | None,
    from_json_file: Path | None,
    archive_source_file: Path | None,
    dependency_result: dict[str, Any] | None,
) -> dict[str, Path | None]:
    operation = args.command.replace("-", "_")
    driver_shape = args.driver_shape or ("VM.Standard.E4.Flex" if args.command == "create-application" and args.from_json_file is None else None)
    executor_shape = args.executor_shape or ("VM.Standard.E4.Flex" if args.command == "create-application" and args.from_json_file is None else None)
    driver_shape_config = default_flex_shape_config(
        driver_shape,
        parse_shape_config(args.driver_shape_config_json, args.driver_shape_ocpus, args.driver_shape_memory_gbs),
    )
    executor_shape_config = default_flex_shape_config(
        executor_shape,
        parse_shape_config(args.executor_shape_config_json, args.executor_shape_ocpus, args.executor_shape_memory_gbs),
    )
    extra: dict[str, Any] = {
        "runtime": args.runtime,
        "oci_mode": args.oci_mode if args.runtime == "oci" else None,
        "archive_uri": args.archive_uri,
        "file_uri": args.file_uri,
        "logs_bucket_uri": args.logs_bucket_uri,
        "display_name": args.display_name or args.application_name,
        "compartment_id": args.compartment_id,
        "application_id": args.application_id,
        "private_endpoint_id": args.private_endpoint_id,
        "private_endpoint_name": args.private_endpoint_name,
        "subnet_id": args.subnet_id,
        "dns_zones_json": args.dns_zones_json,
        "nsg_ids": args.nsg_id,
        "application_type": args.application_type,
        "driver_shape": driver_shape,
        "driver_shape_config": driver_shape_config,
        "executor_shape": executor_shape,
        "executor_shape_config": executor_shape_config,
        "num_executors": args.num_executors,
        "spark_version": args.spark_version,
        "language": args.language,
        "wait_for_state": args.wait_for_state,
        "max_wait_seconds": args.max_wait_seconds,
        "wait_interval_seconds": args.wait_interval_seconds,
        "force": args.force,
        "dependency_archive_path": dependency_result["archive_path"] if dependency_result else None,
        "dependency_manifest_path": dependency_result["manifest_path"] if dependency_result else None,
    }
    return write_data_flow_application(
        context,
        args.application_name,
        source_dir,
        args.main_file,
        extra,
        json_source_file=from_json_file,
        archive_source_file=archive_source_file,
        operation=operation,
    )


def build_application_command(
    args: argparse.Namespace,
    execution: OciExecutionContext,
    from_json_file: Path | None,
) -> list[str]:
    create_mode = args.command == "create-application"
    command = ["data-flow", "application", "create" if create_mode else "update"]
    if not create_mode:
        if not args.application_id:
            raise SystemExit("--application-id es requerido para update-application")
        command.extend(["--application-id", args.application_id])

    if from_json_file is not None:
        command.extend(["--from-json", f"file://{execution.host_to_container_path(from_json_file)}"])

    if create_mode:
        if not args.compartment_id:
            raise SystemExit("--compartment-id es requerido en runtime oci para create-application")
        ensure_service_compartment_id(args.compartment_id)
        command.extend(["--compartment-id", args.compartment_id])

    if args.display_name or from_json_file is None:
        command.extend(["--display-name", args.display_name or args.application_name])

    driver_shape = args.driver_shape or ("VM.Standard.E4.Flex" if create_mode and from_json_file is None else None)
    executor_shape = args.executor_shape or ("VM.Standard.E4.Flex" if create_mode and from_json_file is None else None)
    language = args.language or ("PYTHON" if create_mode and from_json_file is None else None)
    spark_version = args.spark_version or ("3.5.0" if create_mode and from_json_file is None else None)
    application_type = args.application_type or ("BATCH" if create_mode and from_json_file is None else None)

    if driver_shape:
        command.extend(["--driver-shape", driver_shape])
    driver_shape_config = default_flex_shape_config(
        driver_shape,
        parse_shape_config(args.driver_shape_config_json, args.driver_shape_ocpus, args.driver_shape_memory_gbs),
    )
    if driver_shape_config:
        command.extend(["--driver-shape-config", json.dumps(driver_shape_config, ensure_ascii=True)])

    if executor_shape:
        command.extend(["--executor-shape", executor_shape])
    executor_shape_config = default_flex_shape_config(
        executor_shape,
        parse_shape_config(args.executor_shape_config_json, args.executor_shape_ocpus, args.executor_shape_memory_gbs),
    )
    if executor_shape_config:
        command.extend(["--executor-shape-config", json.dumps(executor_shape_config, ensure_ascii=True)])

    num_executors = args.num_executors if args.num_executors is not None else (1 if create_mode and from_json_file is None else None)
    if num_executors is not None:
        command.extend(["--num-executors", str(num_executors)])
    if application_type:
        command.extend(["--type", application_type])
    if language:
        command.extend(["--language", language])
    if spark_version:
        command.extend(["--spark-version", spark_version])
    if args.file_uri:
        command.extend(["--file-uri", args.file_uri])
    elif create_mode and from_json_file is None:
        raise SystemExit("--file-uri es requerido en runtime oci para create-application cuando no se usa --from-json-file")
    if args.private_endpoint_id:
        command.extend(["--private-endpoint-id", args.private_endpoint_id])
    if args.archive_uri:
        command.extend(["--archive-uri", args.archive_uri])
    if args.logs_bucket_uri:
        command.extend(["--logs-bucket-uri", args.logs_bucket_uri])
    if args.force:
        command.append("--force")
    add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
    return command


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-data-flow-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument(
        "--command",
        required=True,
        choices=("package-dependencies", "validate-archive", "create-private-endpoint", "create-application", "update-application", "run-application", "collect-run-report"),
    )
    parser.add_argument("--application-name")
    parser.add_argument("--private-endpoint-name")
    parser.add_argument("--source-dir")
    parser.add_argument("--dependency-root")
    parser.add_argument("--main-file", default="main.py")
    parser.add_argument("--from-json-file")
    parser.add_argument("--archive-source-file")
    parser.add_argument("--artifact-uri", dest="archive_uri")
    parser.add_argument("--archive-uri", dest="archive_uri")
    parser.add_argument("--file-uri")
    parser.add_argument("--compartment-id")
    parser.add_argument("--subnet-id")
    parser.add_argument("--application-id")
    parser.add_argument("--private-endpoint-id")
    parser.add_argument("--dns-zones-json")
    parser.add_argument("--nsg-id", action="append", default=[])
    parser.add_argument("--driver-shape")
    parser.add_argument("--executor-shape")
    parser.add_argument("--driver-shape-config-json")
    parser.add_argument("--executor-shape-config-json")
    parser.add_argument("--driver-shape-ocpus", type=float)
    parser.add_argument("--driver-shape-memory-gbs", type=float)
    parser.add_argument("--executor-shape-ocpus", type=float)
    parser.add_argument("--executor-shape-memory-gbs", type=float)
    parser.add_argument("--num-executors", type=int)
    parser.add_argument("--spark-version")
    parser.add_argument("--logs-bucket-uri")
    parser.add_argument("--language")
    parser.add_argument("--application-type")
    parser.add_argument("--display-name")
    parser.add_argument("--python-version", default="3.11")
    parser.add_argument("--archive-name", default="archive.zip")
    parser.add_argument("--packager-image")
    parser.add_argument("--parameter", action="append", default=[])
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--wait-for-state", action="append", default=[])
    parser.add_argument("--max-wait-seconds", type=int)
    parser.add_argument("--wait-interval-seconds", type=int)
    parser.add_argument("--state", default="SUCCEEDED")
    parser.add_argument("--driver-log-uri")
    parser.add_argument("--executor-log-uri")
    parser.add_argument("--rows-in", type=int)
    parser.add_argument("--rows-out", type=int)
    parser.add_argument("--rows-rejected", type=int)
    add_standard_runtime_args(parser)
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    runtime_payload = runtime_payload_from_args(args)
    parameters = parse_parameters(args.parameter)
    packager_image = args.packager_image
    dependency_root = ensure_optional_directory(args.dependency_root, "directorio de dependencias")
    source_dir = ensure_optional_directory(args.source_dir, "directorio fuente")
    from_json_file = ensure_optional_file(args.from_json_file, "application json")
    archive_source_file = ensure_optional_file(args.archive_source_file, "archive fuente")

    if args.command == "package-dependencies":
        if not args.application_name:
            raise SystemExit("--application-name es requerido para package-dependencies")
        if dependency_root is None:
            raise SystemExit("--dependency-root es requerido para package-dependencies")
        result = package_dependency_archive(
            context=context,
            application_name=args.application_name,
            dependency_root=dependency_root,
            python_version=args.python_version,
            image=packager_image,
            archive_name=args.archive_name,
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_flow",
            "package_dependencies",
            "mirrored",
            extra={"application_name": args.application_name, "archive_path": result["archive_path"]},
        )
        print(json.dumps({"status": "ok", "command": args.command, **result, "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "validate-archive":
        if not args.application_name:
            raise SystemExit("--application-name es requerido para validate-archive")
        if dependency_root is None:
            raise SystemExit("--dependency-root es requerido para validate-archive")
        result = validate_dependency_archive(
            dependency_root=dependency_root,
            repo_root=context.repo_root,
            python_version=args.python_version,
            image=packager_image,
            archive_name=args.archive_name,
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_flow",
            "validate_archive",
            "mirrored",
            extra={"application_name": args.application_name, "archive_path": result["archive_path"]},
        )
        print(json.dumps({"status": "ok", "command": args.command, **result, "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-private-endpoint":
        if not args.private_endpoint_name:
            raise SystemExit("--private-endpoint-name es requerido para create-private-endpoint")
        if args.runtime == "oci":
            if not args.compartment_id or not args.subnet_id or not args.dns_zones_json:
                raise SystemExit("--compartment-id, --subnet-id y --dns-zones-json son requeridos en runtime oci para create-private-endpoint")
            ensure_service_compartment_id(args.compartment_id)
            execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
            command = [
                "data-flow",
                "private-endpoint",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--display-name",
                args.private_endpoint_name,
                "--subnet-id",
                args.subnet_id,
                "--dns-zones",
                json.dumps(json.loads(args.dns_zones_json), ensure_ascii=True),
            ]
            if args.nsg_id:
                command.extend(["--nsg-ids", json.dumps(args.nsg_id, ensure_ascii=True)])
            add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
            result = execute_oci(execution, "data_flow", context, "create-private-endpoint", command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            manifest = create_data_flow_private_endpoint(
                context,
                args.private_endpoint_name,
                {
                    "runtime": "oci",
                    "oci_mode": args.oci_mode,
                    "compartment_id": args.compartment_id,
                    "subnet_id": args.subnet_id,
                    "nsg_ids": args.nsg_id,
                    "dns_zones": json.loads(args.dns_zones_json),
                    "private_endpoint_id": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_flow",
                "create_private_endpoint",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={
                    "private_endpoint_name": args.private_endpoint_name,
                    "private_endpoint_id": oci_data.get("id"),
                    "private_endpoint_manifest": str(manifest),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "command": args.command,
                        "private_endpoint_manifest": str(manifest),
                        "private_endpoint_id": oci_data.get("id"),
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "plan_path": result.get("plan_path"),
                        "result_path": result.get("result_path"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        manifest = create_data_flow_private_endpoint(
            context,
            args.private_endpoint_name,
            {
                "runtime": "local",
                "subnet_id": args.subnet_id,
                "nsg_ids": args.nsg_id,
                "dns_zones": json.loads(args.dns_zones_json) if args.dns_zones_json else [],
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_flow",
            "create_private_endpoint",
            "mirrored",
            extra={"private_endpoint_name": args.private_endpoint_name, "private_endpoint_manifest": str(manifest)},
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "command": args.command,
                    "private_endpoint_manifest": str(manifest),
                    "control_paths": control_paths,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command in ("create-application", "update-application"):
        if not args.application_name:
            raise SystemExit("--application-name es requerido para create-application y update-application")
        if source_dir is None and from_json_file is None and archive_source_file is None and args.runtime == "local":
            raise SystemExit("Debes informar --source-dir, --from-json-file o --archive-source-file en modo local")

        dependency_result = package_dependencies_if_requested(context, args, dependency_root, packager_image)

        if args.runtime == "oci":
            extra_mounts = tuple({path.parent for path in (from_json_file,) if path is not None})
            execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile, extra_mounts=extra_mounts)
            mirrored = mirror_application(context, args, source_dir, from_json_file, archive_source_file, dependency_result)
            command = build_application_command(args, execution, from_json_file)
            result = execute_oci(execution, "data_flow", context, args.command, command, args.oci_mode)
            oci_data = parse_oci_result_data(result)
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "data_flow",
                args.command.replace("-", "_"),
                "applied" if args.oci_mode == "apply" else "planned",
                extra={
                    "application_name": args.application_name,
                    "application_id": oci_data.get("id"),
                    "private_endpoint_id": args.private_endpoint_id,
                    "manifest_path": str(mirrored["manifest_path"]) if mirrored["manifest_path"] else None,
                    "archive_path": str(mirrored["archive_path"]) if mirrored["archive_path"] else None,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "command": args.command,
                        "application_id": oci_data.get("id"),
                        "private_endpoint_id": args.private_endpoint_id,
                        "lifecycle_state": oci_data.get("lifecycle-state"),
                        "manifest_path": str(mirrored["manifest_path"]) if mirrored["manifest_path"] else None,
                        "archive_path": str(mirrored["archive_path"]) if mirrored["archive_path"] else None,
                        "application_json_path": str(mirrored["application_json_path"]) if mirrored["application_json_path"] else None,
                        "dependency_archive_path": dependency_result["archive_path"] if dependency_result else None,
                        "dependency_manifest_path": dependency_result["manifest_path"] if dependency_result else None,
                        "plan_path": result.get("plan_path"),
                        "result_path": result.get("result_path"),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        mirrored = mirror_application(context, args, source_dir, from_json_file, archive_source_file, dependency_result)
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_flow",
            args.command.replace("-", "_"),
            "mirrored",
            extra={
                "application_name": args.application_name,
                "private_endpoint_id": args.private_endpoint_id,
                "manifest_path": str(mirrored["manifest_path"]) if mirrored["manifest_path"] else None,
                "archive_path": str(mirrored["archive_path"]) if mirrored["archive_path"] else None,
            },
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "command": args.command,
                    "manifest_path": str(mirrored["manifest_path"]) if mirrored["manifest_path"] else None,
                    "private_endpoint_id": args.private_endpoint_id,
                    "archive_path": str(mirrored["archive_path"]) if mirrored["archive_path"] else None,
                    "application_json_path": str(mirrored["application_json_path"]) if mirrored["application_json_path"] else None,
                    "dependency_archive_path": dependency_result["archive_path"] if dependency_result else None,
                    "dependency_manifest_path": dependency_result["manifest_path"] if dependency_result else None,
                    "control_paths": control_paths,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command == "collect-run-report":
        if not args.application_name:
            raise SystemExit("--application-name es requerido para collect-run-report")
        metrics = {
            "rows_in": args.rows_in,
            "rows_out": args.rows_out,
            "rows_rejected": args.rows_rejected,
        }
        report = collect_data_flow_run_report(
            context,
            args.application_name,
            {
                "state": args.state,
                "service_run_ref": runtime_payload.get("service_run_ref"),
                "workflow_id": runtime_payload.get("workflow_id"),
                "run_id": runtime_payload.get("run_id"),
                "slice_key": runtime_payload.get("slice_key"),
                "driver_log_uri": args.driver_log_uri,
                "executor_log_uri": args.executor_log_uri,
                "metrics": metrics,
                "parameters": parameters,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_flow",
            "collect_run_report",
            args.state.lower(),
            metrics=metrics,
            extra={"application_name": args.application_name, "report_path": str(report)},
        )
        lineage_path = None
        if runtime_payload.get("lineage_enabled") and runtime_payload.get("control_database_name"):
            lineage_payload = build_openlineage_event(
                runtime_payload,
                "COMPLETE" if args.state.upper() == "SUCCEEDED" else "FAIL",
                args.application_name,
                inputs=[runtime_payload["source_asset_ref"]] if runtime_payload.get("source_asset_ref") else [],
                outputs=[runtime_payload["target_asset_ref"]] if runtime_payload.get("target_asset_ref") else [],
                event_facets={"dataflow": {"state": args.state, "metrics": metrics}},
            )
            lineage_path = queue_lineage_event(
                context,
                runtime_payload["control_database_name"],
                runtime_payload,
                "data_flow_run",
                lineage_payload,
            )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "command": args.command,
                    "report_path": str(report),
                    "control_paths": control_paths,
                    "lineage_outbox_path": str(lineage_path) if lineage_path else None,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.runtime == "oci":
        if not args.application_name:
            raise SystemExit("--application-name es requerido para run-application")
        execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
        if not args.application_id or not args.compartment_id:
            raise SystemExit("--application-id y --compartment-id son requeridos en runtime oci para run-application")
        ensure_service_compartment_id(args.compartment_id)
        command = [
            "data-flow",
            "run",
            "create",
            "--application-id",
            args.application_id,
            "--compartment-id",
            args.compartment_id,
            "--display-name",
            args.display_name or f"{args.application_name}-run",
        ]
        if parameters:
            command.extend(["--parameters", json.dumps(to_parameter_array(parameters), ensure_ascii=True)])
        if args.logs_bucket_uri:
            command.extend(["--logs-bucket-uri", args.logs_bucket_uri])
        if args.force:
            command.append("--force")
        add_wait_options(command, args.wait_for_state, args.max_wait_seconds, args.wait_interval_seconds)
        result = execute_oci(execution, "data_flow", context, "run-application", command, args.oci_mode)
        oci_data = parse_oci_result_data(result)
        run_report = run_data_flow_application(
            context,
            args.application_name,
            {
                "parameters": parameters,
                "runtime": "oci",
                "oci_mode": args.oci_mode,
                "application_id": args.application_id,
                "service_run_ref": oci_data.get("id"),
                "lifecycle_state": oci_data.get("lifecycle-state"),
                "wait_for_state": args.wait_for_state,
                "max_wait_seconds": args.max_wait_seconds,
                "wait_interval_seconds": args.wait_interval_seconds,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "data_flow",
            "run_application",
            "applied" if args.oci_mode == "apply" else "planned",
                extra={
                    "application_name": args.application_name,
                    "service_run_ref": oci_data.get("id"),
                    "run_report": str(run_report),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "runtime": "oci",
                    "command": args.command,
                    "service_run_ref": oci_data.get("id"),
                    "lifecycle_state": oci_data.get("lifecycle-state"),
                    "run_report": str(run_report),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                    "control_paths": control_paths,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if not args.application_name:
        raise SystemExit("--application-name es requerido para run-application")

    run_report = run_data_flow_application(
        context,
        args.application_name,
        {
            "parameters": parameters,
            "wait_for_state": args.wait_for_state,
            "max_wait_seconds": args.max_wait_seconds,
            "wait_interval_seconds": args.wait_interval_seconds,
        },
    )
    control_paths = record_control_runtime(
        context,
        runtime_payload,
        "data_flow",
        "run_application",
        "mirrored",
        extra={"application_name": args.application_name, "run_report": str(run_report)},
    )
    print(json.dumps({"status": "ok", "command": args.command, "run_report": str(run_report), "control_paths": control_paths}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
