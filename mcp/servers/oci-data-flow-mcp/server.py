from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import create_data_flow_application, run_data_flow_application
from mcp.common.dataflow_packager import LEGACY_IMAGE, package_dependency_archive, validate_dependency_archive
from mcp.common.oci_cli import OciExecutionContext, execute_oci
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-data-flow-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument("--command", required=True, choices=("package-dependencies", "validate-archive", "create-application", "run-application"))
    parser.add_argument("--application-name", required=True)
    parser.add_argument("--source-dir")
    parser.add_argument("--dependency-root")
    parser.add_argument("--main-file", default="main.py")
    parser.add_argument("--artifact-uri")
    parser.add_argument("--file-uri")
    parser.add_argument("--compartment-id")
    parser.add_argument("--application-id")
    parser.add_argument("--driver-shape", default="VM.Standard.E4.Flex")
    parser.add_argument("--executor-shape", default="VM.Standard.E4.Flex")
    parser.add_argument("--num-executors", type=int, default=1)
    parser.add_argument("--spark-version", default="3.5.0")
    parser.add_argument("--logs-bucket-uri")
    parser.add_argument("--language", default="PYTHON")
    parser.add_argument("--display-name")
    parser.add_argument("--python-version", default="3.11")
    parser.add_argument("--archive-name", default="archive.zip")
    parser.add_argument("--packager-image")
    parser.add_argument("--use-legacy-packager-image", action="store_true")
    parser.add_argument("--parameter", action="append", default=[])
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    parameters = parse_parameters(args.parameter)
    packager_image = LEGACY_IMAGE if args.use_legacy_packager_image else args.packager_image

    if args.command == "package-dependencies":
        if not args.dependency_root:
            raise SystemExit("--dependency-root es requerido para package-dependencies")
        result = package_dependency_archive(
            context=context,
            application_name=args.application_name,
            dependency_root=Path(args.dependency_root),
            python_version=args.python_version,
            image=packager_image,
            archive_name=args.archive_name,
        )
        print(json.dumps({"status": "ok", "command": args.command, **result}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "validate-archive":
        if not args.dependency_root:
            raise SystemExit("--dependency-root es requerido para validate-archive")
        result = validate_dependency_archive(
            dependency_root=Path(args.dependency_root),
            python_version=args.python_version,
            image=packager_image,
            archive_name=args.archive_name,
        )
        print(json.dumps({"status": "ok", "command": args.command, **result}, indent=2, ensure_ascii=True))
        return 0

    if args.runtime == "oci":
        execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
        if args.command == "create-application":
            if not args.compartment_id or not args.file_uri:
                raise SystemExit("--compartment-id y --file-uri son requeridos en runtime oci para create-application")
            dependency_result = None
            if args.dependency_root:
                dependency_result = package_dependency_archive(
                    context=context,
                    application_name=args.application_name,
                    dependency_root=Path(args.dependency_root),
                    python_version=args.python_version,
                    image=packager_image,
                    archive_name=args.archive_name,
                )
            if args.source_dir:
                source_dir = Path(args.source_dir).resolve()
                if not source_dir.exists():
                    raise FileNotFoundError(f"No existe el directorio fuente: {source_dir}")
                archive = create_data_flow_application(
                    context,
                    args.application_name,
                    source_dir,
                    args.main_file,
                    {"artifact_uri": args.artifact_uri, "file_uri": args.file_uri, "runtime": "oci", "oci_mode": args.oci_mode},
                )
            else:
                archive = None

            command = [
                "data-flow",
                "application",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--display-name",
                args.display_name or args.application_name,
                "--driver-shape",
                args.driver_shape,
                "--executor-shape",
                args.executor_shape,
                "--language",
                args.language,
                "--num-executors",
                str(args.num_executors),
                "--spark-version",
                args.spark_version,
                "--file-uri",
                args.file_uri,
            ]
            if args.artifact_uri:
                command.extend(["--archive-uri", args.artifact_uri])
            if args.logs_bucket_uri:
                command.extend(["--logs-bucket-uri", args.logs_bucket_uri])
            result = execute_oci(execution, "data_flow", context, "create-application", command, args.oci_mode)
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "command": args.command,
                        "archive_path": str(archive) if archive else None,
                        "dependency_archive_path": dependency_result["archive_path"] if dependency_result else None,
                        "plan_path": result.get("plan_path"),
                        "result_path": result.get("result_path"),
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if not args.application_id or not args.compartment_id:
            raise SystemExit("--application-id y --compartment-id son requeridos en runtime oci para run-application")
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
            command.extend(["--parameters", json.dumps(to_parameter_array(parameters))])
        if args.logs_bucket_uri:
            command.extend(["--logs-bucket-uri", args.logs_bucket_uri])
        result = execute_oci(execution, "data_flow", context, "run-application", command, args.oci_mode)
        run_report = run_data_flow_application(
            context,
            args.application_name,
            {"parameters": parameters, "runtime": "oci", "oci_mode": args.oci_mode, "application_id": args.application_id},
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "runtime": "oci",
                    "command": args.command,
                    "run_report": str(run_report),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command == "create-application":
        if not args.source_dir:
            raise SystemExit("--source-dir es requerido para create-application")
        dependency_result = None
        if args.dependency_root:
            dependency_result = package_dependency_archive(
                context=context,
                application_name=args.application_name,
                dependency_root=Path(args.dependency_root),
                python_version=args.python_version,
                image=packager_image,
                archive_name=args.archive_name,
            )
        source_dir = Path(args.source_dir).resolve()
        if not source_dir.exists():
            raise FileNotFoundError(f"No existe el directorio fuente: {source_dir}")
        archive = create_data_flow_application(
            context,
            args.application_name,
            source_dir,
            args.main_file,
            {"artifact_uri": args.artifact_uri, "dependency_archive_path": dependency_result["archive_path"] if dependency_result else None},
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "command": args.command,
                    "archive_path": str(archive),
                    "dependency_archive_path": dependency_result["archive_path"] if dependency_result else None,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    run_report = run_data_flow_application(
        context,
        args.application_name,
        {"parameters": parameters},
    )
    print(json.dumps({"status": "ok", "command": args.command, "run_report": str(run_report)}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
