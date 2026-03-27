from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import create_bucket_manifest, sync_bucket_manifest, upload_object_to_bucket
from mcp.common.medallion_runtime import add_standard_runtime_args, parse_bool_string, record_control_runtime, runtime_payload_from_args
from mcp.common.oci_cli import OciExecutionContext, ensure_service_compartment_id, execute_oci, parse_oci_result_data
from mcp.common.runtime import MirrorContext, ensure_directory, sanitize_name, write_json


def default_par_expiry() -> str:
    return (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-object-storage-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument("--command", required=True, choices=("create-bucket", "upload-object", "sync-bucket-manifest", "create-par"))
    parser.add_argument("--bucket-name", required=True)
    parser.add_argument("--display-name")
    parser.add_argument("--storage-tier", default="Standard")
    parser.add_argument("--compartment-id")
    parser.add_argument("--namespace-name")
    parser.add_argument("--source-file")
    parser.add_argument("--object-name")
    parser.add_argument("--par-name")
    parser.add_argument("--access-type", default="ObjectRead")
    parser.add_argument("--time-expires")
    parser.add_argument("--managed-by-factory", default="true")
    parser.add_argument("--ingestion-outside-flow", default="false")
    parser.add_argument("--bucket-purpose")
    parser.add_argument("--existing-state", default="new")
    add_standard_runtime_args(parser)
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    runtime_payload = runtime_payload_from_args(args)
    bucket_metadata = {
        "display_name": args.display_name or args.bucket_name,
        "storage_tier": args.storage_tier,
        "layer": runtime_payload.get("layer"),
        "managed_by_factory": parse_bool_string(args.managed_by_factory, default=True),
        "ingestion_outside_flow": parse_bool_string(args.ingestion_outside_flow, default=False),
        "bucket_purpose": args.bucket_purpose,
        "existing_state": args.existing_state,
        "source_asset_ref": runtime_payload.get("source_asset_ref"),
        "target_asset_ref": runtime_payload.get("target_asset_ref"),
    }

    if args.runtime == "oci":
        if args.command in ("create-bucket", "sync-bucket-manifest", "create-par"):
            execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
            if args.command == "create-bucket" and not args.compartment_id:
                raise SystemExit("--compartment-id es requerido en runtime oci para create-bucket")
            if args.command == "create-bucket":
                ensure_service_compartment_id(args.compartment_id)
                command = [
                    "os",
                    "bucket",
                    "create",
                    "--name",
                    args.bucket_name,
                    "--compartment-id",
                    args.compartment_id,
                    "--storage-tier",
                    args.storage_tier,
                ]
                if args.display_name:
                    command.extend(["--metadata", json.dumps({"display_name": args.display_name})])
                if args.namespace_name:
                    command.extend(["--namespace-name", args.namespace_name])
                result = execute_oci(execution, "buckets", context, "create-bucket", command, args.oci_mode)
                manifest = create_bucket_manifest(
                    context,
                    args.bucket_name,
                    {
                        **bucket_metadata,
                        "oci_mode": args.oci_mode,
                        "oci_profile": args.oci_profile,
                        "compartment_id": args.compartment_id,
                        "namespace_name": args.namespace_name,
                        "plan_path": result.get("plan_path"),
                        "result_path": result.get("result_path"),
                    },
                )
                control_paths = record_control_runtime(
                    context,
                    runtime_payload,
                    "buckets",
                    "create_bucket",
                    "applied" if args.oci_mode == "apply" else "planned",
                    extra={"bucket_name": args.bucket_name, "manifest_path": str(manifest)},
                )
                print(
                    json.dumps(
                        {
                            "status": "ok",
                            "runtime": "oci",
                            "command": args.command,
                            "manifest_path": str(manifest),
                            "control_paths": control_paths,
                        },
                        indent=2,
                        ensure_ascii=True,
                    )
                )
                return 0

            if args.command == "create-par":
                par_name = args.par_name or f"{args.bucket_name}-{sanitize_name(args.object_name or 'bucket')}-par"
                time_expires = args.time_expires or default_par_expiry()
                command = [
                    "os",
                    "preauth-request",
                    "create",
                    "--bucket-name",
                    args.bucket_name,
                    "--name",
                    par_name,
                    "--access-type",
                    args.access_type,
                    "--time-expires",
                    time_expires,
                ]
                if args.namespace_name:
                    command.extend(["--namespace-name", args.namespace_name])
                if args.object_name:
                    command.extend(["--object-name", args.object_name])
                result = execute_oci(execution, "buckets", context, "create-preauthenticated-request", command, args.oci_mode)
                oci_data = parse_oci_result_data(result)
                manifest_path = context.bucket_root(args.bucket_name) / "preauth" / f"{sanitize_name(par_name)}.json"
                ensure_directory(manifest_path.parent)
                write_json(
                    manifest_path,
                    {
                        "bucket_name": args.bucket_name,
                        "namespace_name": args.namespace_name,
                        "object_name": args.object_name,
                        "par_name": par_name,
                        "access_type": args.access_type,
                        "time_expires": time_expires,
                        "access_uri": oci_data.get("access-uri"),
                        "id": oci_data.get("id"),
                        "runtime": "oci",
                        "oci_mode": args.oci_mode,
                        "plan_path": result.get("plan_path"),
                        "result_path": result.get("result_path"),
                    },
                )
                control_paths = record_control_runtime(
                    context,
                    runtime_payload,
                    "buckets",
                    "create_preauthenticated_request",
                    "applied" if args.oci_mode == "apply" else "planned",
                    extra={
                        "bucket_name": args.bucket_name,
                        "object_name": args.object_name,
                        "par_name": par_name,
                        "manifest_path": str(manifest_path),
                        "access_uri": oci_data.get("access-uri"),
                    },
                )
                print(
                    json.dumps(
                        {
                            "status": "ok",
                            "runtime": "oci",
                            "command": args.command,
                            "par_name": par_name,
                            "preauth_request_id": oci_data.get("id"),
                            "access_uri": oci_data.get("access-uri"),
                            "manifest_path": str(manifest_path),
                            "control_paths": control_paths,
                        },
                        indent=2,
                        ensure_ascii=True,
                    )
                )
                return 0

            manifest = sync_bucket_manifest(
                context,
                args.bucket_name,
                {
                    **bucket_metadata,
                    "oci_mode": args.oci_mode,
                    "oci_profile": args.oci_profile,
                    "compartment_id": args.compartment_id,
                    "namespace_name": args.namespace_name,
                },
            )
            control_paths = record_control_runtime(
                context,
                runtime_payload,
                "buckets",
                "sync_bucket_manifest",
                "applied" if args.oci_mode == "apply" else "planned",
                extra={"bucket_name": args.bucket_name, "manifest_path": str(manifest)},
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "command": args.command,
                        "manifest_path": str(manifest),
                        "control_paths": control_paths,
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

        if not args.source_file:
            raise SystemExit("--source-file es requerido para upload-object")
        source_file = Path(args.source_file).resolve()
        if not source_file.exists():
            raise FileNotFoundError(f"No existe el archivo fuente: {source_file}")
        execution = OciExecutionContext(
            repo_root=context.repo_root,
            profile=args.oci_profile,
            extra_mounts=(source_file.parent,),
        )
        command = [
            "os",
            "object",
            "put",
            "--bucket-name",
            args.bucket_name,
            "--file",
            execution.host_to_container_path(source_file),
            "--force",
        ]
        if args.namespace_name:
            command.extend(["--namespace-name", args.namespace_name])
        if args.object_name:
            command.extend(["--name", args.object_name])
        result = execute_oci(execution, "buckets", context, "upload-object", command, args.oci_mode)
        stored = upload_object_to_bucket(context, args.bucket_name, source_file, args.object_name)
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "buckets",
            "upload_object",
            "applied" if args.oci_mode == "apply" else "planned",
            extra={"bucket_name": args.bucket_name, "stored_at": str(stored), "plan_path": result.get("plan_path")},
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "runtime": "oci",
                    "command": args.command,
                    "stored_at": str(stored),
                    "plan_path": result.get("plan_path"),
                    "control_paths": control_paths,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command == "create-bucket":
        result = create_bucket_manifest(
            context,
            args.bucket_name,
            bucket_metadata,
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "buckets",
            "create_bucket",
            "mirrored",
            extra={"bucket_name": args.bucket_name, "manifest_path": str(result)},
        )
        print(json.dumps({"status": "ok", "command": args.command, "manifest_path": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "sync-bucket-manifest":
        result = sync_bucket_manifest(context, args.bucket_name, bucket_metadata)
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "buckets",
            "sync_bucket_manifest",
            "mirrored",
            extra={"bucket_name": args.bucket_name, "manifest_path": str(result)},
        )
        print(json.dumps({"status": "ok", "command": args.command, "manifest_path": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-par":
        par_name = args.par_name or f"{args.bucket_name}-{sanitize_name(args.object_name or 'bucket')}-par"
        manifest_path = context.bucket_root(args.bucket_name) / "preauth" / f"{sanitize_name(par_name)}.json"
        ensure_directory(manifest_path.parent)
        write_json(
            manifest_path,
            {
                "bucket_name": args.bucket_name,
                "namespace_name": args.namespace_name,
                "object_name": args.object_name,
                "par_name": par_name,
                "access_type": args.access_type,
                "time_expires": args.time_expires or default_par_expiry(),
                "runtime": "local",
                "oci_mode": None,
            },
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "buckets",
            "create_preauthenticated_request",
            "mirrored",
            extra={"bucket_name": args.bucket_name, "object_name": args.object_name, "par_name": par_name, "manifest_path": str(manifest_path)},
        )
        print(json.dumps({"status": "ok", "command": args.command, "manifest_path": str(manifest_path), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if not args.source_file:
        raise SystemExit("--source-file es requerido para upload-object")

    source_file = Path(args.source_file).resolve()
    if not source_file.exists():
        raise FileNotFoundError(f"No existe el archivo fuente: {source_file}")

    result = upload_object_to_bucket(context, args.bucket_name, source_file, args.object_name)
    control_paths = record_control_runtime(
        context,
        runtime_payload,
        "buckets",
        "upload_object",
        "mirrored",
        extra={"bucket_name": args.bucket_name, "stored_at": str(result)},
    )
    print(json.dumps({"status": "ok", "command": args.command, "stored_at": str(result), "control_paths": control_paths}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
