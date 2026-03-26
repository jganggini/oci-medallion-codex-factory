from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import create_bucket_manifest, upload_object_to_bucket
from mcp.common.oci_cli import OciExecutionContext, execute_oci
from mcp.common.runtime import MirrorContext


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-object-storage-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument("--command", required=True, choices=("create-bucket", "upload-object"))
    parser.add_argument("--bucket-name", required=True)
    parser.add_argument("--display-name")
    parser.add_argument("--storage-tier", default="Standard")
    parser.add_argument("--compartment-id")
    parser.add_argument("--namespace-name")
    parser.add_argument("--source-file")
    parser.add_argument("--object-name")
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)

    if args.runtime == "oci":
        if args.command == "create-bucket":
            execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
            if not args.compartment_id:
                raise SystemExit("--compartment-id es requerido en runtime oci para create-bucket")
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
                    "display_name": args.display_name or args.bucket_name,
                    "storage_tier": args.storage_tier,
                    "oci_mode": args.oci_mode,
                    "oci_profile": args.oci_profile,
                    "compartment_id": args.compartment_id,
                    "namespace_name": args.namespace_name,
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                },
            )
            print(json.dumps({"status": "ok", "runtime": "oci", "command": args.command, "manifest_path": str(manifest)}, indent=2, ensure_ascii=True))
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
        print(json.dumps({"status": "ok", "runtime": "oci", "command": args.command, "stored_at": str(stored), "plan_path": result.get("plan_path")}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "create-bucket":
        result = create_bucket_manifest(
            context,
            args.bucket_name,
            {"display_name": args.display_name or args.bucket_name, "storage_tier": args.storage_tier},
        )
        print(json.dumps({"status": "ok", "command": args.command, "manifest_path": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if not args.source_file:
        raise SystemExit("--source-file es requerido para upload-object")

    source_file = Path(args.source_file).resolve()
    if not source_file.exists():
        raise FileNotFoundError(f"No existe el archivo fuente: {source_file}")

    result = upload_object_to_bucket(context, args.bucket_name, source_file, args.object_name)
    print(json.dumps({"status": "ok", "command": args.command, "stored_at": str(result)}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
