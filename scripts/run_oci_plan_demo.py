from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(repo_root: Path, args: list[str]) -> None:
    subprocess.run([sys.executable, *args], cwd=repo_root, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera un plan OCI completo usando los MCPs del factory.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--environment", default="dev", choices=("dev", "qa", "prod"))
    parser.add_argument("--compartment-id", default="ocid1.compartment.oc1..exampleuniqueID")
    parser.add_argument("--namespace-name", default="example-ns")
    parser.add_argument("--workspace-id", default="ocid1.disworkspace.oc1..exampleuniqueID")
    parser.add_argument("--folder-key", default="FOLDER_001")
    parser.add_argument("--application-id", default="ocid1.dataflowapplication.oc1..exampleuniqueID")
    parser.add_argument("--secret-id", default="ocid1.vaultsecret.oc1..exampleuniqueID")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()

    run_command(
        repo_root,
        [
            "mcp/servers/oci-object-storage-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-bucket",
            "--bucket-name",
            "bucket-trusted",
            "--compartment-id",
            args.compartment_id,
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-object-storage-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "upload-object",
            "--bucket-name",
            "bucket-trusted",
            "--namespace-name",
            args.namespace_name,
            "--source-file",
            str(repo_root / "README.md"),
            "--object-name",
            "docs/README.md",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-flow-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-application",
            "--application-name",
            "bronze-to-silver",
            "--source-dir",
            str(repo_root / "templates" / "data_flow" / "minimal_app"),
            "--compartment-id",
            args.compartment_id,
            "--file-uri",
            f"oci://bucket-trusted@{args.namespace_name}/apps/bronze-to-silver/main.py",
            "--artifact-uri",
            f"oci://bucket-trusted@{args.namespace_name}/apps/bronze-to-silver/archive.zip",
            "--logs-bucket-uri",
            f"oci://bucket-trusted@{args.namespace_name}/logs/",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-flow-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "run-application",
            "--application-name",
            "bronze-to-silver",
            "--application-id",
            args.application_id,
            "--compartment-id",
            args.compartment_id,
            "--parameter",
            "process_date=2026-03-25",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-integration-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-workspace",
            "--workspace-name",
            "ws-di-medallion-dev",
            "--compartment-id",
            args.compartment_id,
            "--is-private-network",
            "true",
            "--subnet-id",
            "ocid1.subnet.oc1..exampleuniqueID",
            "--vcn-id",
            "ocid1.vcn.oc1..exampleuniqueID",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-integration-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-project",
            "--workspace-name",
            "ws-di-medallion-dev",
            "--workspace-id",
            args.workspace_id,
            "--project-name",
            "Medallion Trafico Datos",
            "--identifier",
            "MEDALLION_TRAFICO_DATOS",
            "--label",
            "medallion",
            "--label",
            "data-integration",
            "--favorite",
            "false",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-integration-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-folder",
            "--workspace-name",
            "ws-di-medallion-dev",
            "--workspace-id",
            args.workspace_id,
            "--folder-name",
            "Data Flow Tasks",
            "--identifier",
            "DATA_FLOW_TASKS",
            "--aggregator-key",
            args.folder_key,
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-integration-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-task-from-dataflow",
            "--workspace-name",
            "ws-di-medallion-dev",
            "--workspace-id",
            args.workspace_id,
            "--folder-key",
            args.folder_key,
            "--task-name",
            "run-bronze-silver",
            "--task-key",
            "RUN_BRONZE_SILVER_KEY",
            "--application-name",
            "bronze-to-silver",
            "--application-id",
            args.application_id,
            "--application-compartment-id",
            args.compartment_id,
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-autonomous-database-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-adb-definition",
            "--database-name",
            "adb_trafico_gold",
            "--database-user",
            "app_gold",
            "--compartment-id",
            args.compartment_id,
            "--db-name",
            "ADWTRAFICO",
            "--display-name",
            "ADW_CLARO_GOLD",
            "--secret-id",
            args.secret_id,
        ],
    )

    print("OCI plan demo completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
