from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(repo_root: Path, args: list[str]) -> None:
    subprocess.run([sys.executable, *args], cwd=repo_root, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecuta un flujo local de demo usando los MCPs operativos.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--environment", default="dev", choices=("dev", "qa", "prod"))
    parser.add_argument("--workspace-name", default="ws-di-medallion-dev")
    parser.add_argument("--application-name", default="demo-app")
    parser.add_argument("--database-name", default="adb_trafico_gold")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    template_app = repo_root / "templates" / "data_flow" / "minimal_app"
    readme_file = repo_root / "README.md"

    run_command(
        repo_root,
        [
            "mcp/servers/oci-object-storage-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-bucket",
            "--bucket-name",
            "bucket-raw",
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
            "--command",
            "upload-object",
            "--bucket-name",
            "bucket-raw",
            "--source-file",
            str(readme_file),
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
            "--command",
            "create-application",
            "--application-name",
            args.application_name,
            "--source-dir",
            str(template_app),
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
            "--command",
            "run-application",
            "--application-name",
            args.application_name,
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
            "--command",
            "create-workspace",
            "--workspace-name",
            args.workspace_name,
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
            "--command",
            "create-project",
            "--workspace-name",
            args.workspace_name,
            "--project-name",
            "Medallion Demo",
            "--identifier",
            "MEDALLION_DEMO",
            "--label",
            "medallion",
            "--label",
            "demo",
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
            "--command",
            "create-folder",
            "--workspace-name",
            args.workspace_name,
            "--folder-name",
            "Data Flow Tasks",
            "--identifier",
            "DATA_FLOW_TASKS",
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
            "--command",
            "create-task-from-dataflow",
            "--workspace-name",
            args.workspace_name,
            "--task-name",
            "run-demo-app",
            "--application-name",
            args.application_name,
            "--aggregator-key",
            "MEDALLION_DEMO",
            "--task-key",
            "RUN_DEMO_APP_KEY",
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
            "--command",
            "create-pipeline",
            "--workspace-name",
            args.workspace_name,
            "--pipeline-name",
            "medallion-demo",
            "--task",
            "run-demo-app",
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
            "--command",
            "create-adb-definition",
            "--database-name",
            args.database_name,
            "--database-user",
            "app_gold",
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
            "--command",
            "create-database-user",
            "--database-name",
            args.database_name,
            "--database-user",
            "app_gold",
            "--password-placeholder",
            "APP_GOLD_PASSWORD",
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
            "--command",
            "bootstrap-schema",
            "--database-name",
            args.database_name,
            "--database-user",
            "app_gold",
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
            "--command",
            "load-gold-object",
            "--database-name",
            args.database_name,
            "--object-name",
            "agg_resumen_archivos_trafico",
            "--source-file",
            str(readme_file),
        ],
    )

    print("Local publish demo completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
