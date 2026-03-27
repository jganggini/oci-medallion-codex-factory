from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def run_json(repo_root: Path, args: list[str]) -> dict[str, object]:
    result = subprocess.run([sys.executable, *args], cwd=repo_root, check=True, capture_output=True, text=True)
    return json.loads(result.stdout.strip())


def run_command(repo_root: Path, args: list[str]) -> None:
    subprocess.run([sys.executable, *args], cwd=repo_root, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecuta un demo local de QA para buckets y Autonomous.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--environment", default="dev", choices=("dev", "qa", "prod"))
    parser.add_argument("--database-name", default="adb_trafico_gold")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    sample_csv = repo_root / "workspace" / "migration-input" / "trafico-datos" / "quality" / "samples" / "agg_resumen_archivos_trafico_sample.csv"
    contract_file = repo_root / "workspace" / "migration-input" / "trafico-datos" / "quality" / "contracts" / "agg_resumen_archivos_trafico.contract.json"

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
            "bucket-gold-refined",
            "--layer",
            "gold_refined",
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
            "bucket-gold-refined",
            "--source-file",
            str(sample_csv),
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
            "mcp/servers/oci-data-quality-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "profile-bucket-data",
            "--bucket-name",
            "bucket-gold-refined",
            "--object-glob",
            "objects/agg_resumen_archivos_trafico_*.csv",
            "--data-format",
            "csv",
            "--target-name",
            "gold_sample",
        ],
    )
    result = run_json(
        repo_root,
        [
            "mcp/servers/oci-data-quality-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "run-contract",
            "--contract-file",
            str(contract_file),
        ],
    )
    gate = run_json(
        repo_root,
        [
            "mcp/servers/oci-data-quality-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "gate-migration",
            "--result-path",
            str(result["result_path"]),
        ],
    )

    print(
        json.dumps(
            {
                "quality_result_path": result["result_path"],
                "quality_summary": result["summary"],
                "gate_path": gate["gate_path"],
                "gate_summary": gate["summary"],
            },
            indent=2,
            ensure_ascii=True,
        )
    )
    print("Quality demo completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
