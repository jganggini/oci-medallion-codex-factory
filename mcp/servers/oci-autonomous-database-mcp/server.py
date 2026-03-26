from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import create_adb_definition, register_adb_load, write_adb_bootstrap
from mcp.common.oci_cli import OciExecutionContext, execute_oci
from mcp.common.runtime import MirrorContext


def default_bootstrap_sql(db_user: str, password_placeholder: str) -> str:
    return "\n".join(
        [
            f"CREATE USER {db_user} IDENTIFIED BY {password_placeholder} DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;",
            f"GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO {db_user};",
            f"GRANT READ, WRITE ON DIRECTORY DATA_PUMP_DIR TO {db_user};",
        ]
    ) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-autonomous-database-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument("--command", required=True, choices=("create-adb-definition", "bootstrap-schema", "load-gold-object"))
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--database-user", default="app_gold")
    parser.add_argument("--load-strategy", default="single-writer-batch")
    parser.add_argument("--wallet-dir")
    parser.add_argument("--compartment-id")
    parser.add_argument("--db-name")
    parser.add_argument("--db-workload", default="DW")
    parser.add_argument("--compute-count", default="1")
    parser.add_argument("--compute-model", default="ECPU")
    parser.add_argument("--data-storage-size-in-tbs", default="1")
    parser.add_argument("--display-name")
    parser.add_argument("--db-version", default="19c")
    parser.add_argument("--secret-id")
    parser.add_argument("--is-mtls-connection-required", default="false")
    parser.add_argument("--password-placeholder", default="replace_me")
    parser.add_argument("--sql-file")
    parser.add_argument("--object-name")
    parser.add_argument("--source-file")
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)

    if args.runtime == "oci":
        execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
        if args.command == "create-adb-definition":
            if not args.compartment_id or not args.db_name:
                raise SystemExit("--compartment-id y --db-name son requeridos en runtime oci para create-adb-definition")
            command = [
                "db",
                "autonomous-database",
                "create",
                "--compartment-id",
                args.compartment_id,
                "--db-name",
                args.db_name,
                "--display-name",
                args.display_name or args.database_name,
                "--db-workload",
                args.db_workload,
                "--compute-model",
                args.compute_model,
                "--compute-count",
                args.compute_count,
                "--data-storage-size-in-tbs",
                args.data_storage_size_in_tbs,
                "--db-version",
                args.db_version,
                "--is-mtls-connection-required",
                args.is_mtls_connection_required,
            ]
            if args.secret_id:
                command.extend(["--secret-id", args.secret_id])
            result = execute_oci(execution, "autonomous_database", context, "create-adb-definition", command, args.oci_mode)
            wallet_dir = Path(args.wallet_dir).resolve() if args.wallet_dir else None
            manifest = create_adb_definition(
                context,
                args.database_name,
                args.database_user,
                args.load_strategy,
                wallet_dir,
            )
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "runtime": "oci",
                        "manifest_path": str(manifest),
                        "plan_path": result.get("plan_path"),
                        "result_path": result.get("result_path"),
                    },
                    indent=2,
                    ensure_ascii=True,
                )
            )
            return 0

    if args.command == "create-adb-definition":
        wallet_dir = Path(args.wallet_dir).resolve() if args.wallet_dir else None
        result = create_adb_definition(context, args.database_name, args.database_user, args.load_strategy, wallet_dir)
        print(json.dumps({"status": "ok", "manifest_path": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "bootstrap-schema":
        if args.sql_file:
            sql_text = Path(args.sql_file).resolve().read_text(encoding="utf-8")
        else:
            sql_text = default_bootstrap_sql(args.database_user, args.password_placeholder)
        result = write_adb_bootstrap(context, args.database_name, args.database_user, sql_text)
        print(json.dumps({"status": "ok", "bootstrap_script": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if not args.object_name or not args.source_file:
        raise SystemExit("--object-name y --source-file son requeridos para load-gold-object")

    source_file = Path(args.source_file).resolve()
    if not source_file.exists():
        raise FileNotFoundError(f"No existe el archivo fuente: {source_file}")
    result = register_adb_load(context, args.database_name, args.object_name, source_file)
    print(json.dumps({"status": "ok", "load_receipt": str(result)}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
