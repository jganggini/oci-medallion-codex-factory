from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import (
    create_adb_definition,
    register_adb_load,
    register_adb_sql_execution,
    register_adb_user,
    write_adb_bootstrap,
)
from mcp.common.medallion_runtime import (
    DEFAULT_CONTROL_SCHEMA,
    DEFAULT_CONTROL_USER,
    add_standard_runtime_args,
    build_openlineage_event,
    control_plane_root,
    ensure_control_plane_manifest,
    queue_lineage_event,
    record_control_runtime,
    register_checkpoint,
    register_reprocess_request,
    runtime_payload_from_args,
)
from mcp.common.oci_cli import OciExecutionContext, execute_oci
from mcp.common.runtime import MirrorContext, ensure_directory


DEFAULT_IGNORE_EXISTS_CODES = {955, 1918, 1920}
DEFAULT_GRANTS = (
    "CREATE SESSION",
    "CREATE TABLE",
    "CREATE VIEW",
    "CREATE SEQUENCE",
    "CREATE PROCEDURE",
    "CREATE SYNONYM",
    "READ, WRITE ON DIRECTORY DATA_PUMP_DIR",
    "EXECUTE ON DBMS_CLOUD",
)
DEFINE_PATTERN = re.compile(r"^DEFINE\s+([A-Za-z][A-Za-z0-9_]*)\s*=\s*(.+?);?\s*$", re.IGNORECASE)
CONTROL_PLANE_TEMPLATE = REPO_ROOT_DEFAULT / "templates" / "autonomous" / "control_plane_bootstrap.sql"


def parse_bool_string(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in ("true", "1", "yes", "y"):
        return True
    if normalized in ("false", "0", "no", "n"):
        return False
    raise ValueError(f"Valor booleano invalido: {value}")


def parse_defines(items: list[str]) -> dict[str, str]:
    defines: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Define invalido: {item}. Usa name=value.")
        key, value = item.split("=", 1)
        defines[key.strip().lower()] = value
    return defines


def quote_password(password: str) -> str:
    return '"' + password.replace('"', '""') + '"'


def default_create_user_sql(db_user: str, password_token: str) -> str:
    statements = [
        f"CREATE USER {db_user} IDENTIFIED BY {quote_password(password_token)} DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS",
    ]
    statements.extend(f"GRANT {grant} TO {db_user}" for grant in DEFAULT_GRANTS)
    return ";\n".join(statements) + ";\n"


def default_bootstrap_sql(db_user: str, password_placeholder: str) -> str:
    return default_create_user_sql(db_user, password_placeholder)


def resolve_secret(explicit: str | None, env_name: str | None, fallback_envs: tuple[str, ...] = ()) -> str | None:
    if explicit:
        return explicit
    candidates = []
    if env_name:
        candidates.append(env_name)
    candidates.extend(fallback_envs)
    for candidate in candidates:
        value = os.getenv(candidate)
        if value:
            return value
    return None


def resolve_optional_path(path_value: str | None, label: str) -> Path | None:
    if not path_value:
        return None
    path = Path(path_value).resolve()
    if not path.exists():
        raise FileNotFoundError(f"No existe {label}: {path}")
    return path


def load_sql_sources(sql_files: list[str], sql_dir: str | None, sql_pattern: str) -> list[Path]:
    resolved: list[Path] = []
    seen: set[str] = set()
    for item in sql_files:
        source = Path(item).resolve()
        if not source.exists():
            raise FileNotFoundError(f"No existe el archivo SQL: {source}")
        key = str(source).lower()
        if key not in seen:
            seen.add(key)
            resolved.append(source)

    if sql_dir:
        directory = Path(sql_dir).resolve()
        if not directory.exists():
            raise FileNotFoundError(f"No existe el directorio SQL: {directory}")
        for source in sorted(item for item in directory.glob(sql_pattern) if item.is_file()):
            key = str(source.resolve()).lower()
            if key not in seen:
                seen.add(key)
                resolved.append(source.resolve())
    return resolved


def preprocess_sql_text(sql_text: str, cli_defines: dict[str, str]) -> str:
    defines = dict(cli_defines)
    lines: list[str] = []
    for raw_line in sql_text.splitlines():
        match = DEFINE_PATTERN.match(raw_line.strip())
        if match:
            key = match.group(1).lower()
            raw_value = match.group(2).strip()
            if raw_value.endswith(";"):
                raw_value = raw_value[:-1].rstrip()
            if len(raw_value) >= 2 and raw_value[0] == raw_value[-1] and raw_value[0] in ("'", '"'):
                raw_value = raw_value[1:-1]
            defines.setdefault(key, raw_value)
            continue
        lines.append(raw_line)

    rendered = "\n".join(lines)
    for key, value in defines.items():
        rendered = re.sub(rf"&{re.escape(key)}\b", value, rendered, flags=re.IGNORECASE)
    return rendered


def iter_sql_statements(sql_text: str):
    buffer: list[str] = []
    in_plsql_block = False
    block_prefixes = (
        "CREATE OR REPLACE PROCEDURE",
        "CREATE OR REPLACE PACKAGE",
        "CREATE OR REPLACE FUNCTION",
        "CREATE OR REPLACE TRIGGER",
        "DECLARE",
        "BEGIN",
    )

    for raw_line in sql_text.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            continue

        upper = stripped.upper()
        if any(upper.startswith(prefix) for prefix in block_prefixes):
            in_plsql_block = True

        if stripped == "/":
            statement = "\n".join(buffer).strip()
            if statement:
                yield statement
            buffer = []
            in_plsql_block = False
            continue

        buffer.append(line)

        if not in_plsql_block and stripped.endswith(";"):
            statement = "\n".join(buffer).strip()
            if statement.endswith(";"):
                statement = statement[:-1]
            if statement:
                yield statement
            buffer = []

    trailing = "\n".join(buffer).strip()
    if trailing:
        yield trailing


def prepare_sql_bundle(source_files: list[Path], defines: dict[str, str]) -> tuple[list[dict[str, Any]], str]:
    bundle: list[dict[str, Any]] = []
    rendered_parts: list[str] = []
    for source_file in source_files:
        rendered = preprocess_sql_text(source_file.read_text(encoding="utf-8"), defines)
        statement_count = sum(1 for _ in iter_sql_statements(rendered))
        bundle.append(
            {
                "source_file": source_file,
                "rendered_sql": rendered,
                "statement_count": statement_count,
            }
        )
        rendered_parts.append(f"-- source: {source_file.name}\n{rendered.strip()}")
    combined_sql = "\n\n".join(part for part in rendered_parts if part).strip()
    if combined_sql:
        combined_sql += "\n"
    return bundle, combined_sql


def render_control_plane_sql(control_schema: str, control_password: str) -> str:
    if not CONTROL_PLANE_TEMPLATE.exists():
        raise FileNotFoundError(f"No existe la plantilla de control plane: {CONTROL_PLANE_TEMPLATE}")
    sql_text = CONTROL_PLANE_TEMPLATE.read_text(encoding="utf-8")
    return preprocess_sql_text(
        sql_text,
        {
            "control_schema": control_schema,
            "control_password": control_password,
        },
    )


def inline_sql_bundle(source_name: str, rendered_sql: str) -> list[dict[str, Any]]:
    return [
        {
            "source_file": Path(source_name),
            "rendered_sql": rendered_sql,
            "statement_count": sum(1 for _ in iter_sql_statements(rendered_sql)),
        }
    ]


def write_control_plane_bootstrap(context: MirrorContext, database_name: str, control_schema: str, sql_text: str) -> Path:
    root = ensure_directory(control_plane_root(context, database_name) / "bootstrap")
    path = root / f"{control_schema.lower()}-bootstrap.sql"
    path.write_text(sql_text, encoding="utf-8")
    return path


def load_oracledb_module():
    try:
        import oracledb  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "No se encontro el modulo oracledb. Instala python-oracledb para usar --runtime oci --oci-mode apply en Autonomous."
        ) from exc
    return oracledb


def open_adb_connection(user: str, password: str, dsn: str, wallet_dir: Path, wallet_password: str | None):
    oracledb = load_oracledb_module()
    return oracledb.connect(
        user=user,
        password=password,
        dsn=dsn,
        config_dir=str(wallet_dir),
        wallet_location=str(wallet_dir),
        wallet_password=wallet_password,
    )


def execute_sql_text(cursor: Any, oracledb_module: Any, sql_text: str, ignore_exists: bool) -> tuple[int, list[dict[str, Any]]]:
    executed = 0
    ignored_errors: list[dict[str, Any]] = []
    for statement in iter_sql_statements(sql_text):
        try:
            cursor.execute(statement)
            executed += 1
        except oracledb_module.DatabaseError as exc:
            error = exc.args[0]
            code = getattr(error, "code", None)
            if ignore_exists and code in DEFAULT_IGNORE_EXISTS_CODES:
                ignored_errors.append(
                    {
                        "code": code,
                        "message": str(error),
                        "statement_preview": statement[:160],
                    }
                )
                continue
            raise
    return executed, ignored_errors


def execute_sql_bundle(
    connect_user: str,
    connect_password: str,
    dsn: str,
    wallet_dir: Path,
    wallet_password: str | None,
    bundle: list[dict[str, Any]],
    ignore_exists: bool,
) -> dict[str, Any]:
    oracledb = load_oracledb_module()
    connection = open_adb_connection(connect_user, connect_password, dsn, wallet_dir, wallet_password)
    try:
        cursor = connection.cursor()
        total_statements = 0
        ignored_errors: list[dict[str, Any]] = []
        files: list[dict[str, Any]] = []
        for item in bundle:
            executed, ignored = execute_sql_text(cursor, oracledb, item["rendered_sql"], ignore_exists)
            total_statements += executed
            ignored_errors.extend(ignored)
            files.append(
                {
                    "source_file": str(item["source_file"]),
                    "statement_count": item["statement_count"],
                    "executed_statements": executed,
                    "ignored_errors": len(ignored),
                }
            )
        connection.commit()
        return {
            "executed_statements": total_statements,
            "ignored_errors": ignored_errors,
            "files": files,
        }
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-autonomous-database-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--oci-profile")
    parser.add_argument(
        "--command",
        required=True,
        choices=(
            "create-adb-definition",
            "create-autonomous-database",
            "bootstrap-control-plane",
            "bootstrap-schema",
            "create-database-user",
            "apply-sql",
            "apply-sql-bundle",
            "register-checkpoint",
            "create-reprocess-request",
            "load-gold-object",
            "load-gold-objects",
        ),
    )
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--database-user", default="app_gold")
    parser.add_argument("--control-schema", default=DEFAULT_CONTROL_SCHEMA)
    parser.add_argument("--control-user", default=DEFAULT_CONTROL_USER)
    parser.add_argument("--load-strategy", default="single-writer-batch")
    parser.add_argument("--wallet-dir")
    parser.add_argument("--dsn", default=os.getenv("ADW_DSN", "dbclarogold_high"))
    parser.add_argument("--admin-user", default=os.getenv("DB_USER", "ADMIN"))
    parser.add_argument("--admin-password")
    parser.add_argument("--admin-password-env", default="DB_PASSWORD")
    parser.add_argument("--database-password")
    parser.add_argument("--database-password-env", default="APP_GOLD_PASSWORD")
    parser.add_argument("--control-password")
    parser.add_argument("--control-password-env", default="MDL_CTL_PASSWORD")
    parser.add_argument("--wallet-password")
    parser.add_argument("--wallet-password-env", default="DB_WALLET_PASSWORD")
    parser.add_argument("--connect-user")
    parser.add_argument("--connect-password")
    parser.add_argument("--connect-password-env")
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
    parser.add_argument("--sql-file", action="append", default=[])
    parser.add_argument("--sql-dir")
    parser.add_argument("--sql-pattern", default="*.sql")
    parser.add_argument("--define", action="append", default=[])
    parser.add_argument("--ignore-exists", default="true")
    parser.add_argument("--object-name")
    parser.add_argument("--source-file")
    parser.add_argument("--checkpoint-type")
    parser.add_argument("--checkpoint-value")
    parser.add_argument("--requested-reason")
    parser.add_argument("--requested-by", default="codex")
    add_standard_runtime_args(parser, include_control_database=False)
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    command_aliases = {
        "create-autonomous-database": "create-adb-definition",
        "apply-sql-bundle": "apply-sql",
        "load-gold-objects": "load-gold-object",
    }
    requested_command = args.command
    args.command = command_aliases.get(args.command, args.command)
    runtime_payload = runtime_payload_from_args(args)
    wallet_dir = resolve_optional_path(args.wallet_dir, "el wallet")
    source_file = resolve_optional_path(args.source_file, "el archivo fuente")
    cli_defines = parse_defines(args.define)

    if args.runtime == "oci" and args.command == "create-adb-definition":
        execution = OciExecutionContext(repo_root=context.repo_root, profile=args.oci_profile)
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
        manifest = create_adb_definition(
            context,
            args.database_name,
            args.database_user,
            args.load_strategy,
            wallet_dir,
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "autonomous_database",
            "create_adb_definition",
            "applied" if args.oci_mode == "apply" else "planned",
            database_name=args.database_name,
            extra={"manifest_path": str(manifest)},
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "runtime": "oci",
                    "manifest_path": str(manifest),
                    "plan_path": result.get("plan_path"),
                    "result_path": result.get("result_path"),
                    "control_paths": control_paths,
                    "requested_command": requested_command,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command == "create-adb-definition":
        result = create_adb_definition(context, args.database_name, args.database_user, args.load_strategy, wallet_dir)
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "autonomous_database",
            "create_adb_definition",
            "mirrored",
            database_name=args.database_name,
            extra={"manifest_path": str(result)},
        )
        print(json.dumps({"status": "ok", "manifest_path": str(result), "control_paths": control_paths, "requested_command": requested_command}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "bootstrap-control-plane":
        safe_sql = render_control_plane_sql(args.control_schema, args.password_placeholder)
        script_path = write_control_plane_bootstrap(context, args.database_name, args.control_schema, safe_sql)
        control_manifest = ensure_control_plane_manifest(
            context,
            args.database_name,
            runtime_payload,
            extra={
                "schema_name": args.control_schema,
                "control_user": args.control_user,
                "bootstrap_script": str(script_path),
            },
        )
        metadata: dict[str, Any] = {
            "runtime": args.runtime,
            "oci_mode": args.oci_mode if args.runtime == "oci" else None,
            "database_name": args.database_name,
            "control_schema": args.control_schema,
            "control_user": args.control_user,
            "status": "planned" if args.runtime == "oci" else "mirrored",
        }
        control_password = resolve_secret(args.control_password, args.control_password_env)
        if args.runtime == "oci" and args.oci_mode == "apply":
            admin_password = resolve_secret(args.admin_password, args.admin_password_env, ("DB_PASSWORD",))
            wallet_password = resolve_secret(args.wallet_password, args.wallet_password_env)
            if wallet_dir is None:
                raise SystemExit("--wallet-dir es requerido para bootstrap-control-plane en runtime oci apply")
            if not admin_password:
                raise SystemExit("No se encontro la clave del usuario ADMIN")
            if not control_password:
                raise SystemExit("No se encontro la clave del usuario de control")

            applied_sql = render_control_plane_sql(args.control_schema, control_password)
            execution_result = execute_sql_bundle(
                args.admin_user,
                admin_password,
                args.dsn,
                wallet_dir,
                wallet_password,
                inline_sql_bundle(str(CONTROL_PLANE_TEMPLATE), applied_sql),
                ignore_exists=parse_bool_string(args.ignore_exists, default=True),
            )
            metadata.update({"status": "applied", **execution_result})

        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "autonomous_database",
            "bootstrap_control_plane",
            metadata["status"],
            database_name=args.database_name,
            extra={
                "schema_name": args.control_schema,
                "control_user": args.control_user,
                "bootstrap_script": str(script_path),
                "control_manifest": str(control_manifest),
            },
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "bootstrap_script": str(script_path),
                    "control_manifest": str(control_manifest),
                    "metadata": metadata,
                    "control_paths": control_paths,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command == "bootstrap-schema":
        sql_sources = load_sql_sources(args.sql_file, args.sql_dir, args.sql_pattern)
        if sql_sources:
            bundle, combined_sql = prepare_sql_bundle(sql_sources, cli_defines)
            sql_text = combined_sql
            source_paths = [str(item["source_file"]) for item in bundle]
        else:
            sql_text = default_bootstrap_sql(args.database_user, args.password_placeholder)
            source_paths = []
        result = write_adb_bootstrap(context, args.database_name, args.database_user, sql_text)
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "autonomous_database",
            "bootstrap_schema",
            "mirrored",
            database_name=args.database_name,
            extra={"bootstrap_script": str(result), "source_files": source_paths},
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "bootstrap_script": str(result),
                    "source_files": source_paths,
                    "note": "Usa create-database-user y apply-sql para ejecutar cambios reales en ADW.",
                    "control_paths": control_paths,
                    "requested_command": requested_command,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command == "create-database-user":
        admin_password = resolve_secret(args.admin_password, args.admin_password_env, ("DB_PASSWORD",))
        database_password = resolve_secret(args.database_password, args.database_password_env, ("DB_PASSWORD",))
        wallet_password = resolve_secret(args.wallet_password, args.wallet_password_env)

        safe_sql = default_create_user_sql(args.database_user, args.password_placeholder)
        metadata: dict[str, Any] = {
            "runtime": args.runtime,
            "oci_mode": args.oci_mode if args.runtime == "oci" else None,
            "dsn": args.dsn,
            "wallet_dir": str(wallet_dir) if wallet_dir else None,
            "admin_user": args.admin_user,
            "database_password_env": args.database_password_env,
            "status": "planned" if args.runtime == "oci" else "mirrored",
        }

        if args.runtime == "oci" and args.oci_mode == "apply":
            if wallet_dir is None:
                raise SystemExit("--wallet-dir es requerido para create-database-user en runtime oci apply")
            if not admin_password:
                raise SystemExit("No se encontro la clave del usuario ADMIN")
            if not database_password:
                raise SystemExit("No se encontro la clave del usuario aplicativo")

            oracledb = load_oracledb_module()
            connection = open_adb_connection(args.admin_user, admin_password, args.dsn, wallet_dir, wallet_password)
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :1", [args.database_user.upper()])
                user_preexisted = cursor.fetchone()[0] > 0
                applied_sql = default_create_user_sql(args.database_user, database_password)
                executed_statements, ignored_errors = execute_sql_text(
                    cursor,
                    oracledb,
                    applied_sql,
                    ignore_exists=parse_bool_string(args.ignore_exists, default=True),
                )
                connection.commit()
            finally:
                connection.close()

            metadata.update(
                {
                    "status": "applied",
                    "user_preexisted": user_preexisted,
                    "executed_statements": executed_statements,
                    "ignored_errors": ignored_errors,
                }
            )

        result = register_adb_user(context, args.database_name, args.database_user, safe_sql, metadata)
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "autonomous_database",
            "create_database_user",
            metadata["status"],
            database_name=args.database_name,
            extra={"script_path": str(result["script_path"]), "receipt_path": str(result["receipt_path"])},
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "command": args.command,
                    "script_path": str(result["script_path"]),
                    "receipt_path": str(result["receipt_path"]),
                    "control_paths": control_paths,
                    "requested_command": requested_command,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command == "apply-sql":
        sql_sources = load_sql_sources(args.sql_file, args.sql_dir, args.sql_pattern)
        if not sql_sources:
            raise SystemExit("--sql-file o --sql-dir es requerido para apply-sql")

        bundle, combined_sql = prepare_sql_bundle(sql_sources, cli_defines)
        connect_user = args.connect_user or args.database_user
        admin_password = resolve_secret(args.admin_password, args.admin_password_env, ("DB_PASSWORD",))
        database_password = resolve_secret(args.database_password, args.database_password_env, ("DB_PASSWORD",))
        connect_password = resolve_secret(
            args.connect_password,
            args.connect_password_env,
            ((args.database_password_env,) if args.connect_user == args.database_user else ()),
        )
        if not connect_password:
            if connect_user.upper() == args.admin_user.upper():
                connect_password = admin_password
            elif connect_user.upper() == args.database_user.upper():
                connect_password = database_password
        wallet_password = resolve_secret(args.wallet_password, args.wallet_password_env)

        metadata = {
            "runtime": args.runtime,
            "oci_mode": args.oci_mode if args.runtime == "oci" else None,
            "dsn": args.dsn,
            "wallet_dir": str(wallet_dir) if wallet_dir else None,
            "connect_user": connect_user,
            "connect_password_env": args.connect_password_env,
            "status": "planned" if args.runtime == "oci" else "mirrored",
            "ignore_exists": parse_bool_string(args.ignore_exists, default=True),
            "source_file_count": len(sql_sources),
        }

        if args.runtime == "oci" and args.oci_mode == "apply":
            if wallet_dir is None:
                raise SystemExit("--wallet-dir es requerido para apply-sql en runtime oci apply")
            if not connect_password:
                raise SystemExit("No se encontro la clave del usuario con el que se ejecutara el SQL")
            execution_result = execute_sql_bundle(
                connect_user,
                connect_password,
                args.dsn,
                wallet_dir,
                wallet_password,
                bundle,
                ignore_exists=parse_bool_string(args.ignore_exists, default=True),
            )
            metadata.update({"status": "applied", **execution_result})

        receipt = register_adb_sql_execution(
            context,
            args.database_name,
            "apply_sql",
            sql_sources,
            metadata,
            rendered_sql=combined_sql,
        )
        checkpoint_path = None
        if runtime_payload.get("slice_key"):
            checkpoint_path = register_checkpoint(
                context,
                args.database_name,
                runtime_payload,
                "sql_bundle",
                str(receipt),
                status=metadata["status"],
                extra={"operation": "apply_sql", "connect_user": connect_user},
            )
        lineage_path = None
        if runtime_payload.get("lineage_enabled"):
            lineage_payload = build_openlineage_event(
                runtime_payload,
                "COMPLETE" if metadata["status"] == "applied" else "START",
                runtime_payload.get("workflow_id") or f"{args.database_name}.apply_sql",
                inputs=[runtime_payload["source_asset_ref"]] if runtime_payload.get("source_asset_ref") else [],
                outputs=[runtime_payload["target_asset_ref"]] if runtime_payload.get("target_asset_ref") else [],
                event_facets={"adbSql": {"status": metadata["status"], "source_file_count": len(sql_sources)}},
            )
            lineage_path = queue_lineage_event(
                context,
                args.database_name,
                runtime_payload,
                "adb_sql",
                lineage_payload,
            )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "autonomous_database",
            "apply_sql",
            metadata["status"],
            database_name=args.database_name,
            metrics={
                "rows_in": metadata.get("source_file_count"),
                "rows_out": metadata.get("executed_statements"),
                "rows_rejected": len(metadata.get("ignored_errors", [])),
            },
            extra={
                "receipt_path": str(receipt),
                "connect_user": connect_user,
                "checkpoint_path": str(checkpoint_path) if checkpoint_path else None,
                "lineage_outbox_path": str(lineage_path) if lineage_path else None,
            },
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "command": args.command,
                    "receipt_path": str(receipt),
                    "checkpoint_path": str(checkpoint_path) if checkpoint_path else None,
                    "lineage_outbox_path": str(lineage_path) if lineage_path else None,
                    "control_paths": control_paths,
                    "requested_command": requested_command,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command == "register-checkpoint":
        if not args.checkpoint_type or not args.checkpoint_value:
            raise SystemExit("--checkpoint-type y --checkpoint-value son requeridos para register-checkpoint")
        checkpoint_path = register_checkpoint(
            context,
            args.database_name,
            runtime_payload,
            args.checkpoint_type,
            args.checkpoint_value,
            status="ready",
            extra={"requested_command": requested_command},
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "autonomous_database",
            "register_checkpoint",
            "ready",
            database_name=args.database_name,
            extra={"checkpoint_path": str(checkpoint_path)},
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "checkpoint_path": str(checkpoint_path),
                    "control_paths": control_paths,
                    "requested_command": requested_command,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if args.command == "create-reprocess-request":
        if not args.requested_reason:
            raise SystemExit("--requested-reason es requerido para create-reprocess-request")
        request_path = register_reprocess_request(
            context,
            args.database_name,
            runtime_payload,
            args.requested_reason,
            args.requested_by,
            extra={"requested_command": requested_command},
        )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "autonomous_database",
            "create_reprocess_request",
            "requested",
            database_name=args.database_name,
            extra={"reprocess_request_path": str(request_path)},
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "reprocess_request_path": str(request_path),
                    "control_paths": control_paths,
                    "requested_command": requested_command,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if not args.object_name or source_file is None:
        raise SystemExit("--object-name y --source-file son requeridos para load-gold-object")

    result = register_adb_load(context, args.database_name, args.object_name, source_file)
    checkpoint_path = register_checkpoint(
        context,
        args.database_name,
        runtime_payload,
        "gold_load",
        str(result),
        status="ready",
        extra={"object_name": args.object_name, "source_file": str(source_file)},
    )
    lineage_path = None
    if runtime_payload.get("lineage_enabled"):
        lineage_payload = build_openlineage_event(
            runtime_payload,
            "COMPLETE",
            runtime_payload.get("workflow_id") or f"{args.database_name}.load_gold",
            inputs=[runtime_payload["source_asset_ref"]] if runtime_payload.get("source_asset_ref") else [args.object_name],
            outputs=[runtime_payload["target_asset_ref"]] if runtime_payload.get("target_asset_ref") else [args.object_name],
            event_facets={"adbLoad": {"object_name": args.object_name}},
        )
        lineage_path = queue_lineage_event(
            context,
            args.database_name,
            runtime_payload,
            "adb_load_gold",
            lineage_payload,
        )
    control_paths = record_control_runtime(
        context,
        runtime_payload,
        "autonomous_database",
        "load_gold_object",
        "mirrored",
        database_name=args.database_name,
        extra={
            "load_receipt": str(result),
            "checkpoint_path": str(checkpoint_path),
            "lineage_outbox_path": str(lineage_path) if lineage_path else None,
            "object_name": args.object_name,
        },
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "load_receipt": str(result),
                "checkpoint_path": str(checkpoint_path),
                "lineage_outbox_path": str(lineage_path) if lineage_path else None,
                "control_paths": control_paths,
                "requested_command": requested_command,
            },
            indent=2,
            ensure_ascii=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
