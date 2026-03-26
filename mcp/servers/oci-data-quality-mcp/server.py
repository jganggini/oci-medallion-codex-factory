from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.medallion_runtime import add_standard_runtime_args, record_control_runtime, register_quality_result, runtime_payload_from_args
from mcp.common.runtime import MirrorContext, append_jsonl, ensure_directory, sanitize_name, utc_timestamp, write_json


SEVERITY_ORDER = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}


@dataclass
class BucketDataset:
    target_name: str
    bucket_name: str
    object_glob: str
    data_format: str
    matched_files: list[Path]
    file_count: int
    total_bytes: int
    row_count: int
    columns: list[str]
    sample_rows: list[dict[str, Any]]
    rows: list[dict[str, Any]]
    row_level_supported: bool
    unsupported_reason: str | None


def quality_root(context: MirrorContext) -> Path:
    return ensure_directory(context.service_root("quality"))


def quality_report(context: MirrorContext, operation: str, payload: dict[str, Any]) -> Path:
    root = quality_root(context)
    report_path = root / "reports" / f"{utc_timestamp()}-{sanitize_name(operation)}.json"
    write_json(report_path, payload)
    append_jsonl(root / "operations.log.jsonl", {"operation": operation, **payload})
    context.report("quality", operation, payload)
    return report_path


def resolve_secret(explicit: str | None, env_name: str | None, fallback_envs: tuple[str, ...] = ()) -> str | None:
    if explicit:
        return explicit
    candidates: list[str] = []
    if env_name:
        candidates.append(env_name)
    candidates.extend(fallback_envs)
    for candidate in candidates:
        value = os.getenv(candidate)
        if value:
            return value
    return None


def resolve_path(repo_root: Path, base_dir: Path, path_value: str, label: str) -> Path:
    candidate = Path(path_value)
    if candidate.is_absolute():
        resolved = candidate.resolve()
    else:
        relative = (base_dir / candidate).resolve()
        resolved = relative if relative.exists() else (repo_root / candidate).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"No existe {label}: {resolved}")
    return resolved


def load_json_contract(repo_root: Path, contract_path_value: str) -> tuple[Path, dict[str, Any]]:
    contract_path = resolve_path(repo_root, repo_root, contract_path_value, "el contrato de calidad")
    payload = json.loads(contract_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("El contrato de calidad debe ser un objeto JSON.")
    return contract_path, payload


def normalize_severity(value: str | None) -> str:
    severity = (value or "medium").strip().lower()
    if severity not in SEVERITY_ORDER:
        raise ValueError(f"Severity invalida: {value}")
    return severity


def contract_run_root(context: MirrorContext, contract_name: str) -> Path:
    return ensure_directory(quality_root(context) / "runs" / f"{utc_timestamp()}-{sanitize_name(contract_name)}")


def gate_root(context: MirrorContext, gate_name: str) -> Path:
    return ensure_directory(quality_root(context) / "gates" / f"{utc_timestamp()}-{sanitize_name(gate_name)}")


def infer_data_format(path: Path, preferred: str | None) -> str:
    if preferred and preferred != "auto":
        return preferred.lower()
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in (".jsonl", ".ndjson"):
        return "jsonl"
    if suffix == ".json":
        return "json"
    return "binary"


def to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip()
    if text == "":
        return Decimal("0")
    return Decimal(text)


def load_bucket_dataset(context: MirrorContext, target_name: str, target: dict[str, Any], sample_size: int) -> BucketDataset:
    bucket_name = target["bucket_name"]
    object_glob = target.get("object_glob", "objects/**/*")
    bucket_root = context.bucket_root(bucket_name)
    matched_files = sorted(item for item in bucket_root.glob(object_glob) if item.is_file())
    data_format = infer_data_format(matched_files[0], target.get("data_format")) if matched_files else (target.get("data_format", "auto").lower())

    total_bytes = sum(item.stat().st_size for item in matched_files)
    row_count = 0
    columns: list[str] = []
    sample_rows: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    row_level_supported = data_format in ("csv", "json", "jsonl")
    unsupported_reason = None if row_level_supported else f"Formato no soportado para perfilado fila a fila: {data_format}"

    if row_level_supported:
        for source_file in matched_files:
            if data_format == "csv":
                with source_file.open("r", encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle)
                    if reader.fieldnames:
                        for name in reader.fieldnames:
                            if name not in columns:
                                columns.append(name)
                    for row in reader:
                        normalized = {key: value for key, value in row.items()}
                        rows.append(normalized)
                        row_count += 1
                        if len(sample_rows) < sample_size:
                            sample_rows.append(normalized)
            elif data_format == "jsonl":
                with source_file.open("r", encoding="utf-8") as handle:
                    for raw_line in handle:
                        stripped = raw_line.strip()
                        if not stripped:
                            continue
                        loaded = json.loads(stripped)
                        normalized = loaded if isinstance(loaded, dict) else {"value": loaded}
                        for name in normalized:
                            if name not in columns:
                                columns.append(name)
                        rows.append(normalized)
                        row_count += 1
                        if len(sample_rows) < sample_size:
                            sample_rows.append(normalized)
            elif data_format == "json":
                loaded = json.loads(source_file.read_text(encoding="utf-8"))
                iterable = loaded if isinstance(loaded, list) else [loaded]
                for item in iterable:
                    normalized = item if isinstance(item, dict) else {"value": item}
                    for name in normalized:
                        if name not in columns:
                            columns.append(name)
                    rows.append(normalized)
                    row_count += 1
                    if len(sample_rows) < sample_size:
                        sample_rows.append(normalized)

    return BucketDataset(
        target_name=target_name,
        bucket_name=bucket_name,
        object_glob=object_glob,
        data_format=data_format,
        matched_files=matched_files,
        file_count=len(matched_files),
        total_bytes=total_bytes,
        row_count=row_count,
        columns=columns,
        sample_rows=sample_rows,
        rows=rows,
        row_level_supported=row_level_supported,
        unsupported_reason=unsupported_reason,
    )


def dataset_profile_payload(dataset: BucketDataset) -> dict[str, Any]:
    return {
        "target_name": dataset.target_name,
        "bucket_name": dataset.bucket_name,
        "object_glob": dataset.object_glob,
        "data_format": dataset.data_format,
        "matched_files": [str(item) for item in dataset.matched_files],
        "file_count": dataset.file_count,
        "total_bytes": dataset.total_bytes,
        "row_count": dataset.row_count,
        "columns": dataset.columns,
        "sample_rows": dataset.sample_rows,
        "row_level_supported": dataset.row_level_supported,
        "unsupported_reason": dataset.unsupported_reason,
    }


def make_check_result(
    check: dict[str, Any],
    category: str,
    status: str,
    message: str,
    actual: Any = None,
    expected: Any = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "name": check["name"],
        "category": category,
        "type": check["type"],
        "severity": normalize_severity(check.get("severity")),
        "status": status,
        "message": message,
        "actual": actual,
        "expected": expected,
        "details": details or {},
    }
    if "metric_key" in check:
        payload["metric_key"] = check["metric_key"]
    return payload


def store_metric(metrics: dict[str, Any], check: dict[str, Any], value: Any) -> None:
    metric_key = check.get("metric_key")
    if metric_key:
        metrics[metric_key] = value


def evaluate_bucket_check(check: dict[str, Any], datasets: dict[str, BucketDataset], metrics: dict[str, Any]) -> dict[str, Any]:
    target_name = check["target"]
    if target_name not in datasets:
        return make_check_result(check, "bucket", "failed", f"No existe el target de bucket {target_name}")

    dataset = datasets[target_name]
    check_type = check["type"]

    if check_type == "file_presence":
        passed = dataset.file_count > 0
        return make_check_result(check, "bucket", "passed" if passed else "failed", "Se encontraron archivos" if passed else "No se encontraron archivos", actual=dataset.file_count, expected="> 0")

    if check_type == "file_count_at_least":
        expected = int(check["expected"])
        passed = dataset.file_count >= expected
        store_metric(metrics, check, dataset.file_count)
        return make_check_result(check, "bucket", "passed" if passed else "failed", f"Cantidad de archivos: {dataset.file_count}", actual=dataset.file_count, expected=expected)

    if check_type == "row_count_at_least":
        if not dataset.row_level_supported:
            return make_check_result(check, "bucket", "skipped", dataset.unsupported_reason or "No se puede contar filas para este formato")
        expected = int(check["expected"])
        passed = dataset.row_count >= expected
        store_metric(metrics, check, dataset.row_count)
        return make_check_result(check, "bucket", "passed" if passed else "failed", f"Cantidad de filas: {dataset.row_count}", actual=dataset.row_count, expected=expected)

    if check_type == "required_columns":
        if not dataset.row_level_supported:
            return make_check_result(check, "bucket", "skipped", dataset.unsupported_reason or "No se puede leer columnas para este formato")
        expected_columns = list(check["columns"])
        missing = [item for item in expected_columns if item not in dataset.columns]
        passed = len(missing) == 0
        return make_check_result(check, "bucket", "passed" if passed else "failed", "Columnas requeridas presentes" if passed else f"Faltan columnas: {', '.join(missing)}", actual=dataset.columns, expected=expected_columns, details={"missing_columns": missing})

    if check_type == "not_null":
        if not dataset.row_level_supported:
            return make_check_result(check, "bucket", "skipped", dataset.unsupported_reason or "No se puede validar nulos para este formato")
        columns = list(check["columns"])
        null_counts: dict[str, int] = {}
        for column in columns:
            null_counts[column] = sum(1 for row in dataset.rows if str(row.get(column, "")).strip() == "")
        passed = all(value == 0 for value in null_counts.values())
        return make_check_result(check, "bucket", "passed" if passed else "failed", "Columnas sin nulos" if passed else f"Hay nulos en {', '.join(column for column, count in null_counts.items() if count > 0)}", actual=null_counts, expected={column: 0 for column in columns})

    if check_type == "unique_key":
        if not dataset.row_level_supported:
            return make_check_result(check, "bucket", "skipped", dataset.unsupported_reason or "No se puede validar llaves para este formato")
        columns = list(check["columns"])
        counter: Counter[tuple[Any, ...]] = Counter()
        for row in dataset.rows:
            counter.update([tuple(row.get(column) for column in columns)])
        duplicates = sum(count - 1 for count in counter.values() if count > 1)
        passed = duplicates == 0
        return make_check_result(check, "bucket", "passed" if passed else "failed", "Llave unica sin duplicados" if passed else f"Se encontraron {duplicates} duplicados", actual=duplicates, expected=0, details={"key_columns": columns})

    if check_type == "sum_equals":
        if not dataset.row_level_supported:
            return make_check_result(check, "bucket", "skipped", dataset.unsupported_reason or "No se puede sumar columnas para este formato")
        column = check["column"]
        actual = sum((to_decimal(row.get(column, 0)) for row in dataset.rows), start=Decimal("0"))
        expected = to_decimal(check["expected"])
        passed = actual == expected
        normalized = float(actual) if actual % 1 else int(actual)
        store_metric(metrics, check, normalized)
        return make_check_result(check, "bucket", "passed" if passed else "failed", f"Suma de {column}: {actual}", actual=str(actual), expected=str(expected))

    if check_type == "file_name_regex":
        expression = check["pattern"]
        import re

        failing = [item.name for item in dataset.matched_files if re.search(expression, item.name) is None]
        passed = len(failing) == 0 and dataset.file_count > 0
        return make_check_result(check, "bucket", "passed" if passed else "failed", "Todos los archivos cumplen el patron" if passed else f"Archivos fuera del patron: {', '.join(failing)}", actual=[item.name for item in dataset.matched_files], expected=expression)

    return make_check_result(check, "bucket", "failed", f"Tipo de check de bucket no soportado: {check_type}")


def load_oracledb_module():
    try:
        import oracledb  # type: ignore
    except ImportError as exc:
        raise RuntimeError("No se encontro el modulo oracledb. Instala python-oracledb para ejecutar SQL QA contra Autonomous.") from exc
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


def execute_scalar_sql(
    sql_text: str,
    connect_user: str,
    connect_password: str,
    dsn: str,
    wallet_dir: Path,
    wallet_password: str | None,
) -> Any:
    connection = open_adb_connection(connect_user, connect_password, dsn, wallet_dir, wallet_password)
    try:
        cursor = connection.cursor()
        cursor.execute(sql_text)
        row = cursor.fetchone()
        if row is None:
            return None
        return row[0]
    finally:
        connection.close()


def adb_root(context: MirrorContext, database_name: str) -> Path:
    return context.service_root("autonomous_database") / sanitize_name(database_name)


def execute_sql_check(
    check: dict[str, Any],
    repo_root: Path,
    contract_dir: Path,
    runtime: str,
    oci_mode: str,
    wallet_dir: Path | None,
    dsn: str,
    wallet_password: str | None,
    admin_user: str,
    admin_password: str | None,
    database_user: str,
    database_password: str | None,
    default_connect_user: str | None,
    default_connect_password: str | None,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    if runtime != "oci" or oci_mode != "apply":
        return make_check_result(check, "adb", "skipped", "El check SQL requiere --runtime oci --oci-mode apply")
    if wallet_dir is None:
        return make_check_result(check, "adb", "skipped", "Falta --wallet-dir para ejecutar checks SQL")

    connect_user = check.get("connect_user") or default_connect_user or database_user
    connect_password = resolve_secret(check.get("connect_password"), check.get("connect_password_env"))
    if not connect_password:
        if connect_user.upper() == admin_user.upper():
            connect_password = admin_password
        elif default_connect_user and connect_user.upper() == default_connect_user.upper():
            connect_password = default_connect_password
        elif connect_user.upper() == database_user.upper():
            connect_password = database_password
    if not connect_password:
        return make_check_result(check, "adb", "skipped", f"No se encontro password para {connect_user}")

    sql_path = resolve_path(repo_root, contract_dir, check["sql_file"], "el SQL de calidad")
    sql_text = sql_path.read_text(encoding="utf-8")
    actual = execute_scalar_sql(sql_text, connect_user, connect_password, dsn, wallet_dir, wallet_password)
    check_type = check["type"]

    if check_type == "sql_scalar_equals":
        expected = check["expected"]
        passed = actual == expected
    elif check_type == "sql_scalar_at_least":
        expected = check["expected"]
        passed = to_decimal(actual) >= to_decimal(expected)
    elif check_type == "sql_scalar_between":
        minimum = check["minimum"]
        maximum = check["maximum"]
        numeric_actual = to_decimal(actual)
        expected = {"minimum": minimum, "maximum": maximum}
        passed = numeric_actual >= to_decimal(minimum) and numeric_actual <= to_decimal(maximum)
    else:
        return make_check_result(check, "adb", "failed", f"Tipo de check SQL no soportado: {check_type}")

    store_metric(metrics, check, actual)
    if check_type == "sql_scalar_between":
        return make_check_result(check, "adb", "passed" if passed else "failed", f"Resultado SQL: {actual}", actual=actual, expected=expected, details={"sql_file": str(sql_path), "connect_user": connect_user})
    return make_check_result(check, "adb", "passed" if passed else "failed", f"Resultado SQL: {actual}", actual=actual, expected=check["expected"], details={"sql_file": str(sql_path), "connect_user": connect_user})


def evaluate_adb_check(
    check: dict[str, Any],
    context: MirrorContext,
    repo_root: Path,
    contract_dir: Path,
    database_name: str,
    runtime: str,
    oci_mode: str,
    wallet_dir: Path | None,
    dsn: str,
    wallet_password: str | None,
    admin_user: str,
    admin_password: str | None,
    database_user: str,
    database_password: str | None,
    default_connect_user: str | None,
    default_connect_password: str | None,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    check_type = check["type"]
    root = adb_root(context, database_name)

    if check_type == "mirror_path_exists":
        target = root / check["path"]
        passed = target.exists()
        return make_check_result(check, "adb", "passed" if passed else "failed", "Artefacto encontrado" if passed else f"No existe {target}", actual=str(target), expected="exists")

    if check_type == "manifest_field_equals":
        manifest_path = root / check.get("manifest_path", "database.manifest.json")
        if not manifest_path.exists():
            return make_check_result(check, "adb", "failed", f"No existe el manifest {manifest_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        field_path = check["field"].split(".")
        value: Any = manifest
        for item in field_path:
            if not isinstance(value, dict) or item not in value:
                return make_check_result(check, "adb", "failed", f"No existe el campo {check['field']} en {manifest_path}")
            value = value[item]
        passed = value == check["expected"]
        return make_check_result(check, "adb", "passed" if passed else "failed", f"Campo {check['field']}: {value}", actual=value, expected=check["expected"])

    if check_type.startswith("sql_"):
        return execute_sql_check(
            check,
            repo_root,
            contract_dir,
            runtime,
            oci_mode,
            wallet_dir,
            dsn,
            wallet_password,
            admin_user,
            admin_password,
            database_user,
            database_password,
            default_connect_user,
            default_connect_password,
            metrics,
        )

    return make_check_result(check, "adb", "failed", f"Tipo de check ADB no soportado: {check_type}")


def evaluate_reconciliation_check(check: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    left_metric = check["left_metric"]
    left_value = metrics.get(left_metric)
    if left_value is None:
        return make_check_result(check, "reconciliation", "skipped", f"No existe la metrica {left_metric}")

    if "right_metric" in check:
        right_metric = check["right_metric"]
        right_value = metrics.get(right_metric)
        if right_value is None:
            return make_check_result(check, "reconciliation", "skipped", f"No existe la metrica {right_metric}")
        expected = right_value
    else:
        right_metric = None
        right_value = check["expected"]
        expected = right_value

    operator = check.get("operator", "equals")
    try:
        left_decimal = to_decimal(left_value)
        right_decimal = to_decimal(right_value)
    except InvalidOperation:
        left_decimal = None
        right_decimal = None

    if operator == "equals":
        passed = left_value == right_value
    elif operator == "at_least":
        passed = left_decimal is not None and right_decimal is not None and left_decimal >= right_decimal
    elif operator == "at_most":
        passed = left_decimal is not None and right_decimal is not None and left_decimal <= right_decimal
    elif operator == "difference_lte":
        tolerance = to_decimal(check["tolerance"])
        passed = left_decimal is not None and right_decimal is not None and abs(left_decimal - right_decimal) <= tolerance
    else:
        return make_check_result(check, "reconciliation", "failed", f"Operador no soportado: {operator}")

    details = {"left_metric": left_metric, "right_metric": right_metric, "operator": operator}
    if "tolerance" in check:
        details["tolerance"] = check["tolerance"]
    return make_check_result(check, "reconciliation", "passed" if passed else "failed", f"Comparacion {left_metric} {operator} {right_metric or 'expected'}", actual=left_value, expected=expected, details=details)


def summarize_checks(checks: list[dict[str, Any]]) -> dict[str, Any]:
    by_status = Counter(item["status"] for item in checks)
    by_severity = Counter(item["severity"] for item in checks)
    overall = "passed"
    if any(item["status"] == "failed" for item in checks):
        overall = "failed"
    elif any(item["status"] == "skipped" for item in checks):
        overall = "warn"
    return {
        "overall_status": overall,
        "total_checks": len(checks),
        "by_status": dict(by_status),
        "by_severity": dict(by_severity),
    }


def run_contract(args: argparse.Namespace) -> dict[str, Any]:
    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    runtime_payload = runtime_payload_from_args(args)
    repo_root = context.repo_root
    contract_path, contract = load_json_contract(repo_root, args.contract_file)
    contract_name = contract.get("contract_name") or contract_path.stem
    contract_dir = contract_path.parent
    run_dir = contract_run_root(context, contract_name)
    write_json(run_dir / "contract.snapshot.json", contract)

    bucket_targets = contract.get("bucket_targets", [])
    datasets: dict[str, BucketDataset] = {}
    profiles_payload: dict[str, Any] = {}
    for target in bucket_targets:
        target_name = target["target_name"]
        dataset = load_bucket_dataset(context, target_name, target, sample_size=args.sample_size)
        datasets[target_name] = dataset
        profiles_payload[target_name] = dataset_profile_payload(dataset)

    metrics: dict[str, Any] = {}
    checks: list[dict[str, Any]] = []

    for check in contract.get("bucket_checks", []):
        result = evaluate_bucket_check(check, datasets, metrics)
        checks.append(result)

    wallet_dir = resolve_path(repo_root, repo_root, args.wallet_dir, "el wallet") if args.wallet_dir else None
    wallet_password = resolve_secret(args.wallet_password, args.wallet_password_env)
    admin_password = resolve_secret(args.admin_password, args.admin_password_env, ("DB_PASSWORD",))
    database_user = args.database_user or contract.get("adb", {}).get("database_user", "APP_GOLD")
    database_password = resolve_secret(args.database_password, args.database_password_env, ("DB_PASSWORD",))
    default_connect_user = args.connect_user or contract.get("adb", {}).get("connect_user")
    default_connect_password = resolve_secret(args.connect_password, args.connect_password_env)
    database_name = args.database_name or contract.get("adb", {}).get("database_name")
    adb_checks = contract.get("adb_checks", [])
    if adb_checks and not database_name:
        raise ValueError("El contrato o los argumentos deben incluir database_name para los checks ADB.")

    for check in adb_checks:
        result = evaluate_adb_check(
            check,
            context,
            repo_root,
            contract_dir,
            database_name,
            args.runtime,
            args.oci_mode,
            wallet_dir,
            args.dsn,
            wallet_password,
            args.admin_user,
            admin_password,
            database_user,
            database_password,
            default_connect_user,
            default_connect_password,
            metrics,
        )
        checks.append(result)

    for check in contract.get("reconciliation_checks", []):
        result = evaluate_reconciliation_check(check, metrics)
        checks.append(result)

    summary = summarize_checks(checks)
    payload = {
        "contract_name": contract_name,
        "contract_path": str(contract_path),
        "dataset": contract.get("dataset"),
        "layer": contract.get("layer"),
        "project_id": runtime_payload.get("project_id"),
        "workflow_id": runtime_payload.get("workflow_id"),
        "run_id": runtime_payload.get("run_id"),
        "slice_key": runtime_payload.get("slice_key"),
        "runtime": args.runtime,
        "oci_mode": args.oci_mode,
        "created_at_utc": utc_timestamp(),
        "bucket_profiles": profiles_payload,
        "metrics": metrics,
        "checks": checks,
        "summary": summary,
        "database_name": database_name,
        "gate": contract.get("gate", {}),
    }
    result_path = run_dir / "result.json"
    write_json(result_path, payload)
    quality_report(context, "run-contract", {"contract_name": contract_name, "result_path": str(result_path), "summary": summary})
    return {"result_path": result_path, "payload": payload}


def evaluate_gate(result_payload: dict[str, Any], threshold: str) -> dict[str, Any]:
    threshold_rank = SEVERITY_ORDER[threshold]
    failed_at_threshold = [
        item
        for item in result_payload["checks"]
        if item["status"] == "failed" and SEVERITY_ORDER[item["severity"]] >= threshold_rank
    ]
    failed_below_threshold = [
        item
        for item in result_payload["checks"]
        if item["status"] == "failed" and SEVERITY_ORDER[item["severity"]] < threshold_rank
    ]
    skipped_checks = [item for item in result_payload["checks"] if item["status"] == "skipped"]

    if failed_at_threshold:
        status = "FAIL"
    elif failed_below_threshold or skipped_checks:
        status = "WARN"
    else:
        status = "PASS"

    return {
        "status": status,
        "severity_threshold": threshold,
        "failed_at_threshold": len(failed_at_threshold),
        "failed_below_threshold": len(failed_below_threshold),
        "skipped": len(skipped_checks),
        "blocking_checks": [item["name"] for item in failed_at_threshold],
        "warning_checks": [item["name"] for item in failed_below_threshold + skipped_checks],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-data-quality-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--command", required=True, choices=("profile-bucket-data", "run-contract", "gate-migration"))
    parser.add_argument("--bucket-name")
    parser.add_argument("--object-glob", default="objects/**/*")
    parser.add_argument("--data-format", default="auto")
    parser.add_argument("--target-name", default="default")
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--contract-file")
    parser.add_argument("--result-path")
    parser.add_argument("--gate-name")
    parser.add_argument("--severity-threshold", choices=tuple(SEVERITY_ORDER.keys()))
    parser.add_argument("--database-name")
    parser.add_argument("--database-user")
    parser.add_argument("--wallet-dir")
    parser.add_argument("--dsn", default=os.getenv("ADW_DSN", "dbclarogold_high"))
    parser.add_argument("--wallet-password")
    parser.add_argument("--wallet-password-env", default="DB_WALLET_PASSWORD")
    parser.add_argument("--admin-user", default=os.getenv("DB_USER", "ADMIN"))
    parser.add_argument("--admin-password")
    parser.add_argument("--admin-password-env", default="DB_PASSWORD")
    parser.add_argument("--database-password")
    parser.add_argument("--database-password-env", default="APP_GOLD_PASSWORD")
    parser.add_argument("--connect-user")
    parser.add_argument("--connect-password")
    parser.add_argument("--connect-password-env")
    add_standard_runtime_args(parser)
    args = parser.parse_args()

    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)
    runtime_payload = runtime_payload_from_args(args)

    if args.command == "profile-bucket-data":
        if not args.bucket_name:
            raise SystemExit("--bucket-name es requerido para profile-bucket-data")
        dataset = load_bucket_dataset(
            context,
            args.target_name,
            {
                "bucket_name": args.bucket_name,
                "object_glob": args.object_glob,
                "data_format": args.data_format,
            },
            sample_size=args.sample_size,
        )
        payload = dataset_profile_payload(dataset)
        profile_path = quality_root(context) / "profiles" / f"{utc_timestamp()}-{sanitize_name(args.target_name)}.json"
        write_json(profile_path, payload)
        quality_report(context, "profile-bucket-data", {"target_name": args.target_name, "profile_path": str(profile_path), "bucket_name": args.bucket_name})
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "quality",
            "profile_bucket_data",
            "mirrored",
            metrics={"rows_in": payload["row_count"]},
            extra={"profile_path": str(profile_path), "bucket_name": args.bucket_name, "target_name": args.target_name},
        )
        print(json.dumps({"status": "ok", "profile_path": str(profile_path), "control_paths": control_paths}, indent=2, ensure_ascii=True))
        return 0

    if args.command == "run-contract":
        if not args.contract_file:
            raise SystemExit("--contract-file es requerido para run-contract")
        result = run_contract(args)
        control_database_name = runtime_payload.get("control_database_name") or args.database_name or result["payload"].get("database_name")
        quality_path = None
        if control_database_name:
            quality_path = register_quality_result(
                context,
                control_database_name,
                runtime_payload,
                result["payload"]["contract_name"],
                result["payload"]["summary"]["overall_status"],
                result["payload"]["summary"],
                str(result["result_path"]),
                extra={
                    "layer": result["payload"].get("layer"),
                    "severity_threshold": result["payload"].get("gate", {}).get("severity_threshold"),
                },
            )
        control_paths = record_control_runtime(
            context,
            runtime_payload,
            "quality",
            "run_contract",
            result["payload"]["summary"]["overall_status"],
            database_name=control_database_name,
            metrics={
                "rows_in": result["payload"]["metrics"].get("bucket_row_count"),
                "rows_out": result["payload"]["summary"]["total_checks"],
                "rows_rejected": result["payload"]["summary"]["by_status"].get("failed", 0),
            },
            extra={
                "result_path": str(result["result_path"]),
                "quality_result_path": str(quality_path) if quality_path else None,
                "contract_name": result["payload"]["contract_name"],
            },
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "result_path": str(result["result_path"]),
                    "summary": result["payload"]["summary"],
                    "quality_result_path": str(quality_path) if quality_path else None,
                    "control_paths": control_paths,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    if not args.result_path:
        raise SystemExit("--result-path es requerido para gate-migration")

    result_path = resolve_path(context.repo_root, context.repo_root, args.result_path, "el resultado de calidad")
    result_payload = json.loads(result_path.read_text(encoding="utf-8"))
    threshold = normalize_severity(args.severity_threshold or result_payload.get("gate", {}).get("severity_threshold") or "high")
    gate_name = args.gate_name or result_payload.get("contract_name") or result_path.stem
    gate_summary = evaluate_gate(result_payload, threshold)
    output = {
        "gate_name": gate_name,
        "result_path": str(result_path),
        "created_at_utc": utc_timestamp(),
        "summary": gate_summary,
    }
    root = gate_root(context, gate_name)
    gate_path = root / "gate.json"
    write_json(gate_path, output)
    quality_report(context, "gate-migration", {"gate_name": gate_name, "gate_path": str(gate_path), "summary": gate_summary})
    control_database_name = runtime_payload.get("control_database_name") or result_payload.get("database_name") or args.database_name
    control_paths = record_control_runtime(
        context,
        runtime_payload,
        "quality",
        "gate_migration",
        gate_summary["status"].lower(),
        database_name=control_database_name,
        metrics={"rows_rejected": gate_summary["failed_at_threshold"]},
        extra={"gate_path": str(gate_path), "gate_name": gate_name},
    )
    print(json.dumps({"status": "ok", "gate_path": str(gate_path), "summary": gate_summary, "control_paths": control_paths}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
