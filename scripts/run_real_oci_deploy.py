from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePath
from typing import Any, Callable
from urllib.parse import urlparse


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[1]
DEFAULT_BUSINESS_DATE = "2026-03-26"
DEFAULT_BATCH_ID = "001"
MCP_ROOT = Path("mcp") / "servers"

if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.oci_cli import OciExecutionContext, _prepare_host_oci_dir, build_oci_command
from mcp.common.local_services import (
    create_data_flow_private_endpoint,
    create_network_nsg,
    create_network_route_table,
    create_network_service_gateway,
    create_network_subnet,
    create_network_vcn,
    update_network_nsg,
    update_network_route_table,
)
from mcp.common.runtime import MirrorContext, append_run_log, mirror_run_log_paths

GOLD_SAMPLE_CSV = textwrap.dedent(
    """\
    ID_DWH_DIA,ID_DWH_PERIODO,FILE_OUTPUT,TIPO_TRAF,Q_REGISTROS,VOLUMEN_ORIGEN,SEGUNDOS_ORIGEN,EVENTOS_ORIGEN,Q_REGISTROS_OK,VOLUMEN_OK,SEGUNDOS_OK,EVENTOS_OK,CANTEVENTOS_DISCARD,VOLUMEN_DISCARD,SEGUNDOS_DISCARD,EVENTOS_DISCARD,CANTEVENTOS_SUSPEND,VOLUMEN_SUSPEND,SEGUNDOS_SUSPEND,EVENTOS_SUSPEND,CANTEVENTOS_REJECT,VOLUMEN_REJECT,SEGUNDOS_REJECT,EVENTOS_REJECT,CANTEVENTOS_FILT,VOLUMEN_FILT,SEGUNDOS_FILT,EVENTOS_FILT,CANTEVENTOS_ENCOLADO,VOLUMEN_ENCOLADO,SEGUNDOS_ENCOLADO,EVENTOS_ENCOLADO
    20260325,1,trafico_001.csv,DATA,125,0,0,0,120,0,0,0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    20260325,2,trafico_002.csv,DATA,300,0,0,0,260,0,0,0,40,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    20260326,1,trafico_003.csv,VOICE,425,0,0,0,390,0,0,0,35,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    """
)

QUALITY_SQL_FILES = {
    "010_check_app_gold_user_exists.sql": "SELECT COUNT(*) AS VALUE FROM ALL_USERS WHERE USERNAME = 'APP_GOLD'\n",
    "020_check_agg_table_exists.sql": (
        "SELECT COUNT(*) AS VALUE\n"
        "FROM ALL_TABLES\n"
        "WHERE OWNER = 'APP_GOLD'\n"
        "  AND TABLE_NAME = 'AGG_RESUMEN_ARCHIVOS_TRAFICO'\n"
    ),
    "030_check_agg_row_count.sql": "SELECT COUNT(*) AS VALUE FROM APP_GOLD.AGG_RESUMEN_ARCHIVOS_TRAFICO\n",
    "040_check_sum_q_registros.sql": (
        "SELECT NVL(SUM(Q_REGISTROS), 0) AS VALUE\n"
        "FROM APP_GOLD.AGG_RESUMEN_ARCHIVOS_TRAFICO\n"
    ),
    "050_check_sum_q_registros_ok.sql": (
        "SELECT NVL(SUM(Q_REGISTROS_OK), 0) AS VALUE\n"
        "FROM APP_GOLD.AGG_RESUMEN_ARCHIVOS_TRAFICO\n"
    ),
    "060_check_sum_discard.sql": (
        "SELECT NVL(SUM(CANTEVENTOS_DISCARD), 0) AS VALUE\n"
        "FROM APP_GOLD.AGG_RESUMEN_ARCHIVOS_TRAFICO\n"
    ),
}

CATALOG_TYPE_ALIASES = {
    "oracle_object_storage": "Oracle Object Storage",
    "oracle object storage": "Oracle Object Storage",
}


class CommandError(RuntimeError):
    def __init__(self, command: list[str], stdout: str, stderr: str) -> None:
        self.command = command
        self.stdout = stdout
        self.stderr = stderr
        payload = {
            "message": "Command failed",
            "command": command,
            "stdout": stdout,
            "stderr": stderr,
        }
        super().__init__(json.dumps(payload, ensure_ascii=True))


@dataclass(frozen=True)
class DeployNames:
    project_id: str
    tag: str
    environment: str
    region: str
    namespace: str
    tenancy_id: str
    compartment_name: str
    project_prefix: str
    operator_group_name: str
    dataflow_admin_group_name: str
    adb_dynamic_group_name: str
    catalog_dynamic_group_name: str
    operators_policy_name: str
    dataflow_policy_name: str
    di_service_policy_name: str
    adb_policy_name: str
    di_policy_name: str
    catalog_policy_name: str
    landing_bucket: str
    bronze_bucket: str
    silver_bucket: str
    gold_bucket: str
    database_name: str
    database_user: str
    control_schema: str
    control_user: str
    adb_db_name: str
    adb_display_name: str
    wallet_dir: Path
    workspace_name: str
    di_project_name: str
    di_folder_name: str
    di_pipeline_name: str
    catalog_name: str
    catalog_asset_name: str
    vcn_name: str
    service_gateway_name: str
    subnet_name: str
    data_flow_subnet_name: str
    autonomous_subnet_name: str
    nsg_name: str
    route_table_name: str
    dns_label: str
    subnet_dns_label: str
    data_flow_subnet_dns_label: str
    autonomous_subnet_dns_label: str
    data_flow_private_endpoint_name: str
    adb_private_endpoint_label: str
    workflow_id: str
    run_id: str
    replay_run_id: str
    reprocess_request_id: str
    slice_key: str
    quality_contract_name: str
    gold_object_name: str
    landing_root_uri: str
    bronze_root_uri: str
    silver_root_uri: str
    gold_root_uri: str
    gold_source_uri: str


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _json_default(value: Any) -> str:
    if isinstance(value, PurePath):
        return str(value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def write_text(path: Path, content: str) -> None:
    ensure_directory(path.parent)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=_json_default) + "\n", encoding="utf-8")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sanitize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def normalize_entity_name(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")


def run_command(
    command: list[str],
    cwd: Path,
    timeout_seconds: int = 600,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        env=merged_env,
        check=False,
    )
    if result.returncode != 0:
        raise CommandError(command, result.stdout, result.stderr)
    return result


def run_json_command(
    command: list[str],
    cwd: Path,
    timeout_seconds: int = 600,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    result = run_command(command, cwd=cwd, timeout_seconds=timeout_seconds, env=env)
    stdout = result.stdout.strip()
    if not stdout:
        return {}
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            json.dumps(
                {
                    "message": "Command did not return JSON",
                    "command": command,
                    "stdout": stdout,
                    "stderr": result.stderr,
                },
                ensure_ascii=True,
            )
        ) from exc


def run_repo_script_json(
    repo_root: Path,
    relative_script: str,
    args: list[str],
    *,
    timeout_seconds: int = 600,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    command = [sys.executable, str(repo_root / relative_script), *args]
    return run_json_command(command, cwd=repo_root, timeout_seconds=timeout_seconds, env=env)


def call_mcp_json(
    repo_root: Path,
    server_name: str,
    args: list[str],
    *,
    timeout_seconds: int = 900,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    server_path = repo_root / MCP_ROOT / server_name / "server.py"
    command = [sys.executable, str(server_path), "--repo-root", str(repo_root), *args]
    return run_json_command(command, cwd=repo_root, timeout_seconds=timeout_seconds, env=env)


def normalize_repo_mounted_paths(repo_root: Path, payload: Any) -> Any:
    if isinstance(payload, dict):
        return {key: normalize_repo_mounted_paths(repo_root, value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [normalize_repo_mounted_paths(repo_root, value) for value in payload]
    if isinstance(payload, str):
        normalized = payload.replace("\\", "/")
        if normalized == "/workspace":
            return str(repo_root)
        if normalized.startswith("/workspace/"):
            relative = normalized.removeprefix("/workspace/").strip("/")
            return str((repo_root / Path(relative)).resolve())
    return payload


def call_mcp_json_in_docker(
    repo_root: Path,
    server_name: str,
    args: list[str],
    *,
    timeout_seconds: int = 900,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    server_path = repo_root / MCP_ROOT / server_name / "server.py"
    if os.name == "nt":
        command = [
            "powershell",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(repo_root / "scripts" / "docker_repo_python.ps1"),
            str(server_path),
            "--repo-root",
            str(repo_root),
            *args,
        ]
    else:
        command = [
            "bash",
            str(repo_root / "scripts" / "docker_repo_python.sh"),
            str(server_path),
            "--repo-root",
            str(repo_root),
            *args,
        ]
    payload = run_json_command(command, cwd=repo_root, timeout_seconds=timeout_seconds, env=env)
    return normalize_repo_mounted_paths(repo_root, payload)


def run_oci_cli_json(
    repo_root: Path,
    args: list[str],
    *,
    timeout_seconds: int = 300,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    execution = OciExecutionContext(repo_root=repo_root)
    prepared_oci_dir = _prepare_host_oci_dir(execution)
    command = build_oci_command(execution, args, host_oci_dir=prepared_oci_dir)
    try:
        return run_json_command(command, cwd=repo_root, timeout_seconds=timeout_seconds, env=env)
    finally:
        shutil.rmtree(prepared_oci_dir, ignore_errors=True)


def retry(
    description: str,
    action: Callable[[], Any],
    *,
    attempts: int = 3,
    delay_seconds: int = 10,
) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return action()
        except Exception as exc:  # pragma: no cover - operational retry path
            last_error = exc
            if attempt >= attempts:
                raise
            time.sleep(delay_seconds)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Retry failed for {description}")


def parse_test_env(env_path: Path) -> dict[str, list[str]]:
    values: dict[str, list[str]] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values.setdefault(key.strip(), []).append(value.strip())
    return values


def apply_sensitive_environment(env_values: dict[str, list[str]]) -> dict[str, str]:
    admin_user = env_values["ora26ai_user_admin"][0]
    all_passwords = env_values["ora26ai_user_password"]
    if len(all_passwords) < 2:
        raise ValueError("Se esperaban al menos dos ocurrencias de ora26ai_user_password en .test/.env")
    admin_password = all_passwords[0]
    app_password = all_passwords[-1]
    wallet_password = env_values["ora26ai_wallet_password"][0]
    app_user = env_values["ora26ai_user_dev"][0]

    os.environ["DB_USER"] = admin_user
    os.environ["DB_PASSWORD"] = admin_password
    os.environ["APP_GOLD_PASSWORD"] = app_password
    os.environ["MDL_CTL_PASSWORD"] = app_password
    os.environ["DB_WALLET_PASSWORD"] = wallet_password
    os.environ["ADW_USER"] = app_user
    return {
        "admin_user": admin_user,
        "app_user": app_user,
    }


def collect_test_sources(source_root: Path, suffixes: tuple[str, ...]) -> list[Path]:
    return sorted(item for item in source_root.iterdir() if item.is_file() and item.suffix.lower() in suffixes)


def choose_tag(explicit_tag: str | None) -> str:
    if explicit_tag:
        normalized = re.sub(r"[^a-z0-9]+", "", explicit_tag.lower())
        if not normalized:
            raise ValueError(f"Tag invalido: {explicit_tag}")
        return normalized[:12]
    return "r" + datetime.now(timezone.utc).strftime("%m%d%H%M")


def load_oci_profile(repo_root: Path) -> dict[str, str]:
    config_path = repo_root / ".local" / "oci" / "config"
    if not config_path.exists():
        raise FileNotFoundError(f"No existe la configuracion OCI stageada en {config_path}")
    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read(config_path, encoding="utf-8")
    defaults = parser.defaults()
    tenancy = defaults.get("tenancy")
    region = defaults.get("region")
    if not tenancy or not region:
        raise ValueError(f"El archivo OCI config en {config_path} no incluye tenancy y region")
    return {"tenancy_id": tenancy, "region": region}


def get_namespace(repo_root: Path) -> str:
    payload = run_oci_cli_json(repo_root, ["os", "ns", "get"])
    namespace = payload.get("data")
    if not namespace:
        raise RuntimeError("No se pudo obtener el namespace OCI")
    return str(namespace)


def get_object_storage_service(repo_root: Path) -> dict[str, str]:
    payload = run_oci_cli_json(repo_root, ["network", "service", "list", "--all"])
    items = payload.get("data", [])
    if not isinstance(items, list):
        raise RuntimeError("OCI no devolvio la lista de network services")
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("cidr-block")) == "oci-ord-objectstorage":
            return {
                "service_id": str(item.get("id")),
                "service_name": str(item.get("name")),
                "cidr_block": str(item.get("cidr-block")),
            }
    raise RuntimeError("No se encontro el service gateway target para OCI Object Storage")


def wait_for_compartment_state(repo_root: Path, compartment_id: str, target_state: str = "ACTIVE", timeout_seconds: int = 900) -> dict[str, Any]:
    started = time.time()
    desired = target_state.upper()
    while True:
        try:
            payload = run_oci_cli_json(repo_root, ["iam", "compartment", "get", "--compartment-id", compartment_id], timeout_seconds=120)
        except CommandError as exc:
            retryable_error = "NotAuthorizedOrNotFound" in exc.stderr or '"status": 404' in exc.stderr
            if retryable_error and time.time() - started <= timeout_seconds:
                time.sleep(10)
                continue
            raise
        data = payload.get("data", {})
        lifecycle_state = str(data.get("lifecycle-state", "")).upper()
        if lifecycle_state == desired:
            return data
        if time.time() - started > timeout_seconds:
            raise TimeoutError(f"El compartment {compartment_id} no llego a {desired} en {timeout_seconds}s. Estado actual: {lifecycle_state}")
        time.sleep(10)


def wait_for_bucket_exists(repo_root: Path, bucket_name: str, namespace: str, timeout_seconds: int = 900) -> dict[str, Any]:
    started = time.time()
    while True:
        try:
            payload = run_oci_cli_json(
                repo_root,
                ["os", "bucket", "get", "--bucket-name", bucket_name, "--namespace-name", namespace],
                timeout_seconds=120,
            )
            data = payload.get("data", {})
            if data:
                return data
        except CommandError as exc:
            retryable_error = "BucketNotFound" in exc.stderr or '"status": 404' in exc.stderr
            if not retryable_error:
                raise
        if time.time() - started > timeout_seconds:
            raise TimeoutError(f"El bucket {bucket_name} no estuvo disponible en {timeout_seconds}s")
        time.sleep(10)


def get_bucket_if_exists(repo_root: Path, bucket_name: str, namespace: str) -> dict[str, Any] | None:
    try:
        payload = run_oci_cli_json(
            repo_root,
            ["os", "bucket", "get", "--bucket-name", bucket_name, "--namespace-name", namespace],
            timeout_seconds=120,
        )
    except CommandError as exc:
        if "BucketNotFound" in exc.stderr or '"status": 404' in exc.stderr:
            return None
        raise
    data = payload.get("data", {})
    return data or None


def find_compartment_by_name(repo_root: Path, parent_compartment_id: str, compartment_name: str) -> dict[str, Any] | None:
    payload = run_oci_cli_json(
        repo_root,
        [
            "iam",
            "compartment",
            "list",
            "--compartment-id",
            parent_compartment_id,
            "--compartment-id-in-subtree",
            "true",
            "--access-level",
            "ACCESSIBLE",
            "--all",
        ],
        timeout_seconds=180,
    )
    matches: list[dict[str, Any]] = []
    for item in payload.get("data", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("name", "")).strip() != compartment_name:
            continue
        if str(item.get("lifecycle-state", "")).upper() == "DELETED":
            continue
        matches.append(item)
    if not matches:
        return None
    direct_children = [item for item in matches if str(item.get("compartment-id", "")).strip() == parent_compartment_id]
    selected = direct_children[0] if direct_children else matches[0]
    return selected


def find_named_oci_resource(
    repo_root: Path,
    command: list[str],
    resource_name: str,
    *,
    name_keys: tuple[str, ...] = ("display-name", "name"),
    deleted_states: tuple[str, ...] = ("TERMINATED", "TERMINATING", "DELETED"),
) -> dict[str, Any] | None:
    payload = run_oci_cli_json(repo_root, command, timeout_seconds=180)
    matches: list[dict[str, Any]] = []
    for item in payload.get("data", []):
        if not isinstance(item, dict):
            continue
        item_name = next((str(item.get(key, "")).strip() for key in name_keys if str(item.get(key, "")).strip()), "")
        if item_name != resource_name:
            continue
        lifecycle_state = str(item.get("lifecycle-state", item.get("state", ""))).upper()
        if lifecycle_state in deleted_states:
            continue
        matches.append(item)
    return matches[0] if matches else None


def find_vcn_by_name(repo_root: Path, compartment_id: str, vcn_name: str) -> dict[str, Any] | None:
    return find_named_oci_resource(
        repo_root,
        ["network", "vcn", "list", "--compartment-id", compartment_id, "--all"],
        vcn_name,
    )


def find_route_table_by_name(repo_root: Path, compartment_id: str, vcn_id: str, route_table_name: str) -> dict[str, Any] | None:
    return find_named_oci_resource(
        repo_root,
        ["network", "route-table", "list", "--compartment-id", compartment_id, "--vcn-id", vcn_id, "--all"],
        route_table_name,
    )


def find_service_gateway_by_name(repo_root: Path, compartment_id: str, vcn_id: str, service_gateway_name: str) -> dict[str, Any] | None:
    return find_named_oci_resource(
        repo_root,
        ["network", "service-gateway", "list", "--compartment-id", compartment_id, "--vcn-id", vcn_id, "--all"],
        service_gateway_name,
    )


def find_nsg_by_name(repo_root: Path, compartment_id: str, vcn_id: str, nsg_name: str) -> dict[str, Any] | None:
    return find_named_oci_resource(
        repo_root,
        ["network", "nsg", "list", "--compartment-id", compartment_id, "--vcn-id", vcn_id, "--all"],
        nsg_name,
    )


def find_subnet_by_name(repo_root: Path, compartment_id: str, vcn_id: str, subnet_name: str) -> dict[str, Any] | None:
    return find_named_oci_resource(
        repo_root,
        ["network", "subnet", "list", "--compartment-id", compartment_id, "--vcn-id", vcn_id, "--all"],
        subnet_name,
    )


def find_data_flow_private_endpoint_by_name(repo_root: Path, compartment_id: str, endpoint_name: str) -> dict[str, Any] | None:
    return find_named_oci_resource(
        repo_root,
        ["data-flow", "private-endpoint", "list", "--compartment-id", compartment_id, "--all"],
        endpoint_name,
    )


def list_nsg_rules(repo_root: Path, nsg_id: str) -> list[dict[str, Any]]:
    payload = run_oci_cli_json(repo_root, ["network", "nsg", "rules", "list", "--nsg-id", nsg_id, "--all"], timeout_seconds=180)
    items = payload.get("data", [])
    return [item for item in items if isinstance(item, dict)]


def get_route_table_details(repo_root: Path, route_table_id: str) -> dict[str, Any]:
    payload = run_oci_cli_json(repo_root, ["network", "route-table", "get", "--rt-id", route_table_id], timeout_seconds=180)
    data = payload.get("data", {})
    return data if isinstance(data, dict) else {}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def normalize_private_endpoint_host(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if "://" in candidate:
        parsed = urlparse(candidate)
        return parsed.hostname or candidate
    if "/" in candidate:
        parsed = urlparse(f"https://{candidate}")
        return parsed.hostname or candidate.split("/", 1)[0]
    return candidate


def get_autonomous_database_details(repo_root: Path, autonomous_database_id: str) -> dict[str, Any]:
    payload = run_oci_cli_json(
        repo_root,
        ["db", "autonomous-database", "get", "--autonomous-database-id", autonomous_database_id],
        timeout_seconds=180,
    )
    data = payload.get("data", {})
    return data if isinstance(data, dict) else {}


def resolve_autonomous_private_endpoint(repo_root: Path, autonomous_database_id: str) -> str | None:
    data = get_autonomous_database_details(repo_root, autonomous_database_id)
    for key in ("private-endpoint", "private_endpoint", "private-endpoint-url", "private_endpoint_url"):
        host = normalize_private_endpoint_host(str(data.get(key, "") or ""))
        if host:
            return host
    return None


def build_names(repo_root: Path, project_id: str, environment: str, region: str, namespace: str, tenancy_id: str, tag: str) -> DeployNames:
    suffix = f"{environment}-{tag}"
    project_slug = sanitize_token(project_id)
    compartment_name = f"data-medallion-{environment}"
    database_name = f"adb_trafico_{tag}"
    adb_db_name = f"TD{tag.upper()[:8]}"
    workspace_name = f"ws-trafico-{suffix}"
    catalog_name = f"dc-trafico-{suffix}"
    wallet_dir = repo_root / ".local" / "autonomous" / "wallets" / environment / database_name
    dns_label = f"mdl{environment}".lower()[:15]
    project_prefix = f"projects/{project_slug}"
    gold_object_name = f"{project_prefix}/exports/agg_resumen_archivos_trafico/process_date=2026-03-26/agg_resumen_archivos_trafico_sample.csv"
    landing_bucket = "bucket-landing-external"
    bronze_bucket = "bucket-bronze-raw"
    silver_bucket = "bucket-silver-trusted"
    gold_bucket = "bucket-gold-refined"
    service_gateway_name = f"sgw-data-medallion-{environment}"
    route_table_name = f"rt-data-medallion-{environment}"
    vcn_name = f"vcn-data-medallion-{environment}"
    subnet_name = f"subnet-di-{environment}"
    data_flow_subnet_name = f"subnet-dataflow-{environment}"
    autonomous_subnet_name = f"subnet-autonomous-{environment}"
    nsg_name = f"nsg-private-services-{environment}"
    subnet_dns_label = "disubnet"
    data_flow_subnet_dns_label = "dfsubnet"
    autonomous_subnet_dns_label = "adbsubnet"
    data_flow_private_endpoint_name = f"dflow-pe-{suffix}"
    adb_private_endpoint_label = ("adb" + re.sub(r"[^a-z0-9]+", "", tag.lower()))[:15]
    landing_root_uri = f"oci://{landing_bucket}@{namespace}/{project_prefix}/"
    bronze_root_uri = f"oci://{bronze_bucket}@{namespace}/{project_prefix}/"
    silver_root_uri = f"oci://{silver_bucket}@{namespace}/{project_prefix}/"
    gold_root_uri = f"oci://{gold_bucket}@{namespace}/{project_prefix}/"
    return DeployNames(
        project_id=project_id,
        tag=tag,
        environment=environment,
        region=region,
        namespace=namespace,
        tenancy_id=tenancy_id,
        compartment_name=compartment_name,
        project_prefix=project_prefix,
        operator_group_name=f"grp-trafico-operators-{suffix}",
        dataflow_admin_group_name=f"grp-trafico-dataflow-{suffix}",
        adb_dynamic_group_name=f"dg-trafico-adb-{suffix}",
        catalog_dynamic_group_name=f"dg-trafico-catalog-{suffix}",
        operators_policy_name=f"plc-trafico-operators-{suffix}",
        dataflow_policy_name=f"plc-trafico-dataflow-{suffix}",
        di_service_policy_name=f"plc-trafico-di-service-{suffix}",
        adb_policy_name=f"plc-trafico-adb-{suffix}",
        di_policy_name=f"plc-trafico-di-{suffix}",
        catalog_policy_name=f"plc-trafico-catalog-{suffix}",
        landing_bucket=landing_bucket,
        bronze_bucket=bronze_bucket,
        silver_bucket=silver_bucket,
        gold_bucket=gold_bucket,
        database_name=database_name,
        database_user="APP_GOLD",
        control_schema="MDL_CTL",
        control_user="MDL_CTL",
        adb_db_name=adb_db_name,
        adb_display_name=f"ADW_TRAFICO_{tag.upper()}",
        wallet_dir=wallet_dir,
        workspace_name=workspace_name,
        di_project_name=f"TRAFICO_{tag.upper()}",
        di_folder_name=f"DATAFLOW_{tag.upper()}",
        di_pipeline_name=f"trafico-orchestrator-{suffix}",
        catalog_name=catalog_name,
        catalog_asset_name=f"gold-refined-{suffix}",
        vcn_name=vcn_name,
        service_gateway_name=service_gateway_name,
        subnet_name=subnet_name,
        data_flow_subnet_name=data_flow_subnet_name,
        autonomous_subnet_name=autonomous_subnet_name,
        nsg_name=nsg_name,
        route_table_name=route_table_name,
        dns_label=dns_label,
        subnet_dns_label=subnet_dns_label,
        data_flow_subnet_dns_label=data_flow_subnet_dns_label,
        autonomous_subnet_dns_label=autonomous_subnet_dns_label,
        data_flow_private_endpoint_name=data_flow_private_endpoint_name,
        adb_private_endpoint_label=adb_private_endpoint_label,
        workflow_id=f"wf-{project_id}",
        run_id=f"run-{project_id}-001",
        replay_run_id=f"run-{project_id}-002",
        reprocess_request_id=f"rpr-{project_id}-001",
        slice_key=f"entity=agg_resumen_archivos_trafico/business_date={DEFAULT_BUSINESS_DATE}/batch_id={DEFAULT_BATCH_ID}",
        quality_contract_name="agg_resumen_archivos_trafico_gold",
        gold_object_name=gold_object_name,
        landing_root_uri=landing_root_uri,
        bronze_root_uri=bronze_root_uri,
        silver_root_uri=silver_root_uri,
        gold_root_uri=gold_root_uri,
        gold_source_uri=f"oci://{gold_bucket}@{namespace}/{gold_object_name}",
    )


def operator_policy_statements(compartment_name: str, operator_group_name: str) -> list[str]:
    return [
        f"Allow group {operator_group_name} to inspect compartments in tenancy",
        f"Allow group {operator_group_name} to manage buckets in compartment {compartment_name}",
        f"Allow group {operator_group_name} to manage objects in compartment {compartment_name}",
        f"Allow group {operator_group_name} to manage autonomous-database-family in compartment {compartment_name}",
        f"Allow group {operator_group_name} to use virtual-network-family in compartment {compartment_name}",
        f"Allow group {operator_group_name} to manage dataflow-family in compartment {compartment_name}",
        f"Allow group {operator_group_name} to manage dis-family in compartment {compartment_name}",
        f"Allow group {operator_group_name} to manage data-catalog-family in compartment {compartment_name}",
        f"Allow group {operator_group_name} to manage data-catalog-private-endpoints in compartment {compartment_name}",
        f"Allow group {operator_group_name} to manage dataflow-private-endpoint in tenancy",
        f"Allow group {operator_group_name} to manage vaults in compartment {compartment_name}",
        f"Allow group {operator_group_name} to manage secret-family in compartment {compartment_name}",
        f"Allow group {operator_group_name} to read log-groups in compartment {compartment_name}",
        f"Allow group {operator_group_name} to read log-content in compartment {compartment_name}",
        f"Allow group {operator_group_name} to manage work-requests in compartment {compartment_name}",
    ]


def dataflow_policy_statements(compartment_name: str, dataflow_admin_group_name: str) -> list[str]:
    return [
        f"Allow group {dataflow_admin_group_name} to inspect compartments in tenancy",
        f"Allow group {dataflow_admin_group_name} to manage dataflow-family in compartment {compartment_name}",
        f"Allow group {dataflow_admin_group_name} to manage dataflow-private-endpoint in tenancy",
        f"Allow group {dataflow_admin_group_name} to use virtual-network-family in compartment {compartment_name}",
        f"Allow group {dataflow_admin_group_name} to read objectstorage-namespaces in tenancy",
        f"Allow group {dataflow_admin_group_name} to read buckets in compartment {compartment_name}",
        f"Allow group {dataflow_admin_group_name} to manage objects in compartment {compartment_name}",
        f"Allow group {dataflow_admin_group_name} to read log-groups in compartment {compartment_name}",
        f"Allow group {dataflow_admin_group_name} to use log-content in compartment {compartment_name}",
    ]


def adb_resource_principal_statements(compartment_name: str, dynamic_group_name: str) -> list[str]:
    return [
        f"Allow dynamic-group {dynamic_group_name} to read objectstorage-namespaces in tenancy",
        f"Allow dynamic-group {dynamic_group_name} to manage buckets in compartment {compartment_name}",
        f"Allow dynamic-group {dynamic_group_name} to manage objects in compartment {compartment_name}",
    ]


def di_workspace_policy_statements(compartment_name: str, workspace_id: str) -> list[str]:
    condition = f"where all {{request.principal.type='disworkspace', request.principal.id='{workspace_id}'}}"
    return [
        f"Allow any-user to use virtual-network-family in compartment {compartment_name} {condition}",
        f"Allow any-user to use secret-family in compartment {compartment_name} {condition}",
        f"Allow any-user to read secret-bundles in compartment {compartment_name} {condition}",
        f"Allow any-user to read objectstorage-namespaces in tenancy {condition}",
        f"Allow any-user to manage buckets in compartment {compartment_name} {condition}",
        f"Allow any-user to manage objects in compartment {compartment_name} {condition}",
    ]


def di_service_bootstrap_policy_statements(compartment_name: str) -> list[str]:
    return [
        f"Allow service dataintegration to use virtual-network-family in compartment {compartment_name}",
    ]


def data_catalog_policy_statements(compartment_name: str, dynamic_group_name: str, catalog_id: str) -> list[str]:
    return [
        f"Allow dynamic-group {dynamic_group_name} to read object-family in compartment {compartment_name}",
        f"Allow dynamic-group {dynamic_group_name} to read dis-workspaces-lineage in compartment {compartment_name}",
        "Allow any-user to manage data-catalog-data-assets in compartment "
        f"{compartment_name} where all {{request.principal.type='dataflowrun', target.catalog.id='{catalog_id}', target.resource.kind='dataFlow'}}",
    ]


def collect_landing_samples(data_root: Path, project_prefix: str) -> list[tuple[Path, str]]:
    selected: list[tuple[Path, str]] = []
    for top_level in sorted(item for item in data_root.iterdir() if item.is_dir()):
        files = sorted(item for item in top_level.rglob("*") if item.is_file())
        if not files:
            continue
        chosen_files = files if top_level.name.upper() == "LK" else [files[0]]
        for source_file in chosen_files:
            entity = normalize_entity_name(source_file.stem if top_level.name.upper() == "LK" else top_level.name)
            object_name = (
                f"{project_prefix}/source_system=trafico/entity={entity}/business_date={DEFAULT_BUSINESS_DATE}/batch_id={DEFAULT_BATCH_ID}/"
                f"{source_file.name}"
            )
            selected.append((source_file, object_name))
    return selected


def ensure_shared_network_resources(repo_root: Path, mirror_context: MirrorContext, names: DeployNames, compartment_id: str) -> dict[str, Any]:
    vcn_cidr = "10.42.0.0/16"
    object_storage_service = get_object_storage_service(repo_root)

    def ensure_vcn() -> dict[str, Any]:
        existing = find_vcn_by_name(repo_root, compartment_id, names.vcn_name)
        if existing:
            manifest = create_network_vcn(
                mirror_context,
                names.vcn_name,
                [vcn_cidr],
                {
                    "runtime": "oci",
                    "oci_mode": "reuse",
                    "compartment_id": compartment_id,
                    "dns_label": names.dns_label,
                    "resource_id": existing.get("id"),
                    "lifecycle_state": existing.get("lifecycle-state"),
                    "reused_existing": True,
                },
            )
            return {
                "manifest_path": str(manifest),
                "vcn_id": existing.get("id"),
                "lifecycle_state": existing.get("lifecycle-state"),
                "reused_existing": True,
            }
        return call_mcp_json(
            repo_root,
            "oci-network-mcp",
            [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-vcn",
                "--compartment-id",
                compartment_id,
                "--vcn-name",
                names.vcn_name,
                "--cidr-block",
                vcn_cidr,
                "--dns-label",
                names.dns_label,
            ],
            timeout_seconds=900,
        )

    def ensure_route_table(vcn_id: str) -> dict[str, Any]:
        existing = find_route_table_by_name(repo_root, compartment_id, vcn_id, names.route_table_name)
        if existing:
            manifest = create_network_route_table(
                mirror_context,
                names.route_table_name,
                {
                    "runtime": "oci",
                    "oci_mode": "reuse",
                    "compartment_id": compartment_id,
                    "vcn_name": names.vcn_name,
                    "vcn_id": vcn_id,
                    "resource_id": existing.get("id"),
                    "lifecycle_state": existing.get("lifecycle-state"),
                    "reused_existing": True,
                },
            )
            return {
                "manifest_path": str(manifest),
                "route_table_id": existing.get("id"),
                "lifecycle_state": existing.get("lifecycle-state"),
                "reused_existing": True,
            }
        return call_mcp_json(
            repo_root,
            "oci-network-mcp",
            [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-route-table",
                "--compartment-id",
                compartment_id,
                "--vcn-id",
                vcn_id,
                "--route-table-name",
                names.route_table_name,
            ],
            timeout_seconds=900,
        )

    def ensure_service_gateway(vcn_id: str) -> dict[str, Any]:
        existing = find_service_gateway_by_name(repo_root, compartment_id, vcn_id, names.service_gateway_name)
        if existing:
            manifest = create_network_service_gateway(
                mirror_context,
                names.service_gateway_name,
                {
                    "runtime": "oci",
                    "oci_mode": "reuse",
                    "compartment_id": compartment_id,
                    "vcn_name": names.vcn_name,
                    "vcn_id": vcn_id,
                    "service_ids": [object_storage_service["service_id"]],
                    "resource_id": existing.get("id"),
                    "lifecycle_state": existing.get("lifecycle-state"),
                    "reused_existing": True,
                },
            )
            return {
                "manifest_path": str(manifest),
                "service_gateway_id": existing.get("id"),
                "lifecycle_state": existing.get("lifecycle-state"),
                "reused_existing": True,
            }
        return call_mcp_json(
            repo_root,
            "oci-network-mcp",
            [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-service-gateway",
                "--compartment-id",
                compartment_id,
                "--vcn-id",
                vcn_id,
                "--service-gateway-name",
                names.service_gateway_name,
                "--service-id",
                object_storage_service["service_id"],
            ],
            timeout_seconds=900,
        )

    def ensure_private_services_nsg(vcn_id: str) -> dict[str, Any]:
        existing = find_nsg_by_name(repo_root, compartment_id, vcn_id, names.nsg_name)
        if existing:
            manifest = create_network_nsg(
                mirror_context,
                names.nsg_name,
                {
                    "runtime": "oci",
                    "oci_mode": "reuse",
                    "compartment_id": compartment_id,
                    "vcn_name": names.vcn_name,
                    "vcn_id": vcn_id,
                    "resource_id": existing.get("id"),
                    "lifecycle_state": existing.get("lifecycle-state"),
                    "reused_existing": True,
                },
            )
            return {
                "manifest_path": str(manifest),
                "nsg_id": existing.get("id"),
                "lifecycle_state": existing.get("lifecycle-state"),
                "reused_existing": True,
            }
        return call_mcp_json(
            repo_root,
            "oci-network-mcp",
            [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-nsg",
                "--compartment-id",
                compartment_id,
                "--vcn-id",
                vcn_id,
                "--nsg-name",
                names.nsg_name,
            ],
            timeout_seconds=900,
        )

    def ensure_subnet(subnet_name: str, cidr_block: str, dns_label: str) -> dict[str, Any]:
        existing = find_subnet_by_name(repo_root, compartment_id, vcn["vcn_id"], subnet_name)
        if existing:
            manifest = create_network_subnet(
                mirror_context,
                subnet_name,
                cidr_block,
                {
                    "runtime": "oci",
                    "oci_mode": "reuse",
                    "compartment_id": compartment_id,
                    "vcn_name": names.vcn_name,
                    "vcn_id": vcn["vcn_id"],
                    "dns_label": dns_label,
                    "route_table_id": route_table["route_table_id"],
                    "resource_id": existing.get("id"),
                    "lifecycle_state": existing.get("lifecycle-state"),
                    "reused_existing": True,
                },
            )
            return {
                "manifest_path": str(manifest),
                "subnet_id": existing.get("id"),
                "lifecycle_state": existing.get("lifecycle-state"),
                "reused_existing": True,
            }
        return call_mcp_json(
            repo_root,
            "oci-network-mcp",
            [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-subnet",
                "--compartment-id",
                compartment_id,
                "--vcn-id",
                vcn["vcn_id"],
                "--subnet-name",
                subnet_name,
                "--cidr-block",
                cidr_block,
                "--route-table-id",
                route_table["route_table_id"],
                "--dns-label",
                dns_label,
            ],
            timeout_seconds=900,
        )

    vcn = ensure_vcn()
    route_table = ensure_route_table(vcn["vcn_id"])
    service_gateway = ensure_service_gateway(vcn["vcn_id"])

    current_route_table = get_route_table_details(repo_root, route_table["route_table_id"])
    current_route_rules = [
        rule
        for rule in current_route_table.get("route-rules", [])
        if isinstance(rule, dict)
    ]
    route_rule_description = "Object Storage access for shared medallion services"
    required_route_rule = {
        "destination": object_storage_service["cidr_block"],
        "destinationType": "SERVICE_CIDR_BLOCK",
        "networkEntityId": service_gateway["service_gateway_id"],
        "description": route_rule_description,
    }
    merged_route_rules = [rule for rule in current_route_rules if str(rule.get("description", "")) != route_rule_description]
    merged_route_rules.append(required_route_rule)
    if canonical_json(merged_route_rules) != canonical_json(current_route_rules):
        route_table = call_mcp_json(
            repo_root,
            "oci-network-mcp",
            [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "update-route-table",
                "--route-table-id",
                route_table["route_table_id"],
                "--route-table-name",
                names.route_table_name,
            ]
            + [item for rule in merged_route_rules for item in ("--route-rule-json", json.dumps(rule, ensure_ascii=True))],
            timeout_seconds=900,
        )
    else:
        manifest = update_network_route_table(
            mirror_context,
            names.route_table_name,
            {
                "runtime": "oci",
                "oci_mode": "reuse",
                "compartment_id": compartment_id,
                "vcn_name": names.vcn_name,
                "vcn_id": vcn["vcn_id"],
                "route_table_id": route_table["route_table_id"],
                "route_rules_json": merged_route_rules,
                "resource_id": route_table["route_table_id"],
                "reused_existing": True,
            },
        )
        route_table = {
            **route_table,
            "manifest_path": str(manifest),
            "reused_existing": True,
        }

    nsg = ensure_private_services_nsg(vcn["vcn_id"])
    existing_rule_descriptions = {
        str(rule.get("description", "")).strip()
        for rule in list_nsg_rules(repo_root, nsg["nsg_id"])
        if isinstance(rule, dict)
    }
    required_nsg_rules = [
        {
            "direction": "INGRESS",
            "protocol": "6",
            "source": vcn_cidr,
            "sourceType": "CIDR_BLOCK",
            "isStateless": False,
            "description": "allow-vcn-tcp-ingress",
        },
        {
            "direction": "EGRESS",
            "protocol": "6",
            "destination": vcn_cidr,
            "destinationType": "CIDR_BLOCK",
            "isStateless": False,
            "description": "allow-vcn-tcp-egress",
        },
        {
            "direction": "EGRESS",
            "protocol": "6",
            "destination": object_storage_service["cidr_block"],
            "destinationType": "SERVICE_CIDR_BLOCK",
            "isStateless": False,
            "description": "allow-objectstorage-https-egress",
            "tcpOptions": {
                "destinationPortRange": {
                    "min": 443,
                    "max": 443,
                }
            },
        },
    ]
    missing_nsg_rules = [rule for rule in required_nsg_rules if rule["description"] not in existing_rule_descriptions]
    if missing_nsg_rules:
        nsg_rules = call_mcp_json(
            repo_root,
            "oci-network-mcp",
            [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "add-nsg-rules",
                "--nsg-name",
                names.nsg_name,
                "--nsg-id",
                nsg["nsg_id"],
                "--vcn-id",
                vcn["vcn_id"],
                "--security-rules-json",
                json.dumps(missing_nsg_rules, ensure_ascii=True),
            ],
            timeout_seconds=900,
        )
    else:
        manifest = update_network_nsg(
            mirror_context,
            names.nsg_name,
            {
                "runtime": "oci",
                "oci_mode": "reuse",
                "compartment_id": compartment_id,
                "vcn_name": names.vcn_name,
                "vcn_id": vcn["vcn_id"],
                "nsg_id": nsg["nsg_id"],
                "security_rules": required_nsg_rules,
                "resource_id": nsg["nsg_id"],
                "reused_existing": True,
            },
        )
        nsg_rules = {
            "manifest_path": str(manifest),
            "nsg_id": nsg["nsg_id"],
            "reused_existing": True,
        }

    di_subnet = ensure_subnet(names.subnet_name, "10.42.10.0/24", names.subnet_dns_label)
    data_flow_subnet = ensure_subnet(names.data_flow_subnet_name, "10.42.20.0/24", names.data_flow_subnet_dns_label)
    autonomous_subnet = ensure_subnet(names.autonomous_subnet_name, "10.42.30.0/24", names.autonomous_subnet_dns_label)

    return {
        "object_storage_service": object_storage_service,
        "vcn": vcn,
        "route_table": route_table,
        "service_gateway": service_gateway,
        "nsg": nsg,
        "nsg_rules": nsg_rules,
        "subnets": {
            "data_integration": di_subnet,
            "data_flow": data_flow_subnet,
            "autonomous": autonomous_subnet,
        },
    }


def ensure_data_flow_private_connectivity(
    repo_root: Path,
    mirror_context: MirrorContext,
    names: DeployNames,
    *,
    compartment_id: str,
    subnet_id: str,
    nsg_id: str,
    adb_private_endpoint: str,
) -> dict[str, Any]:
    dns_zone_host = normalize_private_endpoint_host(adb_private_endpoint)
    if not dns_zone_host:
        raise RuntimeError("No se pudo resolver el host privado de Autonomous Database para crear el private endpoint de Data Flow")

    existing = find_data_flow_private_endpoint_by_name(repo_root, compartment_id, names.data_flow_private_endpoint_name)
    if existing:
        manifest = create_data_flow_private_endpoint(
            mirror_context,
            names.data_flow_private_endpoint_name,
            {
                "runtime": "oci",
                "oci_mode": "reuse",
                "compartment_id": compartment_id,
                "subnet_id": subnet_id,
                "nsg_ids": [nsg_id],
                "dns_zones": [dns_zone_host],
                "private_endpoint_id": existing.get("id"),
                "lifecycle_state": existing.get("lifecycle-state"),
                "reused_existing": True,
            },
        )
        return {
            "private_endpoint_manifest": str(manifest),
            "private_endpoint_id": existing.get("id"),
            "lifecycle_state": existing.get("lifecycle-state"),
            "reused_existing": True,
        }

    return call_mcp_json(
        repo_root,
        "oci-data-flow-mcp",
        [
            "--environment",
            names.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "apply",
            "--command",
            "create-private-endpoint",
            "--private-endpoint-name",
            names.data_flow_private_endpoint_name,
            "--compartment-id",
            compartment_id,
            "--subnet-id",
            subnet_id,
            "--dns-zones-json",
            json.dumps([dns_zone_host], ensure_ascii=True),
            "--nsg-id",
            nsg_id,
            "--wait-for-state",
            "ACTIVE",
            "--max-wait-seconds",
            "2400",
            "--wait-interval-seconds",
            "30",
        ],
        timeout_seconds=3000,
    )


def choose_high_dsn(wallet_dir: Path) -> str:
    tnsnames = wallet_dir / "tnsnames.ora"
    if not tnsnames.exists():
        raise FileNotFoundError(f"No existe tnsnames.ora en {wallet_dir}")

    aliases: list[str] = []
    for raw_line in tnsnames.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = re.match(r"^([A-Za-z0-9_]+)\s*=", stripped)
        if match:
            aliases.append(match.group(1))

    for alias in aliases:
        if alias.lower().endswith("_high"):
            return alias
    if aliases:
        return aliases[0]
    raise RuntimeError(f"No se encontraron aliases TNS en {tnsnames}")


def ensure_dataflow_dependency_root(repo_root: Path, project_id: str, application_name: str) -> Path:
    template_root = repo_root / "templates" / "data_flow" / "dependency_package"
    target_root = repo_root / "workspace" / "generated" / project_id / "data_flow" / "dependencies" / application_name
    for item in sorted(template_root.rglob("*")):
        relative = item.relative_to(template_root)
        target = target_root / relative
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)
    return target_root


def write_quality_assets(project_root: Path, names: DeployNames) -> dict[str, str]:
    quality_root = ensure_directory(project_root / "quality")
    samples_root = ensure_directory(quality_root / "samples")
    contracts_root = ensure_directory(quality_root / "contracts")
    sql_root = ensure_directory(quality_root / "sql")
    relative_sql_dir = Path("..") / "sql"

    sample_path = samples_root / "agg_resumen_archivos_trafico_sample.csv"
    write_text(sample_path, GOLD_SAMPLE_CSV)

    for file_name, sql_text in QUALITY_SQL_FILES.items():
        write_text(sql_root / file_name, sql_text)

    contract_path = contracts_root / "agg_resumen_archivos_trafico.contract.json"
    contract_payload = {
        "contract_name": names.quality_contract_name,
        "dataset": "agg_resumen_archivos_trafico",
        "layer": "gold",
        "bucket_targets": [
            {
                "target_name": "gold_sample",
                "bucket_name": names.gold_bucket,
                "object_glob": f"objects/{names.project_prefix}/**/*agg_resumen_archivos_trafico*.csv",
                "data_format": "csv",
            }
        ],
        "bucket_checks": [
            {
                "name": "gold_sample_present",
                "type": "file_presence",
                "target": "gold_sample",
                "severity": "critical",
            },
            {
                "name": "gold_sample_row_count_at_least_expected",
                "type": "row_count_at_least",
                "target": "gold_sample",
                "expected": 3,
                "severity": "high",
                "metric_key": "bucket.gold_sample.row_count",
            },
            {
                "name": "gold_sample_required_columns",
                "type": "required_columns",
                "target": "gold_sample",
                "columns": [
                    "ID_DWH_DIA",
                    "ID_DWH_PERIODO",
                    "FILE_OUTPUT",
                    "TIPO_TRAF",
                    "Q_REGISTROS",
                    "Q_REGISTROS_OK",
                    "CANTEVENTOS_DISCARD",
                ],
                "severity": "critical",
            },
            {
                "name": "gold_sample_unique_key",
                "type": "unique_key",
                "target": "gold_sample",
                "columns": [
                    "ID_DWH_DIA",
                    "ID_DWH_PERIODO",
                    "FILE_OUTPUT",
                    "TIPO_TRAF",
                ],
                "severity": "high",
            },
            {
                "name": "gold_sample_not_null_core_columns",
                "type": "not_null",
                "target": "gold_sample",
                "columns": [
                    "ID_DWH_DIA",
                    "FILE_OUTPUT",
                    "TIPO_TRAF",
                    "Q_REGISTROS",
                ],
                "severity": "high",
            },
            {
                "name": "gold_sample_file_name_pattern",
                "type": "file_name_regex",
                "target": "gold_sample",
                "pattern": ".*agg_resumen_archivos_trafico.*\\.csv$",
                "severity": "medium",
            },
        ],
        "adb": {
            "database_name": names.database_name,
            "database_user": names.database_user,
            "connect_user": names.database_user,
        },
        "adb_checks": [
            {
                "name": "adb_manifest_exists",
                "type": "mirror_path_exists",
                "path": "database.manifest.json",
                "severity": "critical",
            },
            {
                "name": "app_gold_user_receipt_exists",
                "type": "mirror_path_exists",
                "path": "users/APP_GOLD/receipts",
                "severity": "high",
            },
            {
                "name": "app_gold_user_script_exists",
                "type": "mirror_path_exists",
                "path": "users/APP_GOLD/create-user.sql",
                "severity": "high",
            },
            {
                "name": "sql_execution_receipts_exist",
                "type": "mirror_path_exists",
                "path": "sql_runs",
                "severity": "high",
            },
            {
                "name": "manifest_database_user_matches",
                "type": "manifest_field_equals",
                "field": "database_user",
                "expected": names.database_user,
                "severity": "medium",
            },
            {
                "name": "app_gold_user_exists_in_adb",
                "type": "sql_scalar_equals",
                "sql_file": str((relative_sql_dir / "010_check_app_gold_user_exists.sql").as_posix()),
                "expected": 1,
                "severity": "critical",
                "metric_key": "adb.app_gold.user_exists",
            },
            {
                "name": "agg_table_exists_in_adb",
                "type": "sql_scalar_equals",
                "sql_file": str((relative_sql_dir / "020_check_agg_table_exists.sql").as_posix()),
                "expected": 1,
                "severity": "critical",
                "metric_key": "adb.agg.table_exists",
            },
            {
                "name": "agg_row_count_matches_expected",
                "type": "sql_scalar_equals",
                "sql_file": str((relative_sql_dir / "030_check_agg_row_count.sql").as_posix()),
                "expected": 3,
                "severity": "critical",
                "metric_key": "adb.agg.row_count",
            },
            {
                "name": "agg_sum_q_registros_matches_expected",
                "type": "sql_scalar_equals",
                "sql_file": str((relative_sql_dir / "040_check_sum_q_registros.sql").as_posix()),
                "expected": 850,
                "severity": "high",
                "metric_key": "adb.agg.sum_q_registros",
            },
            {
                "name": "agg_sum_q_registros_ok_matches_expected",
                "type": "sql_scalar_equals",
                "sql_file": str((relative_sql_dir / "050_check_sum_q_registros_ok.sql").as_posix()),
                "expected": 770,
                "severity": "high",
                "metric_key": "adb.agg.sum_q_registros_ok",
            },
            {
                "name": "agg_sum_discard_matches_expected",
                "type": "sql_scalar_equals",
                "sql_file": str((relative_sql_dir / "060_check_sum_discard.sql").as_posix()),
                "expected": 80,
                "severity": "high",
                "metric_key": "adb.agg.sum_discard",
            },
        ],
        "reconciliation_checks": [
            {
                "name": "gold_sample_equals_final_adb_dataset",
                "type": "metric_compare",
                "left_metric": "bucket.gold_sample.row_count",
                "operator": "equals",
                "right_metric": "adb.agg.row_count",
                "severity": "medium",
            }
        ],
        "gate": {
            "severity_threshold": "high",
        },
    }
    write_json(contract_path, contract_payload)

    readme_path = quality_root / "README.md"
    write_text(
        readme_path,
        "\n".join(
            [
                "# Quality assets",
                "",
                "Contrato y SQL de QA generados automaticamente para el despliegue real de prueba.",
                "",
                f"- Contract: `{contract_path}`",
                f"- Sample: `{sample_path}`",
            ]
        )
        + "\n",
    )

    return {
        "quality_root": str(quality_root),
        "sample_path": str(sample_path),
        "contract_path": str(contract_path),
    }


def render_manifest(names: DeployNames, *, compartment_id: str, workspace_id: str, catalog_id: str) -> str:
    return textwrap.dedent(
        f"""\
        project_id: {names.project_id}
        domain: trafico
        environment: {names.environment}
        deployment_scope: end_to_end_gold
        delivery_target: gold_adb
        migration_input_root: workspace/migration-input/{names.project_id}

        sql_sources:
          - sql/

        script_sources:
          - scripts/

        data_sources:
          - data/
          - samples/
          - exports/

        doc_sources:
          - docs/
          - notes/

        sample_sources:
          - samples/
          - exports/

        reference_doc_sources:
          - references/

        provisioning_order:
          - compartment
          - iam_policies
          - storage_layers
          - network
          - autonomous
          - landing_ingestion
          - data_flow
          - data_integration
          - validation

        pending_input_deliveries: []

        target_layers:
          landing_external: true
          bronze_raw: true
          silver_trusted: true
          gold_refined: true
          gold_adb: true

        existing_buckets:
          - name: {names.landing_bucket}
            exists: true
            layer: landing_external
            managed_by_factory: true
            ingestion_outside_flow: false
          - name: {names.bronze_bucket}
            exists: true
            layer: bronze_raw
            managed_by_factory: true
            ingestion_outside_flow: false
          - name: {names.silver_bucket}
            exists: true
            layer: silver_trusted
            managed_by_factory: true
            ingestion_outside_flow: false
          - name: {names.gold_bucket}
            exists: true
            layer: gold_refined
            managed_by_factory: true
            ingestion_outside_flow: false

        source_assets:
          - name: {names.project_id}-landing
            type: object_storage
            exists: true
            layer: landing_external
            uri: {names.landing_root_uri}source_system=trafico/
            managed_by_factory: true
            ingestion_outside_flow: false

        iam_baseline:
          enabled: true
          operator_group_name: {names.operator_group_name}
          dataflow_admin_group_name: {names.dataflow_admin_group_name}
          workspace_ocid: {workspace_id}
          catalog_ocid: {catalog_id}
          dynamic_groups:
            autonomous_resource_principal:
              name: {names.adb_dynamic_group_name}
              matching_rule: ALL {{resource.type = 'autonomousdatabase', resource.compartment.id = '{compartment_id}'}}
            data_catalog_harvest:
              name: {names.catalog_dynamic_group_name}
              matching_rule: Any {{resource.id = '{catalog_id}'}}

        network_profile:
          compartment_strategy: by-environment
          compartment_name: {names.compartment_name}
          compartment_id: {compartment_id}
          vcn_name: {names.vcn_name}
          shared_vcn_per_environment: true
          subnet_strategy: shared-vcn-private-subnets
          route_table_name: {names.route_table_name}
          service_gateway_name: {names.service_gateway_name}
          subnets:
            data_integration:
              name: {names.subnet_name}
              dns_label: {names.subnet_dns_label}
            data_flow:
              name: {names.data_flow_subnet_name}
              dns_label: {names.data_flow_subnet_dns_label}
            autonomous:
              name: {names.autonomous_subnet_name}
              dns_label: {names.autonomous_subnet_dns_label}
          private_endpoints:
            data_flow: true
            autonomous: true
            data_catalog: false
          nsgs:
            - {names.nsg_name}

        control_plane:
          enabled: true
          database_name: {names.database_name}
          schema_name: {names.control_schema}
          control_user: {names.control_user}
          source_type: object_storage
          partition_pattern: source_system={{source_system}}/entity={{entity}}/business_date={{business_date}}/batch_id={{batch_id}}
          default_reprocess_granularity: run+slice

        autonomous_profile:
          enabled: true
          database_name: {names.database_name}
          database_user: {names.database_user}
          wallet_dir: .local/autonomous/wallets/{names.environment}/{names.database_name}
          subnet_name: {names.autonomous_subnet_name}
          private_endpoint_label: {names.adb_private_endpoint_label}
          private_access_required: true
          bootstrap_runner_requires_private_network_access: true
          load_strategy: single-writer-batch
          cold_history_strategy: hybrid-partitioned
          publish_objects:
            - agg_resumen_archivos_trafico

        dataflow_jobs:
          - name: landing_to_bronze_{names.tag}
            enabled: true
            layer_from: landing_external
            layer_to: bronze_raw
            private_endpoint_name: {names.data_flow_private_endpoint_name}
          - name: bronze_to_silver_{names.tag}
            enabled: true
            layer_from: bronze_raw
            layer_to: silver_trusted
            private_endpoint_name: {names.data_flow_private_endpoint_name}
          - name: silver_to_gold_{names.tag}
            enabled: true
            layer_from: silver_trusted
            layer_to: gold_refined
            private_endpoint_name: {names.data_flow_private_endpoint_name}
          - name: gold_loader_{names.tag}
            enabled: true
            target: gold_adb
            delivery_mode: bucket_to_adb
            source_layer: gold_refined
            file_uri_template: {names.gold_source_uri}
            load_procedure: APP_GOLD.LOAD_AGG_RESUMEN_ARCHIVOS_TRAFICO

        di_pipeline:
          enabled: true
          workspace_name: {names.workspace_name}
          application: {names.di_pipeline_name}
          subnet_name: {names.subnet_name}
          private_workspace: true
          sync_lineage_with_data_catalog: true

        data_catalog:
          enabled: true
          catalog_name: {names.catalog_name}
          private_endpoint_required: false
          harvest_object_storage: true
          harvest_autonomous_database: false
          sync_data_integration: true

        lineage:
          strategy: hybrid
          native_sources:
            - data_flow
            - data_integration
          custom_openlineage:
            enabled: true
            namespace: oci-medallion.{names.environment}.{names.project_id}
            provider_key: oci-medallion-codex-factory
            publish_for:
              - adb_sql
              - dbms_cloud_load
              - custom_transform
              - reprocess_event

        reprocess:
          enabled: true
          default_scope: run+slice
          supported_keys:
            - entity
            - business_date
            - batch_id
            - object_name
          checkpoint_reuse: true
          allow_row_level_repair: false

        quality_profile:
          enabled: true
          contracts_root: workspace/migration-input/{names.project_id}/quality/contracts
          sql_checks_root: workspace/migration-input/{names.project_id}/quality/sql
          gate_before_cutover: true
          gate_severity_threshold: high
          enforce_by_slice: true

        validation_rules:
          require_sql: true
          require_docs: true
          require_samples_or_exports: true
          require_ddl: false
          require_mappings: false
          require_control_plane: true
          require_lineage_strategy: true

        approvals:
          provisioning_required: true
          publish_required: true
          ddl_required: true
          lineage_required: true
        """
    )


def write_project_manifest(project_root: Path, names: DeployNames, *, compartment_id: str, workspace_id: str, catalog_id: str) -> Path:
    manifest_path = project_root / "project.medallion.yaml"
    write_text(manifest_path, render_manifest(names, compartment_id=compartment_id, workspace_id=workspace_id, catalog_id=catalog_id))
    return manifest_path


def list_catalog_types(repo_root: Path, catalog_id: str) -> list[dict[str, Any]]:
    payload = run_oci_cli_json(
        repo_root,
        ["data-catalog", "type", "list", "--catalog-id", catalog_id, "--all"],
        timeout_seconds=600,
    )
    items = payload.get("data", {}).get("items", [])
    return items if isinstance(items, list) else []


def resolve_catalog_type(
    repo_root: Path,
    catalog_id: str,
    requested_type: str,
    *,
    type_category: str | None = None,
    parent_type_key: str | None = None,
    parent_type_name: str | None = None,
) -> tuple[str, str]:
    normalized = requested_type.strip()
    alias_target = CATALOG_TYPE_ALIASES.get(normalized.lower(), normalized)

    try:
        payload = run_oci_cli_json(
            repo_root,
            ["data-catalog", "type", "get", "--catalog-id", catalog_id, "--type-key", normalized],
            timeout_seconds=300,
        )
        data = payload.get("data", {})
        key = data.get("key") or normalized
        name = data.get("name") or alias_target
        return str(key), str(name)
    except Exception:
        pass

    items = list_catalog_types(repo_root, catalog_id)
    for item in items:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key", "")).strip()
        name = str(item.get("name", "")).strip()
        if not key or not name:
            continue
        if type_category and str(item.get("type-category", "")).strip().lower() != type_category.lower():
            continue
        if parent_type_key and str(item.get("parent-type-key", "")).strip() != parent_type_key:
            continue
        if parent_type_name and str(item.get("parent-type-name", "")).strip().lower() != parent_type_name.lower():
            continue
        if key == normalized or name.lower() == normalized.lower() or name.lower() == alias_target.lower():
            return key, name

    if alias_target.lower() == "oracle object storage":
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if str(item.get("type-category", "")).strip().lower() != "dataasset":
                continue
            if "object storage" in name.lower():
                return str(item.get("key")), name

    raise RuntimeError(
        json.dumps(
            {
                "message": "No se pudo resolver el tipo de asset en Data Catalog",
                "requested_type": requested_type,
                "catalog_id": catalog_id,
                "type_category": type_category,
                "parent_type_key": parent_type_key,
                "parent_type_name": parent_type_name,
            },
            ensure_ascii=True,
        )
    )


def discover_catalog_asset_properties(repo_root: Path, catalog_id: str, type_key: str, names: DeployNames) -> dict[str, str]:
    try:
        payload = run_oci_cli_json(
            repo_root,
            [
                "data-catalog",
                "type",
                "get",
                "--catalog-id",
                catalog_id,
                "--type-key",
                type_key,
                "--fields",
                "properties",
            ],
            timeout_seconds=300,
        )
    except Exception:
        return {}

    data = payload.get("data", {})
    raw_properties = data.get("properties")
    if isinstance(raw_properties, dict):
        raw_properties = raw_properties.get("UI")
    if not isinstance(raw_properties, list):
        return {}

    guessed: dict[str, str] = {}
    for item in raw_properties:
        if not isinstance(item, dict):
            continue
        property_name = item.get("key") or item.get("name")
        if not property_name:
            continue
        normalized = str(property_name).lower()
        if "namespace" in normalized:
            guessed[str(property_name)] = names.namespace
        elif normalized == "url":
            guessed[str(property_name)] = f"https://swiftobjectstorage.{names.region}.oraclecloud.com"
        elif "bucket" in normalized:
            guessed[str(property_name)] = names.gold_bucket
        elif normalized in ("region", "oci_region", "objectstorage_region"):
            guessed[str(property_name)] = names.region
    return guessed


def discover_catalog_connection_properties(
    repo_root: Path,
    catalog_id: str,
    type_key: str,
    names: DeployNames,
    compartment_id: str,
) -> dict[str, str]:
    try:
        payload = run_oci_cli_json(
            repo_root,
            [
                "data-catalog",
                "type",
                "get",
                "--catalog-id",
                catalog_id,
                "--type-key",
                type_key,
                "--fields",
                "properties",
            ],
            timeout_seconds=300,
        )
    except Exception:
        return {}

    data = payload.get("data", {})
    raw_properties = data.get("properties")
    if isinstance(raw_properties, dict):
        raw_properties = raw_properties.get("UI")
    if not isinstance(raw_properties, list):
        return {}

    guessed: dict[str, str] = {}
    for item in raw_properties:
        if not isinstance(item, dict):
            continue
        property_name = item.get("key") or item.get("name")
        if not property_name:
            continue
        normalized = str(property_name).lower()
        if "region" in normalized:
            guessed[str(property_name)] = names.region
        elif "compartment" in normalized:
            guessed[str(property_name)] = compartment_id
        elif "namespace" in normalized:
            guessed[str(property_name)] = names.namespace
        elif "bucket" in normalized:
            guessed[str(property_name)] = names.gold_bucket
    return guessed


def is_di_task_service_blocker(exc: Exception) -> bool:
    message = str(exc)
    patterns = (
        "DOS_TASK_0002",
        "Metadata object with key",
        "SaveMetadataObjects operation in Metadata service",
        "Too many requests for the tenant",
    )
    return any(pattern in message for pattern in patterns)


def summarize_quality_result(result_path: Path) -> dict[str, Any]:
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    summary = payload.get("summary", {})
    metrics = payload.get("metrics", {})
    return {
        "contract_name": payload.get("contract_name"),
        "summary": summary,
        "metrics": metrics,
        "result_path": str(result_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecuta un despliegue OCI real end-to-end usando .test como insumo canonico.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", default="dev", choices=("dev", "qa", "prod"))
    parser.add_argument("--project-id", default="trafico-real-oci")
    parser.add_argument("--test-root", default=".test")
    parser.add_argument("--tag")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    test_root = (repo_root / args.test_root).resolve() if not Path(args.test_root).is_absolute() else Path(args.test_root).resolve()
    if not test_root.exists():
        raise FileNotFoundError(f"No existe la carpeta de pruebas: {test_root}")

    env_values = parse_test_env(test_root / ".env")
    sensitive = apply_sensitive_environment(env_values)
    profile = load_oci_profile(repo_root)
    namespace = get_namespace(repo_root)
    tag = choose_tag(args.tag)
    names = build_names(repo_root, args.project_id, args.environment, profile["region"], namespace, profile["tenancy_id"], tag)
    mirror_context = MirrorContext(repo_root=repo_root, environment=names.environment)
    deployment_session_id = f"{utc_timestamp()}-{sanitize_token(names.project_id)}-{names.environment}-{names.tag}"
    os.environ["OCI_MEDALLION_MIRROR_COMPARTMENT_NAME"] = names.compartment_name
    os.environ["OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING"] = "True"
    os.environ["OCI_MEDALLION_DEPLOYMENT_ID"] = deployment_session_id
    os.environ["OCI_MEDALLION_DEPLOYMENT_PROJECT_ID"] = names.project_id
    os.environ["OCI_MEDALLION_DEPLOYMENT_RUN_ID"] = names.run_id
    os.environ["OCI_MEDALLION_DEPLOYMENT_ENVIRONMENT"] = names.environment
    os.environ["OCI_MEDALLION_DEPLOYMENT_TAG"] = names.tag

    project_root = repo_root / "workspace" / "migration-input" / names.project_id
    inventory_root = ensure_directory(project_root / "_inventory")
    report_path = inventory_root / "real-deploy-report.json"
    vars_path = inventory_root / "real-deploy-vars.json"
    deployment_log_paths = mirror_run_log_paths(repo_root, names.environment, names.compartment_name)

    steps: list[dict[str, Any]] = []
    warnings: list[str] = []
    ids: dict[str, Any] = {}
    artifacts: dict[str, Any] = {
        "repo_root": str(repo_root),
        "test_root": str(test_root),
        "project_root": str(project_root),
        "deployment_session_id": deployment_session_id,
        "run_log_paths": [str(path) for path in deployment_log_paths],
        "sensitive_users": sensitive,
    }
    append_run_log(
        deployment_log_paths,
        "DEPLOYMENT_STARTED",
        {
            "status": "running",
            "compartment_name": names.compartment_name,
            "workflow_id": names.workflow_id,
            "report_path": str(report_path),
            "project_root": str(project_root),
            "test_root": str(test_root),
        },
    )

    def flush_report(status: str, error: str | None = None) -> None:
        payload = {
            "status": status,
            "error": error,
            "generated_at_utc": utc_timestamp(),
            "names": asdict(names),
            "ids": ids,
            "artifacts": artifacts,
            "warnings": warnings,
            "steps": steps,
        }
        write_json(report_path, payload)

    def record_step(step_name: str, action: Callable[[], Any]) -> Any:
        entry: dict[str, Any] = {
            "name": step_name,
            "started_at_utc": utc_timestamp(),
        }
        started_at = time.perf_counter()
        append_run_log(
            deployment_log_paths,
            "STEP_STARTED",
            {
                "step": step_name,
                "status": "running",
            },
        )
        try:
            result = action()
            duration_seconds = round(time.perf_counter() - started_at, 3)
            entry["status"] = "ok"
            entry["finished_at_utc"] = utc_timestamp()
            entry["duration_seconds"] = duration_seconds
            entry["output"] = result
            steps.append(entry)
            append_run_log(
                deployment_log_paths,
                "STEP_COMPLETED",
                {
                    "step": step_name,
                    "status": "ok",
                    "duration_seconds": duration_seconds,
                    "details": result,
                },
            )
            flush_report("running")
            return result
        except Exception as exc:
            duration_seconds = round(time.perf_counter() - started_at, 3)
            entry["status"] = "error"
            entry["finished_at_utc"] = utc_timestamp()
            entry["duration_seconds"] = duration_seconds
            entry["error"] = str(exc)
            steps.append(entry)
            append_run_log(
                deployment_log_paths,
                "STEP_FAILED",
                {
                    "step": step_name,
                    "status": "error",
                    "duration_seconds": duration_seconds,
                    "error": str(exc),
                },
            )
            flush_report("failed", str(exc))
            raise

    def shared_runtime_args(run_id: str, layer: str, *, parent_run_id: str | None = None, reprocess_request_id: str | None = None) -> list[str]:
        args_list = [
            "--project-id",
            names.project_id,
            "--workflow-id",
            names.workflow_id,
            "--run-id",
            run_id,
            "--entity-name",
            "agg_resumen_archivos_trafico",
            "--layer",
            layer,
            "--slice-key",
            names.slice_key,
            "--business-date",
            DEFAULT_BUSINESS_DATE,
            "--batch-id",
            DEFAULT_BATCH_ID,
            "--control-database-name",
            names.database_name,
            "--lineage-enabled",
            "true",
        ]
        if parent_run_id:
            args_list.extend(["--parent-run-id", parent_run_id])
        if reprocess_request_id:
            args_list.extend(["--reprocess-request-id", reprocess_request_id])
        return args_list

    try:
        init_result = record_step(
            "init_workspace",
            lambda: run_repo_script_json(
                repo_root,
                "scripts/init_workspace.py",
                ["--repo-root", str(repo_root), "--project-id", names.project_id],
                timeout_seconds=300,
            ),
        )
        artifacts["init_workspace"] = init_result

        sql_sources = collect_test_sources(test_root / "source", (".sql",))
        doc_sources = collect_test_sources(test_root / "source", (".doc", ".docx", ".pdf", ".txt"))
        export_sources = collect_test_sources(test_root / "source", (".csv",))

        stage_args = [
            "--repo-root",
            str(repo_root),
            "--project-id",
            names.project_id,
            "--environment",
            names.environment,
            "--adb-name",
            names.database_name,
            "--data-source",
            str(test_root / "data"),
            "--oci-config-source",
            str(test_root / "oci" / "config"),
            "--oci-key-source",
            str(test_root / "oci" / "key.pem"),
            "--replace-existing",
        ]
        for source_file in sql_sources:
            stage_args.extend(["--sql-source", str(source_file)])
        for source_file in doc_sources:
            stage_args.extend(["--docs-source", str(source_file)])
        for source_file in export_sources:
            stage_args.extend(["--exports-source", str(source_file)])

        stage_result = record_step(
            "stage_test_assets",
            lambda: run_repo_script_json(
                repo_root,
                "scripts/stage_local_assets.py",
                stage_args,
                timeout_seconds=1200,
            ),
        )
        artifacts["stage_report"] = stage_result.get("report_path")

        intake_result = record_step(
            "migration_intake",
            lambda: run_repo_script_json(
                repo_root,
                "scripts/migration_intake.py",
                ["--repo-root", str(repo_root), "--project-id", names.project_id],
                timeout_seconds=300,
            ),
        )
        artifacts["inventory_dir"] = intake_result.get("inventory_dir")

        quality_assets = record_step("write_quality_assets", lambda: write_quality_assets(project_root, names))
        artifacts.update(quality_assets)

        def ensure_platform_compartment() -> dict[str, Any]:
            existing = find_compartment_by_name(repo_root, names.tenancy_id, names.compartment_name)
            if existing:
                manifest_result = call_mcp_json(
                    repo_root,
                    "oci-iam-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--command",
                        "create-compartment",
                        "--compartment-name",
                        names.compartment_name,
                        "--parent-compartment-id",
                        names.tenancy_id,
                        "--description",
                        f"Shared medallion platform compartment for {names.environment}",
                    ],
                    timeout_seconds=300,
                )
                return {
                    **manifest_result,
                    "compartment_id": existing.get("id"),
                    "lifecycle_state": existing.get("lifecycle-state"),
                    "reused_existing": True,
                }
            return call_mcp_json(
                repo_root,
                "oci-iam-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-compartment",
                    "--compartment-name",
                    names.compartment_name,
                    "--parent-compartment-id",
                    names.tenancy_id,
                    "--description",
                    f"Shared medallion platform compartment for {names.environment}",
                ],
                timeout_seconds=900,
            )

        create_compartment_result = record_step("ensure_platform_compartment", ensure_platform_compartment)
        ids["compartment_id"] = create_compartment_result["compartment_id"]

        wait_compartment_result = record_step(
            "wait_platform_compartment_active",
            lambda: wait_for_compartment_state(repo_root, ids["compartment_id"]),
        )
        ids["compartment_state"] = wait_compartment_result.get("lifecycle-state")

        def create_initial_iam() -> dict[str, Any]:
            group_results = {
                "operator_group": retry(
                    "create operator group",
                    lambda: call_mcp_json(
                        repo_root,
                        "oci-iam-mcp",
                        [
                            "--environment",
                            names.environment,
                            "--runtime",
                            "oci",
                            "--oci-mode",
                            "apply",
                            "--command",
                            "create-group",
                            "--group-name",
                            names.operator_group_name,
                            "--compartment-id",
                            names.tenancy_id,
                            "--description",
                            f"Operators for {names.project_id}",
                        ],
                        timeout_seconds=600,
                    ),
                ),
                "dataflow_admin_group": retry(
                    "create dataflow admin group",
                    lambda: call_mcp_json(
                        repo_root,
                        "oci-iam-mcp",
                        [
                            "--environment",
                            names.environment,
                            "--runtime",
                            "oci",
                            "--oci-mode",
                            "apply",
                            "--command",
                            "create-group",
                            "--group-name",
                            names.dataflow_admin_group_name,
                            "--compartment-id",
                            names.tenancy_id,
                            "--description",
                            f"Data Flow admins for {names.project_id}",
                        ],
                        timeout_seconds=600,
                    ),
                ),
            }
            time.sleep(15)
            policy_results = {}
            for policy_name, statements in (
                (
                    names.operators_policy_name,
                    operator_policy_statements(names.compartment_name, names.operator_group_name),
                ),
                (
                    names.dataflow_policy_name,
                    dataflow_policy_statements(names.compartment_name, names.dataflow_admin_group_name),
                ),
                (
                    names.di_service_policy_name,
                    di_service_bootstrap_policy_statements(names.compartment_name),
                ),
            ):
                command = [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-policy",
                    "--policy-name",
                    policy_name,
                    "--compartment-id",
                    names.tenancy_id,
                    "--description",
                    f"Policy {policy_name}",
                ]
                for statement in statements:
                    command.extend(["--statement", statement])
                policy_results[policy_name] = retry(
                    f"create policy {policy_name}",
                    lambda command=command: call_mcp_json(repo_root, "oci-iam-mcp", command, timeout_seconds=600),
                    attempts=6,
                    delay_seconds=20,
                )
                time.sleep(10)
            return {
                "groups": group_results,
                "policies": policy_results,
            }

        record_step("create_initial_iam_baseline", create_initial_iam)

        def create_buckets() -> dict[str, Any]:
            results: dict[str, Any] = {}
            for bucket_name, layer, purpose in (
                (names.landing_bucket, "landing_external", "landing"),
                (names.bronze_bucket, "bronze_raw", "bronze"),
                (names.silver_bucket, "silver_trusted", "silver"),
                (names.gold_bucket, "gold_refined", "gold"),
            ):
                existing_bucket = get_bucket_if_exists(repo_root, bucket_name, names.namespace)
                command = [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "sync-bucket-manifest" if existing_bucket else "create-bucket",
                    "--bucket-name",
                    bucket_name,
                    "--compartment-id",
                    ids["compartment_id"],
                    "--namespace-name",
                    names.namespace,
                    "--layer",
                    layer,
                    "--bucket-purpose",
                    purpose,
                    "--managed-by-factory",
                    "true",
                    "--ingestion-outside-flow",
                    "false",
                    "--existing-state",
                    "existing" if existing_bucket else "new",
                ]
                results[bucket_name] = call_mcp_json(
                    repo_root,
                    "oci-object-storage-mcp",
                    command,
                    timeout_seconds=600,
                )
                results[bucket_name]["reused_existing"] = bool(existing_bucket)
            return results

        record_step("create_storage_layers", create_buckets)
        record_step(
            "wait_storage_layers_ready",
            lambda: {
                bucket_name: wait_for_bucket_exists(repo_root, bucket_name, names.namespace)
                for bucket_name in (
                    names.landing_bucket,
                    names.bronze_bucket,
                    names.silver_bucket,
                    names.gold_bucket,
                )
            },
        )

        def upload_landing_samples() -> dict[str, Any]:
            uploads: list[dict[str, Any]] = []
            for source_file, object_name in collect_landing_samples(test_root / "data", names.project_prefix):
                result = retry(
                    f"upload landing sample {object_name}",
                    lambda source_file=source_file, object_name=object_name: call_mcp_json(
                        repo_root,
                        "oci-object-storage-mcp",
                        [
                            "--environment",
                            names.environment,
                            "--runtime",
                            "oci",
                            "--oci-mode",
                            "apply",
                            "--command",
                            "upload-object",
                            "--bucket-name",
                            names.landing_bucket,
                            "--namespace-name",
                            names.namespace,
                            "--source-file",
                            str(source_file),
                            "--object-name",
                            object_name,
                            "--project-id",
                            names.project_id,
                            "--workflow-id",
                            names.workflow_id,
                            "--run-id",
                            names.run_id,
                            "--entity-name",
                            "landing_ingestion",
                            "--layer",
                            "landing_external",
                            "--slice-key",
                            names.slice_key,
                            "--business-date",
                            DEFAULT_BUSINESS_DATE,
                            "--batch-id",
                            DEFAULT_BATCH_ID,
                            "--source-asset-ref",
                            f"oci://{names.landing_bucket}@{names.namespace}/{object_name}",
                            "--target-asset-ref",
                            f"oci://{names.landing_bucket}@{names.namespace}/{object_name}",
                        ],
                        timeout_seconds=600,
                    ),
                    attempts=8,
                    delay_seconds=15,
                )
                uploads.append({"source_file": str(source_file), "object_name": object_name, "result": result})
            return {"upload_count": len(uploads), "uploads": uploads}

        record_step("upload_landing_samples", upload_landing_samples)

        network_result = record_step(
            "create_network_foundation",
            lambda: ensure_shared_network_resources(repo_root, mirror_context, names, ids["compartment_id"]),
        )
        ids["vcn_id"] = network_result["vcn"]["vcn_id"]
        ids["route_table_id"] = network_result["route_table"]["route_table_id"]
        ids["service_gateway_id"] = network_result["service_gateway"]["service_gateway_id"]
        ids["nsg_id"] = network_result["nsg"]["nsg_id"]
        ids["data_integration_subnet_id"] = network_result["subnets"]["data_integration"]["subnet_id"]
        ids["subnet_id"] = ids["data_integration_subnet_id"]
        ids["data_flow_subnet_id"] = network_result["subnets"]["data_flow"]["subnet_id"]
        ids["autonomous_subnet_id"] = network_result["subnets"]["autonomous"]["subnet_id"]

        create_adb_result = record_step(
            "create_autonomous_database",
            lambda: call_mcp_json(
                repo_root,
                "oci-autonomous-database-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-autonomous-database",
                    "--database-name",
                    names.database_name,
                    "--database-user",
                    names.database_user,
                    "--compartment-id",
                    ids["compartment_id"],
                    "--subnet-id",
                    ids["autonomous_subnet_id"],
                    "--nsg-id",
                    ids["nsg_id"],
                    "--private-endpoint-label",
                    names.adb_private_endpoint_label,
                    "--db-name",
                    names.adb_db_name,
                    "--display-name",
                    names.adb_display_name,
                    "--compute-count",
                    "2",
                    "--compute-model",
                    "ECPU",
                    "--data-storage-size-in-tbs",
                    "1",
                    "--wait-for-state",
                    "AVAILABLE",
                    "--max-wait-seconds",
                    "7200",
                    "--wait-interval-seconds",
                    "30",
                ],
                timeout_seconds=7500,
            ),
        )
        ids["autonomous_database_id"] = create_adb_result["autonomous_database_id"]
        ids["adb_private_endpoint"] = create_adb_result.get("private_endpoint") or resolve_autonomous_private_endpoint(
            repo_root,
            ids["autonomous_database_id"],
        )
        if not ids.get("adb_private_endpoint"):
            raise RuntimeError("No se pudo resolver el private endpoint de Autonomous Database despues del aprovisionamiento")
        if ids.get("adb_private_endpoint"):
            warnings.append(
                "Autonomous Database fue aprovisionada con private endpoint; el bootstrap SQL desde el runner requiere conectividad privada hacia la VCN del ambiente."
            )

        data_flow_private_endpoint_result = record_step(
            "create_data_flow_private_endpoint",
            lambda: ensure_data_flow_private_connectivity(
                repo_root,
                mirror_context,
                names,
                compartment_id=ids["compartment_id"],
                subnet_id=ids["data_flow_subnet_id"],
                nsg_id=ids["nsg_id"],
                adb_private_endpoint=str(ids["adb_private_endpoint"]),
            ),
        )
        ids["data_flow_private_endpoint_id"] = data_flow_private_endpoint_result["private_endpoint_id"]
        artifacts["data_flow_private_endpoint_manifest"] = data_flow_private_endpoint_result.get("private_endpoint_manifest")

        wallet_result = record_step(
            "download_wallet",
            lambda: call_mcp_json(
                repo_root,
                "oci-autonomous-database-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "download-wallet-metadata",
                    "--database-name",
                    names.database_name,
                    "--database-user",
                    names.database_user,
                    "--autonomous-database-id",
                    ids["autonomous_database_id"],
                    "--wallet-dir",
                    str(names.wallet_dir),
                ],
                timeout_seconds=1800,
            ),
        )
        artifacts["wallet_dir"] = wallet_result["wallet_dir"]

        dsn_result = record_step("choose_wallet_dsn", lambda: {"dsn": choose_high_dsn(names.wallet_dir)})
        dsn = dsn_result["dsn"]
        os.environ["ADW_DSN"] = dsn
        ids["adb_dsn"] = dsn

        record_step(
            "bootstrap_control_plane",
            lambda: call_mcp_json(
                repo_root,
                "oci-autonomous-database-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "bootstrap-control-plane",
                    "--database-name",
                    names.database_name,
                    "--database-user",
                    names.database_user,
                    "--control-schema",
                    names.control_schema,
                    "--control-user",
                    names.control_user,
                    "--wallet-dir",
                    str(names.wallet_dir),
                    "--dsn",
                    dsn,
                    "--project-id",
                    names.project_id,
                    "--control-database-name",
                    names.database_name,
                ],
                timeout_seconds=1800,
            ),
        )

        record_step(
            "create_app_gold_user",
            lambda: call_mcp_json(
                repo_root,
                "oci-autonomous-database-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-database-user",
                    "--database-name",
                    names.database_name,
                    "--database-user",
                    names.database_user,
                    "--wallet-dir",
                    str(names.wallet_dir),
                    "--dsn",
                    dsn,
                    "--password-placeholder",
                    "APP_GOLD_PASSWORD",
                ],
                timeout_seconds=1800,
            ),
        )

        sql_files = [
            repo_root / "examples" / "trafico-datos" / "sql" / "020_create_agg_resumen_archivos_trafico.sql",
            repo_root / "examples" / "trafico-datos" / "sql" / "040_create_load_agg_resumen_archivos_trafico.sql",
        ]
        apply_sql_args = [
            "--environment",
            names.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "apply",
            "--command",
            "apply-sql",
            "--database-name",
            names.database_name,
            "--database-user",
            names.database_user,
            "--wallet-dir",
            str(names.wallet_dir),
            "--dsn",
            dsn,
            "--connect-user",
            names.database_user,
            "--source-asset-ref",
            f"workspace://{names.project_id}/sql",
            "--target-asset-ref",
            "APP_GOLD.AGG_RESUMEN_ARCHIVOS_TRAFICO",
        ]
        apply_sql_args.extend(shared_runtime_args(names.run_id, "gold_adb"))
        for sql_file in sql_files:
            apply_sql_args.extend(["--sql-file", str(sql_file)])

        record_step(
            "apply_gold_schema_sql",
            lambda: call_mcp_json(
                repo_root,
                "oci-autonomous-database-mcp",
                apply_sql_args,
                timeout_seconds=2400,
            ),
        )

        dataflow_apps = [
            {
                "application_name": f"landing-to-bronze-{names.tag}",
                "dependency_job_name": "landing-to-bronze",
                "source_layer": "landing_external",
                "target_layer": "bronze_raw",
            },
            {
                "application_name": f"bronze-to-silver-{names.tag}",
                "dependency_job_name": "bronze-to-silver",
                "source_layer": "bronze_raw",
                "target_layer": "silver_trusted",
            },
            {
                "application_name": f"silver-to-gold-{names.tag}",
                "dependency_job_name": "silver-to-gold",
                "source_layer": "silver_trusted",
                "target_layer": "gold_refined",
            },
        ]

        def upload_dataflow_sources() -> dict[str, Any]:
            results: dict[str, Any] = {}
            source_file = repo_root / "templates" / "data_flow" / "minimal_app" / "main.py"
            for app in dataflow_apps:
                object_name = f"{names.project_prefix}/apps/{app['application_name']}/main.py"
                dependency_root = ensure_dataflow_dependency_root(
                    repo_root,
                    names.project_id,
                    str(app["dependency_job_name"]),
                )
                package_result = call_mcp_json(
                    repo_root,
                    "oci-data-flow-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--command",
                        "package-dependencies",
                        "--application-name",
                        app["application_name"],
                        "--dependency-root",
                        str(dependency_root),
                    ],
                    timeout_seconds=2400,
                )
                validate_result = call_mcp_json(
                    repo_root,
                    "oci-data-flow-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--command",
                        "validate-archive",
                        "--application-name",
                        app["application_name"],
                        "--dependency-root",
                        str(dependency_root),
                    ],
                    timeout_seconds=2400,
                )
                archive_source_file = Path(str(package_result["archive_path"]))
                archive_object_name = f"{names.project_prefix}/apps/{app['application_name']}/archive.zip"
                upload_source_result = retry(
                    f"upload dataflow source {app['application_name']}",
                    lambda object_name=object_name: call_mcp_json(
                        repo_root,
                        "oci-object-storage-mcp",
                        [
                            "--environment",
                            names.environment,
                            "--runtime",
                            "oci",
                            "--oci-mode",
                            "apply",
                            "--command",
                            "upload-object",
                            "--bucket-name",
                            names.silver_bucket,
                            "--namespace-name",
                            names.namespace,
                            "--source-file",
                            str(source_file),
                            "--object-name",
                            object_name,
                        ],
                        timeout_seconds=600,
                    ),
                    attempts=8,
                    delay_seconds=15,
                )
                upload_archive_result = retry(
                    f"upload dataflow archive {app['application_name']}",
                    lambda archive_object_name=archive_object_name, archive_source_file=archive_source_file: call_mcp_json(
                        repo_root,
                        "oci-object-storage-mcp",
                        [
                            "--environment",
                            names.environment,
                            "--runtime",
                            "oci",
                            "--oci-mode",
                            "apply",
                            "--command",
                            "upload-object",
                            "--bucket-name",
                            names.silver_bucket,
                            "--namespace-name",
                            names.namespace,
                            "--source-file",
                            str(archive_source_file),
                            "--object-name",
                            archive_object_name,
                        ],
                        timeout_seconds=1200,
                    ),
                    attempts=8,
                    delay_seconds=15,
                )
                app["file_uri"] = f"oci://{names.silver_bucket}@{names.namespace}/{object_name}"
                app["archive_uri"] = f"oci://{names.silver_bucket}@{names.namespace}/{archive_object_name}"
                app["dependency_root"] = str(dependency_root)
                results[app["application_name"]] = {
                    "package": package_result,
                    "validate": validate_result,
                    "upload_source": upload_source_result,
                    "upload_archive": upload_archive_result,
                    "file_uri": app["file_uri"],
                    "archive_uri": app["archive_uri"],
                }
            return results

        record_step("upload_dataflow_sources", upload_dataflow_sources)

        def create_and_run_dataflow() -> dict[str, Any]:
            bucket_uri_map = {
                "landing_external": names.landing_root_uri,
                "bronze_raw": names.bronze_root_uri,
                "silver_trusted": names.silver_root_uri,
                "gold_refined": names.gold_root_uri,
            }
            results: dict[str, Any] = {}
            for index, app in enumerate(dataflow_apps, start=1):
                create_result = call_mcp_json(
                    repo_root,
                    "oci-data-flow-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--runtime",
                        "oci",
                        "--oci-mode",
                        "apply",
                        "--command",
                        "create-application",
                        "--application-name",
                        app["application_name"],
                        "--source-dir",
                        str(repo_root / "templates" / "data_flow" / "minimal_app"),
                        "--compartment-id",
                        ids["compartment_id"],
                        "--file-uri",
                        app["file_uri"],
                        "--archive-uri",
                        app["archive_uri"],
                        "--private-endpoint-id",
                        ids["data_flow_private_endpoint_id"],
                        "--logs-bucket-uri",
                        f"{names.silver_root_uri}logs/",
                        "--wait-for-state",
                        "ACTIVE",
                        "--max-wait-seconds",
                        "1800",
                        "--wait-interval-seconds",
                        "20",
                    ],
                    timeout_seconds=2400,
                )
                app["application_id"] = create_result["application_id"]
                run_result = call_mcp_json(
                    repo_root,
                    "oci-data-flow-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--runtime",
                        "oci",
                        "--oci-mode",
                        "apply",
                        "--command",
                        "run-application",
                        "--application-name",
                        app["application_name"],
                        "--application-id",
                        app["application_id"],
                        "--compartment-id",
                        ids["compartment_id"],
                        "--display-name",
                        f"{app['application_name']}-run",
                        "--logs-bucket-uri",
                        f"{names.silver_root_uri}logs/",
                        "--wait-for-state",
                        "SUCCEEDED",
                        "--max-wait-seconds",
                        "2400",
                        "--wait-interval-seconds",
                        "30",
                    ]
                    + shared_runtime_args(names.run_id, app["target_layer"])
                    + [
                        "--source-asset-ref",
                        bucket_uri_map[app["source_layer"]],
                        "--target-asset-ref",
                        bucket_uri_map[app["target_layer"]],
                    ],
                    timeout_seconds=3000,
                )
                collect_result = call_mcp_json(
                    repo_root,
                    "oci-data-flow-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--command",
                        "collect-run-report",
                        "--application-name",
                        app["application_name"],
                        "--state",
                        "SUCCEEDED",
                        "--driver-log-uri",
                        f"{names.silver_root_uri}logs/{app['application_name']}/driver.log",
                        "--executor-log-uri",
                        f"{names.silver_root_uri}logs/{app['application_name']}/executor.log",
                        "--rows-in",
                        str(index),
                        "--rows-out",
                        str(index),
                        "--rows-rejected",
                        "0",
                    ]
                    + shared_runtime_args(names.run_id, app["target_layer"])
                    + [
                        "--service-run-ref",
                        run_result["service_run_ref"],
                        "--source-asset-ref",
                        bucket_uri_map[app["source_layer"]],
                        "--target-asset-ref",
                        bucket_uri_map[app["target_layer"]],
                    ],
                    timeout_seconds=600,
                )
                results[app["application_name"]] = {
                    "create": create_result,
                    "run": run_result,
                    "collect": collect_result,
                }
            return results

        record_step("create_and_run_dataflow", create_and_run_dataflow)

        def create_data_integration_resources() -> dict[str, Any]:
            workspace = retry(
                "create DI workspace",
                lambda: call_mcp_json(
                    repo_root,
                    "oci-data-integration-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--runtime",
                        "oci",
                        "--oci-mode",
                        "apply",
                        "--command",
                        "create-workspace",
                        "--workspace-name",
                        names.workspace_name,
                        "--compartment-id",
                        ids["compartment_id"],
                        "--is-private-network",
                        "true",
                        "--subnet-id",
                        ids["data_integration_subnet_id"],
                        "--vcn-id",
                        ids["vcn_id"],
                        "--wait-for-state",
                        "SUCCEEDED",
                        "--max-wait-seconds",
                        "2400",
                        "--wait-interval-seconds",
                        "30",
                    ],
                    timeout_seconds=3000,
                ),
                attempts=10,
                delay_seconds=45,
            )
            ids["workspace_id"] = workspace["workspace_id"]
            project = call_mcp_json(
                repo_root,
                "oci-data-integration-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-project",
                    "--workspace-name",
                    names.workspace_name,
                    "--workspace-id",
                    ids["workspace_id"],
                    "--project-name",
                    names.di_project_name,
                    "--identifier",
                    names.di_project_name,
                    "--label",
                    "medallion",
                    "--label",
                    "trafico",
                    "--favorite",
                    "false",
                ],
                timeout_seconds=900,
            )
            ids["di_project_key"] = project["project_key"]
            folder = call_mcp_json(
                repo_root,
                "oci-data-integration-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-folder",
                    "--workspace-name",
                    names.workspace_name,
                    "--workspace-id",
                    ids["workspace_id"],
                    "--folder-name",
                    names.di_folder_name,
                    "--identifier",
                    names.di_folder_name,
                    "--aggregator-key",
                    ids["di_project_key"],
                ],
                timeout_seconds=900,
            )
            ids["di_folder_key"] = folder["folder_key"]
            task_results: dict[str, Any] = {}
            task_failures: dict[str, Any] = {}
            task_names: list[str] = []
            for app in dataflow_apps:
                task_name = f"run-{app['application_name']}"
                task_names.append(task_name)
                try:
                    task_results[task_name] = call_mcp_json(
                        repo_root,
                        "oci-data-integration-mcp",
                        [
                            "--environment",
                            names.environment,
                            "--runtime",
                            "oci",
                            "--oci-mode",
                            "apply",
                            "--command",
                            "create-task-from-dataflow",
                            "--workspace-name",
                            names.workspace_name,
                            "--workspace-id",
                            ids["workspace_id"],
                            "--folder-key",
                            ids["di_folder_key"],
                            "--task-name",
                            task_name,
                            "--application-name",
                            app["application_name"],
                            "--application-id",
                            app["application_id"],
                            "--application-compartment-id",
                            ids["compartment_id"],
                            "--aggregator-key",
                            ids["di_project_key"],
                        ],
                        timeout_seconds=900,
                    )
                except Exception as exc:
                    failure = {
                        "application_name": app["application_name"],
                        "application_id": app["application_id"],
                        "error": str(exc),
                    }
                    task_failures[task_name] = failure
                    if is_di_task_service_blocker(exc):
                        blocker_path = inventory_root / "di-task-service-blocker.json"
                        write_json(
                            blocker_path,
                            {
                                "workspace_id": ids["workspace_id"],
                                "project_key": ids["di_project_key"],
                                "folder_key": ids["di_folder_key"],
                                "task_name": task_name,
                                "task_failures": task_failures,
                            },
                        )
                        artifacts["di_service_blocker_path"] = str(blocker_path)
                        warnings.append(
                            "OCI Data Integration no pudo crear tareas desde Data Flow por un error del servicio de metadata; "
                            "se continuara con workspace/project/folder creados y se omitira el pipeline OCI."
                        )
                        break
                    raise

            if task_failures:
                return {
                    "workspace": workspace,
                    "project": project,
                    "folder": folder,
                    "tasks": task_results,
                    "task_failures": task_failures,
                    "pipeline": None,
                    "orchestration_status": "service_blocked",
                }

            pipeline_args = [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-pipeline",
                "--workspace-name",
                names.workspace_name,
                "--workspace-id",
                ids["workspace_id"],
                "--pipeline-name",
                names.di_pipeline_name,
                "--identifier",
                names.di_pipeline_name.upper().replace("-", "_"),
                "--folder-key",
                ids["di_folder_key"],
                "--aggregator-key",
                ids["di_project_key"],
            ]
            for task_name in task_names:
                pipeline_args.extend(["--task", task_name])
            pipeline = call_mcp_json(
                repo_root,
                "oci-data-integration-mcp",
                pipeline_args,
                timeout_seconds=900,
            )
            ids["di_pipeline_key"] = pipeline["pipeline_key"]
            return {
                "workspace": workspace,
                "project": project,
                "folder": folder,
                "tasks": task_results,
                "task_failures": task_failures,
                "pipeline": pipeline,
                "orchestration_status": "created",
            }

        record_step("create_data_integration_resources", create_data_integration_resources)

        def create_di_workspace_policy() -> dict[str, Any]:
            command = [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-policy",
                "--policy-name",
                names.di_policy_name,
                "--compartment-id",
                names.tenancy_id,
                "--description",
                f"DI runtime policy for {names.project_id}",
            ]
            for statement in di_workspace_policy_statements(names.compartment_name, ids["workspace_id"]):
                command.extend(["--statement", statement])
            return call_mcp_json(repo_root, "oci-iam-mcp", command, timeout_seconds=600)

        record_step(
            "create_di_workspace_policy",
            lambda: retry(
                "create DI workspace policy",
                create_di_workspace_policy,
                attempts=6,
                delay_seconds=20,
            ),
        )

        catalog_result = record_step(
            "create_data_catalog",
            lambda: call_mcp_json(
                repo_root,
                "oci-data-catalog-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-catalog",
                    "--catalog-name",
                    names.catalog_name,
                    "--compartment-id",
                    ids["compartment_id"],
                    "--wait-for-state",
                    "SUCCEEDED",
                    "--max-wait-seconds",
                    "2400",
                    "--wait-interval-seconds",
                    "30",
                ],
                timeout_seconds=3000,
            ),
        )
        ids["catalog_id"] = catalog_result["catalog_id"]

        def create_catalog_iam() -> dict[str, Any]:
            dynamic_group = retry(
                "create catalog dynamic group",
                lambda: call_mcp_json(
                    repo_root,
                    "oci-iam-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--runtime",
                        "oci",
                        "--oci-mode",
                        "apply",
                        "--command",
                        "create-dynamic-group",
                        "--dynamic-group-name",
                        names.catalog_dynamic_group_name,
                        "--matching-rule",
                        f"Any {{resource.id = '{ids['catalog_id']}'}}",
                        "--description",
                        f"Catalog dynamic group for {names.project_id}",
                    ],
                    timeout_seconds=600,
                ),
            )
            time.sleep(15)
            command = [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-policy",
                "--policy-name",
                names.catalog_policy_name,
                "--compartment-id",
                names.tenancy_id,
                "--description",
                f"Catalog policy for {names.project_id}",
            ]
            for statement in data_catalog_policy_statements(
                names.compartment_name,
                names.catalog_dynamic_group_name,
                ids["catalog_id"],
            ):
                command.extend(["--statement", statement])
            policy = retry(
                "create catalog policy",
                lambda: call_mcp_json(repo_root, "oci-iam-mcp", command, timeout_seconds=600),
                attempts=6,
                delay_seconds=20,
            )
            return {"dynamic_group": dynamic_group, "policy": policy}

        record_step("create_catalog_iam", create_catalog_iam)

        def create_catalog_asset() -> dict[str, Any]:
            resolved_type_key, resolved_type_name = resolve_catalog_type(repo_root, ids["catalog_id"], "oracle_object_storage")
            guessed_properties = discover_catalog_asset_properties(repo_root, ids["catalog_id"], resolved_type_key, names)
            property_payload: dict[str, Any] = {}
            if guessed_properties:
                values_are_nested = all(isinstance(value, dict) for value in guessed_properties.values())
                property_payload = guessed_properties if values_are_nested else {"default": guessed_properties}
            base_args = [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-data-asset",
                "--catalog-id",
                ids["catalog_id"],
                "--asset-name",
                names.catalog_asset_name,
                "--asset-type-key",
                resolved_type_key,
            ]
            if property_payload:
                base_args.extend(["--asset-properties-json", json.dumps(property_payload, ensure_ascii=True)])
            result = call_mcp_json(repo_root, "oci-data-catalog-mcp", base_args, timeout_seconds=900)
            ids["data_asset_key"] = result["data_asset_key"]
            result["resolved_asset_type_key"] = resolved_type_key
            result["resolved_asset_type_name"] = resolved_type_name
            result["guessed_properties"] = property_payload
            return result

        catalog_asset_result = record_step("create_catalog_asset", create_catalog_asset)
        ids["catalog_asset_type_key"] = catalog_asset_result["resolved_asset_type_key"]
        artifacts["catalog_asset_manifest"] = catalog_asset_result.get("asset_manifest")

        gold_sample_path = Path(quality_assets["sample_path"])
        upload_gold_result = record_step(
            "upload_gold_sample",
            lambda: retry(
                "upload gold sample",
                lambda: call_mcp_json(
                    repo_root,
                    "oci-object-storage-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--runtime",
                        "oci",
                        "--oci-mode",
                        "apply",
                        "--command",
                        "upload-object",
                        "--bucket-name",
                        names.gold_bucket,
                        "--namespace-name",
                        names.namespace,
                        "--source-file",
                        str(gold_sample_path),
                        "--object-name",
                        names.gold_object_name,
                    ]
                    + shared_runtime_args(names.run_id, "gold_refined")
                    + [
                        "--source-asset-ref",
                        names.gold_source_uri,
                        "--target-asset-ref",
                        names.gold_source_uri,
                    ],
                    timeout_seconds=900,
                ),
                attempts=8,
                delay_seconds=15,
            ),
        )
        artifacts["gold_sample_stored_at"] = upload_gold_result.get("stored_at")

        def create_catalog_connection() -> dict[str, Any]:
            connection_type_key, connection_type_name = resolve_catalog_type(
                repo_root,
                ids["catalog_id"],
                "resource principal",
                type_category="connection",
                parent_type_key=ids["catalog_asset_type_key"],
            )
            guessed_properties = discover_catalog_connection_properties(
                repo_root,
                ids["catalog_id"],
                connection_type_key,
                names,
                ids["compartment_id"],
            )
            property_payload = {"default": guessed_properties} if guessed_properties else {}
            command = [
                "--environment",
                names.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "apply",
                "--command",
                "create-connection",
                "--catalog-id",
                ids["catalog_id"],
                "--connection-name",
                f"{names.catalog_asset_name}-rp",
                "--connection-type-key",
                connection_type_key,
                "--data-asset-key",
                ids["data_asset_key"],
                "--wait-for-state",
                "ACTIVE",
                "--max-wait-seconds",
                "1200",
                "--wait-interval-seconds",
                "30",
            ]
            if property_payload:
                command.extend(["--connection-properties-json", json.dumps(property_payload, ensure_ascii=True)])
            result = call_mcp_json(repo_root, "oci-data-catalog-mcp", command, timeout_seconds=1800)
            ids["catalog_connection_key"] = result["connection_key"]
            result["resolved_connection_type_key"] = connection_type_key
            result["resolved_connection_type_name"] = connection_type_name
            result["guessed_properties"] = property_payload
            return result

        catalog_connection_result = record_step(
            "create_catalog_connection",
            lambda: retry("create data catalog connection", create_catalog_connection, attempts=6, delay_seconds=20),
        )
        artifacts["catalog_connection_manifest"] = catalog_connection_result.get("connection_manifest")

        def create_catalog_harvest_job_definition() -> dict[str, Any]:
            result = call_mcp_json(
                repo_root,
                "oci-data-catalog-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-harvest-job-definition",
                    "--catalog-id",
                    ids["catalog_id"],
                    "--job-name",
                    f"{names.catalog_asset_name}-harvest",
                    "--job-type",
                    "HARVEST",
                    "--connection-key",
                    ids["catalog_connection_key"],
                    "--data-asset-key",
                    ids["data_asset_key"],
                ],
                timeout_seconds=1200,
            )
            ids["catalog_job_definition_key"] = result["job_definition_key"]
            return result

        catalog_job_definition_result = record_step(
            "create_catalog_harvest_job_definition",
            lambda: retry("create harvest job definition", create_catalog_harvest_job_definition, attempts=6, delay_seconds=20),
        )
        artifacts["catalog_job_definition_manifest"] = catalog_job_definition_result.get("job_definition_manifest")

        def create_catalog_harvest_job() -> dict[str, Any]:
            result = call_mcp_json(
                repo_root,
                "oci-data-catalog-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-job",
                    "--catalog-id",
                    ids["catalog_id"],
                    "--job-name",
                    f"{names.catalog_asset_name}-harvest-job",
                    "--job-definition-key",
                    ids["catalog_job_definition_key"],
                    "--connection-key",
                    ids["catalog_connection_key"],
                ],
                timeout_seconds=1200,
            )
            ids["catalog_job_key"] = result["job_key"]
            return result

        catalog_job_result = record_step(
            "create_catalog_harvest_job",
            lambda: retry("create harvest job", create_catalog_harvest_job, attempts=6, delay_seconds=20),
        )
        artifacts["catalog_job_manifest"] = catalog_job_result.get("job_manifest")

        def create_catalog_pattern() -> dict[str, Any]:
            result = call_mcp_json(
                repo_root,
                "oci-data-catalog-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "create-pattern",
                    "--catalog-id",
                    ids["catalog_id"],
                    "--pattern-name",
                    f"gold-agg-expr-{names.tag}",
                    "--pattern-description",
                    "Expression pattern for gold agg sample files",
                    "--pattern-expression",
                    f"{{bucketName:{names.gold_bucket}}}/{names.project_prefix}/exports/{{logicalEntity:agg_resumen_archivos_trafico}}/process_date=.*/.*.csv",
                    "--wait-for-state",
                    "ACTIVE",
                    "--max-wait-seconds",
                    "1200",
                    "--wait-interval-seconds",
                    "30",
                ],
                timeout_seconds=1800,
            )
            ids["catalog_pattern_key"] = result["pattern_key"]
            return result

        catalog_pattern_result = record_step(
            "create_catalog_pattern",
            lambda: retry("create catalog pattern", create_catalog_pattern, attempts=6, delay_seconds=20),
        )
        artifacts["catalog_pattern_manifest"] = catalog_pattern_result.get("pattern_manifest")

        catalog_pattern_attachment_result = record_step(
            "attach_catalog_pattern_to_asset",
            lambda: retry(
                "attach catalog pattern",
                lambda: call_mcp_json(
                    repo_root,
                    "oci-data-catalog-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--runtime",
                        "oci",
                        "--oci-mode",
                        "apply",
                        "--command",
                        "attach-data-selector-patterns",
                        "--catalog-id",
                        ids["catalog_id"],
                        "--data-asset-key",
                        ids["data_asset_key"],
                        "--asset-name",
                        names.catalog_asset_name,
                        "--pattern-key",
                        ids["catalog_pattern_key"],
                        "--wait-for-state",
                        "ACTIVE",
                        "--max-wait-seconds",
                        "1200",
                        "--wait-interval-seconds",
                        "30",
                    ],
                    timeout_seconds=1800,
                ),
                attempts=6,
                delay_seconds=20,
            ),
        )
        artifacts["catalog_pattern_attachment_manifest"] = catalog_pattern_attachment_result.get("attachment_manifest")

        catalog_harvest_result = record_step(
            "run_catalog_harvest_job",
            lambda: retry(
                "run catalog harvest job",
                lambda: call_mcp_json(
                    repo_root,
                    "oci-data-catalog-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--runtime",
                        "oci",
                        "--oci-mode",
                        "apply",
                        "--command",
                        "run-harvest-job",
                        "--catalog-id",
                        ids["catalog_id"],
                        "--job-key",
                        ids["catalog_job_key"],
                        "--job-name",
                        f"{names.catalog_asset_name}-harvest-job",
                        "--wait-for-state",
                        "SUCCEEDED",
                        "--wait-for-state",
                        "SUCCEEDED_WITH_WARNINGS",
                        "--wait-for-state",
                        "FAILED",
                        "--max-wait-seconds",
                        "2400",
                        "--wait-interval-seconds",
                        "30",
                    ],
                    timeout_seconds=3000,
                ),
                attempts=6,
                delay_seconds=20,
            ),
        )
        artifacts["catalog_harvest_job_manifest"] = catalog_harvest_result.get("job_manifest")
        ids["catalog_job_execution_id"] = catalog_harvest_result.get("job_execution_id")

        gold_par_result = record_step(
            "create_gold_preauth_request",
            lambda: retry(
                "create gold preauthenticated request",
                lambda: call_mcp_json(
                    repo_root,
                    "oci-object-storage-mcp",
                    [
                        "--environment",
                        names.environment,
                        "--runtime",
                        "oci",
                        "--oci-mode",
                        "apply",
                        "--command",
                        "create-par",
                        "--bucket-name",
                        names.gold_bucket,
                        "--namespace-name",
                        names.namespace,
                        "--object-name",
                        names.gold_object_name,
                        "--par-name",
                        f"{names.project_id}-gold-object-par",
                        "--access-type",
                        "ObjectRead",
                    ]
                    + shared_runtime_args(names.run_id, "gold_refined")
                    + [
                        "--source-asset-ref",
                        names.gold_source_uri,
                        "--target-asset-ref",
                        names.gold_source_uri,
                    ],
                    timeout_seconds=900,
                ),
                attempts=8,
                delay_seconds=15,
            ),
        )
        gold_access_uri = gold_par_result.get("access_uri")
        if not gold_access_uri:
            raise RuntimeError("No se pudo crear el pre-authenticated request para el objeto gold")
        gold_access_url = f"https://objectstorage.{names.region}.oraclecloud.com{gold_access_uri}"
        artifacts["gold_preauth_manifest"] = gold_par_result.get("manifest_path")
        artifacts["gold_preauth_access_url"] = gold_access_url

        initial_load_args = [
            "--environment",
            names.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "apply",
            "--command",
            "load-gold-objects",
            "--database-name",
            names.database_name,
            "--database-user",
            names.database_user,
            "--wallet-dir",
            str(names.wallet_dir),
            "--dsn",
            dsn,
            "--connect-user",
            names.database_user,
            "--object-name",
            "agg_resumen_archivos_trafico",
            "--source-uri",
            gold_access_url,
            "--load-procedure",
            "APP_GOLD.LOAD_AGG_RESUMEN_ARCHIVOS_TRAFICO",
            "--process-date",
            DEFAULT_BUSINESS_DATE,
            "--source-asset-ref",
            names.gold_source_uri,
            "--target-asset-ref",
            "APP_GOLD.AGG_RESUMEN_ARCHIVOS_TRAFICO",
        ] + shared_runtime_args(names.run_id, "gold_refined")

        initial_load = record_step(
            "load_gold_to_adb_initial",
            lambda: call_mcp_json(
                repo_root,
                "oci-autonomous-database-mcp",
                initial_load_args,
                timeout_seconds=2400,
            ),
        )
        artifacts["initial_lineage_outbox_path"] = initial_load.get("lineage_outbox_path")
        artifacts["initial_checkpoint_path"] = initial_load.get("checkpoint_path")

        reprocess_request = record_step(
            "create_reprocess_request",
            lambda: call_mcp_json(
                repo_root,
                "oci-autonomous-database-mcp",
                [
                    "--environment",
                    names.environment,
                    "--command",
                    "create-reprocess-request",
                    "--database-name",
                    names.database_name,
                    "--requested-reason",
                    "Replay smoke test for run+slice validation",
                    "--requested-by",
                    "codex",
                    "--project-id",
                    names.project_id,
                    "--workflow-id",
                    names.workflow_id,
                    "--parent-run-id",
                    names.run_id,
                    "--reprocess-request-id",
                    names.reprocess_request_id,
                    "--entity-name",
                    "agg_resumen_archivos_trafico",
                    "--layer",
                    "gold_refined",
                    "--slice-key",
                    names.slice_key,
                    "--business-date",
                    DEFAULT_BUSINESS_DATE,
                    "--batch-id",
                    DEFAULT_BATCH_ID,
                    "--control-database-name",
                    names.database_name,
                ],
                timeout_seconds=600,
            ),
        )
        artifacts["reprocess_request_path"] = reprocess_request.get("reprocess_request_path")

        replay_load_args = [
            "--environment",
            names.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "apply",
            "--command",
            "load-gold-objects",
            "--database-name",
            names.database_name,
            "--database-user",
            names.database_user,
            "--wallet-dir",
            str(names.wallet_dir),
            "--dsn",
            dsn,
            "--connect-user",
            names.database_user,
            "--object-name",
            "agg_resumen_archivos_trafico_replay",
            "--source-uri",
            gold_access_url,
            "--load-procedure",
            "APP_GOLD.LOAD_AGG_RESUMEN_ARCHIVOS_TRAFICO",
            "--process-date",
            DEFAULT_BUSINESS_DATE,
            "--source-asset-ref",
            names.gold_source_uri,
            "--target-asset-ref",
            "APP_GOLD.AGG_RESUMEN_ARCHIVOS_TRAFICO",
        ] + shared_runtime_args(
            names.replay_run_id,
            "gold_refined",
            parent_run_id=names.run_id,
            reprocess_request_id=names.reprocess_request_id,
        )

        replay_load = record_step(
            "load_gold_to_adb_replay",
            lambda: call_mcp_json(
                repo_root,
                "oci-autonomous-database-mcp",
                replay_load_args,
                timeout_seconds=2400,
            ),
        )
        artifacts["replay_lineage_outbox_path"] = replay_load.get("lineage_outbox_path")
        artifacts["replay_checkpoint_path"] = replay_load.get("checkpoint_path")

        if not replay_load.get("lineage_outbox_path"):
            warnings.append("No se genero lineage_outbox_path en la corrida replay; se intentara importar el evento inicial.")

        lineage_source = replay_load.get("lineage_outbox_path") or initial_load.get("lineage_outbox_path")
        if not lineage_source:
            raise RuntimeError("No se encontro ningun evento de lineage para importar en Data Catalog")

        import_lineage = record_step(
            "import_openlineage_to_catalog",
            lambda: call_mcp_json(
                repo_root,
                "oci-data-catalog-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "import-openlineage",
                    "--catalog-id",
                    ids["catalog_id"],
                    "--data-asset-key",
                    ids["data_asset_key"],
                    "--from-outbox-file",
                    lineage_source,
                    "--lineage-name",
                    f"{names.project_id}-gold-lineage",
                    "--project-id",
                    names.project_id,
                    "--workflow-id",
                    names.workflow_id,
                    "--run-id",
                    names.replay_run_id,
                    "--entity-name",
                    "agg_resumen_archivos_trafico",
                    "--layer",
                    "gold_refined",
                    "--slice-key",
                    names.slice_key,
                    "--business-date",
                    DEFAULT_BUSINESS_DATE,
                    "--batch-id",
                    DEFAULT_BATCH_ID,
                    "--control-database-name",
                    names.database_name,
                ],
                timeout_seconds=900,
            ),
        )
        artifacts["lineage_import_manifest"] = import_lineage.get("import_manifest")

        lineage_report = record_step(
            "collect_lineage_report",
            lambda: call_mcp_json(
                repo_root,
                "oci-data-catalog-mcp",
                [
                    "--environment",
                    names.environment,
                    "--command",
                    "collect-lineage-report",
                    "--catalog-name",
                    names.catalog_name,
                    "--lineage-name",
                    f"{names.project_id}-lineage-report",
                    "--control-database-name",
                    names.database_name,
                    "--project-id",
                    names.project_id,
                    "--workflow-id",
                    names.workflow_id,
                    "--run-id",
                    names.replay_run_id,
                    "--entity-name",
                    "agg_resumen_archivos_trafico",
                    "--layer",
                    "gold_refined",
                    "--slice-key",
                    names.slice_key,
                    "--business-date",
                    DEFAULT_BUSINESS_DATE,
                    "--batch-id",
                    DEFAULT_BATCH_ID,
                ],
                timeout_seconds=600,
            ),
        )
        artifacts["lineage_report_manifest"] = lineage_report.get("report_manifest")

        quality_profile = record_step(
            "profile_gold_bucket",
            lambda: call_mcp_json_in_docker(
                repo_root,
                "oci-data-quality-mcp",
                [
                    "--environment",
                    names.environment,
                        "--command",
                        "profile-bucket-data",
                        "--bucket-name",
                        names.gold_bucket,
                        "--object-glob",
                        f"objects/{names.project_prefix}/**/*agg_resumen_archivos_trafico*.csv",
                        "--data-format",
                        "csv",
                        "--target-name",
                    "gold_sample",
                ]
                + shared_runtime_args(
                    names.replay_run_id,
                    "gold_refined",
                    parent_run_id=names.run_id,
                    reprocess_request_id=names.reprocess_request_id,
                ),
                timeout_seconds=1200,
            ),
        )
        artifacts["quality_profile_path"] = quality_profile.get("profile_path")

        quality_contract_result = record_step(
            "run_quality_contract",
            lambda: call_mcp_json_in_docker(
                repo_root,
                "oci-data-quality-mcp",
                [
                    "--environment",
                    names.environment,
                    "--runtime",
                    "oci",
                    "--oci-mode",
                    "apply",
                    "--command",
                    "run-contract",
                    "--contract-file",
                    quality_assets["contract_path"],
                    "--database-name",
                    names.database_name,
                    "--database-user",
                    names.database_user,
                    "--wallet-dir",
                    str(names.wallet_dir),
                    "--dsn",
                    dsn,
                    "--connect-user",
                    names.database_user,
                ]
                + shared_runtime_args(
                    names.replay_run_id,
                    "gold_adb",
                    parent_run_id=names.run_id,
                    reprocess_request_id=names.reprocess_request_id,
                ),
                timeout_seconds=3000,
            ),
        )
        artifacts["quality_result_path"] = quality_contract_result.get("result_path")
        if quality_contract_result.get("result_path"):
            artifacts["quality_result_summary"] = summarize_quality_result(Path(quality_contract_result["result_path"]))

        gate_result = record_step(
            "gate_migration",
            lambda: call_mcp_json_in_docker(
                repo_root,
                "oci-data-quality-mcp",
                [
                    "--environment",
                    names.environment,
                    "--command",
                    "gate-migration",
                    "--result-path",
                    quality_contract_result["result_path"],
                    "--severity-threshold",
                    "high",
                    "--database-name",
                    names.database_name,
                ]
                + shared_runtime_args(
                    names.replay_run_id,
                    "gold_adb",
                    parent_run_id=names.run_id,
                    reprocess_request_id=names.reprocess_request_id,
                ),
                timeout_seconds=1200,
            ),
        )
        artifacts["gate_path"] = gate_result.get("gate_path")
        artifacts["gate_summary"] = gate_result.get("summary")

        if gate_result.get("summary", {}).get("status") != "PASS":
            raise RuntimeError(f"El gate final no paso: {json.dumps(gate_result.get('summary', {}), ensure_ascii=True)}")

        manifest_path = record_step(
            "write_final_project_manifest",
            lambda: {
                "manifest_path": str(
                    write_project_manifest(
                        project_root,
                        names,
                        compartment_id=ids["compartment_id"],
                        workspace_id=ids["workspace_id"],
                        catalog_id=ids["catalog_id"],
                    )
                )
            },
        )["manifest_path"]
        artifacts["project_manifest"] = manifest_path

        write_json(
            vars_path,
            {
                "project_id": names.project_id,
                "environment": names.environment,
                "tag": names.tag,
                "run_log_path": str(deployment_log_paths[0]),
                "region": names.region,
                "namespace": names.namespace,
                "compartment_name": names.compartment_name,
                "compartment_id": ids["compartment_id"],
                "landing_bucket": names.landing_bucket,
                "bronze_bucket": names.bronze_bucket,
                "silver_bucket": names.silver_bucket,
                "gold_bucket": names.gold_bucket,
                "database_name": names.database_name,
                "database_user": names.database_user,
                "autonomous_database_id": ids["autonomous_database_id"],
                "wallet_dir": str(names.wallet_dir),
                "dsn": dsn,
                "workspace_name": names.workspace_name,
                "workspace_id": ids["workspace_id"],
                "di_pipeline_key": ids.get("di_pipeline_key"),
                "di_service_blocker_path": artifacts.get("di_service_blocker_path"),
                "catalog_name": names.catalog_name,
                "catalog_id": ids["catalog_id"],
                "data_asset_key": ids["data_asset_key"],
                "catalog_connection_key": ids.get("catalog_connection_key"),
                "catalog_job_definition_key": ids.get("catalog_job_definition_key"),
                "catalog_job_key": ids.get("catalog_job_key"),
                "catalog_pattern_key": ids.get("catalog_pattern_key"),
                "catalog_job_execution_id": ids.get("catalog_job_execution_id"),
                "vcn_name": names.vcn_name,
                "vcn_id": ids["vcn_id"],
                "subnet_name": names.subnet_name,
                "subnet_id": ids["subnet_id"],
                "data_integration_subnet_name": names.subnet_name,
                "data_integration_subnet_id": ids.get("data_integration_subnet_id"),
                "data_flow_subnet_name": names.data_flow_subnet_name,
                "data_flow_subnet_id": ids.get("data_flow_subnet_id"),
                "autonomous_subnet_name": names.autonomous_subnet_name,
                "autonomous_subnet_id": ids.get("autonomous_subnet_id"),
                "nsg_name": names.nsg_name,
                "nsg_id": ids["nsg_id"],
                "route_table_name": names.route_table_name,
                "route_table_id": ids["route_table_id"],
                "data_flow_private_endpoint_name": names.data_flow_private_endpoint_name,
                "data_flow_private_endpoint_id": ids.get("data_flow_private_endpoint_id"),
                "adb_private_endpoint": ids.get("adb_private_endpoint"),
                "adb_private_endpoint_label": names.adb_private_endpoint_label,
                "workflow_id": names.workflow_id,
                "run_id": names.run_id,
                "replay_run_id": names.replay_run_id,
                "reprocess_request_id": names.reprocess_request_id,
                "gold_source_uri": names.gold_source_uri,
                "quality_contract_path": quality_assets["contract_path"],
                "quality_result_path": artifacts.get("quality_result_path"),
                "gate_path": artifacts.get("gate_path"),
            },
        )
        artifacts["vars_path"] = str(vars_path)

        flush_report("completed")
        append_run_log(
            deployment_log_paths,
            "DEPLOYMENT_COMPLETED",
            {
                "status": "completed",
                "total_steps": len(steps),
                "report_path": str(report_path),
                "vars_path": str(vars_path),
                "quality_result_path": artifacts.get("quality_result_path"),
                "gate_path": artifacts.get("gate_path"),
            },
        )
        print(
            json.dumps(
                {
                    "status": "ok",
                    "report_path": str(report_path),
                    "vars_path": str(vars_path),
                    "run_log_path": str(deployment_log_paths[0]),
                    "project_manifest": manifest_path,
                    "compartment_id": ids["compartment_id"],
                    "autonomous_database_id": ids["autonomous_database_id"],
                    "workspace_id": ids["workspace_id"],
                    "catalog_id": ids["catalog_id"],
                    "data_asset_key": ids["data_asset_key"],
                    "quality_result_path": artifacts.get("quality_result_path"),
                    "gate_path": artifacts.get("gate_path"),
                    "gate_summary": artifacts.get("gate_summary"),
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0
    except Exception as exc:  # pragma: no cover - operational error path
        flush_report("failed", str(exc))
        append_run_log(
            deployment_log_paths,
            "DEPLOYMENT_FAILED",
            {
                "status": "failed",
                "total_steps": len(steps),
                "report_path": str(report_path),
                "error": str(exc),
            },
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
