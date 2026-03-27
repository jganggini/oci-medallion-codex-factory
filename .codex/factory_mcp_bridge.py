from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_PROTOCOL_VERSION = "2025-06-18"
DEFAULT_ENVIRONMENT = "dev"
DEFAULT_RUNTIME = "local"
DEFAULT_OCI_MODE = "plan"
SERVER_VERSION = "0.1.0"

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

REPO_ROOT = Path(__file__).resolve().parents[1]

FLAG_KEYS = {"force", "use_legacy_packager_image"}
ARRAY_KEYS = {
    "cidr_block",
    "config_binding",
    "define",
    "dns_zone",
    "label",
    "nsg_id",
    "parameter",
    "route_rule_json",
    "service_id",
    "sql_file",
    "source_uri",
    "statement",
    "procedure_arg",
    "task",
    "variable",
    "wait_for_state",
}
INTEGER_KEYS = {
    "bytes_read",
    "bytes_written",
    "compute_count",
    "data_storage_size_in_tbs",
    "max_wait_seconds",
    "num_executors",
    "registry_version",
    "rows_in",
    "rows_out",
    "rows_rejected",
    "sample_size",
    "wait_interval_seconds",
}
NUMBER_KEYS = {
    "driver_shape_memory_gbs",
    "driver_shape_ocpus",
    "executor_shape_memory_gbs",
    "executor_shape_ocpus",
}
BOOLEAN_KEYS = {
    "favorite",
    "ingestion_outside_flow",
    "is_mtls_connection_required",
    "is_private_network",
    "managed_by_factory",
    "prohibit_public_ip_on_vnic",
    "truncate_before_load",
}
JSON_KEYS = {
    "asset_properties_json",
    "connection_properties_json",
    "copy_format_json",
    "driver_shape_config_json",
    "executor_shape_config_json",
    "job_properties_json",
    "route_rule_json",
}
COMMON_RUNTIME_PROPS = (
    "environment",
    "runtime",
    "oci_mode",
    "oci_profile",
    "workflow_id",
    "run_id",
    "parent_run_id",
    "slice_key",
    "watermark_low",
    "watermark_high",
    "reprocess_request_id",
    "quality_profile",
    "source_asset_ref",
    "target_asset_ref",
    "service_run_ref",
    "layer",
)
READ_ONLY = {
    "collect_lineage_report",
    "collect_run_report",
    "collect_task_run_report",
    "export_iam_manifest",
    "export_network_manifest",
    "export_stack_report",
    "inventory_sources",
    "summarize_readiness",
    "validate_archive",
    "validate_input_structure",
}
ALIASES = {
    "asset_properties": "asset_properties_json",
    "cidr_blocks": "cidr_block",
    "config_bindings": "config_binding",
    "connection_properties": "connection_properties_json",
    "defines": "define",
    "dns_zones": "dns_zone",
    "driver_shape_config": "driver_shape_config_json",
    "executor_shape_config": "executor_shape_config_json",
    "job_properties": "job_properties_json",
    "labels": "label",
    "parameters": "parameter",
    "route_rules": "route_rule_json",
    "service_ids": "service_id",
    "sql_files": "sql_file",
    "statements": "statement",
    "tasks": "task",
    "wait_for_states": "wait_for_state",
}


def prop_schema(name: str) -> dict[str, object]:
    label = name.replace("_", " ")
    if name in ARRAY_KEYS:
        return {"type": "array", "items": {"type": "string"}, "description": f"Lista para {label}."}
    if name in INTEGER_KEYS:
        return {"type": "integer", "description": f"Valor entero para {label}."}
    if name in NUMBER_KEYS:
        return {"type": "number", "description": f"Valor numerico para {label}."}
    if name in FLAG_KEYS or name in BOOLEAN_KEYS:
        return {"type": "boolean", "description": f"Bandera booleana para {label}."}
    if name in JSON_KEYS or name.endswith("_json"):
        return {"anyOf": [{"type": "object"}, {"type": "string"}], "description": f"JSON para {label}."}
    if name == "environment":
        return {"type": "string", "enum": ["dev", "qa", "prod"], "description": "Ambiente OCI. Default: dev."}
    if name == "runtime":
        return {"type": "string", "enum": ["local", "oci"], "description": "Modo de ejecucion. Default: local."}
    if name == "oci_mode":
        return {"type": "string", "enum": ["plan", "apply"], "description": "Modo OCI cuando runtime=oci. Default: plan."}
    return {"type": "string", "description": f"Valor para {label}."}


def schema(*props: str, required: tuple[str, ...] = ()) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": "object",
        "properties": {name: prop_schema(name) for name in props},
        "additionalProperties": True,
    }
    if required:
        payload["required"] = list(required)
    return payload


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    props: tuple[str, ...]
    required: tuple[str, ...] = ()
    command: str | None = None
    custom_mode: str | None = None

    def payload(self, include_common_runtime: bool = True) -> dict[str, object]:
        runtime_props = COMMON_RUNTIME_PROPS if include_common_runtime else ()
        input_schema = schema(*(runtime_props + self.props), required=self.required)
        payload: dict[str, object] = {
            "name": self.name,
            "description": self.description,
            "inputSchema": input_schema,
        }
        if self.name in READ_ONLY:
            payload["annotations"] = {"readOnlyHint": True}
        return payload


@dataclass(frozen=True)
class ServerSpec:
    key: str
    codex_name: str
    script_path: str
    description: str
    tools: tuple[ToolSpec, ...]
    tool_index: dict[str, ToolSpec] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "tool_index", {tool.name: tool for tool in self.tools})


def tool(
    name: str,
    description: str,
    *props: str,
    required: tuple[str, ...] = (),
    command: str | None = None,
    custom_mode: str | None = None,
) -> ToolSpec:
    return ToolSpec(name=name, description=description, props=props, required=required, command=command, custom_mode=custom_mode)


SERVERS: dict[str, ServerSpec] = {
    "migration-intake-mcp": ServerSpec(
        key="migration-intake-mcp",
        codex_name="oci_medallion_migration_intake",
        script_path="mcp/servers/migration-intake-mcp/server.py",
        description="Inventario y readiness de insumos de migracion.",
        tools=(
            tool("inventory_sources", "Indexa los insumos del proyecto.", "project_id", required=("project_id",), command="inventory"),
            tool("validate_input_structure", "Valida blockers y warnings del intake.", "project_id", required=("project_id",), command="validate"),
            tool("summarize_readiness", "Resume si el proyecto esta listo para avanzar.", "project_id", required=("project_id",), command="summarize"),
            tool("block_if_missing_required_inputs", "Bloquea si faltan insumos requeridos.", "project_id", required=("project_id",), command="validate", custom_mode="block_on_not_ready"),
        ),
    ),
    "oci-iam-mcp": ServerSpec(
        key="oci-iam-mcp",
        codex_name="oci_medallion_iam",
        script_path="mcp/servers/oci-iam-mcp/server.py",
        description="Foundation IAM del ambiente medallion.",
        tools=(
            tool("create_compartment", "Crea un compartment o registra su plan.", "compartment_name", "parent_compartment_id", "description", required=("compartment_name", "parent_compartment_id")),
            tool("create_group", "Crea un grupo IAM o registra su plan.", "group_name", "compartment_id", "description", required=("group_name", "compartment_id")),
            tool("create_dynamic_group", "Crea un dynamic group IAM o registra su plan.", "dynamic_group_name", "matching_rule", "description", required=("dynamic_group_name", "matching_rule")),
            tool("create_policy", "Crea una policy IAM o registra su plan.", "policy_name", "compartment_id", "description", "statement", required=("policy_name", "compartment_id", "statement")),
            tool("export_iam_manifest", "Exporta el manifiesto IAM del mirror."),
        ),
    ),
    "oci-network-mcp": ServerSpec(
        key="oci-network-mcp",
        codex_name="oci_medallion_network",
        script_path="mcp/servers/oci-network-mcp/server.py",
        description="VCN, subnet, NSG y route tables.",
        tools=(
            tool("create_vcn", "Crea una VCN o registra su plan.", "compartment_id", "vcn_name", "cidr_block", "dns_label", "description", required=("compartment_id", "vcn_name", "cidr_block")),
            tool("create_subnet", "Crea una subnet o registra su plan.", "compartment_id", "vcn_id", "subnet_name", "cidr_block", "dns_label", "route_table_id", "nsg_id", "prohibit_public_ip_on_vnic", "description", required=("compartment_id", "vcn_id", "subnet_name", "cidr_block")),
            tool("create_nsg", "Crea un NSG o registra su plan.", "compartment_id", "vcn_id", "nsg_name", "description", required=("compartment_id", "vcn_id", "nsg_name")),
            tool("create_route_table", "Crea una route table o registra su plan.", "compartment_id", "vcn_id", "route_table_name", "route_rule_json", "description", required=("compartment_id", "vcn_id", "route_table_name")),
            tool("create_service_gateway", "Crea un Service Gateway o registra su plan.", "compartment_id", "vcn_id", "service_gateway_name", "service_id", "description", required=("compartment_id", "vcn_id", "service_gateway_name", "service_id")),
            tool("update_route_table", "Actualiza reglas de una route table.", "route_table_id", "route_rule_json", required=("route_table_id", "route_rule_json")),
            tool("export_network_manifest", "Exporta el manifiesto de red del mirror."),
        ),
    ),
    "oci-object-storage-mcp": ServerSpec(
        key="oci-object-storage-mcp",
        codex_name="oci_medallion_object_storage",
        script_path="mcp/servers/oci-object-storage-mcp/server.py",
        description="Buckets por capa y trazabilidad de Object Storage.",
        tools=(
            tool("create_bucket", "Crea o registra un bucket.", "bucket_name", "display_name", "storage_tier", "compartment_id", "namespace_name", "managed_by_factory", "ingestion_outside_flow", "bucket_purpose", "existing_state", required=("bucket_name",)),
            tool("upload_object", "Sube un objeto al bucket.", "bucket_name", "source_file", "object_name", "namespace_name", required=("bucket_name", "source_file")),
            tool("sync_bucket_manifest", "Sincroniza metadata de un bucket existente.", "bucket_name", "display_name", "storage_tier", "namespace_name", "managed_by_factory", "ingestion_outside_flow", "bucket_purpose", "existing_state", required=("bucket_name",)),
        ),
    ),
    "oci-resource-manager-mcp": ServerSpec(
        key="oci-resource-manager-mcp",
        codex_name="oci_medallion_resource_manager",
        script_path="mcp/servers/oci-resource-manager-mcp/server.py",
        description="Stacks Terraform compatibles con OCI Resource Manager.",
        tools=(
            tool("create_stack", "Crea un stack en el mirror.", "stack_name", "compartment_id", "working_directory", "description", "config_source_file", "variable", required=("stack_name",)),
            tool("plan_stack", "Registra el plan de un stack.", "stack_name", "job_id", required=("stack_name",)),
            tool("apply_stack", "Registra la aplicacion de un stack.", "stack_name", "job_id", required=("stack_name",)),
            tool("export_stack_report", "Exporta el reporte del stack.", "stack_name", required=("stack_name",)),
        ),
    ),
    "oci-data-flow-mcp": ServerSpec(
        key="oci-data-flow-mcp",
        codex_name="oci_medallion_data_flow",
        script_path="mcp/servers/oci-data-flow-mcp/server.py",
        description="Empaquetado, definicion y ejecucion de Data Flow.",
        tools=(
            tool("package_dependencies", "Empaqueta dependencias para Data Flow.", "application_name", "dependency_root", "python_version", "archive_name", "packager_image", "use_legacy_packager_image", required=("application_name", "dependency_root")),
            tool("validate_archive", "Valida el archive.zip generado.", "application_name", "dependency_root", "archive_name", required=("application_name", "dependency_root")),
            tool("create_application", "Crea una aplicacion Data Flow.", "application_name", "source_dir", "dependency_root", "main_file", "from_json_file", "archive_source_file", "archive_uri", "file_uri", "compartment_id", "display_name", "driver_shape", "executor_shape", "driver_shape_config_json", "executor_shape_config_json", "driver_shape_ocpus", "driver_shape_memory_gbs", "executor_shape_ocpus", "executor_shape_memory_gbs", "num_executors", "spark_version", "logs_bucket_uri", "language", "application_type", "python_version", "archive_name", "packager_image", "use_legacy_packager_image", "wait_for_state", "max_wait_seconds", "wait_interval_seconds", required=("application_name",)),
            tool("update_application", "Actualiza una aplicacion Data Flow.", "application_name", "application_id", "source_dir", "dependency_root", "main_file", "from_json_file", "archive_source_file", "archive_uri", "file_uri", "display_name", "driver_shape", "executor_shape", "driver_shape_config_json", "executor_shape_config_json", "driver_shape_ocpus", "driver_shape_memory_gbs", "executor_shape_ocpus", "executor_shape_memory_gbs", "num_executors", "spark_version", "logs_bucket_uri", "language", "application_type", "force", "wait_for_state", "max_wait_seconds", "wait_interval_seconds", required=("application_name", "application_id")),
            tool("run_application", "Ejecuta una aplicacion Data Flow.", "application_name", "application_id", "parameter", "wait_for_state", "max_wait_seconds", "wait_interval_seconds", required=("application_name",)),
            tool("collect_run_report", "Registra metricas por run y slice.", "application_name", "run_id", "slice_key", "state", "driver_log_uri", "executor_log_uri", "rows_in", "rows_out", "rows_rejected", required=("application_name", "run_id")),
        ),
    ),
    "oci-data-integration-mcp": ServerSpec(
        key="oci-data-integration-mcp",
        codex_name="oci_medallion_data_integration",
        script_path="mcp/servers/oci-data-integration-mcp/server.py",
        description="Workspaces, proyectos, folders, tasks y pipelines de DI.",
        tools=(
            tool("create_workspace", "Crea un workspace DI.", "workspace_name", "compartment_id", "is_private_network", "subnet_id", "vcn_id", "description", "wait_for_state", "max_wait_seconds", "wait_interval_seconds", required=("workspace_name",)),
            tool("create_project", "Crea un proyecto DI.", "workspace_name", "workspace_id", "project_name", "identifier", "description", "aggregator_key", "registry_version", "parent_ref", "label", "favorite", required=("workspace_name", "workspace_id", "project_name")),
            tool("create_folder", "Crea un folder DI.", "workspace_name", "workspace_id", "folder_name", "identifier", "description", "aggregator_key", "folder_key", "registry_version", "parent_ref", "label", "favorite", required=("workspace_name", "workspace_id", "folder_name")),
            tool("create_application_from_template", "Clona una application runtime dentro del workspace DI.", "workspace_name", "workspace_id", "application_name", "application_key", "template_application_key", "copy_type", "wait_for_state", "max_wait_seconds", "wait_interval_seconds", required=("workspace_name", "workspace_id", "application_name")),
            tool("list_published_objects", "Lista published objects de una application runtime.", "workspace_name", "workspace_id", "application_name", "application_key", required=("workspace_name", "workspace_id", "application_key")),
            tool("create_task_from_dataflow", "Crea una task DI que ejecuta Data Flow.", "workspace_name", "workspace_id", "task_name", "task_key", "application_name", "application_id", "application_compartment_id", "aggregator_key", "folder_key", "identifier", "description", "registry_version", "parent_ref", "label", "favorite", required=("workspace_name", "workspace_id", "task_name")),
            tool("create_task_run", "Ejecuta un TaskRun DI desde un published object.", "workspace_name", "workspace_id", "application_name", "application_key", "published_object_key", "task_key", "aggregator_key", "task_run_name", "config_binding", "wait_for_state", "max_wait_seconds", "wait_interval_seconds", required=("workspace_name", "workspace_id", "application_key")),
            tool("get_task_run", "Consulta un TaskRun DI existente.", "workspace_name", "workspace_id", "application_name", "application_key", "task_run_key", required=("workspace_name", "workspace_id", "application_key", "task_run_key")),
            tool("create_pipeline", "Crea un pipeline DI.", "workspace_name", "pipeline_name", "task", "description", required=("workspace_name", "pipeline_name", "task")),
            tool("collect_task_run_report", "Registra metricas de un TaskRun.", "workspace_name", "task_name", "task_run_key", "run_id", "slice_key", "state", "bytes_read", "bytes_written", "rows_in", "rows_out", "rows_rejected", required=("workspace_name", "task_name", "run_id")),
        ),
    ),
    "oci-autonomous-database-mcp": ServerSpec(
        key="oci-autonomous-database-mcp",
        codex_name="oci_medallion_autonomous_database",
        script_path="mcp/servers/oci-autonomous-database-mcp/server.py",
        description="ADB, control plane, SQL, usuarios y reprocesos.",
        tools=(
            tool("create_adb_definition", "Genera la definicion de Autonomous Database.", "database_name", "database_user", "compartment_id", "db_name", "db_workload", "compute_count", "compute_model", "data_storage_size_in_tbs", "display_name", "db_version", "secret_id", "admin_password", "admin_password_env", "is_mtls_connection_required", "wait_for_state", "max_wait_seconds", "wait_interval_seconds", "password_placeholder", required=("database_name",)),
            tool("create_autonomous_database", "Alias compatible para crear o definir ADB.", "database_name", "database_user", "compartment_id", "db_name", "db_workload", "compute_count", "compute_model", "data_storage_size_in_tbs", "display_name", "db_version", "secret_id", "admin_password", "admin_password_env", "is_mtls_connection_required", "wait_for_state", "max_wait_seconds", "wait_interval_seconds", "password_placeholder", required=("database_name",), command="create-autonomous-database"),
            tool("download_wallet_metadata", "Descarga y registra el wallet de Autonomous Database.", "database_name", "database_user", "autonomous_database_id", "wallet_dir", "wallet_password", "wallet_password_env", "generate_type", required=("database_name",), command="download-wallet-metadata"),
            tool("bootstrap_control_plane", "Bootstrapea el schema de control medallion.", "database_name", "control_schema", "control_user", "database_user", "wallet_dir", "dsn", "admin_user", "admin_password", "admin_password_env", "control_password", "control_password_env", "wallet_password", "wallet_password_env", required=("database_name",)),
            tool("bootstrap_schema", "Consolida scripts SQL base del schema.", "database_name", "database_user", "sql_file", "sql_dir", "sql_pattern", "define", "ignore_exists", required=("database_name",)),
            tool("create_database_user", "Crea el usuario/schema de aplicacion en ADB.", "database_name", "database_user", "wallet_dir", "dsn", "admin_user", "admin_password", "admin_password_env", "database_password", "database_password_env", "control_schema", "control_user", "control_password", "control_password_env", required=("database_name", "database_user")),
            tool("apply_sql", "Ejecuta SQL o DDL real en ADB.", "database_name", "database_user", "wallet_dir", "dsn", "connect_user", "connect_password", "connect_password_env", "wallet_password", "wallet_password_env", "sql_file", "sql_dir", "sql_pattern", "define", "ignore_exists", required=("database_name",)),
            tool("register_checkpoint", "Registra un checkpoint operativo.", "database_name", "run_id", "slice_key", "checkpoint_type", "checkpoint_value", required=("database_name", "run_id", "checkpoint_type", "checkpoint_value")),
            tool("create_reprocess_request", "Registra una solicitud de reproceso.", "database_name", "workflow_id", "parent_run_id", "slice_key", "requested_reason", "requested_by", required=("database_name", "workflow_id", "parent_run_id", "slice_key", "requested_reason")),
            tool(
                "load_gold_object",
                "Registra o carga un objeto gold desde archivo local o desde bucket OCI hacia Autonomous.",
                "database_name",
                "database_user",
                "object_name",
                "source_file",
                "source_uri",
                "file_uri_list",
                "load_procedure",
                "procedure_arg",
                "target_table",
                "staging_table",
                "merge_sql_file",
                "credential_name",
                "copy_format_json",
                "file_format",
                "process_date",
                "truncate_before_load",
                "wallet_dir",
                "dsn",
                "connect_user",
                "connect_password",
                "connect_password_env",
                "database_password",
                "database_password_env",
                "wallet_password",
                "wallet_password_env",
                required=("database_name", "object_name"),
            ),
            tool(
                "load_gold_objects",
                "Alias compatible para registrar o cargar objetos gold desde archivo local o bucket OCI.",
                "database_name",
                "database_user",
                "object_name",
                "source_file",
                "source_uri",
                "file_uri_list",
                "load_procedure",
                "procedure_arg",
                "target_table",
                "staging_table",
                "merge_sql_file",
                "credential_name",
                "copy_format_json",
                "file_format",
                "process_date",
                "truncate_before_load",
                "wallet_dir",
                "dsn",
                "connect_user",
                "connect_password",
                "connect_password_env",
                "database_password",
                "database_password_env",
                "wallet_password",
                "wallet_password_env",
                required=("database_name", "object_name"),
                command="load-gold-objects",
            ),
        ),
    ),
    "oci-data-quality-mcp": ServerSpec(
        key="oci-data-quality-mcp",
        codex_name="oci_medallion_data_quality",
        script_path="mcp/servers/oci-data-quality-mcp/server.py",
        description="Contratos QA por run y slice.",
        tools=(
            tool("profile_bucket_data", "Perfila archivos del mirror de Object Storage.", "bucket_name", "object_glob", "data_format", "target_name", "sample_size", required=("bucket_name",)),
            tool("run_contract", "Ejecuta un contrato de calidad.", "contract_file", "database_name", "database_user", "wallet_dir", "dsn", "wallet_password", "wallet_password_env", "admin_user", "admin_password", "admin_password_env", "database_password", "database_password_env", "connect_user", "connect_password", "connect_password_env", required=("contract_file",)),
            tool("gate_migration", "Evalua el gate final de calidad.", "result_path", "gate_name", "severity_threshold", required=("result_path",)),
        ),
    ),
    "oci-data-catalog-mcp": ServerSpec(
        key="oci-data-catalog-mcp",
        codex_name="oci_medallion_data_catalog",
        script_path="mcp/servers/oci-data-catalog-mcp/server.py",
        description="Catalogo, harvests y lineage OpenLineage.",
        tools=(
            tool("create_catalog", "Crea un catalogo Data Catalog.", "catalog_name", "compartment_id", "wait_for_state", "max_wait_seconds", "wait_interval_seconds"),
            tool("create_private_endpoint", "Crea un private endpoint de Data Catalog.", "catalog_name", "catalog_id", "private_endpoint_name", "subnet_id", "vcn_id", "dns_zone", "wait_for_state", "max_wait_seconds", "wait_interval_seconds", required=("private_endpoint_name",)),
            tool("create_data_asset", "Registra un data asset.", "catalog_name", "catalog_id", "asset_name", "asset_type_key", "asset_properties_json", required=("asset_name", "asset_type_key")),
            tool("create_connection", "Registra una conexion en Data Catalog.", "catalog_name", "catalog_id", "data_asset_key", "connection_name", "connection_type_key", "connection_properties_json", required=("connection_name", "connection_type_key")),
            tool("create_harvest_job_definition", "Crea una definicion de harvest.", "catalog_name", "catalog_id", "job_name", "data_asset_key", "job_type", "job_properties_json", required=("job_name", "data_asset_key")),
            tool("run_harvest_job", "Ejecuta un harvest job.", "catalog_name", "catalog_id", "job_name", "job_key", "job_definition_key", "wait_for_state", "max_wait_seconds", "wait_interval_seconds"),
            tool("sync_di_lineage", "Sincroniza lineage nativo de DI.", "workspace_name", "job_key", "catalog_name", "catalog_id", required=("workspace_name",)),
            tool("import_openlineage", "Publica lineage custom desde OpenLineage o outbox.", "catalog_name", "catalog_id", "lineage_file", "from_outbox_file", "from_json_file", "lineage_name"),
            tool("collect_lineage_report", "Consolida un reporte de lineage.", "catalog_name", "catalog_id", "workspace_name", "job_key", "lineage_name"),
        ),
    ),
    "oci-vault-mcp": ServerSpec(
        key="oci-vault-mcp",
        codex_name="oci_medallion_vault",
        script_path="mcp/servers/oci-vault-mcp/server.py",
        description="Vault, secretos y referencias redacted.",
        tools=(
            tool("create_vault", "Crea o registra un vault.", "vault_name", "vault_id", "compartment_id", "key_id", "description", required=("vault_name",)),
            tool("create_secret", "Crea o registra un secreto.", "vault_name", "secret_name", "secret_ref", "description", required=("vault_name", "secret_name")),
            tool("rotate_secret_reference", "Rota la referencia redacted de un secreto.", "vault_name", "secret_name", "secret_ref", "new_secret_ref", required=("vault_name", "secret_name", "secret_ref", "new_secret_ref")),
            tool("export_vault_manifest", "Exporta el manifiesto redacted de Vault."),
        ),
    ),
}


class BridgeError(Exception):
    def __init__(self, message: str, code: int = INTERNAL_ERROR) -> None:
        super().__init__(message)
        self.code = code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bridge MCP stdio para los runtimes del factory.")
    parser.add_argument("--server", required=True, choices=tuple(SERVERS.keys()))
    return parser.parse_args()


def write(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True) + "\n")
    sys.stdout.flush()


def ok(message_id: Any, result: dict[str, Any]) -> None:
    write({"jsonrpc": "2.0", "id": message_id, "result": result})


def fail(message_id: Any, code: int, message: str) -> None:
    write({"jsonrpc": "2.0", "id": message_id, "error": {"code": code, "message": message}})


def invocation_prefix() -> list[str]:
    if platform.system().lower().startswith("win"):
        return ["powershell", "-ExecutionPolicy", "Bypass", "-File", str(REPO_ROOT / "scripts" / "docker_repo_python.ps1")]
    return ["bash", str(REPO_ROOT / "scripts" / "docker_repo_python.sh")]


def normalize_args(raw: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(raw or {})
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        mapped = ALIASES.get(key, key)
        if mapped == "parameter" and key == "parameters" and isinstance(value, dict):
            normalized[mapped] = [f"{item_key}={item_value}" for item_key, item_value in value.items()]
            continue
        if mapped in ARRAY_KEYS and isinstance(value, str):
            normalized[mapped] = [value]
            continue
        normalized[mapped] = value
    normalized.setdefault("environment", DEFAULT_ENVIRONMENT)
    normalized.setdefault("runtime", DEFAULT_RUNTIME)
    normalized.setdefault("oci_mode", DEFAULT_OCI_MODE)
    return normalized


def append_arg(command: list[str], key: str, value: Any) -> None:
    if value is None:
        return
    option = "--" + key.replace("_", "-")
    if key in FLAG_KEYS:
        if bool(value):
            command.append(option)
        return
    if key in BOOLEAN_KEYS:
        command.extend([option, "true" if bool(value) else "false"])
        return
    if key in ARRAY_KEYS:
        items = value if isinstance(value, list) else [value]
        for item in items:
            command.extend([option, json.dumps(item, ensure_ascii=True) if isinstance(item, dict) else str(item)])
        return
    if key in JSON_KEYS and isinstance(value, dict):
        command.extend([option, json.dumps(value, ensure_ascii=True)])
        return
    if isinstance(value, list):
        for item in value:
            command.extend([option, json.dumps(item, ensure_ascii=True) if isinstance(item, dict) else str(item)])
        return
    if isinstance(value, dict):
        command.extend([option, json.dumps(value, ensure_ascii=True)])
        return
    command.extend([option, str(value)])


def runtime_command(server_spec: ServerSpec, tool_spec: ToolSpec, raw_arguments: dict[str, Any]) -> list[str]:
    arguments = normalize_args(raw_arguments)
    command = [*invocation_prefix(), server_spec.script_path, "--repo-root", ".", "--command", tool_spec.command or tool_spec.name.replace("_", "-")]
    if server_spec.key != "migration-intake-mcp":
        for key in COMMON_RUNTIME_PROPS:
            if key in arguments:
                append_arg(command, key, arguments.pop(key))
    else:
        for key in COMMON_RUNTIME_PROPS:
            arguments.pop(key, None)
    for key, value in arguments.items():
        append_arg(command, key, value)
    return command


def parse_stdout(stdout: str) -> tuple[dict[str, Any] | None, str]:
    text = stdout.strip()
    if not text:
        return None, ""
    candidates = [text]
    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        candidates.append(text[first_brace : last_brace + 1])
    first_bracket = text.find("[")
    last_bracket = text.rfind("]")
    if first_bracket != -1 and last_bracket > first_bracket:
        candidates.append(text[first_bracket : last_bracket + 1])
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed, json.dumps(parsed, indent=2, ensure_ascii=True)
        return {"result": parsed}, json.dumps(parsed, indent=2, ensure_ascii=True)
    return None, text


def call_tool(server_spec: ServerSpec, tool_spec: ToolSpec, raw_arguments: dict[str, Any]) -> dict[str, Any]:
    command = runtime_command(server_spec, tool_spec, raw_arguments)
    result = subprocess.run(command, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    structured, text = parse_stdout(result.stdout)
    stderr = result.stderr.strip()

    if result.returncode != 0:
        payload = {
            "status": "error",
            "server": server_spec.key,
            "tool": tool_spec.name,
            "command": command,
            "return_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        return {
            "content": [{"type": "text", "text": json.dumps(payload, indent=2, ensure_ascii=True)}],
            "structuredContent": payload,
            "isError": True,
        }

    if structured is None:
        structured = {"status": "ok", "server": server_spec.key, "tool": tool_spec.name, "stdout": result.stdout}
        text = text or json.dumps(structured, indent=2, ensure_ascii=True)
    if stderr:
        structured = dict(structured)
        structured["stderr"] = stderr
    if tool_spec.custom_mode == "block_on_not_ready":
        ready = bool(structured.get("ready_for_scaffold"))
        if not ready:
            structured = {"status": "blocked", **structured}
            text = json.dumps(structured, indent=2, ensure_ascii=True)
        return {"content": [{"type": "text", "text": text or "{}"}], "structuredContent": structured, "isError": not ready}
    return {"content": [{"type": "text", "text": text or "{}"}], "structuredContent": structured, "isError": False}


def handle(server_spec: ServerSpec, message: dict[str, Any]) -> None:
    if not isinstance(message, dict):
        raise BridgeError("Cada mensaje MCP debe ser un objeto JSON.", INVALID_REQUEST)
    method = message.get("method")
    message_id = message.get("id")
    params = message.get("params") or {}

    if method == "initialize":
        requested_protocol = params.get("protocolVersion", DEFAULT_PROTOCOL_VERSION)
        ok(
            message_id,
            {
                "protocolVersion": requested_protocol,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": server_spec.codex_name, "title": server_spec.codex_name, "version": SERVER_VERSION},
                "instructions": server_spec.description,
            },
        )
        return
    if method == "tools/list":
        include_common_runtime = server_spec.key != "migration-intake-mcp"
        ok(message_id, {"tools": [tool.payload(include_common_runtime=include_common_runtime) for tool in server_spec.tools]})
        return
    if method == "tools/call":
        tool_name = params.get("name")
        if not isinstance(tool_name, str):
            raise BridgeError("tools/call requiere un nombre de tool valido.", INVALID_PARAMS)
        tool_spec = server_spec.tool_index.get(tool_name)
        if tool_spec is None:
            raise BridgeError(f"La tool '{tool_name}' no existe en '{server_spec.key}'.", INVALID_PARAMS)
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, dict):
            raise BridgeError("tools/call requiere que arguments sea un objeto.", INVALID_PARAMS)
        ok(message_id, call_tool(server_spec, tool_spec, arguments))
        return
    if method == "ping":
        ok(message_id, {})
        return
    if method == "logging/setLevel":
        ok(message_id, {})
        return
    if method == "notifications/initialized":
        return
    if message_id is None:
        return
    raise BridgeError(f"Metodo MCP no soportado: {method}", METHOD_NOT_FOUND)


def main() -> int:
    args = parse_args()
    server_spec = SERVERS[args.server]
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            fail(None, PARSE_ERROR, f"No se pudo parsear el mensaje MCP: {exc}")
            continue
        try:
            handle(server_spec, message)
        except BridgeError as exc:
            fail(message.get("id"), exc.code, str(exc))
        except Exception as exc:  # pragma: no cover
            fail(message.get("id"), INTERNAL_ERROR, f"Error interno del bridge: {exc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
