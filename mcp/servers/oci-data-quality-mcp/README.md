# oci-data-quality-mcp

MCP para QA de datos migrados en buckets y Autonomous Database usando contratos por dataset.

## Modos

- `--runtime local`
  valida datos y artefactos sobre el espejo OCI local
- `--runtime oci --oci-mode plan`
  corre el mismo contrato sobre el espejo, pero deja los checks SQL marcados como pendientes
- `--runtime oci --oci-mode apply`
  ejecuta tambien los SQL de calidad en Autonomous usando wallet y `python-oracledb`

## Comandos

- perfilar datos en bucket:
  `py -3 mcp/servers/oci-data-quality-mcp/server.py --environment dev --command profile-bucket-data --bucket-name bucket-trusted --object-glob "objects/qa/agg_resumen_archivos_trafico/*.csv" --data-format csv`
- correr un contrato:
  `py -3 mcp/servers/oci-data-quality-mcp/server.py --environment dev --command run-contract --contract-file workspace/migration-input/trafico-datos/quality/contracts/agg_resumen_archivos_trafico.contract.json`
- generar el gate final:
  `py -3 mcp/servers/oci-data-quality-mcp/server.py --environment dev --command gate-migration --result-path <ruta> --severity-threshold high`

## Tipos de check soportados

- bucket:
  `file_presence`, `file_count_at_least`, `row_count_at_least`, `required_columns`, `not_null`, `unique_key`, `sum_equals`, `file_name_regex`
- autonomous:
  `mirror_path_exists`, `manifest_field_equals`, `sql_scalar_equals`, `sql_scalar_at_least`, `sql_scalar_between`
- reconciliacion:
  `metric_compare`

## Ubicaciones recomendadas

- contratos:
  `workspace/migration-input/<project_id>/quality/contracts/`
- SQL de calidad:
  `workspace/migration-input/<project_id>/quality/sql/`
- evidencias:
  `workspace/oci-mirror/<env>/<compartment>/quality/`
