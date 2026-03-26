# oci-data-integration-mcp

MCP para crear workspaces, proyectos, folders, tasks Data Flow y pipelines DI en el espejo OCI o contra OCI CLI.

## Modos

- `--runtime local`
  genera manifests y metadata en el espejo local
- `--runtime oci --oci-mode plan`
  construye y registra comandos OCI CLI reales
- `--runtime oci --oci-mode apply`
  ejecuta el comando OCI CLI real

## Capacidades

- soporta `create-workspace`, `create-project`, `create-folder`, `create-task-from-dataflow` y `create-pipeline`
- permite `--description`, `--identifier`, `--aggregator-key`, `--folder-key`, `--registry-version`
- permite `--parent-ref`, `--task-key`, `--application-compartment-id`
- permite etiquetas y favorito con `--label` y `--favorite`

## Ejemplos

- crear workspace:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --command create-workspace --workspace-name ws-di-medallion-dev`
- crear proyecto:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --runtime oci --oci-mode plan --command create-project --workspace-name ws-di-medallion-dev --workspace-id ocid1.disworkspace... --project-name "Medallion Trafico Datos 2" --identifier MEDALLION_TRAFICO_DATOS_2`
- crear folder:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --runtime oci --oci-mode plan --command create-folder --workspace-name ws-di-medallion-dev --workspace-id ocid1.disworkspace... --folder-name "Data Flow Tasks" --identifier DATA_FLOW_TASKS --aggregator-key <project_key>`
- crear task desde Data Flow:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --runtime oci --oci-mode plan --command create-task-from-dataflow --workspace-name ws-di-medallion-dev --workspace-id ocid1.disworkspace... --task-name "Run Bronze to Silver Trafico Datos" --application-id ocid1.dataflowapplication... --application-compartment-id ocid1.compartment... --aggregator-key <project_key> --task-key RUN_BRONZE_TO_SILVER_TRAFICO_DATOS_KEY`
- crear pipeline:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --command create-pipeline --workspace-name ws-di-medallion-dev --pipeline-name medallion-pipeline --task bronze-to-silver --task silver-to-gold`
