# oci-data-integration-mcp

MCP operativo en modo local para crear workspaces, folders, tasks y pipelines DI en el espejo OCI.

## Modos

- `--runtime local`
  genera manifests y pipeline metadata en el espejo local
- `--runtime oci --oci-mode plan`
  construye y registra comandos OCI CLI reales

## Comandos

- crear workspace:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --command create-workspace --workspace-name ws-di-medallion-dev`
- crear folder:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --command create-folder --workspace-name ws-di-medallion-dev --folder-name pipelines`
- crear task desde Data Flow:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --command create-task-from-dataflow --workspace-name ws-di-medallion-dev --task-name run-silver-gold --application-name silver-to-gold`
- crear pipeline:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --command create-pipeline --workspace-name ws-di-medallion-dev --pipeline-name medallion-pipeline --task bronze-to-silver --task silver-to-gold`
