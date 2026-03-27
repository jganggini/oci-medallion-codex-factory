# oci-data-integration-mcp

MCP para orquestar OCI Data Integration en espejo local o contra OCI real usando OCI CLI sobre Docker.

## Modos

- `--runtime local`
  genera manifests y reportes en `workspace/oci-mirror/.../data_integration/`
- `--runtime oci --oci-mode plan`
  registra el comando OCI CLI real sin ejecutarlo
- `--runtime oci --oci-mode apply`
  ejecuta el comando OCI CLI real

## Capacidades

- `create-workspace`, `create-project`, `create-folder`, `create-task-from-dataflow`, `create-pipeline`
- `create-application-from-template`
  clona una plantilla Oracle dentro del workspace usando `dis-application create`
- `list-published-objects`
  descubre los published objects de una application runtime
- `create-task-run`, `get-task-run`
  permite ejecutar y consultar task runs reales de DI
- `collect-task-run-report`
  registra TaskRuns y metricas operativas en el espejo local

## Ejemplos

- crear workspace:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --command create-workspace --workspace-name ws-di-medallion-dev`
- clonar una template application Oracle:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --runtime oci --oci-mode apply --command create-application-from-template --workspace-name ws-di-medallion-dev --workspace-id ocid1.disworkspace... --application-name "Object Store Mgmt Runtime" --template-application-key a52f88e4-40da-4cd5-b2f6-34f242e2d792 --copy-type DISCONNECTED --wait-for-state ACTIVE`
- listar published objects:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --runtime oci --oci-mode apply --command list-published-objects --workspace-name ws-di-medallion-dev --workspace-id ocid1.disworkspace... --application-key 3e1aa114-...`
- crear task run desde un published object:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --runtime oci --oci-mode apply --command create-task-run --workspace-name ws-di-medallion-dev --workspace-id ocid1.disworkspace... --application-key 3e1aa114-... --published-object-key 91b31344-... --task-run-name create-bucket-smoke --config-binding REGION=us-chicago-1 --config-binding NAMESPACENAME=axittdqu6jz6 --config-binding COMPARTMENTID=ocid1.compartment... --config-binding BUCKET_NAME=di-smoke-001`
- consultar task run:
  `py -3 mcp/servers/oci-data-integration-mcp/server.py --environment dev --runtime oci --oci-mode apply --command get-task-run --workspace-name ws-di-medallion-dev --workspace-id ocid1.disworkspace... --application-key 3e1aa114-... --task-run-key 11a02e6d-...`

## Hallazgo operativo importante

- La template `CreateBucket` usa el binding `BUCKET_NAME`.
- La template `DeleteBucket` usa el binding `BUCKETNAME`.
- En workspaces DI privados, las pruebas reales mostraron que hace falta:
  `service gateway + route rule` hacia Object Storage en la subnet privada
- Para task runs de bucket management, la policy del workspace DI debe incluir:
  `manage buckets` y `manage objects`, condicionada por `request.principal.type='disworkspace'`.
