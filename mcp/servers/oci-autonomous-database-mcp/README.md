# oci-autonomous-database-mcp

MCP para definir ADB, bootstrapear el control plane medallion, crear usuarios de base, aplicar SQL/DDL con wallet y registrar checkpoints, reprocesos y cargas Gold en el espejo OCI.

## Modos

- `--runtime local`
  registra definiciones, usuarios, scripts y receipts en el espejo local
- `--runtime oci --oci-mode plan`
  registra el plan OCI CLI para crear ADB y deja receipts de lo que se aplicaria en SQL
- `--runtime oci --oci-mode apply`
  usa OCI CLI para `create-adb-definition` y `python-oracledb` con wallet para `create-database-user` y `apply-sql`

Para despliegues reales con `compute-model=ECPU`, usa `--compute-count 2` o superior. En las pruebas actuales de OCI, `1` ECPU ya no es aceptado para esta clase de Autonomous Database.

Si no vas a crear un private endpoint o una ACL de acceso durante el alta, deja `--is-mtls-connection-required true`. En OCI actual, las conexiones one-way TLS (`false`) se rechazan para altas publicas sin ese networking adicional.

## Flujo recomendado

1. `create-adb-definition`
2. `bootstrap-control-plane`
3. `create-database-user`
4. `bootstrap-schema` para generar un script consolidado si se quiere dejar evidencia
5. `apply-sql` para ejecutar tablas, vistas o procedimientos
6. `register-checkpoint` o `create-reprocess-request` cuando aplique
7. `load-gold-object`

`bootstrap-schema` sigue siendo generador de script. La ejecucion real de usuario y DDL vive en `create-database-user` y `apply-sql`.

`load-gold-object` ahora soporta dos patrones:

- `--source-file`
  deja evidencia local o del plan cuando solo quieres registrar el artefacto gold
- `--source-uri` o `--file-uri-list`
  permite cargar desde Object Storage hacia ADB usando un procedimiento de negocio o `DBMS_CLOUD.COPY_DATA`, que es el patron recomendado cuando la capa `gold_refined` vive en bucket

## Ejemplos

- crear definicion de ADB:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --command create-adb-definition --database-name adb_trafico_gold --database-user app_gold`
- generar bootstrap desde varios SQL:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --command bootstrap-schema --database-name adb_trafico_gold --database-user APP_GOLD --sql-file D:\ruta\010.sql --sql-file D:\ruta\020.sql --sql-file D:\ruta\040.sql`
- bootstrapear el control plane `MDL_CTL`:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --command bootstrap-control-plane --database-name adb_trafico_gold --control-schema MDL_CTL`
- crear usuario y grants:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --runtime oci --oci-mode apply --command create-database-user --database-name adb_trafico_gold --database-user APP_GOLD --wallet-dir D:\wallet --dsn dbclarogold_high`
- aplicar DDL reales:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --runtime oci --oci-mode apply --command apply-sql --database-name adb_trafico_gold --database-user APP_GOLD --wallet-dir D:\wallet --dsn dbclarogold_high --sql-dir D:\sql --sql-pattern 0*.sql`
- registrar checkpoint operativo:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --command register-checkpoint --database-name adb_trafico_gold --run-id run-001 --slice-key entity=trafico/business_date=2026-03-25/batch_id=001 --checkpoint-type gold_load --checkpoint-value ok`
- solicitar reproceso parcial:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --command create-reprocess-request --database-name adb_trafico_gold --workflow-id wf-trafico --parent-run-id run-001 --slice-key entity=trafico/business_date=2026-03-25/batch_id=001 --requested-reason "QA mismatch"`
- registrar carga Gold:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --command load-gold-object --database-name adb_trafico_gold --object-name agg_resumen_archivos_trafico --source-file <ruta>`
- cargar desde bucket usando un procedimiento ya bootstrappeado en ADB:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --runtime oci --oci-mode apply --command load-gold-object --database-name adb_trafico_gold --database-user APP_GOLD --wallet-dir D:\wallet --dsn dbclarogold_high --object-name agg_resumen_archivos_trafico --source-uri "oci://bucket-gold-refined@namespace/exports/agg_resumen_archivos_trafico/process_date=20260326/*.csv" --load-procedure APP_GOLD.LOAD_AGG_RESUMEN_ARCHIVOS_TRAFICO --process-date 20260326`
- cargar desde bucket con `DBMS_CLOUD.COPY_DATA` y merge posterior:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --runtime oci --oci-mode apply --command load-gold-object --database-name adb_trafico_gold --database-user APP_GOLD --wallet-dir D:\wallet --dsn dbclarogold_high --object-name agg_resumen_archivos_trafico --source-uri "oci://bucket-gold-refined@namespace/exports/agg_resumen_archivos_trafico/process_date=20260326/*.csv" --staging-table APP_GOLD.STG_AGG_RESUMEN_ARCHIVOS_TRAFICO --merge-sql-file examples/trafico-datos/sql/030_merge_agg_resumen_archivos_trafico.sql --file-format csv`

La referencia concreta tomada de `oci-ai` para `trafico-datos` vive en:

- `examples/trafico-datos/sql/040_create_load_agg_resumen_archivos_trafico.sql`
- `examples/trafico-datos/sql/030_merge_agg_resumen_archivos_trafico.sql`
