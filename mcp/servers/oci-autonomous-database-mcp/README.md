# oci-autonomous-database-mcp

MCP operativo en modo local para registrar definicion de ADB, generar bootstrap y registrar cargas Gold en el espejo OCI.

## Modos

- `--runtime local`
  registra definiciones, bootstrap y cargas en el espejo local
- `--runtime oci --oci-mode plan`
  registra el comando OCI CLI real para provisionar Autonomous

## Comandos

- crear definicion:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --command create-adb-definition --database-name adb_trafico_gold --database-user app_gold`
- generar bootstrap:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --command bootstrap-schema --database-name adb_trafico_gold --database-user app_gold`
- registrar carga:
  `py -3 mcp/servers/oci-autonomous-database-mcp/server.py --environment dev --command load-gold-object --database-name adb_trafico_gold --object-name agg_resumen_archivos_trafico --source-file <ruta>`
