# Local MCP Demo

## Objetivo

Ejecutar un flujo local completo sobre el espejo OCI usando los MCPs operativos del factory.

## Comando

`powershell -ExecutionPolicy Bypass -File .\scripts\docker_repo_python.ps1 scripts/run_local_publish_demo.py --repo-root . --environment dev`

## Resultado esperado

Se generan artefactos en:

- `workspace/oci-mirror/dev/.../buckets/`
- `workspace/oci-mirror/dev/.../data_flow/`
- `workspace/oci-mirror/dev/.../data_integration/`
- `workspace/oci-mirror/dev/.../autonomous_database/`
- `workspace/oci-mirror/dev/.../reports/`

Si agregas una carpeta de dependencias por job, el mismo flujo tambien puede generar el `dependency/archive.zip` oficial para Data Flow.

Todo el flujo corre dentro de Docker. No hace falta Python en host.
