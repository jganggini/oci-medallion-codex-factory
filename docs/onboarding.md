# Onboarding

## Requisitos

- Codex App ya instalado y autenticado fuera del repo
- Cursor o VS Code
- Docker Desktop
- Git
- Acceso OCI y credenciales locales
- No se requiere OCI CLI instalado en host para el flujo recomendado
- Para que Codex pueda levantar los MCP locales del factory se necesita un launcher Python en host: `py`, `python` o `python3`

## Flujo recomendado

Prerequisito: Docker Desktop o Docker Engine con `docker compose` instalado y corriendo.

1. Clona el repo y abre esta carpeta local como workspace de trabajo.
2. Ejecuta `setup-dev.ps1 -ProjectId <project_id>` o `./setup-dev.sh <project_id>`.
3. Si Codex, Cursor o VS Code ya estaban abiertos, recarga el proyecto para que tomen `.codex/config.toml`.
4. Empieza el flujo guiado con `docs/codex-advisor.md` y `oci-medallion-advisor`.
5. Cuando el asesor te pida insumos, usa `scripts/docker_stage_assets.ps1` o `scripts/docker_stage_assets.sh` para copiarlos automaticamente a la ruta correcta del proyecto.
6. Usa el flujo manual de `migration_intake.py`, `project.medallion.yaml` y skills individuales solo si quieres depurar o intervenir una etapa especifica del factory.

## Troubleshooting MCP

Si Codex App muestra `No MCP servers configured` en `/mcp`:

1. confirma que abriste la carpeta exacta del repo y no una carpeta padre
2. valida que exista `.codex/config.toml` dentro del repo abierto
3. ejecuta `setup-dev.ps1 -ProjectId <project_id>` o `./setup-dev.sh <project_id>` si aun no lo hiciste
4. recarga la ventana del editor o vuelve a abrir el repo para que Codex relea la configuracion local

## Resultado esperado

Al terminar el onboarding, el repo debe tener:

- `.local/` inicializado sin secretos reales versionados
- rutas confirmadas para `config`, `.pem` y wallets requeridos
- `workspace/oci-mirror/` inicializado por ambiente
- `workspace/migration-input/<project_id>/` listo para intake
- rutas declaradas para SQL, scripts, data y documentacion de referencia
- stage automatico ejecutado para mover insumos y credenciales locales a su ruta correcta
- inventario de insumos generado
- `project.medallion.yaml` con alcance por defecto hasta `gold_adb`, compartment compartido por ambiente, buckets por capa y assets del proyecto correctamente descritos
- runtime Docker levantado antes de intake, bootstrap y publish
- OCI CLI ejecutandose siempre por Docker
- bootstrap del control plane listo para ADB
- base lista para crear foundation OCI, scaffold del proyecto y QA por slice
