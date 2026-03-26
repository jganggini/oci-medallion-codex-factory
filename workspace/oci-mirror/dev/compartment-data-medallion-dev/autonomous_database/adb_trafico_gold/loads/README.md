# oci-medallion-codex-factory

Base reusable para construir proyectos medallion en OCI con Codex, MCPs por servicio, Terraform compatible con OCI Resource Manager y un espejo local de OCI alineado con ambientes, compartments y servicios.

## Objetivo

Este repo permite que un equipo tome SQL heredado, documentos funcionales, DDL, muestras de datos y exports historicos para generar un proyecto medallion en OCI sin improvisar rutas, carpetas ni contratos entre desarrollo, publicacion y operacion.

## Zonas locales

- `workspace/migration-input/`
  Zona canonica de insumos de migracion versionables y compartibles con el equipo.
- `workspace/oci-mirror/`
  Espejo local del estado publicado en OCI por ambiente y servicio.
- `.local/`
  Configuracion sensible y artefactos no versionados, incluyendo perfiles OCI, wallets y una zona opcional `migration-private/` para material fuente que no deba quedar en Git.

## Estructura

- `infra/`
  Modulos Terraform y stacks base por ambiente.
- `mcp/`
  Catalogo, contratos y manifests de MCPs por servicio OCI.
- `skills/`
  Workflows de Codex para bootstrap, intake, scaffold, publicacion y validacion.
- `templates/`
  Plantillas base de manifiestos y esqueletos de proyecto.
- `examples/`
  Ejemplos de proyectos medallion.
- `docker/`
  Imagenes base para runtime local, OCI CLI y uso futuro con MCPHub.
- `workspace/`
  Insumos, espejo OCI y proyectos generados.
- `scripts/`
  Setup local, intake y validaciones del factory.

## Primer uso

1. Clona el repo.
2. Ejecuta `setup-dev.ps1` o `setup-dev.sh`.
3. Coloca tus insumos del proyecto en `workspace/migration-input/<project_id>/`.
4. Si necesitas mantener copias privadas, usa `.local/migration-private/<project_id>/`.
5. Completa tu configuracion OCI local en `.local/oci/`.
6. Levanta Docker con `docker compose up -d`.
7. Abre el repo con Codex App desde Cursor o VS Code.
8. Ejecuta el intake con `python scripts/migration_intake.py --repo-root . --project-id <project_id>`.
9. Usa las skills `oci-medallion-migration-intake`, `oci-medallion-bootstrap` y `oci-medallion-scaffold`.

## Contratos clave

- Manifiesto del proyecto: `project.medallion.yaml`
- Raiz de insumos: `workspace/migration-input/<project_id>/`
- Espejo local OCI: `workspace/oci-mirror/<env>/compartment-data-medallion-<env>/<service>/`
- Configuracion sensible: `.local/...`

## Documentacion recomendada

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [docs/onboarding.md](docs/onboarding.md)
- [docs/local-zones.md](docs/local-zones.md)
- [docs/project-contract.md](docs/project-contract.md)
- [docs/oci-mirror.md](docs/oci-mirror.md)
- [docs/mcp-and-skills.md](docs/mcp-and-skills.md)
- [docs/local-mcp-demo.md](docs/local-mcp-demo.md)
- [docs/github-publish.md](docs/github-publish.md)

## Alcance del factory

El repo prepara la base para:

- crear foundation OCI por ambiente
- intake de insumos de migracion
- scaffold de proyectos medallion
- publicacion de artefactos a Data Flow, Data Integration y Autonomous Database
- validacion de contratos y estructura local

No incluye secretos reales, wallets reales, OCIDs reales ni reportes con identificadores sensibles.
