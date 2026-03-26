# oci-medallion-codex-factory

Base reusable para construir proyectos medallion en OCI con Codex, MCPs por servicio, Terraform compatible con OCI Resource Manager, control plane operacional en Autonomous Database y un espejo local de OCI alineado con ambientes, compartments y servicios.

## Objetivo

Este repo permite que un equipo tome SQL heredado, documentos funcionales, DDL, muestras de datos y exports historicos para generar un proyecto medallion en OCI sin improvisar rutas, carpetas, contratos ni mecanismos de control de ejecucion, lineage y reproceso.

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
  Catalogo, contratos y manifests base de los MCPs por servicio OCI.
- `skills/`
  Workflows de Codex para asesor guiado, bootstrap, intake, scaffold, publicacion, QA y validacion.
- `templates/`
  Plantillas base de manifiestos, SQL del control plane y esqueletos de proyecto.
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
3. Coloca tus insumos del proyecto en `workspace/migration-input/<project_id>/`, incluyendo SQL, scripts, data y documentacion de referencia cuando existan.
4. Si necesitas mantener copias privadas, usa `.local/migration-private/<project_id>/`.
5. Completa tu configuracion OCI local en `.local/oci/`.
6. Coloca wallets en `.local/autonomous/wallets/<env>/<adb_name>/`.
7. Levanta Docker con `docker compose up -d`.
8. Abre el repo con Codex App desde Cursor o VS Code.
9. Si quieres que Codex te guie paso a paso, empieza con `docs/codex-advisor.md` y la skill `oci-medallion-advisor`.
10. Ejecuta el intake con `python scripts/migration_intake.py --repo-root . --project-id <project_id>`.
11. Ajusta `project.medallion.yaml`.
12. Usa las skills `oci-medallion-advisor`, `oci-medallion-migration-intake`, `oci-medallion-bootstrap`, `oci-medallion-scaffold`, `oci-medallion-publish`, `oci-medallion-qa` y `oci-medallion-validate`.
13. Si Terraform o un recurso OCI no estan claros durante el despliegue, usa `oci-terraform-fallback` como referencia oficial antes de cambiar `infra/` o un MCP.

## Prompt recomendado para clientes

Si quieres usar este repo como asesor guiado, pidele a Codex algo como esto:

```text
Quiero implementar este proyecto:
https://github.com/jganggini/oci-medallion-codex-factory

Actua como asesor guiado de migracion y despliegue para una arquitectura medallion en OCI.

Trabaja asi:
1. inspecciona el repo y detecta la etapa actual
2. hazme preguntas una por una
3. si falta un archivo, dime exactamente en que ruta debe ir y que contenido minimo esperas
4. pregunta explicitamente por SQL, scripts heredados, data o csv/parquet y documentacion de referencia
5. si te digo que luego te pasare archivos, exigeme la ruta exacta donde estan hoy y la ruta destino dentro de workspace/migration-input/<project_id>/ o .local/migration-private/<project_id>/
6. asume por defecto un despliegue end-to-end hasta Autonomous Database, con entrega final en gold_adb
7. solo pregunta por un alcance parcial si yo lo pido de forma explicita
8. pregunta si ya existe algun bucket o source asset con informacion, a que capa pertenece y si la carga se hara por fuera de este flujo
9. no asumas que un bucket con datos significa que ya existen todas las capas landing, bronze, silver, refined o gold
10. no asumas credenciales, wallets, OCIDs ni tfvars
11. antes de ejecutar cambios, resume el plan por etapas
12. cuando cierres las preguntas y el plan inicial, levanta Docker con docker compose up -d antes de intake, bootstrap o publish
13. guiame hasta dejar el proyecto listo para desplegar, migrar, validar y reprocesar por slice
```

Con ese prompt, Codex deberia ayudarte a:

- identificar en que etapa del despliegue o migracion estas
- decirte donde colocar los archivos del proyecto
- pedirte SQL, scripts, data y documentacion de referencia como parte normal de la entrevista
- exigir la ruta actual y la ruta destino cuando prometas entregar archivos despues
- asumir que la ruta normal es `landing_external -> bronze_raw -> silver_trusted -> gold_refined -> gold_adb`
- preguntar si ya existe algun bucket o asset con informacion y si corresponde a `landing_external`, `bronze_raw`, `silver_trusted`, `gold_refined` o `gold_adb`
- no preguntarte si quieres un despliegue parcial o total salvo que tu mismo limites el alcance
- no asumir que tener un bucket con datos implica que toda la arquitectura medallion ya esta creada
- levantar Docker temprano, apenas termine discovery y el plan inicial
- decidir si corresponde intake, scaffold, publish, lineage, QA o validacion

## Contratos clave

- Manifiesto del proyecto: `project.medallion.yaml`
- Raiz de insumos: `workspace/migration-input/<project_id>/`
- Espejo local OCI: `workspace/oci-mirror/<env>/compartment-data-medallion-<env>/<service>/`
- Configuracion sensible: `.local/...`
- Control plane bootstrap: `templates/autonomous/control_plane_bootstrap.sql`

## Documentacion recomendada

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [docs/onboarding.md](docs/onboarding.md)
- [docs/codex-advisor.md](docs/codex-advisor.md)
- [docs/medallion-control-plane.md](docs/medallion-control-plane.md)
- [docs/local-zones.md](docs/local-zones.md)
- [docs/project-contract.md](docs/project-contract.md)
- [docs/oci-mirror.md](docs/oci-mirror.md)
- [docs/mcp-and-skills.md](docs/mcp-and-skills.md)
- [docs/local-mcp-demo.md](docs/local-mcp-demo.md)
- [docs/dataflow-local-runtime.md](docs/dataflow-local-runtime.md)
- [docs/dataflow-dependency-packager.md](docs/dataflow-dependency-packager.md)
- [docs/oci-plan-demo.md](docs/oci-plan-demo.md)
- [docs/github-publish.md](docs/github-publish.md)

## Alcance del factory

El repo prepara la base para:

- crear foundation OCI por ambiente
- intake de insumos de migracion
- scaffold de proyectos medallion
- publicacion de artefactos a Object Storage, Data Flow, Data Integration, Autonomous Database y Data Catalog
- control centralizado de runs, slices, checkpoints, reprocesos y QA
- publicacion de lineage nativo y custom
- validacion de contratos y estructura local

No incluye secretos reales, wallets reales, OCIDs reales ni reportes con identificadores sensibles.
