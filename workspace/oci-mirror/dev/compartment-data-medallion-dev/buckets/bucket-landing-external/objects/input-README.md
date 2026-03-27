# oci-medallion-codex-factory

Base reusable para construir proyectos medallion en OCI con Codex, MCPs por servicio, Terraform compatible con OCI Resource Manager, control plane operacional en Autonomous Database y un espejo local de OCI alineado con ambientes, compartments y servicios.

## Objetivo

Este repo permite que un equipo tome SQL heredado, documentos funcionales, DDL, muestras de datos y exports historicos para generar un proyecto medallion en OCI sin improvisar rutas, carpetas, contratos ni mecanismos de control de ejecucion, lineage y reproceso.

## Runtime Docker-first

- El flujo oficial es `clone-first + Docker-first`: primero clona y abre este repo localmente, luego usa el asesor o las skills dentro del repo.
- `setup-dev.ps1`, `setup-dev.sh`, `scripts/docker_stage_assets.ps1`, `scripts/docker_stage_assets.sh`, `scripts/docker_repo_python.ps1` y `scripts/docker_repo_python.sh` son las entradas recomendadas.
- No se requiere Python ni OCI CLI instalados en el host para el flujo normal.
- El OCI CLI real se ejecuta siempre por Docker desde `oci-runner`, tanto para simulacion local como para `oci plan` o `oci apply`.

## Zonas locales

- `workspace/migration-input/`
  Zona canonica de insumos de migracion versionables y compartibles con el equipo.
- `workspace/oci-mirror/`
  Espejo local del estado publicado en OCI por ambiente y servicio.
- `.local/`
  Configuracion sensible y artefactos no versionados, incluyendo perfiles OCI, wallets y secretos locales.

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
  Imagenes base para runtime local, OCI CLI y Data Flow local.
- `workspace/`
  Insumos, espejo OCI y proyectos generados.
- `scripts/`
  Setup local, intake y validaciones del factory.

## Primer uso

Prerequisito: Docker Desktop o Docker Engine con `docker compose` instalado y corriendo.

1. Clona el repo y abre esa carpeta local en Codex App, Cursor o VS Code.
2. Ejecuta `setup-dev.ps1 -ProjectId <project_id>` o `./setup-dev.sh <project_id>`.
3. Si el editor ya estaba abierto, recarga el proyecto para que tome `.codex/config.toml` y los MCP locales del factory.
4. Empieza el flujo guiado con `docs/codex-advisor.md` o con el prompt recomendado de este README.
5. Cuando el asesor te pida SQL, scripts, data, docs, `config`, `.pem` o wallet, ejecuta `scripts/docker_stage_assets.ps1` o `scripts/docker_stage_assets.sh` con las rutas reales. No necesitas levantar Docker ni correr intake manualmente en el primer uso salvo depuracion.

Si `/mcp` muestra `No MCP servers configured`, primero verifica que abriste exactamente este repo y no una carpeta padre o auxiliar. En este proyecto la ruta correcta debe contener `.git/`, `mcp/`, `skills/`, `workspace/` y `.codex/config.toml`. Luego recarga Codex App para que vuelva a leer la configuracion local.

## Prompt recomendado para clientes

Usa este prompt una vez que el repo ya este clonado y abierto localmente. No es buena practica empezar desde un workspace vacio con solo el link, porque ahi Codex no tendra el repo inspeccionado ni podra apoyarse bien en los MCPs, skills y manifests locales.

Si quieres usar este repo como asesor guiado, pidele a Codex algo como esto:

```text
Ya tengo clonado y abierto localmente este proyecto:
https://github.com/jganggini/oci-medallion-codex-factory

Actua como asesor guiado de migracion y despliegue para una arquitectura medallion en OCI.

Trabaja sobre el repo local que ya tengo abierto. Si detectas que no estoy dentro de este repo, detenme y dime que primero debo clonarlo y abrirlo localmente.

Trabaja asi:
1. inspecciona el repo y detecta la etapa actual
2. hazme preguntas una por una
3. si falta un archivo, dime exactamente en que ruta debe ir y que contenido minimo esperas
4. pregunta explicitamente por SQL, scripts heredados, data o csv/parquet y documentacion de referencia
5. pide tambien la ruta exacta del OCI config, de la llave .pem y del wallet si aplica
6. si te digo que luego te pasare archivos, exigeme la ruta exacta donde estan hoy y la ruta destino dentro de workspace/migration-input/<project_id>/ o .local/oci/ o .local/autonomous/wallets/<env>/<adb_name>/
7. despues de cerrar el plan inicial, ejecuta el staging automatico para copiar los archivos a su ruta correcta antes del intake
8. asume por defecto un despliegue end-to-end hasta Autonomous Database, con entrega final en gold_adb
9. solo pregunta por un alcance parcial si yo lo pido de forma explicita
10. pregunta si ya existe algun bucket o source asset con informacion, a que capa pertenece y si la carga se hara por fuera de este flujo
11. no asumas que un bucket con datos significa que ya existen todas las capas landing, bronze, silver, refined o gold
12. no asumas credenciales, wallets, OCIDs ni tfvars
13. antes de ejecutar cambios, resume el plan por etapas
14. cuando cierres las preguntas, el plan inicial y el staging, levanta Docker con docker compose up -d dev-base oci-runner dataflow-local antes de intake, bootstrap o publish
15. ejecuta siempre los scripts del repo, los MCPs y el OCI CLI usando Docker; no dependas de Python ni OCI CLI instalados en host
16. guiame hasta dejar el proyecto listo para desplegar, migrar, validar y reprocesar por slice
```

Con ese prompt, Codex deberia ayudarte a:

- identificar en que etapa del despliegue o migracion estas
- decirte donde colocar los archivos del proyecto
- pedirte SQL, scripts, data y documentacion de referencia como parte normal de la entrevista
- pedirte la ruta actual de `config`, `.pem` y wallet cuando hagan falta
- exigir la ruta actual y la ruta destino cuando prometas entregar archivos despues
- ejecutar el staging automatico para mover los archivos a su ubicacion correcta antes del intake
- hacer el staging y el intake usando los wrappers Docker del repo
- asumir que la ruta normal es `landing_external -> bronze_raw -> silver_trusted -> gold_refined -> gold_adb`
- preguntar si ya existe algun bucket o asset con informacion y si corresponde a `landing_external`, `bronze_raw`, `silver_trusted`, `gold_refined` o `gold_adb`
- no preguntarte si quieres un despliegue parcial o total salvo que tu mismo limites el alcance
- no asumir que tener un bucket con datos implica que toda la arquitectura medallion ya esta creada
- levantar Docker temprano, apenas termine discovery, el plan inicial y el staging
- ejecutar OCI CLI siempre por Docker
- decidir si corresponde intake, scaffold, publish, lineage, QA o validacion

## Staging automatico

Usa `scripts/docker_stage_assets.ps1` o `scripts/docker_stage_assets.sh` cuando los archivos todavia esten fuera del repo. El wrapper corre `stage_local_assets.py` dentro de `oci-runner`, copia automaticamente hacia la ruta canonica del proyecto y genera un reporte local en `workspace/migration-input/<project_id>/_inventory/stage-report.json`.

Ejemplo:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\docker_stage_assets.ps1 `
  --project-id trafico-datos `
  --sql-source D:\fuentes\trafico\sql `
  --scripts-source D:\fuentes\trafico\scripts `
  --data-source D:\fuentes\trafico\data `
  --docs-source D:\fuentes\trafico\docs `
  --references-source D:\fuentes\trafico\references `
  --oci-config-source C:\Users\usuario\.oci\config `
  --oci-key-source C:\Users\usuario\.oci\oci_api_key.pem `
  --wallet-source D:\wallets\adb_trafico_gold `
  --environment dev `
  --adb-name adb_trafico_gold
```

## Ejecucion de scripts y MCPs

- Para correr scripts del repo usa `scripts/docker_repo_python.ps1` o `scripts/docker_repo_python.sh`.
- Para intake, demos y runtimes locales evita `python ...` directo en host.
- Cuando un MCP entra en `runtime oci`, el OCI CLI se invoca desde Docker y reutiliza `.local/oci/` y los wallets stageados.

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
