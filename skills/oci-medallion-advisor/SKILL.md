---
name: oci-medallion-advisor
description: Asesor guiado para desplegar o migrar proyectos sobre este factory medallion en OCI. Usa esta skill cuando el usuario pida implementar, desplegar, migrar, organizar insumos, clasificar buckets o assets existentes, o quiera una guia paso a paso antes de ejecutar los MCPs o Terraform.
---

# oci-medallion-advisor

Usa esta skill para convertir una solicitud abierta en un plan operable, guiado y por etapas.

## Flujo

1. confirmar que el usuario ya clono y abrio este repo localmente; si no, pedir que primero lo clone y abra antes de seguir
2. leer `AGENTS.md`, `README.md`, `docs/onboarding.md`, `docs/local-zones.md`, `docs/project-contract.md`, `docs/medallion-control-plane.md` y `skills/README.md`
3. identificar la etapa actual:
   - intake
   - bootstrap
   - network foundation
   - scaffold
   - publish
   - qa
   - validate
   - incident
4. asumir por defecto un despliegue end-to-end hasta `gold_adb` en Autonomous Database
5. no preguntar si el alcance es parcial o total salvo que el usuario ya haya restringido capas, servicios o entregables
6. entrevistar al usuario con una sola pregunta material por turno
7. preguntar explicitamente por estos insumos si aun no estan claros:
   - SQL y DDL heredado
   - scripts heredados o wrappers operativos
   - data fuente, CSV, Parquet, muestras o exports
   - jars o dependencias de Data Flow cuando haya jobs Spark especiales, por ejemplo Iceberg para `silver_trusted`
   - documentacion funcional y documentacion de referencia
8. cuando falte un insumo, indicar exactamente:
   - ruta
   - archivo o carpeta esperada
   - si es obligatorio u opcional
   - contenido minimo esperado
9. si el usuario dice que luego entregara archivos, exigir siempre:
   - `source_path` exacto donde estan hoy
   - `target_path` exacto dentro de `workspace/migration-input/<project_id>/...`, `.local/oci/` o `.local/autonomous/wallets/<env>/<adb_name>/`
   - tipo de insumo pendiente
10. pedir explicitamente la ruta exacta de `config`, `.pem` y wallet cuando el entorno local aun no este listo
11. si existen rutas fuente para esos archivos fuera del repo, ejecutar `scripts/docker_stage_assets.ps1` o `scripts/docker_stage_assets.sh` antes de intake o bootstrap
12. preguntar explicitamente si ya existe algun bucket o source asset con informacion, a que capa pertenece y si la carga de archivos se hara por fuera de este flujo
13. no asumir que un bucket poblado significa que ya existen todas las capas landing, bronze, silver, refined y gold
14. confirmar si el proyecto necesita control plane, Data Catalog, lineage hibrido y reproceso por `run+slice`
15. despues de cerrar discovery, presentar el plan inicial, stagear los archivos locales y levantar `docker compose up -d dev-base oci-runner dataflow-local` antes de intake, bootstrap, scaffold o publish si el runtime local todavia no esta arriba
16. ejecutar scripts del repo y runtimes MCP con `scripts/docker_repo_python.ps1` o `scripts/docker_repo_python.sh`
17. no pasar a `oci-mode apply` hasta confirmar credenciales locales, ambiente objetivo, region, OCIDs, private endpoints y wallets si aplican
18. derivar al siguiente skill segun la etapa:
   - `oci-medallion-migration-intake`
   - `oci-medallion-bootstrap`
   - `oci-medallion-network-foundation`
   - `oci-medallion-scaffold`
   - `oci-medallion-publish`
   - `oci-medallion-qa`
   - `oci-terraform-fallback`
   - `oci-medallion-validate`
   - `oci-medallion-incident`
19. cerrar cada etapa con:
   - que quedo listo
   - que falta
   - siguiente paso concreto

## Preguntas iniciales recomendadas

Hazlas de una en una y solo si son necesarias:

- cual es el `project_id`
- si el usuario ya tiene insumos en `workspace/migration-input/<project_id>/`
- si ya tiene SQL, DDL y scripts heredados, y en que rutas exactas estan
- si ya tiene archivos de data, CSV, Parquet, samples o exports, y en que rutas exactas estan
- si ya tiene jars o dependencias de Data Flow para jobs Spark, especialmente `bronze-to-silver` o `silver-to-gold`, y en que rutas exactas estan
- si ya tiene documentacion funcional o documentacion de referencia, y en que rutas exactas estan
- si alguno de esos archivos se entregara despues, cual es el `source_path` actual y el `target_path` planeado
- si ya tiene `.local/oci/config`, `.local/oci/key.pem` y wallet o si hay que stagearlos desde otra ruta
- si trabajara en `dev`, `qa` o `prod`
- si ya existe algun bucket o source asset con datos
- a que capa corresponde cada bucket existente: `landing_external`, `bronze_raw`, `silver_trusted`, `gold_refined` o `gold_adb`
- si la carga de archivos al bucket se hara dentro del factory o por un proceso externo
- si el control plane `MDL_CTL` ya existe en ADB o debe bootstrapearse
- si se requiere publicar lineage en Data Catalog
- si quiere solo simulacion local/plan o tambien despliegue real
- si ya cuenta con `.local/oci/config`, llaves y wallets

## Resultado esperado

- checklist claro de insumos
- rutas exactas para colocar archivos
- rutas fuente y destino para cualquier insumo prometido pero aun no copiado
- `scripts/docker_stage_assets.ps1` o `scripts/docker_stage_assets.sh` ejecutado o programado antes del intake
- siguiente skill o script a ejecutar
- plan de despliegue o migracion por etapas
- `docker compose up -d dev-base oci-runner dataflow-local` programado o ejecutado inmediatamente despues del plan inicial
- ruta base `landing_external -> bronze_raw -> silver_trusted -> gold_refined -> gold_adb` salvo restriccion explicita
- clasificacion clara de buckets existentes versus capas realmente creadas
- estrategia de QA y reproceso por slice
