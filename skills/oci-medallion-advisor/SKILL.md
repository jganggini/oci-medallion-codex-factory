---
name: oci-medallion-advisor
description: Asesor guiado para desplegar o migrar proyectos sobre este factory medallion en OCI. Usa esta skill cuando el usuario pida implementar, desplegar, migrar, organizar insumos, clasificar buckets o assets existentes, o quiera una guia paso a paso antes de ejecutar los MCPs o Terraform.
---

# oci-medallion-advisor

Usa esta skill para convertir una solicitud abierta en un plan operable, guiado y por etapas.

## Flujo

1. leer `AGENTS.md`, `README.md`, `docs/onboarding.md`, `docs/local-zones.md`, `docs/project-contract.md`, `docs/medallion-control-plane.md` y `skills/README.md`
2. identificar la etapa actual:
   - intake
   - bootstrap
   - network foundation
   - scaffold
   - publish
   - qa
   - validate
   - incident
3. asumir por defecto un despliegue end-to-end hasta `gold_adb` en Autonomous Database
4. no preguntar si el alcance es parcial o total salvo que el usuario ya haya restringido capas, servicios o entregables
5. entrevistar al usuario con una sola pregunta material por turno
6. cuando falte un insumo, indicar exactamente:
   - ruta
   - archivo o carpeta esperada
   - si es obligatorio u opcional
   - contenido minimo esperado
7. preguntar explicitamente si ya existe algun bucket o source asset con informacion, a que capa pertenece y si la carga de archivos se hara por fuera de este flujo
8. no asumir que un bucket poblado significa que ya existen todas las capas landing, bronze, silver, refined y gold
9. confirmar si el proyecto necesita control plane, Data Catalog, lineage hibrido y reproceso por `run+slice`
10. no pasar a `oci-mode apply` hasta confirmar credenciales locales, ambiente objetivo, region, OCIDs, private endpoints y wallets si aplican
11. derivar al siguiente skill segun la etapa:
   - `oci-medallion-migration-intake`
   - `oci-medallion-bootstrap`
   - `oci-medallion-network-foundation`
   - `oci-medallion-scaffold`
   - `oci-medallion-publish`
   - `oci-medallion-qa`
   - `oci-terraform-fallback`
   - `oci-medallion-validate`
   - `oci-medallion-incident`
12. cerrar cada etapa con:
   - que quedo listo
   - que falta
   - siguiente paso concreto

## Preguntas iniciales recomendadas

Hazlas de una en una y solo si son necesarias:

- cual es el `project_id`
- si el usuario ya tiene insumos en `workspace/migration-input/<project_id>/`
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
- siguiente skill o script a ejecutar
- plan de despliegue o migracion por etapas
- ruta base `landing_external -> bronze_raw -> silver_trusted -> gold_refined -> gold_adb` salvo restriccion explicita
- clasificacion clara de buckets existentes versus capas realmente creadas
- estrategia de QA y reproceso por slice
