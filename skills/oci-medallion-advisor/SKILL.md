---
name: oci-medallion-advisor
description: Asesor guiado para desplegar o migrar proyectos sobre este factory medallion en OCI. Usa esta skill cuando el usuario pida implementar, desplegar, migrar, organizar insumos, saber donde colocar archivos, o quiera una guia paso a paso antes de ejecutar los MCPs o Terraform.
---

# oci-medallion-advisor

Usa esta skill para convertir una solicitud abierta en un plan operable, guiado y por etapas.

## Flujo

1. leer `AGENTS.md`, `README.md`, `docs/onboarding.md`, `docs/local-zones.md`, `docs/project-contract.md` y `skills/README.md`
2. identificar la etapa actual:
   - intake
   - bootstrap
   - network foundation
   - scaffold
   - publish
   - qa
   - validate
   - incident
3. entrevistar al usuario con una sola pregunta material por turno
4. cuando falte un insumo, indicar exactamente:
   - ruta
   - archivo o carpeta esperada
   - si es obligatorio u opcional
   - contenido minimo esperado
5. no pasar a `oci-mode apply` hasta confirmar credenciales locales, ambiente objetivo, region, OCIDs y wallets si aplican
6. derivar al siguiente skill segun la etapa:
   - `oci-medallion-migration-intake`
   - `oci-medallion-bootstrap`
   - `oci-medallion-network-foundation`
   - `oci-medallion-scaffold`
   - `oci-medallion-publish`
   - `oci-medallion-qa`
   - `oci-terraform-fallback`
   - `oci-medallion-validate`
   - `oci-medallion-incident`
7. cerrar cada etapa con:
   - que quedo listo
   - que falta
   - siguiente paso concreto

## Preguntas iniciales recomendadas

Hazlas de una en una y solo si son necesarias:

- cual es el `project_id`
- si el usuario ya tiene insumos en `workspace/migration-input/<project_id>/`
- si trabajara en `dev`, `qa` o `prod`
- si quiere solo simulacion local/plan o tambien despliegue real
- si ya cuenta con `.local/oci/config`, llaves y wallets

## Resultado esperado

- checklist claro de insumos
- rutas exactas para colocar archivos
- siguiente skill o script a ejecutar
- plan de despliegue o migracion por etapas
