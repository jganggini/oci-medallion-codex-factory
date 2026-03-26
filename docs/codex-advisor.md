# Codex Advisor

## Objetivo

Usa este repo para que Codex actue como un asesor guiado de migracion y despliegue de una arquitectura medallion en OCI.

La idea no es solo ejecutar scripts. La idea es que Codex:

- descubra en que etapa esta el proyecto
- te diga que insumos faltan
- te indique exactamente donde colocar cada archivo
- te lleve paso a paso por intake, bootstrap, publicacion, QA y validacion

## Como pedirselo a Codex

Usa un prompt como este al inicio:

```text
Quiero implementar este proyecto:
https://github.com/jganggini/oci-medallion-codex-factory

Actua como asesor guiado de migracion y despliegue para una arquitectura medallion en OCI.

Trabaja asi:
1. inspecciona el repo y detecta la etapa actual
2. hazme preguntas una por una
3. si falta un archivo, dime exactamente en que ruta debe ir y que contenido minimo esperas
4. pregunta si ya existe algun bucket con informacion, a que capa pertenece y si la carga se hara aparte
5. no asumas credenciales, wallets, OCIDs ni tfvars
6. antes de ejecutar cambios, resume el plan por etapas
7. guiame hasta dejar el proyecto listo para desplegar y migrar
```

## Modo recomendado

- si tu entorno tiene modo plan o tarea, usalo al inicio para discovery, entrevista y plan por etapas
- usa modo agente o ejecucion cuando ya validaste el plan y quieres que Codex cree archivos, ajuste el repo o corra scripts
- si solo cuentas con modo agente, funciona igual si en el prompt le pides explicitamente que primero inspeccione, pregunte una por una y no ejecute cambios todavia

## Respuesta esperada de Codex

Cuando el flujo funciona bien, Codex deberia responder en este orden:

1. etapa actual
2. primer dato faltante
3. ruta exacta donde debes colocar el insumo
4. siguiente accion que hara cuando confirmes

## Secuencia sugerida

1. `oci-medallion-advisor`
2. `oci-medallion-migration-intake`
3. `oci-medallion-bootstrap`
4. `oci-medallion-network-foundation`
5. `oci-medallion-scaffold`
6. `oci-medallion-publish`
7. `oci-medallion-qa`
8. `oci-terraform-fallback` si algun recurso OCI o Terraform no esta claro
9. `oci-medallion-validate`

## Cuando pedir plan y cuando pedir ejecucion

- pide guia o plan cuando aun no sabes que archivos faltan o donde van
- pide ejecucion cuando ya validaste el plan y quieres que Codex avance en el repo
- pide `oci apply` solo cuando ya confirmaste credenciales, ambiente y recursos objetivo
