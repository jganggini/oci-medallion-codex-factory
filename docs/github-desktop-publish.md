# GitHub Desktop Publish

## Objetivo

Publicar la copia del repo ubicada en `D:\Documents\GitHub\oci-medallion-codex-factory`.

## Opcion 1

Usar PowerShell:

`powershell -ExecutionPolicy Bypass -File .\scripts\publish_github.ps1 -RemoteUrl <URL_DEL_REPO>`

## Opcion 2

Usar GitHub Desktop:

1. Abrir `D:\Documents\GitHub\oci-medallion-codex-factory`
2. Revisar cambios
3. Hacer el primer commit en `main`
4. Publicar el repositorio en GitHub

## Verificaciones previas

- ejecutar `py -3 scripts/validate_factory.py --repo-root .`
- revisar que `.local/` no exista en Git
- revisar que `workspace/oci-mirror/` solo tenga contenido redacted
