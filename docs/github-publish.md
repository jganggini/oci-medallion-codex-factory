# GitHub Publish

## Objetivo

Publicar este repo como template repository reusable para proyectos medallion en OCI.

## Checklist previo

1. Ejecuta `setup-dev.ps1` o `setup-dev.sh`.
2. Ejecuta `powershell -ExecutionPolicy Bypass -File .\scripts\docker_repo_python.ps1 scripts/validate_factory.py --repo-root .` o `./scripts/docker_repo_python.sh scripts/validate_factory.py --repo-root .`.
3. Confirma que `.local/` no contiene archivos versionados.
4. Confirma que no existen `token_keys`, wallets reales, `.env` reales ni OCIDs reales.
5. Revisa `.github/workflows/validate-template.yml`.
6. Revisa `SECURITY.md` y `CONTRIBUTING.md`.
7. Verifica que `workspace/migration-input/` solo contiene placeholders o ejemplos anonimizados.
8. Verifica que `workspace/oci-mirror/` solo contiene manifests y reportes redacted.

## Publicacion sugerida

1. Inicializa Git dentro de `oci-medallion-codex-factory`.
2. Crea el repositorio en GitHub.
3. Sube la rama principal.
4. Activa "Template repository".
5. Protege la rama principal.
6. Habilita GitHub Actions.
7. Publica releases cuando cambien contratos o modulos base.

## Recomendaciones

- Mantener este repo agnostico y libre de datos del cliente.
- Publicar ejemplos anonimizados en `examples/`.
- Versionar cambios de contratos en `templates/`, `infra/`, `mcp/` y `skills/`.
