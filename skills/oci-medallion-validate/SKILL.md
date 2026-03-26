# oci-medallion-validate

Usa esta skill para validar contratos, artefactos y espejo OCI.

## Flujo

1. ejecutar `py -3 scripts/validate_factory.py --repo-root .`
2. revisar el intake y el contexto generado
3. verificar que existan manifests en buckets, Data Flow, DI y ADB
4. confirmar que no existan secretos versionados
