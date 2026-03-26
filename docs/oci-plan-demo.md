# OCI Plan Demo

## Objetivo

Generar planes OCI reales con OCI CLI sin crear recursos, dejando evidencia en el espejo local.

## Comando

`py -3 scripts/run_oci_plan_demo.py --repo-root . --environment dev`

## Resultado esperado

Se generan archivos en:

- `workspace/oci-mirror/dev/.../buckets/oci-plans/`
- `workspace/oci-mirror/dev/.../data_flow/oci-plans/`
- `workspace/oci-mirror/dev/.../data_integration/oci-plans/`
- `workspace/oci-mirror/dev/.../autonomous_database/oci-plans/`
- `workspace/oci-mirror/dev/.../reports/`

## Nota

`plan` no crea recursos en OCI. `apply` si intenta ejecutarlos usando OCI CLI y la configuracion de `.local/oci/`.
