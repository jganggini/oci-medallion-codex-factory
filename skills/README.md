# Skills

Skills de Codex para operar el factory medallion en OCI.

## Skills incluidas

- `oci-medallion-advisor`
- `oci-medallion-bootstrap`
- `oci-medallion-migration-intake`
- `oci-medallion-network-foundation`
- `oci-medallion-scaffold`
- `oci-medallion-publish`
- `oci-medallion-qa`
- `oci-terraform-fallback`
- `oci-medallion-validate`
- `oci-medallion-incident`

## Secuencia recomendada

1. advisor
2. `docker compose up -d` despues de discovery y del plan inicial
3. intake
4. bootstrap
5. network foundation
6. scaffold
7. publish
8. qa
9. terraform fallback cuando un recurso OCI no este claro, exista drift o falle el despliegue
10. validate
11. incident solo cuando haya fallos operativos
