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

1. clonar y abrir el repo local
2. advisor
3. `docker compose up -d dev-base oci-runner dataflow-local` despues de discovery, del plan inicial y del staging
4. usar `scripts/docker_stage_assets.ps1` o `scripts/docker_stage_assets.sh` cuando los insumos aun esten fuera del repo
5. intake
6. bootstrap
7. network foundation
8. scaffold
9. publish
10. qa
11. terraform fallback cuando un recurso OCI no este claro, exista drift o falle el despliegue
12. validate
13. incident solo cuando haya fallos operativos
