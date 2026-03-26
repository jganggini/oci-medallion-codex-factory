# Local Zones

## Resumen

El repo separa dos zonas locales operativas y una zona de configuracion sensible.

## `workspace/migration-input/`

Es la zona canonica para que el usuario cargue insumos de migracion.

Estructura:

- `workspace/migration-input/<project_id>/sql/`
- `workspace/migration-input/<project_id>/scripts/`
- `workspace/migration-input/<project_id>/data/`
- `workspace/migration-input/<project_id>/docs/`
- `workspace/migration-input/<project_id>/references/`
- `workspace/migration-input/<project_id>/ddl/`
- `workspace/migration-input/<project_id>/samples/`
- `workspace/migration-input/<project_id>/exports/`
- `workspace/migration-input/<project_id>/mappings/`
- `workspace/migration-input/<project_id>/notes/`

Esta zona es la que leen Codex, las skills y el `migration-intake-mcp`.

## `workspace/oci-mirror/`

Espejo local del estado de OCI por ambiente y servicio.

Estructura:

- `workspace/oci-mirror/dev/`
- `workspace/oci-mirror/qa/`
- `workspace/oci-mirror/prod/`

Cada ambiente contiene el compartment espejo y carpetas por servicio.

## `.local/`

Zona no versionada solo para configuracion sensible y artefactos locales del entorno.

Estructura sugerida:

- `.local/oci/config`
- `.local/oci/key.pem`
- `.local/oci/profiles/<env>/`
- `.local/autonomous/wallets/<env>/<adb_name>/`
- `.local/secrets/project.<env>.env`

No debe usarse `.local/` como segunda ruta de insumos de migracion. El intake oficial y el asesor trabajan solo sobre `workspace/migration-input/<project_id>/`.

Si durante la entrevista el usuario dice que luego entregara archivos, el plan debe registrar siempre:

- ruta fuente exacta donde hoy estan los archivos
- ruta destino exacta dentro de `workspace/migration-input/<project_id>/...`
- tipo de insumo: `sql`, `scripts`, `data`, `references`, `samples` o `exports`
