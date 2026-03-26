# Local Zones

## Resumen

El repo separa tres zonas locales con responsabilidades diferentes.

## `workspace/migration-input/`

Es la zona canonica para que el usuario cargue insumos de migracion.

Estructura:

- `workspace/migration-input/<project_id>/sql/`
- `workspace/migration-input/<project_id>/docs/`
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

Zona no versionada para material sensible y configuracion local.

Estructura sugerida:

- `.local/oci/config`
- `.local/oci/key.pem`
- `.local/oci/profiles/<env>/`
- `.local/autonomous/wallets/<env>/<adb_name>/`
- `.local/secrets/project.<env>.env`
- `.local/migration-private/<project_id>/...`

`migration-private/` existe para casos donde parte del material fuente no debe entrar al workspace versionado. El intake oficial sigue trabajando sobre `workspace/migration-input/<project_id>/`.
