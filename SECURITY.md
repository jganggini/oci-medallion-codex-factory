# Security

## Reglas

- No subir credenciales OCI al repositorio.
- No subir wallets reales.
- No subir archivos `.env` sensibles.
- No subir OCIDs reales de clientes o ambientes operativos.
- No subir reportes con evidencia sensible sin redacción.

## Ubicaciones permitidas para material sensible

- `.local/oci/`
- `.local/autonomous/wallets/`
- `.local/secrets/`

## Publicación

Antes de publicar el repo:

1. Ejecuta los checks de `.github/workflows/validate-template.yml`.
2. Confirma que `.local/` está ignorado.
3. Confirma que no existen `Wallet_*.zip`, `key.pem`, `config` OCI ni `.env` reales.
