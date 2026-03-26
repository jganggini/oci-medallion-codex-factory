# MCP

Esta carpeta contiene el catalogo y los manifests base de los MCPs del factory.

## Principios

- cada MCP representa un servicio OCI o una funcion central del proceso
- cada MCP debe poder actualizar `workspace/oci-mirror/`
- las salidas compartibles deben estar redacted
- la ejecucion real puede usar OCI CLI, SDK o ambos, pero el contrato del espejo se mantiene estable
- los MCPs de QA deben dejar evidencias comparables en `quality/` para gate de migracion

## Servidores

Revisa `mcp/catalog/services.yaml` y `mcp/servers/<server>/server.manifest.yaml`.
