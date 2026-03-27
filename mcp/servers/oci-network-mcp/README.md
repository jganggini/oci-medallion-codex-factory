# oci-network-mcp

MCP para crear VCN, subredes privadas, NSGs, service gateways y tablas de ruta requeridas por el ambiente medallion.

## Capacidades

- `create-vcn`
- `create-subnet`
- `create-nsg`
- `create-route-table`
- `create-service-gateway`
- `update-route-table`
- `export-network-manifest`

## Ejemplos

- crear route table vacia:
  `py -3 mcp/servers/oci-network-mcp/server.py --environment dev --runtime oci --oci-mode apply --command create-route-table --compartment-id ocid1.compartment... --vcn-id ocid1.vcn... --route-table-name rt-data-medallion-dev`
- crear service gateway para Object Storage:
  `py -3 mcp/servers/oci-network-mcp/server.py --environment dev --runtime oci --oci-mode apply --command create-service-gateway --compartment-id ocid1.compartment... --vcn-id ocid1.vcn... --service-gateway-name sgw-data-medallion-dev --service-id ocid1.service.oc1.us-chicago-1...`
- actualizar route table con una regla a Object Storage:
  `py -3 mcp/servers/oci-network-mcp/server.py --environment dev --runtime oci --oci-mode apply --command update-route-table --route-table-id ocid1.routetable... --route-table-name rt-data-medallion-dev --route-rule-json '{"destination":"oci-ord-objectstorage","destinationType":"SERVICE_CIDR_BLOCK","networkEntityId":"ocid1.servicegateway..."}'`
