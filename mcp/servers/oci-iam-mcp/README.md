# oci-iam-mcp

MCP para gestionar foundation IAM del ambiente y sincronizar manifests redacted al espejo OCI.

Soporta:

- `create_compartment`
- `create_group`
- `create_dynamic_group`
- `create_policy`
- `export_iam_manifest`

Ejemplos:

- crear group:
  `py -3 mcp/servers/oci-iam-mcp/server.py --environment dev --command create-group --group-name grp-medallion-operators-dev`
- crear dynamic group:
  `py -3 mcp/servers/oci-iam-mcp/server.py --environment dev --command create-dynamic-group --dynamic-group-name dg-adb-resource-principal-dev --matching-rule "ALL {resource.type = 'autonomousdatabase', resource.compartment.id = 'ocid1.compartment.oc1..replace-me'}"`
- crear policy:
  `py -3 mcp/servers/oci-iam-mcp/server.py --environment dev --command create-policy --policy-name plc-medallion-operators-dev --statement "Allow group grp-medallion-operators-dev to manage buckets in compartment data-medallion-dev"`
