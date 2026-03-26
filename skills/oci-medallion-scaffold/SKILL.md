# oci-medallion-scaffold

Usa esta skill para generar el esqueleto del proyecto medallion a partir del intake y del manifiesto.

## Flujo

1. ejecutar el intake
2. bloquear si `ready_for_scaffold` es `false`
3. preparar estructura de jobs Data Flow
4. preparar bootstrap SQL para Autonomous
5. preparar pipeline de Data Integration
