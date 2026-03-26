# oci-medallion-scaffold

Usa esta skill para generar el esqueleto del proyecto medallion a partir del intake y del manifiesto.

Si el manifiesto no declara un alcance parcial explicito, scaffoldea la ruta completa `landing_external -> bronze_raw -> silver_trusted -> gold_refined -> gold_adb`.

## Flujo

1. ejecutar el intake
2. bloquear si `ready_for_scaffold` es `false`
3. preparar estructura de jobs Data Flow por capa, incluyendo el traspaso final hacia `gold_adb` si aplica
4. preparar bootstrap SQL para Autonomous y para el control plane `MDL_CTL`
5. preparar pipeline de Data Integration para el flujo end-to-end por defecto
6. preparar contratos de QA, source assets y estrategia de lineage si el proyecto los requiere
