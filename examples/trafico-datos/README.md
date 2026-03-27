# trafico-datos

Ejemplo de manifiesto para un caso de migracion orientado a trafico de datos.

Usa este ejemplo como referencia para:

- definir `migration_input_root`
- activar jobs Data Flow por capa
- declarar estrategia de carga a Autonomous
- conectar el proyecto con un workspace de Data Integration
- revisar un ejemplo real de carga `gold_refined -> gold_adb` en `examples/trafico-datos/sql/040_create_load_agg_resumen_archivos_trafico.sql`
- reutilizar el merge SQL para `DBMS_CLOUD.COPY_DATA` en `examples/trafico-datos/sql/030_merge_agg_resumen_archivos_trafico.sql`
