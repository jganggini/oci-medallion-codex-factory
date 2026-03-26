# Templates

Este directorio contiene plantillas reutilizables para nuevos proyectos medallion:

- `project.medallion.yaml`
  Manifiesto base del proyecto.
- `autonomous/control_plane_bootstrap.sql`
  Bootstrap base del schema `MDL_CTL`.
- `data_flow/minimal_app/`
  App minima para Data Flow.
- `data_flow/dependency_package/`
  Estructura base para generar el `archive.zip` oficial de OCI Data Flow.

Las plantillas deben permanecer genericas y no contener datos sensibles ni nombres de cliente.
