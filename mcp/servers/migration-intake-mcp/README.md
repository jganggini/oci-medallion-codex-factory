# migration-intake-mcp

## Objetivo

Validar e indexar la zona `workspace/migration-input/<project_id>/` antes de generar el proyecto.

## Runtime base

Este scaffold incluye `server.py` como wrapper local sobre `scripts/migration_intake.py`.

## Tools esperadas

- `validate_input_structure`
- `inventory_sources`
- `summarize_readiness`
- `block_if_missing_required_inputs`
