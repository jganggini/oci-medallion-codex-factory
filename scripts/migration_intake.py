from __future__ import annotations

import argparse
import json
from pathlib import Path


REQUIRED_DIRS = ("sql", "docs", "ddl", "samples", "exports", "mappings", "notes")
OPTIONAL_DIRS = ("scripts", "data", "references", "quality/contracts", "quality/sql", "lineage", "source-assets")
INTERESTING_SUFFIXES = {
    "sql": {".sql", ".pls", ".pks", ".pkb"},
    "docs": {".doc", ".docx", ".pdf", ".md", ".txt"},
    "ddl": {".sql", ".ddl", ".txt"},
    "samples": {".csv", ".txt", ".dat", ".json", ".parquet"},
    "exports": {".csv", ".xlsx", ".txt", ".json"},
    "mappings": {".csv", ".xlsx", ".json", ".yaml", ".yml"},
    "notes": {".md", ".txt", ".docx"},
    "scripts": {".sql", ".py", ".ps1", ".sh", ".bash", ".bat", ".cmd", ".ksh", ".yaml", ".yml", ".json"},
    "data": {".csv", ".txt", ".dat", ".json", ".parquet", ".avro", ".orc", ".xlsx"},
    "references": {".pdf", ".doc", ".docx", ".md", ".txt", ".xlsx", ".ppt", ".pptx", ".csv"},
    "quality/contracts": {".json"},
    "quality/sql": {".sql", ".txt"},
    "lineage": {".json", ".yaml", ".yml", ".sql", ".md"},
    "source-assets": {".json", ".yaml", ".yml", ".csv", ".md"},
}
IMPORTANT_KEYWORDS = {
    "sql": ("agg", "proc", "package", "trafico", "resumen", "etl"),
    "docs": ("descripcion", "regla", "diccionario", "layout", "script", "analisis"),
    "samples": ("sample", "muestra", "raw", "input"),
    "exports": ("export", "output", "gold", "esperado"),
    "scripts": ("etl", "batch", "load", "spark", "flow", "job", "wrapper", "run"),
    "data": ("data", "input", "source", "landing", "raw", "snapshot", "delta"),
    "references": ("manual", "layout", "diccionario", "regla", "referencia", "lineage", "mapping"),
    "quality/contracts": ("contract", "quality", "qa", "gate"),
    "quality/sql": ("qa", "quality", "reconcile", "count", "hash"),
    "lineage": ("lineage", "openlineage", "catalog"),
    "source-assets": ("bucket", "asset", "source", "landing"),
}


def collect_files(folder: Path, allowed_suffixes: set[str]) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for item in sorted(folder.rglob("*")):
        if not item.is_file():
            continue
        if item.name.startswith("."):
            continue
        if item.suffix.lower() not in allowed_suffixes:
            continue
        results.append(
            {
                "name": item.name,
                "relative_path": str(item.relative_to(folder.parent)).replace("\\", "/"),
                "size_bytes": item.stat().st_size,
            }
        )
    return results


def detect_candidates(section: str, files: list[dict[str, object]]) -> list[str]:
    keywords = IMPORTANT_KEYWORDS.get(section, ())
    candidates: list[str] = []
    for item in files:
        name = str(item["name"]).lower()
        if any(keyword in name for keyword in keywords):
            candidates.append(str(item["relative_path"]))
    return candidates[:10]


def build_inventory(repo_root: Path, project_root: Path) -> dict[str, object]:
    missing = [name for name in REQUIRED_DIRS if not (project_root / name).exists()]
    sections: dict[str, list[dict[str, object]]] = {}
    candidates: dict[str, list[str]] = {}
    optional_sections: dict[str, list[dict[str, object]]] = {}
    optional_candidates: dict[str, list[str]] = {}

    for name in REQUIRED_DIRS:
        folder = project_root / name
        sections[name] = collect_files(folder, INTERESTING_SUFFIXES[name]) if folder.exists() else []
        candidates[name] = detect_candidates(name, sections[name])

    for name in OPTIONAL_DIRS:
        folder = project_root / name
        optional_sections[name] = collect_files(folder, INTERESTING_SUFFIXES[name]) if folder.exists() else []
        optional_candidates[name] = detect_candidates(name, optional_sections[name])

    blockers: list[str] = []
    warnings: list[str] = []

    if not sections["sql"]:
        blockers.append("No se encontraron archivos SQL heredados en sql/.")
    if not sections["docs"]:
        blockers.append("No se encontraron documentos funcionales en docs/.")
    if not sections["samples"] and not sections["exports"] and not optional_sections["data"]:
        blockers.append("No se encontraron archivos de datos en data/, muestras en samples/ ni exports de referencia en exports/.")
    if not sections["ddl"]:
        warnings.append("No se encontraron DDL en ddl/.")
    if not sections["mappings"]:
        warnings.append("No se encontraron mappings en mappings/.")
    if not optional_sections["scripts"]:
        warnings.append("No se encontraron scripts heredados o wrappers tecnicos en scripts/.")
    if not optional_sections["references"]:
        warnings.append("No se encontraron documentos de referencia en references/.")

    next_steps = [
        "Revisar inventory.md y completar los faltantes.",
        "Asegurar que project.medallion.yaml referencie migration_input_root correctamente.",
    ]
    if not optional_sections["scripts"]:
        next_steps.append("Agregar scripts heredados, wrappers o utilitarios en scripts/ si forman parte de la migracion.")
    if not optional_sections["references"]:
        next_steps.append("Agregar documentacion de referencia en references/ si existen layouts, manuales o reglas funcionales adicionales.")
    if not optional_sections["quality/contracts"]:
        next_steps.append("Definir contratos de QA si el proyecto requiere gate de migracion o reconciliacion por slice.")
    if not optional_sections["source-assets"]:
        next_steps.append("Documentar source assets o buckets existentes si la ingesta llega por fuera del factory.")
    if blockers:
        next_steps.append("No iniciar scaffold hasta resolver blockers.")
    else:
        next_steps.append("El proyecto esta listo para bootstrap y scaffold.")

    return {
        "project_id": project_root.name,
        "project_root": str(project_root),
        "missing_directories": missing,
        "sections": sections,
        "candidate_files": candidates,
        "optional_sections": optional_sections,
        "optional_candidate_files": optional_candidates,
        "blockers": blockers,
        "warnings": warnings,
        "ready_for_scaffold": len(blockers) == 0 and len(missing) == 0,
        "next_steps": next_steps,
    }


def render_markdown(inventory: dict[str, object]) -> str:
    lines = [
        f"# Inventory - {inventory['project_id']}",
        "",
        f"Ready for scaffold: `{inventory['ready_for_scaffold']}`",
        "",
    ]

    missing = inventory["missing_directories"]
    if missing:
        lines.append("## Missing directories")
        for item in missing:
            lines.append(f"- `{item}/`")
        lines.append("")

    blockers = inventory["blockers"]
    if blockers:
        lines.append("## Blockers")
        for item in blockers:
            lines.append(f"- {item}")
        lines.append("")

    warnings = inventory["warnings"]
    if warnings:
        lines.append("## Warnings")
        for item in warnings:
            lines.append(f"- {item}")
        lines.append("")

    lines.append("## Sections")
    for section, files in inventory["sections"].items():
        lines.append(f"### {section}")
        if not files:
            lines.append("- No files detected")
        else:
            for item in files:
                lines.append(f"- `{item['relative_path']}` ({item['size_bytes']} bytes)")
        if inventory["candidate_files"].get(section):
            lines.append("- Candidate files:")
            for item in inventory["candidate_files"][section]:
                lines.append(f"  - `{item}`")
        lines.append("")

    lines.append("## Optional sections")
    for section, files in inventory["optional_sections"].items():
        lines.append(f"### {section}")
        if not files:
            lines.append("- No files detected")
        else:
            for item in files:
                lines.append(f"- `{item['relative_path']}` ({item['size_bytes']} bytes)")
        if inventory["optional_candidate_files"].get(section):
            lines.append("- Candidate files:")
            for item in inventory["optional_candidate_files"][section]:
                lines.append(f"  - `{item}`")
        lines.append("")

    lines.append("## Next steps")
    for step in inventory["next_steps"]:
        lines.append(f"- {step}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Indexa y valida los insumos de migracion.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--project-id", required=True)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    project_root = repo_root / "workspace" / "migration-input" / args.project_id
    if not project_root.exists():
        raise FileNotFoundError(f"No existe la ruta de proyecto: {project_root}")

    inventory = build_inventory(repo_root, project_root)
    inventory_dir = project_root / "_inventory"
    inventory_dir.mkdir(parents=True, exist_ok=True)

    (inventory_dir / "inventory.json").write_text(
        json.dumps(inventory, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    (inventory_dir / "inventory.md").write_text(render_markdown(inventory), encoding="utf-8")
    (inventory_dir / "context.json").write_text(
        json.dumps(
            {
                "project_id": inventory["project_id"],
                "migration_input_root": str(project_root),
                "ready_for_scaffold": inventory["ready_for_scaffold"],
                "candidate_files": inventory["candidate_files"],
                "optional_candidate_files": inventory["optional_candidate_files"],
                "next_steps": inventory["next_steps"],
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "project_id": args.project_id,
                "ready_for_scaffold": inventory["ready_for_scaffold"],
                "inventory_dir": str(inventory_dir),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
