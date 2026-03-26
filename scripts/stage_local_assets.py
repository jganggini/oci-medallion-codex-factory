from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


SECTION_DESTINATIONS = {
    "sql": "sql",
    "scripts": "scripts",
    "data": "data",
    "docs": "docs",
    "references": "references",
    "ddl": "ddl",
    "samples": "samples",
    "exports": "exports",
    "mappings": "mappings",
    "notes": "notes",
}

SECTION_ARGS = (
    ("sql", "SQL heredado y scripts SQL"),
    ("scripts", "scripts heredados y wrappers"),
    ("data", "data fuente, CSV, Parquet y archivos base"),
    ("docs", "documentacion funcional"),
    ("references", "documentacion de referencia"),
    ("ddl", "DDL y diccionario de datos"),
    ("samples", "muestras de datos"),
    ("exports", "exports o salidas esperadas"),
    ("mappings", "mapeos y cruces campo a campo"),
    ("notes", "notas y aclaraciones"),
)


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def copy_file(source: Path, target: Path, replace_existing: bool) -> dict[str, object]:
    ensure_directory(target.parent)
    if target.exists() and not replace_existing:
        return {
            "source": str(source),
            "target": str(target),
            "action": "skipped_existing",
        }
    shutil.copy2(source, target)
    return {
        "source": str(source),
        "target": str(target),
        "action": "copied",
    }


def copy_into_directory(source: Path, target_dir: Path, replace_existing: bool) -> list[dict[str, object]]:
    ensure_directory(target_dir)
    results: list[dict[str, object]] = []

    if source.is_dir():
        for item in sorted(source.rglob("*")):
            if not item.is_file():
                continue
            relative_path = item.relative_to(source)
            results.append(copy_file(item, target_dir / relative_path, replace_existing))
        return results

    results.append(copy_file(source, target_dir / source.name, replace_existing))
    return results


def copy_to_exact_path(source: Path, target: Path, replace_existing: bool) -> list[dict[str, object]]:
    if source.is_dir():
        raise IsADirectoryError(f"Se esperaba un archivo y se recibio un directorio: {source}")
    return [copy_file(source, target, replace_existing)]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Copia insumos y credenciales locales a la ruta canonica del factory.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--environment", default="dev")
    parser.add_argument("--adb-name", default="")
    parser.add_argument("--oci-config-source")
    parser.add_argument("--oci-key-source")
    parser.add_argument("--wallet-source")
    parser.add_argument("--replace-existing", action="store_true")

    for section, description in SECTION_ARGS:
        parser.add_argument(
            f"--{section}-source",
            action="append",
            default=[],
            metavar="PATH",
            help=f"Ruta fuente para {description}. Se puede repetir.",
        )

    return parser


def stage_section_sources(
    repo_root: Path,
    project_id: str,
    args: argparse.Namespace,
    replace_existing: bool,
) -> tuple[dict[str, list[dict[str, object]]], list[str]]:
    project_root = repo_root / "workspace" / "migration-input" / project_id
    staged: dict[str, list[dict[str, object]]] = {}
    errors: list[str] = []

    for section, _description in SECTION_ARGS:
        source_values = getattr(args, f"{section}_source", [])
        if not source_values:
            continue

        destination = project_root / SECTION_DESTINATIONS[section]
        section_results: list[dict[str, object]] = []

        for raw_source in source_values:
            source = Path(raw_source).expanduser().resolve()
            if not source.exists():
                errors.append(f"No existe la ruta fuente para {section}: {source}")
                continue
            section_results.extend(copy_into_directory(source, destination, replace_existing))

        if section_results:
            staged[section] = section_results

    return staged, errors


def stage_local_credentials(
    repo_root: Path,
    args: argparse.Namespace,
    replace_existing: bool,
) -> tuple[dict[str, list[dict[str, object]]], list[str]]:
    staged: dict[str, list[dict[str, object]]] = {}
    errors: list[str] = []

    if args.oci_config_source:
        source = Path(args.oci_config_source).expanduser().resolve()
        if not source.exists():
            errors.append(f"No existe la ruta fuente del OCI config: {source}")
        else:
            staged["oci_config"] = copy_to_exact_path(source, repo_root / ".local" / "oci" / "config", replace_existing)

    if args.oci_key_source:
        source = Path(args.oci_key_source).expanduser().resolve()
        if not source.exists():
            errors.append(f"No existe la ruta fuente de la llave .pem: {source}")
        else:
            staged["oci_key"] = copy_to_exact_path(source, repo_root / ".local" / "oci" / "key.pem", replace_existing)

    if args.wallet_source:
        if not args.adb_name:
            errors.append("Debes indicar --adb-name cuando uses --wallet-source.")
        else:
            source = Path(args.wallet_source).expanduser().resolve()
            if not source.exists():
                errors.append(f"No existe la ruta fuente del wallet: {source}")
            else:
                wallet_target = repo_root / ".local" / "autonomous" / "wallets" / args.environment / args.adb_name
                staged["wallet"] = copy_into_directory(source, wallet_target, replace_existing)

    return staged, errors


def write_report(report_path: Path, payload: dict[str, object]) -> None:
    ensure_directory(report_path.parent)
    report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    project_root = repo_root / "workspace" / "migration-input" / args.project_id
    ensure_directory(project_root)
    ensure_directory(project_root / "_inventory")

    staged_inputs, input_errors = stage_section_sources(repo_root, args.project_id, args, args.replace_existing)
    staged_credentials, credential_errors = stage_local_credentials(repo_root, args, args.replace_existing)

    errors = input_errors + credential_errors
    report = {
        "project_id": args.project_id,
        "environment": args.environment,
        "adb_name": args.adb_name,
        "replace_existing": args.replace_existing,
        "staged_inputs": staged_inputs,
        "staged_credentials": staged_credentials,
        "errors": errors,
        "status": "failed" if errors else "ok",
    }

    report_path = project_root / "_inventory" / "stage-report.json"
    write_report(report_path, report)

    print(
        json.dumps(
            {
                "status": report["status"],
                "report_path": str(report_path),
                "errors": errors,
            }
        )
    )

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
