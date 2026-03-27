from __future__ import annotations

import argparse
import configparser
import json
import os
import shutil
from pathlib import Path, PurePath, PureWindowsPath


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

OCI_CONTAINER_DIR = "/mnt/oci"
OCI_CONFIG_ARTIFACT_OPTIONS = ("key_file", "security_token_file")


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def looks_like_windows_absolute_path(value: str) -> bool:
    return len(value) >= 3 and value[1] == ":" and value[0].isalpha() and value[2] in ("\\", "/")


def resolve_host_repo_root(repo_root: Path) -> Path | PurePath:
    raw_value = os.getenv("HOST_REPO_ROOT")
    if not raw_value:
        return repo_root.resolve()
    cleaned = raw_value.strip()
    if looks_like_windows_absolute_path(cleaned):
        return PureWindowsPath(cleaned)
    return Path(cleaned).expanduser().resolve()


def display_path(path: Path, repo_root: Path, host_repo_root: Path | PurePath) -> str:
    resolved = path.resolve()
    resolved_repo_root = repo_root.resolve()
    try:
        return str(host_repo_root / resolved.relative_to(resolved_repo_root))
    except ValueError:
        return str(resolved)


def copy_file(source: Path, target: Path, replace_existing: bool, repo_root: Path, host_repo_root: Path | PurePath) -> dict[str, object]:
    ensure_directory(target.parent)
    if target.exists() and not replace_existing:
        return {
            "source": display_path(source, repo_root, host_repo_root),
            "target": display_path(target, repo_root, host_repo_root),
            "action": "skipped_existing",
        }
    shutil.copy2(source, target)
    return {
        "source": display_path(source, repo_root, host_repo_root),
        "target": display_path(target, repo_root, host_repo_root),
        "action": "copied",
    }


def copy_into_directory(
    source: Path,
    target_dir: Path,
    replace_existing: bool,
    repo_root: Path,
    host_repo_root: Path | PurePath,
) -> list[dict[str, object]]:
    ensure_directory(target_dir)
    results: list[dict[str, object]] = []

    if source.is_dir():
        for item in sorted(source.rglob("*")):
            if not item.is_file():
                continue
            relative_path = item.relative_to(source)
            results.append(copy_file(item, target_dir / relative_path, replace_existing, repo_root, host_repo_root))
        return results

    results.append(copy_file(source, target_dir / source.name, replace_existing, repo_root, host_repo_root))
    return results


def copy_to_exact_path(
    source: Path,
    target: Path,
    replace_existing: bool,
    repo_root: Path,
    host_repo_root: Path | PurePath,
) -> list[dict[str, object]]:
    if source.is_dir():
        raise IsADirectoryError(f"Se esperaba un archivo y se recibio un directorio: {source}")
    return [copy_file(source, target, replace_existing, repo_root, host_repo_root)]


def resolve_config_artifact(raw_value: str, config_source: Path) -> Path | None:
    cleaned = os.path.expandvars(raw_value.strip().strip("'\""))
    if not cleaned:
        return None

    if looks_like_windows_absolute_path(cleaned):
        windows_candidate = PureWindowsPath(cleaned)
        fallback = (config_source.parent / windows_candidate.name).resolve()
        if fallback.exists():
            return fallback

    candidate = Path(cleaned).expanduser()
    if candidate.exists():
        return candidate.resolve()

    if not candidate.is_absolute():
        relative_candidate = (config_source.parent / candidate).resolve()
        if relative_candidate.exists():
            return relative_candidate

    fallback = (config_source.parent / candidate.name).resolve()
    if fallback.exists():
        return fallback
    return None


def update_config_option(parser: configparser.RawConfigParser, section: str, option: str, value: str) -> None:
    if section == parser.default_section:
        parser[parser.default_section][option] = value
        return
    parser.set(section, option, value)


def normalize_staged_oci_config(
    repo_root: Path,
    host_repo_root: Path | PurePath,
    replace_existing: bool,
    config_source: Path,
    config_target: Path,
    staged_key_path: Path | None,
) -> tuple[list[dict[str, object]], list[str]]:
    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read(config_target, encoding="utf-8")

    staged: list[dict[str, object]] = []
    errors: list[str] = []
    copied_artifacts: set[str] = set()
    sections = [parser.default_section, *parser.sections()]

    for section in sections:
        for option in OCI_CONFIG_ARTIFACT_OPTIONS:
            if section == parser.default_section:
                if option not in parser.defaults():
                    continue
                current_value = parser.defaults()[option]
            else:
                if not parser.has_option(section, option):
                    continue
                current_value = parser.get(section, option)

            if option == "key_file" and staged_key_path is not None:
                update_config_option(parser, section, option, f"{OCI_CONTAINER_DIR}/{staged_key_path.name}")
                continue

            artifact = resolve_config_artifact(current_value, config_source)
            if artifact is None:
                errors.append(f"No se pudo resolver {option} desde {config_source}")
                continue

            target = repo_root / ".local" / "oci" / artifact.name
            target_key = str(target).lower()
            if target_key not in copied_artifacts:
                staged.extend(copy_to_exact_path(artifact, target, replace_existing, repo_root, host_repo_root))
                copied_artifacts.add(target_key)
            update_config_option(parser, section, option, f"{OCI_CONTAINER_DIR}/{target.name}")

    with config_target.open("w", encoding="utf-8") as handle:
        parser.write(handle)

    staged.append(
        {
            "source": display_path(config_source, repo_root, host_repo_root),
            "target": display_path(config_target, repo_root, host_repo_root),
            "action": "normalized_for_docker_oci_cli",
        }
    )
    return staged, errors


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
    host_repo_root: Path | PurePath,
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
            section_results.extend(copy_into_directory(source, destination, replace_existing, repo_root, host_repo_root))

        if section_results:
            staged[section] = section_results

    return staged, errors


def stage_local_credentials(
    repo_root: Path,
    host_repo_root: Path | PurePath,
    args: argparse.Namespace,
    replace_existing: bool,
) -> tuple[dict[str, list[dict[str, object]]], list[str]]:
    staged: dict[str, list[dict[str, object]]] = {}
    errors: list[str] = []
    config_source_path: Path | None = None
    config_target_path: Path | None = None
    key_target_path: Path | None = None

    if args.oci_config_source:
        source = Path(args.oci_config_source).expanduser().resolve()
        if not source.exists():
            errors.append(f"No existe la ruta fuente del OCI config: {source}")
        else:
            config_source_path = source
            config_target_path = repo_root / ".local" / "oci" / "config"
            staged["oci_config"] = copy_to_exact_path(
                source,
                config_target_path,
                replace_existing,
                repo_root,
                host_repo_root,
            )

    if args.oci_key_source:
        source = Path(args.oci_key_source).expanduser().resolve()
        if not source.exists():
            errors.append(f"No existe la ruta fuente de la llave .pem: {source}")
        else:
            key_target_path = repo_root / ".local" / "oci" / "key.pem"
            staged["oci_key"] = copy_to_exact_path(
                source,
                key_target_path,
                replace_existing,
                repo_root,
                host_repo_root,
            )

    if config_source_path is not None and config_target_path is not None:
        normalized_entries, normalize_errors = normalize_staged_oci_config(
            repo_root,
            host_repo_root,
            replace_existing,
            config_source_path,
            config_target_path,
            key_target_path,
        )
        staged.setdefault("oci_config", []).extend(normalized_entries)
        errors.extend(normalize_errors)

    if args.wallet_source:
        if not args.adb_name:
            errors.append("Debes indicar --adb-name cuando uses --wallet-source.")
        else:
            source = Path(args.wallet_source).expanduser().resolve()
            if not source.exists():
                errors.append(f"No existe la ruta fuente del wallet: {source}")
            else:
                wallet_target = repo_root / ".local" / "autonomous" / "wallets" / args.environment / args.adb_name
                staged["wallet"] = copy_into_directory(
                    source,
                    wallet_target,
                    replace_existing,
                    repo_root,
                    host_repo_root,
                )

    return staged, errors


def write_report(report_path: Path, payload: dict[str, object]) -> None:
    ensure_directory(report_path.parent)
    report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    host_repo_root = resolve_host_repo_root(repo_root)
    project_root = repo_root / "workspace" / "migration-input" / args.project_id
    ensure_directory(project_root)
    ensure_directory(project_root / "_inventory")

    staged_inputs, input_errors = stage_section_sources(
        repo_root,
        host_repo_root,
        args.project_id,
        args,
        args.replace_existing,
    )
    staged_credentials, credential_errors = stage_local_credentials(
        repo_root,
        host_repo_root,
        args,
        args.replace_existing,
    )

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
                "report_path": display_path(report_path, repo_root, host_repo_root),
                "errors": errors,
            }
        )
    )

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
