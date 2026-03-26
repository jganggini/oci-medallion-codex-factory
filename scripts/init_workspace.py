from __future__ import annotations

import argparse
import json
from pathlib import Path


ENVIRONMENTS = ("dev", "qa", "prod")
SERVICES = (
    "iam",
    "network",
    "buckets",
    "data_flow",
    "data_integration",
    "autonomous_database",
    "vault",
    "reports",
)
LAYER_BUCKETS = ("bucket-raw", "bucket-trusted", "bucket-refined")
MIGRATION_FOLDERS = ("sql", "docs", "ddl", "samples", "exports", "mappings", "notes")


def ensure_file(path: Path, content: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(content, encoding="utf-8")


def ensure_json(path: Path, payload: dict[str, object]) -> None:
    ensure_file(path, json.dumps(payload, indent=2, ensure_ascii=True) + "\n")


def init_local(repo_root: Path, project_id: str) -> None:
    ensure_file(
        repo_root / ".local" / "README.md",
        "\n".join(
            [
                "# .local",
                "",
                "Zona no versionada para configuracion sensible y artefactos locales.",
                "",
                "- `oci/`: config, keys y perfiles OCI",
                "- `autonomous/wallets/`: wallets por ambiente y base",
                "- `secrets/`: archivos `.env` locales",
                "- `migration-private/`: landing zone privada opcional para insumos de migracion sensibles",
            ]
        ),
    )
    ensure_file(
        repo_root / ".local" / "oci" / "config.example",
        "\n".join(
            [
                "[DEFAULT]",
                "user=ocid1.user.oc1..replace-me",
                "fingerprint=replace:me",
                "key_file=.local/oci/key.pem",
                "tenancy=ocid1.tenancy.oc1..replace-me",
                "region=sa-santiago-1",
            ]
        )
        + "\n",
    )
    ensure_file(repo_root / ".local" / "oci" / "key.example.pem", "-----BEGIN PRIVATE KEY-----\nreplace-me\n-----END PRIVATE KEY-----\n")
    for env in ENVIRONMENTS:
        ensure_file(repo_root / ".local" / "oci" / "profiles" / env / ".gitkeep")
        ensure_file(repo_root / ".local" / "autonomous" / "wallets" / env / ".gitkeep")
        ensure_file(
            repo_root / ".local" / "secrets" / f"project.{env}.env.example",
            "\n".join(
                [
                    f"OCI_PROFILE={env.upper()}",
                    "OCI_REGION=sa-santiago-1",
                    "ADB_ADMIN_PASSWORD=replace-me",
                ]
            )
            + "\n",
        )

    private_root = repo_root / ".local" / "migration-private" / project_id
    for folder in MIGRATION_FOLDERS:
        ensure_file(private_root / folder / ".gitkeep")
    ensure_file(
        private_root / "README.md",
        "\n".join(
            [
                f"# Private Migration Input - {project_id}",
                "",
                "Usa esta zona solo para material sensible que no deba quedar en Git.",
                "El intake canonico trabaja sobre `workspace/migration-input/<project_id>/`.",
            ]
        ),
    )


def init_mirror(repo_root: Path) -> None:
    base = repo_root / "workspace" / "oci-mirror"
    ensure_file(
        base / "README.md",
        "\n".join(
            [
                "# OCI Mirror",
                "",
                "Espejo local del estado publicado en OCI por ambiente, compartment y servicio.",
                "No almacena secretos reales, wallets reales ni llaves privadas.",
            ]
        ),
    )

    for env in ENVIRONMENTS:
        env_root = base / env
        compartment_name = f"compartment-data-medallion-{env}"
        compartment_root = env_root / compartment_name
        ensure_json(
            env_root / "environment.manifest.json",
            {
                "environment": env,
                "compartment_name": compartment_name,
                "services": list(SERVICES),
                "buckets": list(LAYER_BUCKETS),
            },
        )
        ensure_json(
            compartment_root / "compartment.manifest.json",
            {
                "name": compartment_name,
                "environment": env,
                "mirror_contract_version": "1.0.0",
                "services": list(SERVICES),
            },
        )
        for service in SERVICES:
            ensure_file(compartment_root / service / ".gitkeep")
        for bucket in LAYER_BUCKETS:
            ensure_file(compartment_root / "buckets" / bucket / ".gitkeep")
        ensure_file(
            compartment_root / "reports" / "README.md",
            "Guarda aqui reportes redacted, manifests efectivos y evidencia de despliegue.\n",
        )


def init_migration_input(repo_root: Path, project_id: str) -> None:
    project_root = repo_root / "workspace" / "migration-input" / project_id
    for folder in MIGRATION_FOLDERS:
        ensure_file(project_root / folder / ".gitkeep")
    ensure_file(project_root / "_inventory" / ".gitkeep")
    ensure_file(
        project_root / "README.md",
        "\n".join(
            [
                f"# Migration Input - {project_id}",
                "",
                "Coloca aqui los insumos base para la migracion:",
                "- `sql/`: procedimientos, queries, paquetes SQL",
                "- `docs/`: documentos funcionales, reglas de negocio y analisis",
                "- `ddl/`: DDL y diccionario de datos",
                "- `samples/`: archivos raw o muestras",
                "- `exports/`: salidas esperadas o historicas",
                "- `mappings/`: cruces campo a campo y taxonomias",
                "- `notes/`: aclaraciones, exclusiones y decisiones",
                "",
                "Esta ruta es la fuente canonica para intake, skills y scaffold.",
            ]
        ),
    )
    ensure_file(repo_root / "workspace" / "generated" / ".gitkeep")


def main() -> int:
    parser = argparse.ArgumentParser(description="Inicializa la estructura local del factory repo.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--project-id", required=True)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    init_local(repo_root, args.project_id)
    init_mirror(repo_root)
    init_migration_input(repo_root, args.project_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
