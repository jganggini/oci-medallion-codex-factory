from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


REQUIRED_ROOT_DIRS = (
    "infra",
    "mcp",
    "skills",
    "templates",
    "examples",
    "docs",
    "docker",
    "workspace",
    ".github",
    ".codex",
)
REQUIRED_ROOT_FILES = (
    "README.md",
    "ARCHITECTURE.md",
    "SECURITY.md",
    ".gitignore",
    "setup-dev.ps1",
    "setup-dev.sh",
    "scripts/docker_repo_python.ps1",
    "scripts/docker_repo_python.sh",
    "scripts/docker_stage_assets.ps1",
    "scripts/docker_stage_assets.sh",
    "scripts/stage_local_assets.py",
    "templates/project.medallion.yaml",
    "templates/lineage/openlineage.sample.json",
    "templates/autonomous/control_plane_bootstrap.sql",
    "docs/medallion-control-plane.md",
    "docs/oci-iam-policy-baseline.md",
    ".codex/README.md",
    ".codex/config.template.toml",
    ".codex/factory_mcp_bridge.py",
)
REQUIRED_GITIGNORE_LINES = (
    ".local/",
    "workspace/migration-input/**/_inventory/",
)
SERVER_IMPORT_CHECKS = (
    "mcp/servers/oci-iam-mcp/server.py",
    "mcp/servers/oci-network-mcp/server.py",
    "mcp/servers/oci-object-storage-mcp/server.py",
    "mcp/servers/oci-resource-manager-mcp/server.py",
    "mcp/servers/oci-data-flow-mcp/server.py",
    "mcp/servers/oci-data-integration-mcp/server.py",
    "mcp/servers/oci-autonomous-database-mcp/server.py",
    "mcp/servers/oci-data-quality-mcp/server.py",
    "mcp/servers/oci-data-catalog-mcp/server.py",
    "mcp/servers/oci-vault-mcp/server.py",
)


def import_check(repo_root: Path, relative_path: str) -> str | None:
    module_path = repo_root / relative_path
    if not module_path.exists():
        return f"{relative_path}: missing file"

    module_name = "validate_factory_" + relative_path.replace("\\", "_").replace("/", "_").replace(".", "_")
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        return f"{relative_path}: unable to build import spec"

    repo_root_str = str(repo_root)
    inserted = False
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)
        inserted = True

    try:
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    except Exception as exc:  # pragma: no cover - validation path
        return f"{relative_path}: {exc.__class__.__name__}: {exc}"
    finally:
        sys.modules.pop(module_name, None)
        if inserted and sys.path and sys.path[0] == repo_root_str:
            sys.path.pop(0)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Valida el scaffold base del factory repo.")
    parser.add_argument("--repo-root", required=True)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    missing_dirs = [item for item in REQUIRED_ROOT_DIRS if not (repo_root / item).exists()]
    missing_files = [item for item in REQUIRED_ROOT_FILES if not (repo_root / item).exists()]

    gitignore = (repo_root / ".gitignore").read_text(encoding="utf-8") if (repo_root / ".gitignore").exists() else ""
    missing_gitignore_lines = [item for item in REQUIRED_GITIGNORE_LINES if item not in gitignore]
    import_errors = [error for error in (import_check(repo_root, path) for path in SERVER_IMPORT_CHECKS) if error]

    result = {
        "repo_root": str(repo_root),
        "missing_dirs": missing_dirs,
        "missing_files": missing_files,
        "missing_gitignore_lines": missing_gitignore_lines,
        "import_errors": import_errors,
        "is_valid": len(missing_dirs) == 0
        and len(missing_files) == 0
        and len(missing_gitignore_lines) == 0
        and len(import_errors) == 0,
    }
    print(json.dumps(result, indent=2, ensure_ascii=True))
    if not result["is_valid"]:
        raise SystemExit(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
