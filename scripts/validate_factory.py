from __future__ import annotations

import argparse
import json
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
)
REQUIRED_ROOT_FILES = (
    "README.md",
    "ARCHITECTURE.md",
    "SECURITY.md",
    ".gitignore",
    "setup-dev.ps1",
    "setup-dev.sh",
    "templates/project.medallion.yaml",
)
REQUIRED_GITIGNORE_LINES = (
    ".local/",
    "workspace/migration-input/**/_inventory/",
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Valida el scaffold base del factory repo.")
    parser.add_argument("--repo-root", required=True)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    missing_dirs = [item for item in REQUIRED_ROOT_DIRS if not (repo_root / item).exists()]
    missing_files = [item for item in REQUIRED_ROOT_FILES if not (repo_root / item).exists()]

    gitignore = (repo_root / ".gitignore").read_text(encoding="utf-8") if (repo_root / ".gitignore").exists() else ""
    missing_gitignore_lines = [item for item in REQUIRED_GITIGNORE_LINES if item not in gitignore]

    result = {
        "repo_root": str(repo_root),
        "missing_dirs": missing_dirs,
        "missing_files": missing_files,
        "missing_gitignore_lines": missing_gitignore_lines,
        "is_valid": len(missing_dirs) == 0 and len(missing_files) == 0 and len(missing_gitignore_lines) == 0,
    }
    print(json.dumps(result, indent=2, ensure_ascii=True))
    if not result["is_valid"]:
        raise SystemExit(1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
