from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def run_intake(repo_root: Path, project_id: str) -> dict[str, object]:
    script = repo_root / "scripts" / "migration_intake.py"
    result = subprocess.run(
        [sys.executable, str(script), "--repo-root", str(repo_root), "--project-id", project_id],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout.strip())


def main() -> int:
    parser = argparse.ArgumentParser(description="Wrapper local del migration-intake-mcp.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--command", default="inventory", choices=("inventory", "validate", "summarize"))
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    result = run_intake(repo_root, args.project_id)
    inventory_dir = Path(result["inventory_dir"])
    inventory = json.loads((inventory_dir / "inventory.json").read_text(encoding="utf-8"))

    if args.command == "summarize":
        print(
            json.dumps(
                {
                    "project_id": inventory["project_id"],
                    "ready_for_scaffold": inventory["ready_for_scaffold"],
                    "blockers": inventory["blockers"],
                    "warnings": inventory["warnings"],
                },
                indent=2,
                ensure_ascii=True,
            )
        )
    else:
        print(json.dumps(inventory if args.command == "inventory" else result, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
