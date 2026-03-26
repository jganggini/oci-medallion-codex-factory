from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()
REPO_ROOT_DEFAULT = CURRENT_FILE.parents[3]
if str(REPO_ROOT_DEFAULT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT_DEFAULT))

from mcp.common.local_services import create_vault_definition, create_vault_secret, export_vault_manifest, rotate_vault_secret_reference
from mcp.common.runtime import MirrorContext


COMMAND_ALIASES = {
    "create-vault": "create_vault",
    "create_vault": "create_vault",
    "create-secret": "create_secret",
    "create_secret": "create_secret",
    "rotate-secret-reference": "rotate_secret_reference",
    "rotate_secret_reference": "rotate_secret_reference",
    "export-vault-manifest": "export_vault_manifest",
    "export_vault_manifest": "export_vault_manifest",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Runtime for oci-vault-mcp.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT_DEFAULT))
    parser.add_argument("--environment", required=True, choices=("dev", "qa", "prod"))
    parser.add_argument("--runtime", default="local", choices=("local", "oci"))
    parser.add_argument("--oci-mode", default="plan", choices=("plan", "apply"))
    parser.add_argument("--command", required=True, choices=tuple(COMMAND_ALIASES.keys()))
    parser.add_argument("--vault-name")
    parser.add_argument("--vault-id")
    parser.add_argument("--compartment-id")
    parser.add_argument("--key-id")
    parser.add_argument("--description")
    parser.add_argument("--secret-name")
    parser.add_argument("--secret-ref")
    parser.add_argument("--new-secret-ref")
    args = parser.parse_args()

    canonical_command = COMMAND_ALIASES[args.command]
    context = MirrorContext(repo_root=Path(args.repo_root).resolve(), environment=args.environment)

    if canonical_command == "export_vault_manifest":
        result = export_vault_manifest(context)
        print(json.dumps({"status": "ok", "command": canonical_command, "manifest_path": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if canonical_command == "create_vault":
        if not args.vault_name:
            raise SystemExit("--vault-name es requerido para create_vault")
        result = create_vault_definition(
            context,
            args.vault_name,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "vault_id": args.vault_id,
                "compartment_id": args.compartment_id,
                "key_id": args.key_id,
                "description": args.description,
            },
        )
        print(json.dumps({"status": "ok", "command": canonical_command, "manifest_path": str(result)}, indent=2, ensure_ascii=True))
        return 0

    if not args.vault_name or not args.secret_name:
        raise SystemExit("--vault-name y --secret-name son requeridos para create_secret y rotate_secret_reference")

    if canonical_command == "create_secret":
        result = create_vault_secret(
            context,
            args.vault_name,
            args.secret_name,
            {
                "runtime": args.runtime,
                "oci_mode": args.oci_mode if args.runtime == "oci" else None,
                "vault_id": args.vault_id,
                "key_id": args.key_id,
                "description": args.description,
                "secret_ref": args.secret_ref,
            },
        )
        print(json.dumps({"status": "ok", "command": canonical_command, "secret_manifest": str(result)}, indent=2, ensure_ascii=True))
        return 0

    result = rotate_vault_secret_reference(
        context,
        args.vault_name,
        args.secret_name,
        {
            "runtime": args.runtime,
            "oci_mode": args.oci_mode if args.runtime == "oci" else None,
            "vault_id": args.vault_id,
            "key_id": args.key_id,
            "previous_secret_ref": args.secret_ref,
            "new_secret_ref": args.new_secret_ref,
        },
    )
    print(json.dumps({"status": "ok", "command": canonical_command, "rotation_receipt": str(result)}, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
