# AGENTS

## Repository purpose

This repository is a reusable OCI medallion factory. It is not a client-specific implementation.

## Working rules

- Keep secrets out of Git.
- Treat `workspace/migration-input/` as the canonical migration intake zone.
- Treat `.local/` as sensitive local-only storage.
- Update `workspace/oci-mirror/` whenever MCP-related changes affect the OCI mirror contract.
- Prefer extending existing MCP runtimes instead of creating disconnected automation.
- Preserve the separation between `local` and `oci` execution modes.

## Validation

- Run `py -3 scripts/validate_factory.py --repo-root .`
- Run `py -3 scripts/run_local_publish_demo.py --repo-root . --environment dev`
- Run `py -3 scripts/run_oci_plan_demo.py --repo-root . --environment dev`

## Publish expectations

- Keep the repo GitHub-template friendly.
- Avoid hardcoded OCIDs, wallets, passwords, tenancy details, or private keys.
- Document any new MCP or skill contract in `docs/`.
