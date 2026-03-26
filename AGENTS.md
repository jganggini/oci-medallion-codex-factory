# AGENTS

## Mission

This repository is a reusable OCI medallion factory for migration projects.
It is a template-style foundation, not a client-specific implementation.

Codex should optimize for:

- keeping the repo reusable and publishable
- preserving the separation between local development, OCI planning, and OCI execution
- avoiding secrets, OCIDs, wallets, or client identifiers in versioned files
- updating the OCI mirror contract whenever automation changes
- behaving as a guided advisor for medallion deployment and migration, not just as a code generator

## What This Repo Contains

### Canonical zones

- `workspace/migration-input/<project_id>/`
  Canonical migration intake zone for SQL, docs, DDL, samples, exports, mappings, and notes.
- `workspace/oci-mirror/<env>/compartment-data-medallion-<env>/`
  Local mirror of OCI resources, manifests, reports, and publishable artifacts.
- `.local/`
  Sensitive local-only storage for OCI config, keys, wallets, env files, and private migration material.

### Main folders

- `infra/`
  Terraform-compatible OCI Resource Manager foundation.
- `mcp/`
  MCP catalog, manifests, common runtimes, and service runners.
- `skills/`
  Project workflow skills. These are project assets, not global Codex config.
- `templates/`
  Base manifests, Data Flow app templates, and dependency package templates.
- `docker/`
  Local runtime images and compose services.
- `scripts/`
  Setup, validation, local publish demos, OCI plan demos, and local runtime runners.
- `docs/`
  Contracts, onboarding, publish flow, mirror conventions, Data Flow runtime docs.

## Codex Behavior In This Repo

### Default guided advisor behavior

Treat this repository as a migration and deployment advisor by default.

When the user asks broad things such as:

- "quiero implementar este proyecto"
- "ayudame a desplegar"
- "guiame paso a paso"
- "dime donde colocar los archivos"
- "quiero migrar esta logica"

Codex should not jump directly into OCI execution or code edits.

Codex should first:

1. inspect the repo contracts and relevant docs
2. identify the current stage: intake, bootstrap, network, scaffold, publish, QA, validate, or incident
3. ask one material question at a time
4. tell the user the exact path, file name, and minimum expected content when an input is missing
5. ask whether any bucket with data already exists, which medallion layer it represents, and whether the file load will happen outside this factory flow
6. explain the next concrete step before moving to the next stage

Codex should explicitly avoid:

- assuming `.local/oci/` is ready
- assuming wallets, OCIDs, tfvars, SQL bundles, or samples already exist
- assuming Object Storage buckets must always be created by this repo
- assuming that one populated bucket means raw, trusted, refined, and gold are already provisioned
- switching to `oci-mode apply` without confirming credentials and target environment
- asking long questionnaires in a single turn unless the user explicitly asks for a checklist

When guidance is needed, prefer this response pattern:

1. current stage
2. what is missing
3. exact location where the user should place files
4. what Codex will do after the user confirms or provides the input

If a placeholder, template, or starter artifact would unblock the user, offer to create it in the repo.

### Use these sources first

1. `AGENTS.md`
2. `README.md`
3. `docs/`
4. `templates/project.medallion.yaml`
5. `mcp/catalog/services.yaml`
6. `skills/oci-medallion-advisor/SKILL.md` for broad deploy/migrate guidance

### Treat these as source-of-truth contracts

- `docs/project-contract.md`
- `docs/oci-mirror.md`
- `docs/local-zones.md`
- `templates/project.medallion.yaml`
- `mcp/servers/*/server.manifest.yaml`

### Treat these as implementation entrypoints

- `scripts/init_workspace.py`
- `scripts/migration_intake.py`
- `scripts/validate_factory.py`
- `scripts/run_local_publish_demo.py`
- `scripts/run_oci_plan_demo.py`
- `scripts/run_dataflow_local.ps1`
- `scripts/run_dataflow_local.sh`
- `mcp/servers/*/server.py`

## Skills And .codex

- Project skills live in `skills/`.
- `.codex/` is only for repository-level Codex helper material such as shared notes or rules.
- Do not recreate project skills under `.codex/skills/`.
- Do not assume `.codex/config.toml` exists in the repo. User-level Codex config is expected to live outside the repository.
- For broad deployment or migration requests, start with `skills/oci-medallion-advisor/SKILL.md` and then route to the specialized skill for the current stage.

## Execution Model

### Local mode

Use local mode when validating contracts, packaging artifacts, or simulating OCI behavior against the mirror.

Examples:

- `oci-object-storage-mcp --runtime local`
- `oci-data-flow-mcp --runtime local`
- `oci-data-integration-mcp --runtime local`
- `oci-autonomous-database-mcp --runtime local`

Expected effects:

- writes manifests under `workspace/oci-mirror/`
- writes reports under `workspace/oci-mirror/<env>/.../reports/`
- never requires real OCI credentials

### OCI plan mode

Use OCI plan mode to generate the exact OCI CLI commands and record them in the mirror without creating resources.

Expected effects:

- writes `oci-plans/` under the target service in the mirror
- writes redacted reports under `reports/`
- requires command construction to match the service contract

### OCI apply mode

Use OCI apply mode only when the repo has valid local credentials in `.local/oci/` and the user explicitly wants real OCI changes.

Expected effects:

- executes OCI CLI using Docker
- records command result JSON in the mirror
- must preserve redaction rules and avoid secrets in Git

## Data Flow Rules

There are two separate concerns for Data Flow and Codex should not mix them:

### 1. Local job execution runtime

Used to run Spark jobs locally before publishing.

Key files:

- `docker/dataflow-local-runtime.Dockerfile`
- `scripts/run_dataflow_local.ps1`
- `scripts/run_dataflow_local.sh`
- `docs/dataflow-local-runtime.md`

Use this when:

- testing PySpark jobs locally
- validating Spark config
- checking local reads and writes against the repo mirror

### 2. Official dependency archive packaging

Used to build the `archive.zip` for OCI Data Flow with the Oracle dependency packager image.

Key files:

- `mcp/common/dataflow_packager.py`
- `templates/data_flow/dependency_package/`
- `docs/dataflow-dependency-packager.md`
- `mcp/servers/oci-data-flow-mcp/server.py`

Use this when:

- building `archive.zip`
- validating dependency archives
- preparing Python libraries and Java jars for publish

### Iceberg guidance

If a job uses Iceberg:

- place the Iceberg jar in `workspace/generated/<project_id>/data_flow/dependencies/<job_name>/java/`
- keep the jar out of hardcoded machine-specific paths
- model the dependency as part of the job dependency package
- use local runtime plus dependency package flow before OCI publish

## MCP Expectations

When changing an MCP:

- update its `server.manifest.yaml` if the contract changes
- keep `local` and `oci` modes explicit
- update `workspace/oci-mirror/` behavior consistently
- add or update docs if the workflow changes

Current operational MCP focus:

- `migration-intake-mcp`
- `oci-object-storage-mcp`
- `oci-data-flow-mcp`
- `oci-data-integration-mcp`
- `oci-autonomous-database-mcp`

Other MCPs may remain scaffolded until implemented.

## Validation Sequence

Run these in order when changing core repo automation:

1. `py -3 scripts/validate_factory.py --repo-root .`
2. `py -3 scripts/migration_intake.py --repo-root . --project-id <project_id>`
3. `py -3 scripts/run_local_publish_demo.py --repo-root . --environment dev`
4. `py -3 scripts/run_oci_plan_demo.py --repo-root . --environment dev`

For Data Flow-specific changes also run:

1. `docker compose build dataflow-local`
2. `docker compose run --rm dataflow-local spark-submit /workspace/templates/data_flow/minimal_app/main.py`
3. `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command package-dependencies --application-name <job_name> --dependency-root <dependency_root>`
4. `py -3 mcp/servers/oci-data-flow-mcp/server.py --environment dev --command validate-archive --application-name <job_name> --dependency-root <dependency_root>`

## Publishing Rules

- Keep the repo GitHub-template friendly.
- Do not version `.local/`.
- Do not version real OCI config, private keys, wallets, passwords, or client-specific exports.
- Prefer examples and redacted mirror artifacts.
- Before commit, avoid leaving temporary runtime outputs in the repo unless they are intentional example artifacts.

## If You Need To Decide Quickly

- Put project workflow logic in `skills/`.
- Put Codex repo guidance in `AGENTS.md`.
- Put local-only sensitive material in `.local/`.
- Put migration evidence in `workspace/migration-input/`.
- Put OCI evidence in `workspace/oci-mirror/`.
- Use `dataflow-local` for Spark execution tests.
- Use `dependency-packager` for `archive.zip`.
