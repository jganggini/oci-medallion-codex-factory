from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


BUCKETS = (
    ("bucket-landing-external", "landing_external", "landing"),
    ("bucket-bronze-raw", "bronze_raw", "bronze"),
    ("bucket-silver-trusted", "silver_trusted", "silver"),
    ("bucket-gold-refined", "gold_refined", "gold"),
)
DATAFLOW_APPS = (
    ("landing-to-bronze", "landing_external", "bronze_raw"),
    ("bronze-to-silver", "bronze_raw", "silver_trusted"),
    ("silver-to-gold", "silver_trusted", "gold_refined"),
)
OPERATOR_GROUP = "grp-medallion-operators-dev"
DATAFLOW_ADMIN_GROUP = "grp-dataflow-admin-dev"
ADB_DYNAMIC_GROUP = "dg-adb-resource-principal-dev"
DATA_CATALOG_DYNAMIC_GROUP = "dg-data-catalog-harvest-dev"
WORKSPACE_OCID_PLACEHOLDER = "ocid1.disworkspace.oc1..exampleWorkspace"
CATALOG_OCID_PLACEHOLDER = "ocid1.datacatalog.oc1..exampleCatalog"
PROJECT_COMPARTMENT_OCID_PLACEHOLDER = "ocid1.compartment.oc1..exampleProjectCompartment"


def operator_policy_statements(compartment_name: str) -> list[str]:
    return [
        f"Allow group {OPERATOR_GROUP} to inspect compartments in tenancy",
        f"Allow group {OPERATOR_GROUP} to manage buckets in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to manage objects in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to manage autonomous-database-family in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to use virtual-network-family in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to manage dataflow-family in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to manage dis-family in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to manage data-catalog-family in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to manage data-catalog-private-endpoints in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to manage vaults in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to manage secret-family in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to read log-groups in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to read log-content in compartment {compartment_name}",
        f"Allow group {OPERATOR_GROUP} to manage work-requests in compartment {compartment_name}",
    ]


def dataflow_policy_statements(compartment_name: str) -> list[str]:
    return [
        f"Allow group {DATAFLOW_ADMIN_GROUP} to inspect compartments in tenancy",
        f"Allow group {DATAFLOW_ADMIN_GROUP} to manage dataflow-family in compartment {compartment_name}",
        f"Allow group {DATAFLOW_ADMIN_GROUP} to manage dataflow-private-endpoint in tenancy",
        f"Allow group {DATAFLOW_ADMIN_GROUP} to use virtual-network-family in compartment {compartment_name}",
        f"Allow group {DATAFLOW_ADMIN_GROUP} to read objectstorage-namespaces in tenancy",
        f"Allow group {DATAFLOW_ADMIN_GROUP} to read buckets in compartment {compartment_name}",
        f"Allow group {DATAFLOW_ADMIN_GROUP} to manage objects in compartment {compartment_name}",
        f"Allow group {DATAFLOW_ADMIN_GROUP} to read log-groups in compartment {compartment_name}",
        f"Allow group {DATAFLOW_ADMIN_GROUP} to use log-content in compartment {compartment_name}",
    ]


def adb_resource_principal_statements(compartment_name: str) -> list[str]:
    return [
        f"Allow dynamic-group {ADB_DYNAMIC_GROUP} to read objectstorage-namespaces in tenancy",
        f"Allow dynamic-group {ADB_DYNAMIC_GROUP} to manage buckets in compartment {compartment_name}",
        f"Allow dynamic-group {ADB_DYNAMIC_GROUP} to manage objects in compartment {compartment_name}",
    ]


def di_workspace_policy_statements(compartment_name: str) -> list[str]:
    condition = f"where all {{request.principal.type='disworkspace', request.principal.id='{WORKSPACE_OCID_PLACEHOLDER}'}}"
    return [
        f"Allow any-user to use virtual-network-family in compartment {compartment_name} {condition}",
        f"Allow any-user to use secret-family in compartment {compartment_name} {condition}",
        f"Allow any-user to read secret-bundles in compartment {compartment_name} {condition}",
        f"Allow any-user to read objectstorage-namespaces in tenancy {condition}",
        f"Allow any-user to manage buckets in compartment {compartment_name} {condition}",
        f"Allow any-user to manage objects in compartment {compartment_name} {condition}",
    ]


def data_catalog_policy_statements(compartment_name: str) -> list[str]:
    return [
        f"Allow dynamic-group {DATA_CATALOG_DYNAMIC_GROUP} to read object-family in compartment {compartment_name}",
        f"Allow dynamic-group {DATA_CATALOG_DYNAMIC_GROUP} to read dis-workspaces-lineage in compartment {compartment_name}",
        "Allow any-user to manage data-catalog-data-assets in compartment "
        f"{compartment_name} where all {{request.principal.type='dataflowrun', target.catalog.id='{CATALOG_OCID_PLACEHOLDER}', target.resource.kind='dataFlow'}}",
    ]


def run_command(repo_root: Path, args: list[str]) -> None:
    subprocess.run([sys.executable, *args], cwd=repo_root, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Ejecuta un flujo local ordenado del factory medallion.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--environment", default="dev", choices=("dev", "qa", "prod"))
    parser.add_argument("--compartment-name", default="data-medallion-dev")
    parser.add_argument("--workspace-name", default="ws-di-medallion-dev")
    parser.add_argument("--database-name", default="adb_trafico_gold")
    parser.add_argument("--vcn-name", default="vcn-data-medallion-dev")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    template_app = repo_root / "templates" / "data_flow" / "minimal_app"
    readme_file = repo_root / "README.md"

    # 1. Project compartment.
    run_command(
        repo_root,
        [
            "mcp/servers/oci-iam-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-compartment",
            "--compartment-name",
            args.compartment_name,
        ],
    )

    # 2. IAM baseline: groups, dynamic groups and policies.
    for group_name in (OPERATOR_GROUP, DATAFLOW_ADMIN_GROUP):
        run_command(
            repo_root,
            [
                "mcp/servers/oci-iam-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--command",
                "create-group",
                "--group-name",
                group_name,
            ],
        )
    for dynamic_group_name, matching_rule in (
        (
            ADB_DYNAMIC_GROUP,
            f"ALL {{resource.type = 'autonomousdatabase', resource.compartment.id = '{PROJECT_COMPARTMENT_OCID_PLACEHOLDER}'}}",
        ),
        (
            DATA_CATALOG_DYNAMIC_GROUP,
            f"Any {{resource.id = '{CATALOG_OCID_PLACEHOLDER}'}}",
        ),
    ):
        run_command(
            repo_root,
            [
                "mcp/servers/oci-iam-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--command",
                "create-dynamic-group",
                "--dynamic-group-name",
                dynamic_group_name,
                "--matching-rule",
                matching_rule,
            ],
        )
    for policy_name, statements in (
        ("plc-medallion-operators-dev", operator_policy_statements(args.compartment_name)),
        ("plc-dataflow-admin-dev", dataflow_policy_statements(args.compartment_name)),
        ("plc-adb-resource-principal-dev", adb_resource_principal_statements(args.compartment_name)),
        ("plc-di-workspace-runtime-dev", di_workspace_policy_statements(args.compartment_name)),
        ("plc-data-catalog-harvest-dev", data_catalog_policy_statements(args.compartment_name)),
    ):
        command = [
            "mcp/servers/oci-iam-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-policy",
            "--policy-name",
            policy_name,
        ]
        for statement in statements:
            command.extend(["--statement", statement])
        run_command(repo_root, command)

    # 3. Storage layers: landing, bronze, silver, gold.
    for bucket_name, layer, purpose in BUCKETS:
        run_command(
            repo_root,
            [
                "mcp/servers/oci-object-storage-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--command",
                "create-bucket",
                "--bucket-name",
                bucket_name,
                "--layer",
                layer,
                "--bucket-purpose",
                purpose,
            ],
        )

    # 4. Autonomous Database as control plane + final gold target.
    run_command(
        repo_root,
        [
            "mcp/servers/oci-autonomous-database-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-adb-definition",
            "--database-name",
            args.database_name,
            "--database-user",
            "app_gold",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-autonomous-database-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "bootstrap-control-plane",
            "--database-name",
            args.database_name,
            "--control-schema",
            "MDL_CTL",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-autonomous-database-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-database-user",
            "--database-name",
            args.database_name,
            "--database-user",
            "app_gold",
            "--password-placeholder",
            "APP_GOLD_PASSWORD",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-autonomous-database-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "bootstrap-schema",
            "--database-name",
            args.database_name,
            "--database-user",
            "app_gold",
        ],
    )

    # 5. Network foundation in the project compartment.
    run_command(
        repo_root,
        [
            "mcp/servers/oci-network-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-vcn",
            "--vcn-name",
            args.vcn_name,
            "--cidr-block",
            "10.10.0.0/16",
            "--dns-label",
            "mdvcn",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-network-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-route-table",
            "--route-table-name",
            "rt-data-medallion-dev",
            "--vcn-name",
            args.vcn_name,
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-network-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-subnet",
            "--subnet-name",
            "subnet-data-private-dev",
            "--cidr-block",
            "10.10.10.0/24",
            "--vcn-name",
            args.vcn_name,
            "--route-table-name",
            "rt-data-medallion-dev",
            "--dns-label",
            "mdsubnet",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-network-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-nsg",
            "--nsg-name",
            "nsg-data-medallion-dev",
            "--vcn-name",
            args.vcn_name,
        ],
    )

    # 6. Upload source files to landing.
    run_command(
        repo_root,
        [
            "mcp/servers/oci-object-storage-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "upload-object",
            "--bucket-name",
            "bucket-landing-external",
            "--source-file",
            str(readme_file),
            "--object-name",
            "input/README.md",
            "--layer",
            "landing_external",
        ],
    )

    # 7. Data Flow jobs for every hop up to gold.
    for index, (application_name, layer_from, layer_to) in enumerate(DATAFLOW_APPS, start=1):
        run_command(
            repo_root,
            [
                "mcp/servers/oci-data-flow-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--command",
                "create-application",
                "--application-name",
                application_name,
                "--source-dir",
                str(template_app),
                "--layer",
                layer_to,
            ],
        )
        run_command(
            repo_root,
            [
                "mcp/servers/oci-data-flow-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--command",
                "run-application",
                "--application-name",
                application_name,
                "--workflow-id",
                "wf-medallion-demo",
                "--run-id",
                f"run-local-{index:03d}",
                "--slice-key",
                f"entity=demo/business_date=2026-03-26/batch_id={index:03d}",
                "--parameter",
                f"source_layer={layer_from}",
                "--parameter",
                f"target_layer={layer_to}",
            ],
        )

    # 8. Data Integration after Data Flow definitions exist.
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-integration-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-workspace",
            "--workspace-name",
            args.workspace_name,
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-integration-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-project",
            "--workspace-name",
            args.workspace_name,
            "--project-name",
            "Medallion Demo Ordered",
            "--identifier",
            "MEDALLION_DEMO_ORDERED",
            "--label",
            "medallion",
            "--label",
            "ordered",
        ],
    )
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-integration-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--command",
            "create-folder",
            "--workspace-name",
            args.workspace_name,
            "--folder-name",
            "Medallion Tasks",
            "--identifier",
            "MEDALLION_TASKS",
        ],
    )

    pipeline_tasks: list[str] = []
    for application_name, _, _ in DATAFLOW_APPS:
        task_name = f"run-{application_name}"
        task_key = task_name.upper().replace("-", "_") + "_KEY"
        pipeline_tasks.append(task_name)
        run_command(
            repo_root,
            [
                "mcp/servers/oci-data-integration-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--command",
                "create-task-from-dataflow",
                "--workspace-name",
                args.workspace_name,
                "--task-name",
                task_name,
                "--application-name",
                application_name,
                "--aggregator-key",
                "MEDALLION_DEMO_ORDERED",
                "--task-key",
                task_key,
            ],
        )

    pipeline_args = [
        "mcp/servers/oci-data-integration-mcp/server.py",
        "--repo-root",
        str(repo_root),
        "--environment",
        args.environment,
        "--command",
        "create-pipeline",
        "--workspace-name",
        args.workspace_name,
        "--pipeline-name",
        "medallion-demo-ordered",
    ]
    for task_name in pipeline_tasks:
        pipeline_args.extend(["--task", task_name])
    run_command(repo_root, pipeline_args)

    print("Local ordered medallion demo completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
