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


def di_workspace_policy_statements(compartment_name: str, workspace_id: str) -> list[str]:
    condition = f"where all {{request.principal.type='disworkspace', request.principal.id='{workspace_id}'}}"
    return [
        f"Allow any-user to use virtual-network-family in compartment {compartment_name} {condition}",
        f"Allow any-user to use secret-family in compartment {compartment_name} {condition}",
        f"Allow any-user to read secret-bundles in compartment {compartment_name} {condition}",
        f"Allow any-user to read objectstorage-namespaces in tenancy {condition}",
        f"Allow any-user to manage buckets in compartment {compartment_name} {condition}",
        f"Allow any-user to manage objects in compartment {compartment_name} {condition}",
    ]


def data_catalog_policy_statements(compartment_name: str, catalog_id: str) -> list[str]:
    return [
        f"Allow dynamic-group {DATA_CATALOG_DYNAMIC_GROUP} to read object-family in compartment {compartment_name}",
        f"Allow dynamic-group {DATA_CATALOG_DYNAMIC_GROUP} to read dis-workspaces-lineage in compartment {compartment_name}",
        "Allow any-user to manage data-catalog-data-assets in compartment "
        f"{compartment_name} where all {{request.principal.type='dataflowrun', target.catalog.id='{catalog_id}', target.resource.kind='dataFlow'}}",
    ]


def run_command(repo_root: Path, args: list[str]) -> None:
    subprocess.run([sys.executable, *args], cwd=repo_root, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Genera un plan OCI ordenado para el factory medallion.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--environment", default="dev", choices=("dev", "qa", "prod"))
    parser.add_argument("--parent-compartment-id", default="ocid1.tenancy.oc1..exampleParentTenancy")
    parser.add_argument("--compartment-id", default="ocid1.compartment.oc1..exampleProjectCompartment")
    parser.add_argument("--project-compartment-name", default="data-medallion-dev")
    parser.add_argument("--namespace-name", default="example-ns")
    parser.add_argument("--workspace-id", default="ocid1.disworkspace.oc1..exampleWorkspace")
    parser.add_argument("--catalog-id", default="ocid1.datacatalog.oc1..exampleCatalog")
    parser.add_argument("--application-id", default="ocid1.dataflowapplication.oc1..exampleApplication")
    parser.add_argument("--secret-id", default="ocid1.vaultsecret.oc1..exampleSecret")
    parser.add_argument("--vcn-id", default="ocid1.vcn.oc1..exampleVcn")
    parser.add_argument("--subnet-id", default="ocid1.subnet.oc1..exampleSubnet")
    parser.add_argument("--route-table-id", default="ocid1.routetable.oc1..exampleRouteTable")
    parser.add_argument("--nsg-id", default="ocid1.networksecuritygroup.oc1..exampleNsg")
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
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-compartment",
            "--compartment-name",
            args.project_compartment_name,
            "--parent-compartment-id",
            args.parent_compartment_id,
        ],
    )

    # 2. IAM baseline.
    for group_name in (OPERATOR_GROUP, DATAFLOW_ADMIN_GROUP):
        run_command(
            repo_root,
            [
                "mcp/servers/oci-iam-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "plan",
                "--command",
                "create-group",
                "--group-name",
                group_name,
                "--compartment-id",
                args.compartment_id,
            ],
        )
    for dynamic_group_name, matching_rule in (
        (
            ADB_DYNAMIC_GROUP,
            f"ALL {{resource.type = 'autonomousdatabase', resource.compartment.id = '{args.compartment_id}'}}",
        ),
        (
            DATA_CATALOG_DYNAMIC_GROUP,
            f"Any {{resource.id = '{args.catalog_id}'}}",
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
                "--runtime",
                "oci",
                "--oci-mode",
                "plan",
                "--command",
                "create-dynamic-group",
                "--dynamic-group-name",
                dynamic_group_name,
                "--matching-rule",
                matching_rule,
            ],
        )
    for policy_name, statements in (
        ("plc-medallion-operators-dev", operator_policy_statements(args.project_compartment_name)),
        ("plc-dataflow-admin-dev", dataflow_policy_statements(args.project_compartment_name)),
        ("plc-adb-resource-principal-dev", adb_resource_principal_statements(args.project_compartment_name)),
        ("plc-di-workspace-runtime-dev", di_workspace_policy_statements(args.project_compartment_name, args.workspace_id)),
        ("plc-data-catalog-harvest-dev", data_catalog_policy_statements(args.project_compartment_name, args.catalog_id)),
    ):
        command = [
            "mcp/servers/oci-iam-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-policy",
            "--policy-name",
            policy_name,
            "--compartment-id",
            args.compartment_id,
        ]
        for statement in statements:
            command.extend(["--statement", statement])
        run_command(repo_root, command)

    # 3. Storage layers.
    for bucket_name, layer, purpose in BUCKETS:
        run_command(
            repo_root,
            [
                "mcp/servers/oci-object-storage-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "plan",
                "--command",
                "create-bucket",
                "--bucket-name",
                bucket_name,
                "--compartment-id",
                args.compartment_id,
                "--namespace-name",
                args.namespace_name,
                "--layer",
                layer,
                "--bucket-purpose",
                purpose,
            ],
        )

    # 4. Autonomous Database.
    run_command(
        repo_root,
        [
            "mcp/servers/oci-autonomous-database-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-adb-definition",
            "--database-name",
            "adb_trafico_gold",
            "--database-user",
            "app_gold",
            "--compartment-id",
            args.compartment_id,
            "--db-name",
            "ADWTRAFICO",
            "--display-name",
            "ADW_TRAFICO_GOLD",
            "--secret-id",
            args.secret_id,
        ],
    )

    # 5. Network in the project compartment.
    run_command(
        repo_root,
        [
            "mcp/servers/oci-network-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-vcn",
            "--compartment-id",
            args.compartment_id,
            "--vcn-name",
            "vcn-data-medallion-dev",
            "--cidr-block",
            "10.20.0.0/16",
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
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-route-table",
            "--compartment-id",
            args.compartment_id,
            "--vcn-id",
            args.vcn_id,
            "--route-table-name",
            "rt-data-medallion-dev",
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
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-subnet",
            "--compartment-id",
            args.compartment_id,
            "--vcn-id",
            args.vcn_id,
            "--subnet-name",
            "subnet-data-private-dev",
            "--cidr-block",
            "10.20.10.0/24",
            "--route-table-id",
            args.route_table_id,
            "--nsg-id",
            args.nsg_id,
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
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-nsg",
            "--compartment-id",
            args.compartment_id,
            "--vcn-id",
            args.vcn_id,
            "--nsg-name",
            "nsg-data-medallion-dev",
        ],
    )

    # 6. Landing upload.
    run_command(
        repo_root,
        [
            "mcp/servers/oci-object-storage-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "upload-object",
            "--bucket-name",
            "bucket-landing-external",
            "--namespace-name",
            args.namespace_name,
            "--source-file",
            str(readme_file),
            "--object-name",
            "input/README.md",
            "--layer",
            "landing_external",
        ],
    )

    # 7. Data Flow applications.
    for application_name, layer_from, layer_to in DATAFLOW_APPS:
        run_command(
            repo_root,
            [
                "mcp/servers/oci-data-flow-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "plan",
                "--command",
                "create-application",
                "--application-name",
                application_name,
                "--source-dir",
                str(template_app),
                "--compartment-id",
                args.compartment_id,
                "--file-uri",
                f"oci://bucket-silver-trusted@{args.namespace_name}/apps/{application_name}/main.py",
                "--artifact-uri",
                f"oci://bucket-silver-trusted@{args.namespace_name}/apps/{application_name}/archive.zip",
                "--logs-bucket-uri",
                f"oci://bucket-silver-trusted@{args.namespace_name}/logs/",
                "--layer",
                layer_to,
                "--workflow-id",
                "wf-medallion-plan-demo",
                "--run-id",
                f"plan-{application_name}",
                "--slice-key",
                f"source={layer_from}/target={layer_to}",
            ],
        )

    # 8. Data Integration after Data Flow is defined.
    run_command(
        repo_root,
        [
            "mcp/servers/oci-data-integration-mcp/server.py",
            "--repo-root",
            str(repo_root),
            "--environment",
            args.environment,
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-workspace",
            "--workspace-name",
            "ws-di-medallion-dev",
            "--compartment-id",
            args.compartment_id,
            "--is-private-network",
            "true",
            "--subnet-id",
            args.subnet_id,
            "--vcn-id",
            args.vcn_id,
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
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-project",
            "--workspace-name",
            "ws-di-medallion-dev",
            "--workspace-id",
            args.workspace_id,
            "--project-name",
            "Medallion Trafico Datos Ordered",
            "--identifier",
            "MEDALLION_TRAFICO_DATOS_ORDERED",
            "--label",
            "medallion",
            "--label",
            "ordered",
            "--favorite",
            "false",
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
            "--runtime",
            "oci",
            "--oci-mode",
            "plan",
            "--command",
            "create-folder",
            "--workspace-name",
            "ws-di-medallion-dev",
            "--workspace-id",
            args.workspace_id,
            "--folder-name",
            "Medallion Tasks",
            "--identifier",
            "MEDALLION_TASKS",
            "--aggregator-key",
            "MEDALLION_TRAFICO_DATOS_ORDERED",
        ],
    )

    pipeline_args = [
        "mcp/servers/oci-data-integration-mcp/server.py",
        "--repo-root",
        str(repo_root),
        "--environment",
        args.environment,
        "--runtime",
        "oci",
        "--oci-mode",
        "plan",
        "--command",
        "create-pipeline",
        "--workspace-name",
        "ws-di-medallion-dev",
        "--pipeline-name",
        "medallion-pipeline-ordered",
    ]
    for application_name, _, _ in DATAFLOW_APPS:
        task_name = f"run-{application_name}"
        run_command(
            repo_root,
            [
                "mcp/servers/oci-data-integration-mcp/server.py",
                "--repo-root",
                str(repo_root),
                "--environment",
                args.environment,
                "--runtime",
                "oci",
                "--oci-mode",
                "plan",
                "--command",
                "create-task-from-dataflow",
                "--workspace-name",
                "ws-di-medallion-dev",
                "--workspace-id",
                args.workspace_id,
                "--folder-key",
                "MEDALLION_TASKS",
                "--task-name",
                task_name,
                "--task-key",
                task_name.upper().replace("-", "_") + "_KEY",
                "--application-name",
                application_name,
                "--application-id",
                args.application_id,
                "--application-compartment-id",
                args.compartment_id,
                "--aggregator-key",
                "MEDALLION_TRAFICO_DATOS_ORDERED",
            ],
        )
        pipeline_args.extend(["--task", task_name])

    run_command(repo_root, pipeline_args)

    print("OCI ordered plan demo completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
