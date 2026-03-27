param(
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$ProjectId = "readme-flow-test",
    [string]$Environment = "dev",
    [string]$TestRoot = ".test",
    [switch]$ReplaceExistingCredentials
)

$ErrorActionPreference = "Stop"

$RepoRootResolved = (Resolve-Path -LiteralPath $RepoRoot).Path
$TestRootResolved = (Resolve-Path -LiteralPath (Join-Path $RepoRootResolved $TestRoot)).Path
$SetupScript = Join-Path $RepoRootResolved "setup-dev.ps1"
$StageScript = Join-Path $RepoRootResolved "scripts\docker_stage_assets.ps1"
$DockerPythonScript = Join-Path $RepoRootResolved "scripts\docker_repo_python.ps1"
$TemplateManifestPath = Join-Path $RepoRootResolved "templates\project.medallion.yaml"
$ProjectRoot = Join-Path $RepoRootResolved "workspace\migration-input\$ProjectId"
$InventoryRoot = Join-Path $ProjectRoot "_inventory"
$ManifestPath = Join-Path $ProjectRoot "project.medallion.yaml"
$StageReportPath = Join-Path $InventoryRoot "stage-report.json"
$InventoryPath = Join-Path $InventoryRoot "inventory.json"
$ContextPath = Join-Path $InventoryRoot "context.json"
$AcceptanceReportPath = Join-Path $InventoryRoot "acceptance-report.json"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message"
}

function Resolve-PythonLauncher {
    foreach ($candidate in @(
        @{ Command = "py"; PrefixArgs = @("-3") },
        @{ Command = "python"; PrefixArgs = @() },
        @{ Command = "python3"; PrefixArgs = @() }
    )) {
        if (Get-Command $candidate.Command -ErrorAction SilentlyContinue) {
            return $candidate
        }
    }
    throw "No se encontro un launcher Python en host (`py`, `python` o `python3`)."
}

function Invoke-Bridge {
    param(
        [hashtable]$PythonLauncher,
        [string]$Server,
        [string]$Tool,
        [hashtable]$Arguments
    )

    $messages = @(
        (@{
                jsonrpc = "2.0"
                id = 1
                method = "initialize"
                params = @{
                    protocolVersion = "2025-06-18"
                    capabilities = @{}
                    clientInfo = @{ name = "readme-acceptance"; version = "1.0" }
                }
            } | ConvertTo-Json -Depth 20 -Compress),
        (@{
                jsonrpc = "2.0"
                id = 2
                method = "tools/call"
                params = @{
                    name = $Tool
                    arguments = $Arguments
                }
            } | ConvertTo-Json -Depth 20 -Compress)
    )

    $bridgeArgs = @()
    $bridgeArgs += $PythonLauncher.PrefixArgs
    $bridgeArgs += @(".codex\factory_mcp_bridge.py", "--server", $Server)

    $rawOutput = ($messages -join [Environment]::NewLine) | & $PythonLauncher.Command @bridgeArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Fallo el bridge MCP para ${Server}/${Tool}: $($rawOutput | Out-String)"
    }

    $responses = @()
    foreach ($line in (($rawOutput | Out-String) -split "`r?`n")) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }
        $responses += ($line | ConvertFrom-Json)
    }

    $last = $responses[-1]
    if ($last.PSObject.Properties.Name -contains "error") {
        throw "Error MCP en ${Server}/${Tool}: $($last.error.message)"
    }
    if ($last.result.isError) {
        $detail = $last.result.structuredContent | ConvertTo-Json -Depth 20
        throw "La tool ${Server}/${Tool} devolvio error: $detail"
    }
    return $last.result.structuredContent
}

function Add-PathArguments {
    param(
        [ref]$ArgumentList,
        [string]$Option,
        [System.IO.FileInfo[]]$Files
    )

    foreach ($file in $Files) {
        $ArgumentList.Value += @($Option, $file.FullName)
    }
}

function Write-ProjectManifest {
    param(
        [string]$ProjectIdValue,
        [string]$ProjectIdDash,
        [string]$WorkspaceName,
        [string]$CatalogName,
        [string]$DatabaseName
    )

    $content = Get-Content -LiteralPath $TemplateManifestPath -Raw
    $replacements = [ordered]@{
        "sample-project" = $ProjectIdValue
        "generic-domain" = "acceptance"
        "adb_sample_gold" = $DatabaseName
        "di-medallion-dev" = $WorkspaceName
        "dc-medallion-dev" = $CatalogName
        "medallion-orchestrator" = "$ProjectIdDash-orchestrator"
        "bucket-sample-landing" = "$ProjectIdDash-landing-external"
        "bucket-sample-gold" = "$ProjectIdDash-gold-refined"
        "sample-landing-asset" = "$ProjectIdDash-landing-asset"
        "agg_sample_resumen" = "lk_status_trf"
        "LOAD_AGG_SAMPLE_RESUMEN" = "LOAD_LK_STATUS_TRF"
        "database_user: app_gold" = "database_user: APP_GOLD"
        "source_system=sample/" = "source_system=trafico/"
    }

    foreach ($entry in $replacements.GetEnumerator()) {
        $content = $content.Replace($entry.Key, $entry.Value)
    }

    $content | Set-Content -LiteralPath $ManifestPath -Encoding UTF8
}

function Invoke-OciReadOnlySmoke {
    param(
        [string]$RepoRootValue,
        [string]$TestRootValue
    )

    $sourceConfig = Join-Path $TestRootValue "oci\config"
    $sourceKey = Join-Path $TestRootValue "oci\key.pem"
    if (-not (Test-Path -LiteralPath $sourceConfig) -or -not (Test-Path -LiteralPath $sourceKey)) {
        return [ordered]@{ status = "skipped"; reason = "No se encontraron config y key.pem en .test/oci." }
    }

    $tempRoot = Join-Path $RepoRootValue ".tmp_oci_cli\acceptance-smoke"
    $tempDir = Join-Path $tempRoot ([DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ"))
    New-Item -ItemType Directory -Force -Path $tempDir | Out-Null

    try {
        Copy-Item -LiteralPath $sourceConfig -Destination (Join-Path $tempDir "config") -Force
        Copy-Item -LiteralPath $sourceKey -Destination (Join-Path $tempDir "key.pem") -Force

        $configPath = Join-Path $tempDir "config"
        $configContent = Get-Content -LiteralPath $configPath -Raw
        $configContent = [regex]::Replace($configContent, '(?im)^\s*key_file\s*=.*$', 'key_file=/mnt/oci/key.pem')
        [System.IO.File]::WriteAllText($configPath, $configContent, [System.Text.UTF8Encoding]::new($false))

        $dockerArgs = @(
            "run", "--rm",
            "-v", "${tempDir}:/mnt/oci",
            "-e", "OCI_CLI_CONFIG_FILE=/mnt/oci/config",
            "-e", "OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING=True",
            "ghcr.io/oracle/oci-cli:latest",
            "os", "ns", "get"
        )

        $stdoutFile = Join-Path $tempDir "oci-smoke.stdout.log"
        $stderrFile = Join-Path $tempDir "oci-smoke.stderr.log"
        $process = Start-Process -FilePath "docker" -ArgumentList $dockerArgs -Wait -PassThru -NoNewWindow -RedirectStandardOutput $stdoutFile -RedirectStandardError $stderrFile
        $rawOutput = @(
            (Get-Content -LiteralPath $stdoutFile -Raw -ErrorAction SilentlyContinue),
            (Get-Content -LiteralPath $stderrFile -Raw -ErrorAction SilentlyContinue)
        ) -join [Environment]::NewLine
        if ($process.ExitCode -ne 0) {
            return [ordered]@{ status = "error"; command = "oci os ns get"; stderr = ($rawOutput | Out-String).Trim() }
        }

        $payload = ($rawOutput | Out-String | ConvertFrom-Json)
        return [ordered]@{ status = "ok"; command = "oci os ns get"; namespace_found = [bool]$payload.data }
    }
    finally {
        if (Test-Path -LiteralPath $tempDir) {
            Remove-Item -LiteralPath $tempDir -Recurse -Force
        }
    }
}

Push-Location $RepoRootResolved
try {
    $pythonLauncher = Resolve-PythonLauncher
    $projectIdDash = (($ProjectId.ToLowerInvariant() -replace '[^a-z0-9]+', '-') -replace '-+', '-').Trim('-')
    $projectIdSnake = (($ProjectId.ToLowerInvariant() -replace '[^a-z0-9]+', '_') -replace '_+', '_').Trim('_')
    $workspaceName = "ws-$projectIdDash-$Environment"
    $catalogName = "dc-$projectIdDash-$Environment"
    $databaseName = "adb_$projectIdSnake"

    Write-Step "setup-dev.ps1"
    & $SetupScript -RepoRoot $RepoRootResolved -ProjectId $ProjectId

    Write-Step "Staging de .test"
    $sourceRoot = Join-Path $TestRootResolved "source"
    $sqlFiles = @(Get-ChildItem -LiteralPath $sourceRoot -Filter "*.sql" -File | Sort-Object Name)
    $docFiles = @(Get-ChildItem -LiteralPath $sourceRoot -File | Where-Object { $_.Extension -in @(".doc", ".docx", ".pdf", ".txt") } | Sort-Object Name)
    $exportFiles = @(Get-ChildItem -LiteralPath $sourceRoot -Filter "*.csv" -File | Sort-Object Name)

    $stageArgs = @("--project-id", $ProjectId, "--environment", $Environment, "--adb-name", $databaseName)
    Add-PathArguments -ArgumentList ([ref]$stageArgs) -Option "--sql-source" -Files $sqlFiles
    Add-PathArguments -ArgumentList ([ref]$stageArgs) -Option "--docs-source" -Files $docFiles
    Add-PathArguments -ArgumentList ([ref]$stageArgs) -Option "--exports-source" -Files $exportFiles
    $stageArgs += @("--data-source", (Join-Path $TestRootResolved "data"))
    $stageArgs += @("--oci-config-source", (Join-Path $TestRootResolved "oci\config"))
    $stageArgs += @("--oci-key-source", (Join-Path $TestRootResolved "oci\key.pem"))
    if ($ReplaceExistingCredentials) {
        $stageArgs += "--replace-existing"
    }
    & $StageScript @stageArgs

    Write-Step "Intake Docker-first"
    & $DockerPythonScript scripts/migration_intake.py --repo-root . --project-id $ProjectId

    Write-Step "project.medallion.yaml"
    Write-ProjectManifest -ProjectIdValue $ProjectId -ProjectIdDash $projectIdDash -WorkspaceName $workspaceName -CatalogName $catalogName -DatabaseName $databaseName

    Write-Step "Smoke OCI CLI read-only"
    $ociSmoke = Invoke-OciReadOnlySmoke -RepoRootValue $RepoRootResolved -TestRootValue $TestRootResolved

    Write-Step "Pruebas MCP bridge"
    $bridgeResults = @()
    $runtimeArgs = @{ environment = $Environment; runtime = "local"; oci_mode = "plan" }
    $sliceArgs = @{ workflow_id = "wf-$projectIdDash"; run_id = "run-$projectIdDash-001"; slice_key = "entity=trafico/business_date=2026-03-26/batch_id=001" }

    $bridgeResults += [ordered]@{
        server = "migration-intake-mcp"
        tool = "summarize_readiness"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "migration-intake-mcp" -Tool "summarize_readiness" -Arguments @{ project_id = $ProjectId }
    }

    $bridgeResults += [ordered]@{
        server = "oci-iam-mcp"
        tool = "create_compartment"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-iam-mcp" -Tool "create_compartment" -Arguments @{
            environment = $Environment
            runtime = "oci"
            oci_mode = "plan"
            compartment_name = "cmp-$projectIdDash"
            parent_compartment_id = "ocid1.tenancy.oc1..exampleuniqueID"
            description = "Readme acceptance plan"
        }
    }

    foreach ($bucketDef in @(
        @{ Name = "$projectIdDash-landing-external"; Layer = "landing_external"; Purpose = "landing" },
        @{ Name = "$projectIdDash-bronze-raw"; Layer = "bronze_raw"; Purpose = "bronze" },
        @{ Name = "$projectIdDash-silver-trusted"; Layer = "silver_trusted"; Purpose = "silver" },
        @{ Name = "$projectIdDash-gold-refined"; Layer = "gold_refined"; Purpose = "gold" }
    )) {
        $bridgeResults += [ordered]@{
            server = "oci-object-storage-mcp"
            tool = "create_bucket"
            result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-object-storage-mcp" -Tool "create_bucket" -Arguments ($runtimeArgs + @{
                    bucket_name = $bucketDef.Name
                    bucket_purpose = $bucketDef.Purpose
                    layer = $bucketDef.Layer
                    managed_by_factory = $true
                    ingestion_outside_flow = $false
                })
        }
    }

    $bridgeResults += [ordered]@{
        server = "oci-object-storage-mcp"
        tool = "upload_object"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-object-storage-mcp" -Tool "upload_object" -Arguments ($runtimeArgs + @{
                bucket_name = "$projectIdDash-landing-external"
                source_file = "workspace/migration-input/$ProjectId/data/LK/LK_STATUS_TRF.csv"
                object_name = "source_system=trafico/entity=lk_status_trf/business_date=2026-03-26/batch_id=001/LK_STATUS_TRF.csv"
            })
    }

    foreach ($toolName in @("create_autonomous_database", "bootstrap_control_plane", "create_database_user")) {
        $adbArgs = $runtimeArgs + @{ database_name = $databaseName; database_user = "APP_GOLD" }
        if ($toolName -eq "bootstrap_control_plane") {
            $adbArgs += @{ control_schema = "MDL_CTL"; control_user = "MDL_CTL" }
            $adbArgs += $sliceArgs
        }
        $bridgeResults += [ordered]@{
            server = "oci-autonomous-database-mcp"
            tool = $toolName
            result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-autonomous-database-mcp" -Tool $toolName -Arguments $adbArgs
        }
    }

    $bridgeResults += [ordered]@{
        server = "oci-autonomous-database-mcp"
        tool = "load_gold_object"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-autonomous-database-mcp" -Tool "load_gold_object" -Arguments ($runtimeArgs + $sliceArgs + @{
                database_name = $databaseName
                database_user = "APP_GOLD"
                object_name = "lk_status_trf"
                source_uri = @("oci://$projectIdDash-gold-refined@example-ns/exports/lk_status_trf/process_date=2026-03-26/*.csv")
                target_table = "APP_GOLD.STG_LK_STATUS_TRF"
                process_date = "2026-03-26"
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-autonomous-database-mcp"
        tool = "create_reprocess_request"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-autonomous-database-mcp" -Tool "create_reprocess_request" -Arguments ($runtimeArgs + @{
                database_name = $databaseName
                workflow_id = "wf-$projectIdDash"
                parent_run_id = "run-$projectIdDash-001"
                slice_key = "entity=trafico/business_date=2026-03-26/batch_id=001"
                requested_reason = "acceptance smoke replay"
                requested_by = "run_readme_acceptance.ps1"
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-network-mcp"
        tool = "create_vcn"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-network-mcp" -Tool "create_vcn" -Arguments ($runtimeArgs + @{
                compartment_id = "ocid1.compartment.oc1..example"
                vcn_name = "vcn-$projectIdDash-$Environment"
                cidr_block = @("10.50.0.0/16")
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-data-flow-mcp"
        tool = "create_application"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-data-flow-mcp" -Tool "create_application" -Arguments ($runtimeArgs + @{
                application_name = "$projectIdDash-landing-to-bronze"
                source_dir = "templates/data_flow/minimal_app"
                layer = "bronze_raw"
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-data-flow-mcp"
        tool = "run_application"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-data-flow-mcp" -Tool "run_application" -Arguments ($runtimeArgs + $sliceArgs + @{
                application_name = "$projectIdDash-landing-to-bronze"
                parameter = @("source_layer=landing_external", "target_layer=bronze_raw")
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-data-integration-mcp"
        tool = "create_workspace"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-data-integration-mcp" -Tool "create_workspace" -Arguments ($runtimeArgs + @{
                workspace_name = $workspaceName
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-data-integration-mcp"
        tool = "create_task_from_dataflow"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-data-integration-mcp" -Tool "create_task_from_dataflow" -Arguments ($runtimeArgs + @{
                workspace_name = $workspaceName
                workspace_id = "ocid1.disworkspace.oc1..example"
                task_name = "run-$projectIdDash-landing-to-bronze"
                application_name = "$projectIdDash-landing-to-bronze"
                application_id = "ocid1.dataflowapplication.oc1..example"
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-data-integration-mcp"
        tool = "create_pipeline"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-data-integration-mcp" -Tool "create_pipeline" -Arguments ($runtimeArgs + @{
                workspace_name = $workspaceName
                pipeline_name = "$projectIdDash-pipeline"
                task = @("run-$projectIdDash-landing-to-bronze")
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-data-catalog-mcp"
        tool = "create_catalog"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-data-catalog-mcp" -Tool "create_catalog" -Arguments ($runtimeArgs + @{
                catalog_name = $catalogName
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-data-catalog-mcp"
        tool = "import_openlineage"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-data-catalog-mcp" -Tool "import_openlineage" -Arguments ($runtimeArgs + @{
                catalog_name = $catalogName
                lineage_name = "$projectIdDash-lineage"
                from_json_file = "templates/lineage/openlineage.sample.json"
            })
    }

    $bridgeResults += [ordered]@{
        server = "oci-data-quality-mcp"
        tool = "profile_bucket_data"
        result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-data-quality-mcp" -Tool "profile_bucket_data" -Arguments ($runtimeArgs + @{
                bucket_name = "$projectIdDash-landing-external"
            })
    }

    foreach ($vaultTool in @(
        @{ Name = "create_vault"; Arguments = @{ vault_name = "vault-$projectIdDash-$Environment" } },
        @{ Name = "create_secret"; Arguments = @{ vault_name = "vault-$projectIdDash-$Environment"; secret_name = "adb-password" } }
    )) {
        $bridgeResults += [ordered]@{
            server = "oci-vault-mcp"
            tool = $vaultTool.Name
            result = Invoke-Bridge -PythonLauncher $pythonLauncher -Server "oci-vault-mcp" -Tool $vaultTool.Name -Arguments ($runtimeArgs + $vaultTool.Arguments)
        }
    }

    Write-Step "validate_factory"
    & $DockerPythonScript scripts/validate_factory.py --repo-root .

    $stageReport = Get-Content -LiteralPath $StageReportPath -Raw | ConvertFrom-Json
    $inventory = Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json
    $context = Get-Content -LiteralPath $ContextPath -Raw | ConvertFrom-Json

    $report = [ordered]@{
        generated_at_utc = [DateTime]::UtcNow.ToString("o")
        project_id = $ProjectId
        environment = $Environment
        setup = [ordered]@{
            codex_config = ".codex/config.toml"
            python_launcher = $pythonLauncher.Command
        }
        staging = [ordered]@{
            status = $stageReport.status
            report_path = "workspace/migration-input/$ProjectId/_inventory/stage-report.json"
            errors = @($stageReport.errors)
        }
        intake = [ordered]@{
            ready_for_scaffold = $inventory.ready_for_scaffold
            blockers = @($inventory.blockers)
            warnings = @($inventory.warnings)
            next_steps = @($context.next_steps)
        }
        project_manifest = [ordered]@{
            path = "workspace/migration-input/$ProjectId/project.medallion.yaml"
            database_name = $databaseName
            workspace_name = $workspaceName
            catalog_name = $catalogName
        }
        oci_cli_smoke = $ociSmoke
        bridge_tests = $bridgeResults
        validate_factory = [ordered]@{ status = "ok" }
        notes = @(
            "La fixture .test no incluye wallet de Autonomous, por lo que esta aceptacion no ejecuta apply real sobre ADB.",
            "El flujo evaluado mantiene el default end-to-end hasta gold_adb."
        )
    }

    $report | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $AcceptanceReportPath -Encoding UTF8

    Write-Step "Aceptacion completada"
    Write-Host "Reporte JSON: $AcceptanceReportPath"
}
finally {
    Pop-Location
}
