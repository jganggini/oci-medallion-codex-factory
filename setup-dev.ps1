param(
    [string]$RepoRoot = $PSScriptRoot,
    [string]$ProjectId = "sample-project"
)

$ErrorActionPreference = "Stop"
$RepoRootResolved = (Resolve-Path -LiteralPath $RepoRoot).Path

Push-Location $RepoRootResolved
try {
    & docker compose up -d dev-base oci-runner dataflow-local | Out-Null
    & "$RepoRootResolved\scripts\docker_repo_python.ps1" scripts/init_workspace.py --repo-root . --project-id $ProjectId
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    & "$RepoRootResolved\scripts\docker_repo_python.ps1" scripts/validate_factory.py --repo-root .
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}
finally {
    Pop-Location
}

Write-Host "Workspace inicializado y validado para $ProjectId usando Docker"
