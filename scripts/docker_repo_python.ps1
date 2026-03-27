param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$ScriptPath,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ScriptArgs
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path -LiteralPath (Split-Path -Parent $PSScriptRoot)).Path
$PathOptions = @(
    "--source-dir",
    "--dependency-root",
    "--from-json-file",
    "--archive-source-file",
    "--lineage-file",
    "--from-outbox-file",
    "--wallet-dir",
    "--sql-file",
    "--sql-dir",
    "--merge-sql-file",
    "--source-file",
    "--contract-file",
    "--result-path",
    "--working-directory",
    "--config-source-file"
)
$PassThroughEnvVars = @(
    "DB_USER",
    "DB_PASSWORD",
    "APP_GOLD_PASSWORD",
    "MDL_CTL_PASSWORD",
    "DB_WALLET_PASSWORD",
    "ADW_USER",
    "ADW_DSN",
    "OCI_MEDALLION_MIRROR_COMPARTMENT_NAME",
    "OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING"
)

function Convert-ToContainerRepoPath {
    param([string]$PathValue)

    $candidate = if ([System.IO.Path]::IsPathRooted($PathValue)) {
        [System.IO.Path]::GetFullPath($PathValue)
    }
    else {
        [System.IO.Path]::GetFullPath((Join-Path $RepoRoot $PathValue))
    }

    if (-not $candidate.StartsWith($RepoRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "La ruta '$PathValue' esta fuera del repo. Primero copiala con scripts/docker_stage_assets.ps1 o usa una ruta dentro de workspace/ o .local/."
    }

    $relativePath = $candidate.Substring($RepoRoot.Length).TrimStart("\", "/")
    if ([string]::IsNullOrWhiteSpace($relativePath)) {
        return "/workspace"
    }
    return "/workspace/" + ($relativePath -replace "\\", "/")
}

$ContainerScript = Convert-ToContainerRepoPath -PathValue $ScriptPath
$ContainerArgs = @()

for ($i = 0; $i -lt $ScriptArgs.Count; $i++) {
    $token = $ScriptArgs[$i]

    if ($token -eq "--repo-root") {
        if ($i + 1 -lt $ScriptArgs.Count -and -not $ScriptArgs[$i + 1].StartsWith("--")) {
            $i++
        }
        $ContainerArgs += @("--repo-root", "/workspace")
        continue
    }

    if ($PathOptions -contains $token) {
        if ($i + 1 -ge $ScriptArgs.Count) {
            throw "Falta el valor para $token"
        }
        $ContainerArgs += @($token, (Convert-ToContainerRepoPath -PathValue $ScriptArgs[++$i]))
        continue
    }

    $ContainerArgs += $token
}

Push-Location $RepoRoot
try {
    $DockerArgs = @("compose", "run", "--rm", "-e", "HOST_REPO_ROOT=$RepoRoot")
    foreach ($envName in $PassThroughEnvVars) {
        $envValue = [Environment]::GetEnvironmentVariable($envName)
        if (-not [string]::IsNullOrWhiteSpace($envValue)) {
            $DockerArgs += @("-e", "$envName=$envValue")
        }
    }
    $DockerArgs += @("oci-runner", "python", $ContainerScript)
    $DockerArgs += $ContainerArgs
    & docker @DockerArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
