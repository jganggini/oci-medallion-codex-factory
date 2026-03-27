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
    "--source-file",
    "--contract-file",
    "--result-path",
    "--working-directory",
    "--config-source-file"
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
    & docker compose run --rm -e "HOST_REPO_ROOT=$RepoRoot" oci-runner python $ContainerScript @ContainerArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
