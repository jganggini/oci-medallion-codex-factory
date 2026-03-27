param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ForwardedArgs
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Split-Path -Parent $PSScriptRoot)
$RepoRootResolved = (Resolve-Path -LiteralPath $RepoRoot).Path
$PathOptions = @(
    "--sql-source",
    "--scripts-source",
    "--data-source",
    "--docs-source",
    "--references-source",
    "--ddl-source",
    "--samples-source",
    "--exports-source",
    "--mappings-source",
    "--notes-source",
    "--dataflow-jar-source",
    "--oci-config-source",
    "--oci-key-source",
    "--wallet-source"
)

function Convert-ToContainerRepoPath {
    param([string]$PathValue)

    $relativePath = $PathValue.Substring($RepoRootResolved.Length).TrimStart("\", "/")
    if ([string]::IsNullOrWhiteSpace($relativePath)) {
        return "/workspace"
    }
    return "/workspace/" + ($relativePath -replace "\\", "/")
}

function Resolve-StagePath {
    param([string]$RawPath)

    try {
        return (Resolve-Path -LiteralPath $RawPath -ErrorAction Stop).Path
    }
    catch {
        $candidate = Join-Path $RepoRootResolved $RawPath
        return (Resolve-Path -LiteralPath $candidate -ErrorAction Stop).Path
    }
}

$MountArgs = @()
$ScriptArgs = @("--repo-root", "/workspace")
$MountIndex = 0

for ($i = 0; $i -lt $ForwardedArgs.Count; $i++) {
    $token = $ForwardedArgs[$i]

    if ($token -eq "--repo-root") {
        $i++
        continue
    }

    if ($PathOptions -contains $token) {
        if ($i + 1 -ge $ForwardedArgs.Count) {
            throw "Falta el valor para $token"
        }

        $resolvedPath = Resolve-StagePath -RawPath $ForwardedArgs[++$i]
        if ($resolvedPath.StartsWith($RepoRootResolved, [System.StringComparison]::OrdinalIgnoreCase)) {
            $containerPath = Convert-ToContainerRepoPath -PathValue $resolvedPath
        }
        else {
            $containerMount = "/mnt/stage/$MountIndex"
            if (Test-Path -LiteralPath $resolvedPath -PathType Container) {
                $mountSource = $resolvedPath
                $containerPath = $containerMount
            }
            else {
                $mountSource = Split-Path -Parent $resolvedPath
                $containerPath = "$containerMount/" + (Split-Path -Leaf $resolvedPath)
            }
            $MountArgs += @("-v", "${mountSource}:${containerMount}:ro")
            $MountIndex += 1
        }

        $ScriptArgs += @($token, $containerPath)
        continue
    }

    $ScriptArgs += $token
}

Push-Location $RepoRootResolved
try {
    & docker compose run --rm -e "HOST_REPO_ROOT=$RepoRootResolved" @MountArgs oci-runner python scripts/stage_local_assets.py @ScriptArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
