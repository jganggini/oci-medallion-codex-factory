param(
    [Parameter(Mandatory = $true)]
    [string]$RemoteUrl,
    [string]$Branch = "main",
    [string]$CommitMessage = "Initial commit: oci medallion codex factory"
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

Push-Location $repoRoot
try {
    if (-not (Test-Path ".git")) {
        throw "La carpeta no es un repositorio Git: $repoRoot"
    }

    git add .

    $status = git status --short
    if ($status) {
        git commit -m $CommitMessage
    }

    $hasOrigin = git remote | Where-Object { $_ -eq "origin" }
    if ($hasOrigin) {
        git remote set-url origin $RemoteUrl
    } else {
        git remote add origin $RemoteUrl
    }

    git push -u origin $Branch
}
finally {
    Pop-Location
}
