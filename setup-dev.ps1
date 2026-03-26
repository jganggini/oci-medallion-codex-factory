param(
    [string]$RepoRoot = $PSScriptRoot,
    [string]$ProjectId = "sample-project"
)

$ErrorActionPreference = "Stop"

function Resolve-Python {
    $candidates = @("py", "python")
    foreach ($candidate in $candidates) {
        $command = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($command) {
            return $candidate
        }
    }

    throw "No se encontro Python en PATH. Instala Python 3.11+ antes de ejecutar el setup."
}

$python = Resolve-Python
$initScript = Join-Path $RepoRoot "scripts\init_workspace.py"
$validateScript = Join-Path $RepoRoot "scripts\validate_factory.py"

if ($python -eq "py") {
    & py -3 $initScript --repo-root $RepoRoot --project-id $ProjectId
    & py -3 $validateScript --repo-root $RepoRoot
} else {
    & python $initScript --repo-root $RepoRoot --project-id $ProjectId
    & python $validateScript --repo-root $RepoRoot
}

Write-Host "Workspace inicializado y validado para $ProjectId"
