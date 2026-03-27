param(
    [string]$RepoRoot = $PSScriptRoot,
    [string]$ProjectId = "sample-project"
)

$ErrorActionPreference = "Stop"
$RepoRootResolved = (Resolve-Path -LiteralPath $RepoRoot).Path
$CodexTemplate = Join-Path $RepoRootResolved ".codex\config.template.toml"
$CodexConfig = Join-Path $RepoRootResolved ".codex\config.toml"

function Resolve-CodexPythonLauncher {
    $candidates = @(
        @{ Command = "py"; PrefixArgs = '"-3", ".codex/factory_mcp_bridge.py"' },
        @{ Command = "python"; PrefixArgs = '".codex/factory_mcp_bridge.py"' },
        @{ Command = "python3"; PrefixArgs = '".codex/factory_mcp_bridge.py"' }
    )

    foreach ($candidate in $candidates) {
        if (Get-Command $candidate.Command -ErrorAction SilentlyContinue) {
            return $candidate
        }
    }

    throw "No se encontro un launcher Python en host (`py`, `python` o `python3`). Los runtimes siguen siendo Docker-first, pero Codex necesita ese launcher local para levantar el bridge MCP del factory."
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker es un prerrequisito. Instala Docker Desktop o Docker Engine con Docker Compose antes de ejecutar setup-dev.ps1."
}

& docker version *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker esta instalado pero no responde. Asegurate de que el daemon o Docker Desktop este corriendo antes de ejecutar setup-dev.ps1."
}

& docker compose version *> $null
if ($LASTEXITCODE -ne 0) {
    throw "No se encontro `docker compose`. Instala el plugin de Docker Compose antes de ejecutar setup-dev.ps1."
}

if (-not (Test-Path -LiteralPath $CodexTemplate)) {
    throw "No se encontro la plantilla MCP en $CodexTemplate"
}

$pythonLauncher = Resolve-CodexPythonLauncher
$codexConfigContent = (Get-Content -LiteralPath $CodexTemplate -Raw).
    Replace("__CODEX_PYTHON_COMMAND__", $pythonLauncher.Command).
    Replace("__CODEX_BRIDGE_PREFIX_ARGS__", $pythonLauncher.PrefixArgs)
$codexConfigContent | Set-Content -LiteralPath $CodexConfig -Encoding UTF8

Push-Location $RepoRootResolved
try {
    & docker compose up -d --build dev-base oci-runner dataflow-local | Out-Null
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

Write-Host "Runtime Docker levantado, workspace inicializado y MCP local sincronizado en .codex/config.toml para $ProjectId"
Write-Host "Launcher MCP detectado para Codex: $($pythonLauncher.Command)"
Write-Host "Si Codex, Cursor o VS Code ya estaban abiertos, recarga el proyecto para que tomen la configuracion local del factory."
