param(
    [Parameter(Mandatory = $true)]
    [string]$JobPath,
    [string]$ProcessDate,
    [string]$ProjectId = "sample-project",
    [string]$Environment = "dev",
    [string[]]$JarPath = @(),
    [string[]]$SparkConf = @(),
    [string[]]$JobArg = @(),
    [switch]$BuildImage
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $repoRoot
try {
    if ($BuildImage) {
        docker compose build dataflow-local
    }

    $sparkSubmitArgs = @("spark-submit")

    if ($JarPath.Count -gt 0) {
        $jarList = ($JarPath | ForEach-Object {
            if ([System.IO.Path]::IsPathRooted($_)) { $_ } else { Join-Path $repoRoot $_ }
        }) -join ","
        $sparkSubmitArgs += @("--jars", $jarList)
    }

    foreach ($conf in $SparkConf) {
        $sparkSubmitArgs += @("--conf", $conf)
    }

    $jobFullPath = if ([System.IO.Path]::IsPathRooted($JobPath)) { $JobPath } else { Join-Path $repoRoot $JobPath }
    $sparkSubmitArgs += $jobFullPath

    if ($ProcessDate) {
        $sparkSubmitArgs += @("--process-date", $ProcessDate)
    }
    if ($ProjectId) {
        $sparkSubmitArgs += @("--project-id", $ProjectId)
    }
    if ($Environment) {
        $sparkSubmitArgs += @("--environment", $Environment)
    }
    if ($JobArg.Count -gt 0) {
        $sparkSubmitArgs += $JobArg
    }

    docker compose run --rm dataflow-local @sparkSubmitArgs
}
finally {
    Pop-Location
}
