param(
    [string]$BootstrapServer = "127.0.0.1:9092",
    [string]$EndpointUrl = "http://localhost:4566"
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$jobScript = Join-Path $PSScriptRoot "..\main\kafka_to_s3_raw_parequet.py"
$processDirectory = Join-Path $projectRoot "data\processes"
$pidFile = Join-Path $processDirectory "kafka_to_s3_raw_parequet.pid"
$stdoutLog = Join-Path $processDirectory "kafka_to_s3_raw_parequet.out.log"
$stderrLog = Join-Path $processDirectory "kafka_to_s3_raw_parequet.err.log"

if (-not (Test-Path $python -PathType Leaf)) {
    throw "Project Python environment was not found: $python"
}
if (-not (Test-Path $jobScript -PathType Leaf)) {
    throw "Streaming script was not found: $jobScript"
}
$jobScript = (Resolve-Path $jobScript).Path

if (Test-Path $pidFile -PathType Leaf) {
    $savedPid = 0
    if ([int]::TryParse((Get-Content $pidFile -Raw).Trim(), [ref]$savedPid)) {
        $existing = Get-CimInstance Win32_Process `
            -Filter "ProcessId = $savedPid" `
            -ErrorAction SilentlyContinue
        if (
            $existing -and
            $existing.ExecutablePath -eq $python -and
            $existing.CommandLine.IndexOf(
                $jobScript,
                [StringComparison]::OrdinalIgnoreCase
            ) -ge 0
        ) {
            Write-Output "Kafka-to-Hudi stream is already running (PID $savedPid)."
            exit 0
        }
    }
    Remove-Item -LiteralPath $pidFile -Force
}

New-Item -ItemType Directory -Path $processDirectory -Force | Out-Null
$arguments = @(
    "-u"
    "`"$jobScript`""
    "--bootstrap-server"
    "`"$BootstrapServer`""
    "--endpoint-url"
    "`"$EndpointUrl`""
) -join " "

$process = Start-Process `
    -FilePath $python `
    -ArgumentList $arguments `
    -WorkingDirectory $projectRoot `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -WindowStyle Hidden `
    -PassThru

Set-Content -LiteralPath $pidFile -Value $process.Id -Encoding ascii
Start-Sleep -Seconds 3
$process.Refresh()
if ($process.HasExited) {
    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
    $details = Get-Content $stderrLog -Tail 30 -ErrorAction SilentlyContinue
    throw "Streaming job exited during startup (code $($process.ExitCode)).`n$details"
}

Write-Output "Kafka-to-Hudi stream started (PID $($process.Id))."
Write-Output "Logs: $stdoutLog and $stderrLog"
