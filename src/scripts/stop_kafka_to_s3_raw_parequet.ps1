$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
$jobScript = Join-Path $PSScriptRoot "..\main\kafka_to_s3_raw_parequet.py"
$pidFile = Join-Path $projectRoot "data\processes\kafka_to_s3_raw_parequet.pid"

if (-not (Test-Path $pidFile -PathType Leaf)) {
    Write-Output "Kafka-to-Hudi stream is not running (no PID file)."
    exit 0
}

$rootPid = 0
$jobScript = (Resolve-Path $jobScript).Path
if (-not [int]::TryParse((Get-Content $pidFile -Raw).Trim(), [ref]$rootPid)) {
    throw "Invalid process ID in $pidFile; inspect and remove that file manually."
}

$allProcesses = @(Get-CimInstance Win32_Process)
$root = $allProcesses | Where-Object ProcessId -eq $rootPid
if (-not $root) {
    Remove-Item -LiteralPath $pidFile -Force
    Write-Output "The recorded stream process is no longer running."
    exit 0
}
if (
    $root.ExecutablePath -ne $python -or
    $root.CommandLine.IndexOf(
        $jobScript,
        [StringComparison]::OrdinalIgnoreCase
    ) -lt 0
) {
    throw "PID $rootPid no longer belongs to this streaming job; refusing to stop it."
}

$processIds = [System.Collections.Generic.List[int]]::new()
$processIds.Add($rootPid)
$added = $true
while ($added) {
    $added = $false
    foreach ($candidate in $allProcesses) {
        $parentId = [int]$candidate.ParentProcessId
        $candidateId = [int]$candidate.ProcessId
        if ($processIds.Contains($parentId) -and -not $processIds.Contains($candidateId)) {
            $processIds.Add($candidateId)
            $added = $true
        }
    }
}

for ($index = $processIds.Count - 1; $index -ge 0; $index--) {
    Stop-Process -Id $processIds[$index] -Force -ErrorAction SilentlyContinue
}
Remove-Item -LiteralPath $pidFile -Force
Write-Output "Kafka-to-Hudi stream process tree stopped."
