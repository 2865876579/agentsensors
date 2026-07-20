$ErrorActionPreference = "Stop"

$taskName = "XiaoanPcAgent"
$workDir = "D:\agentcodex_sensors"
$pythonw = "C:\Users\28658\AppData\Local\Programs\Python\Python311\pythonw.exe"
$agent = Join-Path $workDir "pc_agent.py"
$userId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

if (-not (Test-Path -LiteralPath $pythonw)) {
    throw "Python executable not found: $pythonw"
}
if (-not (Test-Path -LiteralPath $agent)) {
    throw "PC Agent not found: $agent"
}

$action = New-ScheduledTaskAction -Execute $pythonw -Argument ('"' + $agent + '"') -WorkingDirectory $workDir
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $userId
$principal = New-ScheduledTaskPrincipal -UserId $userId -LogonType Interactive -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description "Xiaoan interactive desktop automation agent" `
    -Force | Out-Null

Write-Output "$taskName registered for $userId with highest privileges."
