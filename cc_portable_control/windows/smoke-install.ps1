<# CI-only installed lifecycle regression; no services or firewall mutations. #>
[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$StageDir,
      [Parameter(Mandatory=$true)][string]$TestDirectory)
$ErrorActionPreference = 'Stop'
$testRoot = [IO.Path]::GetFullPath($TestDirectory)
if (Test-Path -LiteralPath $testRoot) { throw 'smoke test requires a new directory' }
New-Item -ItemType Directory -Path $testRoot | Out-Null
$install = Join-Path $testRoot ('Install path ' + [char]0x5B89 + [char]0x88C5)
$metadata = Get-Content -LiteralPath (Join-Path $StageDir 'payload\deploy\release-metadata.json') -Raw | ConvertFrom-Json
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $PSScriptRoot 'build-installer.ps1') -StageDir $StageDir -DistributionVersion $metadata.distribution_version -ProductVersion $metadata.product_version -OutputDir $testRoot -NoServices -IsolatedSmokeTest -OutputName 'smoke-setup'
if ($LASTEXITCODE -ne 0) { throw 'smoke installer compilation failed' }
function Run-AndCheck([string]$Exe, [string[]]$Arguments) {
    $p = Start-Process -FilePath $Exe -ArgumentList $Arguments -WindowStyle Hidden -PassThru
    $null = $p.Handle
    $p.WaitForExit()
    if ($p.ExitCode -ne 0) { throw "lifecycle executable failed: $Exe (exit $($p.ExitCode))" }
    $p.Dispose()
}
Run-AndCheck (Join-Path $testRoot 'smoke-setup.exe') @('/VERYSILENT','/SUPPRESSMSGBOXES','/NORESTART',('/DIR="' + $install + '"'))
$config = Join-Path $install 'config\.env'
$configHash = (Get-FileHash -LiteralPath $config).Hash
& (Join-Path $install 'runtime\.venv\Scripts\python.exe') -c 'from cc_remote.relay.server import create_app; from cc_remote.wrapper.machine import WrapperMachine; print(1)'
if ($LASTEXITCODE -ne 0) { throw 'installed runtime import failed' }
$registered = @(Get-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*' | Where-Object { $_.InstallLocation -and $_.InstallLocation.TrimEnd('\') -eq $install })
if (-not $registered) { throw 'not registered in Installed Apps' }
Run-AndCheck (Join-Path $install 'unins000.exe') @('/VERYSILENT','/SUPPRESSMSGBOXES','/NORESTART')
for ($i=0; $i -lt 20 -and (Test-Path -LiteralPath (Join-Path $install 'unins000.exe')); $i++) { Start-Sleep -Milliseconds 250 }
if ((Get-FileHash -LiteralPath $config).Hash -ne $configHash) { throw 'uninstall altered saved config' }
foreach ($removed in @('runtime','releases','unins000.exe')) {
    if (Test-Path -LiteralPath (Join-Path $install $removed)) { throw "uninstall left $removed" }
}
$remaining = @(Get-ItemProperty 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\*' | Where-Object { $_.InstallLocation -and $_.InstallLocation.TrimEnd('\') -eq $install })
if ($remaining) { throw 'uninstall left its registry entry' }
Write-Host 'Installed lifecycle passed: install, imports, registry, uninstall, config preservation.'
