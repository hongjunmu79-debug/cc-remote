<#
.SYNOPSIS
    Adds/removes the cc-remote Windows Defender Firewall rule.

.DESCRIPTION
    The rule opens only the selected relay TCP port to the LocalSubnet scope —
    never to any-remote or the public internet. The default Windows scope is
    Any, so an explicit LocalSubnet scope is required; without it a remote
    attacker who can reach the machine could hit the login/WS endpoints.

    The rule also requires the program to be the install root's runtime
    python.exe and is only registered for the Private and Domain profiles
    (not Public), matching a LAN deployment.

.PARAMETER Port
    TCP port to open (the relay port, default 8765).

.PARAMETER InstallRoot
    Install root (used for the program filter).

.PARAMETER Remove
    Remove the existing rule instead of adding it.

.EXAMPLE
    & firewall.ps1 -Port 8765 -InstallRoot C:\Users\alice\cc-remote
#>
[CmdletBinding()]
param(
    [int]$Port = 8765,
    [string]$InstallRoot,
    [switch]$Remove
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

if (-not $InstallRoot) { $InstallRoot = Join-Path $env:LOCALAPPDATA "cc-remote" }

if ($Remove) {
    # Check ownership before asking for elevation. An isolated installation
    # must not prompt to remove another installation's rule on the same port.
    $candidateRules = Get-NetFirewallRule -DisplayName "cc-remote-$Port" -ErrorAction SilentlyContinue
    if (-not $candidateRules) { exit 0 }
    $ownedPrograms = @(
        (Join-Path $InstallRoot 'runtime\python\python.exe'),
        (Join-Path $InstallRoot 'runtime\.venv\Scripts\python.exe')
    )
    $ownedRules = $candidateRules | Get-NetFirewallApplicationFilter |
        Where-Object { $_.Program -in $ownedPrograms }
    if (-not $ownedRules) { exit 0 }
}

$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
$isAdministrator = $principal.IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)
if (-not $isAdministrator) {
    $arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`" -Port $Port -InstallRoot `"$InstallRoot`""
    if ($Remove) { $arguments += " -Remove" }
    $elevated = Start-Process powershell.exe -Verb RunAs -ArgumentList $arguments -WindowStyle Hidden -PassThru
    $null = $elevated.Handle
    $elevated.WaitForExit()
    exit $elevated.ExitCode
}

$ruleName = "cc-remote-$Port"
$launcherPath = Join-Path $InstallRoot "runtime\.venv\Scripts\python.exe"
$pythonPath = Join-Path $InstallRoot "runtime\python\python.exe"
if (-not (Test-Path -LiteralPath $pythonPath)) { $pythonPath = $launcherPath }

if ($Remove) {
    $existing = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
    if ($existing) {
        $owned = $existing | Get-NetFirewallApplicationFilter | Where-Object { $_.Program -in @($pythonPath, $launcherPath) }
        if ($owned) { Remove-NetFirewallRule -DisplayName $ruleName }
        Write-Host "[cc-remote] removed firewall rule $ruleName"
    } else {
        Write-Host "[cc-remote] firewall rule $ruleName not present; nothing to remove"
    }
    exit 0
}

if (-not (Test-Path $pythonPath)) {
    throw "runtime venv not found: $pythonPath (run install.ps1 first)"
}
if ($Port -lt 1 -or $Port -gt 65535) {
    throw "invalid port: $Port"
}

# Remove any stale rule for this port so reinstall is idempotent.
$existing = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
if ($existing) { Remove-NetFirewallRule -DisplayName $ruleName }

New-NetFirewallRule `
    -DisplayName $ruleName `
    -Description "cc-remote relay (LAN only, LocalSubnet scope, port $Port)" `
    -Direction Inbound `
    -Action Allow `
    -Profile Private, Domain `
    -Program $pythonPath `
    -Protocol TCP `
    -LocalPort $Port `
    -RemoteAddress LocalSubnet | Out-Null

Write-Host "[cc-remote] added firewall rule $ruleName (TCP $Port, LocalSubnet, Private/Domain profiles)"
