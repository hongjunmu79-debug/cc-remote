<#
.SYNOPSIS
    First-run configuration wizard for the packaged cc-remote Windows install.

.DESCRIPTION
    Collects and validates the relay+wrapper settings for a LAN machine, then
    renders a combined .env with freshly generated cryptographic secrets.

    Validation and rendering are delegated to win_config.py (run with the
    runtime venv's python) so this PowerShell UI and the zero-token unit tests
    share one source of truth. No machine-specific value is hardcoded in the
    distribution; the install root and account are always taken from the
    caller/environment at runtime.

    This script only runs when config/.env does not yet exist. Upgrades never
    touch the existing config: install.ps1 validates it with
    validate_preserved_config and keeps it byte-for-byte.

.PARAMETER VenvPython
    Absolute path to the runtime venv's python.exe used to run win_config.py.

.PARAMETER InstallRoot
    Install root. Config is written to <InstallRoot>\config\.env.

.PARAMETER LoginPassword
    Optional fallback Web login password (min 16 chars when provided).

.PARAMETER MachineName
    Unique machine id for this wrapper (used as CC_REMOTE_MACHINE_ID).

.PARAMETER Workspace
    Default working directory for new sessions (absolute path).

.PARAMETER PublicOrigin
    PUBLIC_ORIGIN, e.g. http://192.168.1.50:8765 or https://remote.example.com.

.PARAMETER RelayPort
    LAN port for the relay. Default 8765.

.PARAMETER AllowInsecureHttp
    Permit plain-http origins. Default $true (LAN only; win_config.py still
    rejects public http origins regardless of this flag).

.PARAMETER ConfigFile
    Optional seed file (.env-style key=value or JSON) whose non-secret values
    prefill the wizard. Secrets are never read from it; they are always
    regenerated on first run.

.PARAMETER StaticDir
    Absolute path to the built web client (WEB_STATIC_DIR). Defaults to
    <InstallRoot>\releases\current\web\dist (installed layout). The portable
    launcher passes <portable-root>\payload\web\dist instead.

.PARAMETER Unattended
    Do not prompt; fail instead when a required value is missing or invalid.

.EXAMPLE
    .\config-first-run.ps1 -VenvPython C:\Users\alice\cc-remote\runtime\.venv\Scripts\python.exe -InstallRoot C:\Users\alice\cc-remote -Unattended -LoginPassword "a-strong-16-char-password" -MachineName desktop-1 -PublicOrigin http://192.168.1.50:8765
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$VenvPython,
    [Parameter(Mandatory = $true)][string]$InstallRoot,
    [string]$LoginPassword = "",
    [string]$MachineName = "",
    [string]$Workspace = "",
    [string]$PublicOrigin = "",
    [int]$RelayPort = 8765,
    [switch]$AllowInsecureHttp,
    [string]$ConfigFile = "",
    [string]$StaticDir = "",
    [switch]$Unattended
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0
# Python stdout is captured below. Pin both ends of that pipe rather than
# relying on the Windows system locale (cp1252 cannot encode Chinese paths).
$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUTF8 = '1'
[Console]::OutputEncoding = New-Object Text.UTF8Encoding($false)
$OutputEncoding = [Console]::OutputEncoding

function Write-Step { param([string]$Message) Write-Host "[cc-remote] $Message" -ForegroundColor Cyan }

function Get-CommandSource {
    param([string]$Name)
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    return $null
}

function Read-NonEmpty {
    param(
        [string]$Prompt,
        [string]$Default = "",
        [bool]$Secret = $false
    )
    $suffix = if ($Default) { " [$Default]" } else { "" }
    while ($true) {
        $value = Read-Host "$Prompt$suffix"
        if (-not $value -and $Default) { $value = $Default }
        if ($value) { return $value }
        if ($Unattended) { throw "missing required value: $Prompt" }
        Write-Host "  Value is required." -ForegroundColor Yellow
    }
}

function Read-Password {
    param([string]$Prompt)
    while ($true) {
        $value = Read-Host $Prompt
        if ($value) { return $value }
        if ($Unattended) { throw "missing required value: $Prompt" }
        Write-Host "  Value is required." -ForegroundColor Yellow
    }
}

function Get-LanIpCandidate {
    param([string]$Python, [string]$WinConfig)
    try {
        # Flatten Windows network facts and let win_config.py apply the same
        # deterministic ranking exercised by CI. A raw Get-NetIPAddress first
        # match commonly selects WSL/Hyper-V instead of the Wi-Fi adapter.
        $records = @()
        foreach ($configuration in @(Get-NetIPConfiguration -ErrorAction Stop)) {
            $adapter = Get-NetAdapter -InterfaceIndex $configuration.InterfaceIndex `
                -ErrorAction SilentlyContinue
            $route = Get-NetRoute -AddressFamily IPv4 -DestinationPrefix "0.0.0.0/0" `
                -InterfaceIndex $configuration.InterfaceIndex -ErrorAction SilentlyContinue |
                Sort-Object @{ Expression = { $_.RouteMetric + $_.InterfaceMetric } } |
                Select-Object -First 1
            $gateway = @($configuration.IPv4DefaultGateway | Select-Object -First 1)
            foreach ($address in @($configuration.IPv4Address)) {
                $records += [pscustomobject]@{
                    ip = [string]$address.IPAddress
                    gateway = if ($gateway.Count -gt 0) { [string]$gateway[0].NextHop } else { "" }
                    interface_alias = [string]$configuration.InterfaceAlias
                    interface_description = [string]$configuration.InterfaceDescription
                    adapter_status = if ($adapter) { [string]$adapter.Status } else { "Up" }
                    hardware_interface = if ($adapter) { [bool]$adapter.HardwareInterface } else { $false }
                    address_state = [string]$address.AddressState
                    skip_as_source = [bool]$address.SkipAsSource
                    metric = if ($route) { [int]($route.RouteMetric + $route.InterfaceMetric) } else { [int]::MaxValue }
                }
            }
        }
        if ($records.Count -eq 0) { return $null }
        $json = ConvertTo-Json -InputObject @($records) -Compress -Depth 4
        $selected = $json | & $Python $WinConfig "select-lan-ip" 2>$null |
            Select-Object -First 1
        $selectorExitCode = $LASTEXITCODE
        Write-Verbose "LAN selector considered $($records.Count) address(es); exit=$selectorExitCode selected=$selected"
        # Windows PowerShell 5 can leave LASTEXITCODE at -1 for a successful
        # native process used in a pipeline. The selector prints nothing on
        # failure, so a non-empty value is the reliable success contract.
        if ($selected) { return ([string]$selected).Trim() }
    } catch {
        Write-Warning "Automatic LAN address detection failed: $($_.Exception.Message)"
    }
    return $null
}

function New-StrongSecret {
    # Cryptographically strong 64-hex-char secret (256 bits of entropy).
    $bytes = New-Object byte[] 32
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $generator.GetBytes($bytes) } finally { $generator.Dispose() }
    return ([System.BitConverter]::ToString($bytes) -replace '-', '').ToLowerInvariant()
}

function Import-SeedFile {
    param([string]$Path)
    $result = @{}
    if (-not $Path -or -not (Test-Path $Path)) { return $result }
    $content = Get-Content $Path -Raw -ErrorAction Stop
    $json = $null
    try { $json = $content | ConvertFrom-Json -ErrorAction Stop } catch { $json = $null }
    if ($null -ne $json) {
        foreach ($property in $json.PSObject.Properties) {
            if ($null -ne $property.Value) { $result[$property.Name] = [string]$property.Value }
        }
        return $result
    }
    foreach ($line in ($content -split "`n")) {
        $line = $line.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) { continue }
        $parts = $line.Split("=", 2)
        $key = $parts[0].Trim()
        $value = $parts[1].Trim().Trim('"').Trim("'")
        $result[$key] = $value
    }
    return $result
}

# --- Resolve paths -----------------------------------------------------------
$configDir = Join-Path $InstallRoot "config"
$envPath = Join-Path $configDir ".env"
$winConfig = Join-Path $PSScriptRoot "win_config.py"

if (-not (Test-Path $winConfig)) { throw "win_config.py not found next to config-first-run.ps1" }
if (-not (Test-Path $VenvPython)) { throw "runtime venv python not found: $VenvPython" }

if (Test-Path $envPath) {
    Write-Step "Config already exists at $envPath; nothing to do (upgrades preserve it)."
    exit 0
}

New-Item -ItemType Directory -Force -Path $configDir | Out-Null

$seed = Import-SeedFile $ConfigFile

# --- Collect answers ----------------------------------------------------------
if (-not $MachineName) { $MachineName = $seed["CC_REMOTE_MACHINE_ID"] }
if (-not $MachineName -and $seed["MACHINE_NAME"]) { $MachineName = $seed["MACHINE_NAME"] }
if (-not $MachineName) {
    $default = $env:COMPUTERNAME
    if ($Unattended) { $MachineName = $default }
    else { $MachineName = Read-NonEmpty -Prompt "Machine name (unique id for this wrapper)" -Default $default }
}

if (-not $Workspace) { $Workspace = $seed["CC_CWD"] }
if (-not $Workspace -and $seed["WORKSPACE"]) { $Workspace = $seed["WORKSPACE"] }
if (-not $Workspace) {
    $candidates = @(
        (Join-Path $HOME "cc-remote-workspace"),
        (Join-Path $HOME "projects"),
        (Join-Path $HOME "Documents"),
        (Join-Path $HOME "Desktop")
    )
    $default = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
    if (-not $default) { $default = $candidates[0] }
    if ($Unattended) { $Workspace = $default }
    else { $Workspace = Read-NonEmpty -Prompt "Default workspace (absolute path, used for new sessions)" -Default $default }
}

if (-not $PublicOrigin) { $PublicOrigin = $seed["PUBLIC_ORIGIN"] }
if (-not $PublicOrigin) {
    $lanIp = Get-LanIpCandidate -Python $VenvPython -WinConfig $winConfig
    $default = if ($lanIp) { "http://$lanIp`:$RelayPort" } else { "http://127.0.0.1`:$RelayPort" }
    if ($Unattended) { $PublicOrigin = $default }
    else { $PublicOrigin = Read-NonEmpty -Prompt "Public origin (http://LAN-IP:8765 or https://your-domain)" -Default $default }
}

if (-not $LoginPassword) { $LoginPassword = $seed["LOGIN_PASSWORD"] }

if ($seed["RELAY_PORT"]) {
    $parsedPort = 0
    if ([int]::TryParse($seed["RELAY_PORT"], [ref]$parsedPort) -and $parsedPort -ge 1 -and $parsedPort -le 65535) {
        $RelayPort = $parsedPort
    }
}

# --- Validate + render via the shared Python source of truth ------------------
$claudeBin = if ($seed["CLAUDE_BIN"]) { $seed["CLAUDE_BIN"] } else { Get-CommandSource "claude" }
$codexBin = if ($seed["CC_REMOTE_CODEX_BIN"]) { $seed["CC_REMOTE_CODEX_BIN"] } else { Get-CommandSource "codex" }

if (-not $StaticDir) { $StaticDir = Join-Path $InstallRoot "releases\current\web\dist" }

$commonArgs = @(
    $winConfig,
    "render-env",
    "--machine-name", $MachineName,
    "--workspace", $Workspace,
    "--public-origin", $PublicOrigin,
    "--relay-port", "$RelayPort",
    "--state-dir", (Join-Path $InstallRoot "state"),
    "--work-root", (Join-Path $InstallRoot "state\work"),
    "--static-dir", $StaticDir
)
if ($LoginPassword) { $commonArgs += "--login-password"; $commonArgs += $LoginPassword }
if ($AllowInsecureHttp) { $commonArgs += "--insecure" }
if ($claudeBin) { $commonArgs += "--claude-bin"; $commonArgs += $claudeBin }
if ($codexBin) { $commonArgs += "--codex-bin"; $commonArgs += $codexBin }

# Validate first (no secrets involved) so we can re-prompt cleanly.
$previousErrorAction = $ErrorActionPreference
$ErrorActionPreference = "Continue"
try {
    $validationArgs = @(
        $winConfig,
        "validate-answers",
        "--machine-name", $MachineName,
        "--workspace", $Workspace,
        "--public-origin", $PublicOrigin,
        "--relay-port", "$RelayPort"
    )
    if ($LoginPassword) { $validationArgs += "--login-password"; $validationArgs += $LoginPassword }
    if ($AllowInsecureHttp) { $validationArgs += "--insecure" }
    $validation = & $VenvPython $validationArgs 2>&1
    $validationCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $previousErrorAction
}
if ($validationCode -ne 0) {
    if ($Unattended) {
        throw "first-run answers invalid:`n$($validation -join "`n")"
    }
    Write-Host "  Invalid answers:" -ForegroundColor Yellow
    foreach ($line in $validation) { Write-Host "    $line" -ForegroundColor Yellow }
    # Re-prompt once with fresh defaults, then fail hard so install.ps1 aborts.
    throw "first-run answers invalid; re-run the wizard to correct them"
}

# Generate fresh secrets and render the env. Secrets cross the process
# boundary through the environment, never on the command line.
$sessionSecret = New-StrongSecret
$wrapperToken = New-StrongSecret
[Environment]::SetEnvironmentVariable("CCW_SESSION_SECRET", $sessionSecret)
[Environment]::SetEnvironmentVariable("CCW_WRAPPER_TOKEN", $wrapperToken)
try {
    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $rendered = & $VenvPython $commonArgs 2>&1
    $renderCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $previousErrorAction
    [Environment]::SetEnvironmentVariable("CCW_SESSION_SECRET", $null)
    [Environment]::SetEnvironmentVariable("CCW_WRAPPER_TOKEN", $null)
}
if ($renderCode -ne 0) {
    throw "config render failed: $($rendered -join "`n")"
}

# The CLI prints the env body on stdout and any warnings on stderr; the PS
# invocation merged both, so keep only lines that look like dotenv entries.
$envLines = @($rendered | Where-Object { $_ -match '^[A-Za-z_][A-Za-z0-9_]*=' -or $_ -match '^#' })
$envContent = ($envLines -join "`r`n").TrimEnd("`r", "`n", " ") + "`r`n"

# Write UTF-8 without BOM (python-dotenv and the relay handle BOM-less CRLF).
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($envPath, $envContent, $utf8NoBom)

# Restrict the config directory to the installing user only. Arguments are
# passed positionally without embedded quotes (icacls would otherwise receive
# the quote characters as part of the path). This mirrors win_layout.acl_commands:
# no deny entry, because every account incl. the principal is a member of
# BUILTIN\Users and deny beats allow.
$icacls = (Get-Command icacls -ErrorAction SilentlyContinue).Source
if (-not $icacls) { throw "icacls not found; cannot restrict config ACLs" }
& $icacls $configDir /inheritance:r 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw "failed to restrict ACLs on $configDir" }
& $icacls $configDir /grant:r "$($env:USERNAME):(OI)(CI)F" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) { throw "failed to restrict ACLs on $configDir" }

Write-Step "Wrote config to $envPath"
Write-Step "Machine id: $MachineName  |  Public origin: $PublicOrigin  |  Relay port: $RelayPort"
Write-Step "SESSION_SECRET and WRAPPER_TOKEN were generated fresh. LOGIN_PASSWORD is an optional fallback."
exit 0
