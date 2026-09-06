<#
.SYNOPSIS
    Compiles the cc-remote Windows installer (.exe) with Inno Setup.

.DESCRIPTION
    Runs ISCC.exe on cc_portable_control\windows\inno\cc-remote.iss with defines that pin
    the canonical distribution/product versions, the staged release root, and
    the output directory. The produced executable is
    cc-remote-v<distribution-version>-windows-x64-setup.exe.

    ISCC is located from -IsccPath, the ISCC environment variable, the standard
    per-user/per-machine install paths, or PATH. When it is absent the script FAILS
    (fail-closed): no silent fallback may produce a fake installer. The .iss is
    validated statically by the zero-token test suite even on machines without
    ISCC.

.PARAMETER SourceRoot
    Repository root. Defaults to the parent of cc_portable_control/windows.

.PARAMETER StageDir
    The staged release root (contains setup.ps1, cc_portable_control/, payload/).

.PARAMETER DistributionVersion
    Canonical distribution_version from deploy/release-metadata.json.

.PARAMETER ProductVersion
    Canonical product_version from deploy/release-metadata.json.

.PARAMETER OutputDir
    Where the .exe lands. Default <SourceRoot>\dist.

.PARAMETER IsccPath
    Explicit path to ISCC.exe.

.PARAMETER NoServices
    Compile the installer so its setup step runs with -NoServices (no
    scheduled tasks / firewall). Used by CI smoke builds that must not touch
    the runner's task scheduler.

.PARAMETER OutputName
    Override the exe filename stem. Default cc-remote-v<dist>-windows-x64-setup.

.EXAMPLE
    & .\build-installer.ps1 -StageDir C:\repo\dist\stage `
        -DistributionVersion 3.0.0-pager.8 -ProductVersion 3.0.0 `
        -OutputDir C:\repo\dist
#>
[CmdletBinding()]
param(
    [string]$SourceRoot,
    [Parameter(Mandatory = $true)][string]$StageDir,
    [Parameter(Mandatory = $true)][string]$DistributionVersion,
    [Parameter(Mandatory = $true)][string]$ProductVersion,
    [string]$OutputDir,
    [string]$IsccPath,
    [switch]$NoServices,
    [switch]$IsolatedSmokeTest,
    [string]$OutputName
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

function Write-Step { param([string]$Message) Write-Host "[cc-remote] $Message" -ForegroundColor Cyan }

if (-not $SourceRoot) { $SourceRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path }
$sourceRoot = [System.IO.Path]::GetFullPath($SourceRoot).TrimEnd('\')
$stageDir = [System.IO.Path]::GetFullPath($StageDir).TrimEnd('\')
if (-not $OutputDir) { $OutputDir = Join-Path $sourceRoot "dist" }
$outputDir = [System.IO.Path]::GetFullPath($OutputDir).TrimEnd('\')
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

# --- Validate the staged release ---------------------------------------------
foreach ($required in @("setup.ps1", "cc_portable_control", "payload")) {
    if (-not (Test-Path (Join-Path $stageDir $required))) {
        throw "staged release is incomplete at $stageDir; missing $required"
    }
}
$manifestPath = Join-Path $stageDir "payload\distribution-manifest.json"
if (-not (Test-Path $manifestPath)) {
    throw "staged payload has no distribution-manifest.json; run build.ps1 first"
}

# --- Locate ISCC (fail-closed when absent) -----------------------------------
$iscc = $IsccPath
if (-not $iscc -and $env:ISCC) { $iscc = $env:ISCC }
if (-not $iscc) {
    foreach ($candidate in @(
        (Join-Path $env:LOCALAPPDATA "Programs\Inno Setup 6\ISCC.exe"),
        "C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
        "C:\Program Files\Inno Setup 6\ISCC.exe",
        "C:\Program Files (x86)\Inno Setup 5\ISCC.exe"
    )) {
        if (Test-Path $candidate) { $iscc = $candidate; break }
    }
}
if (-not $iscc) {
    $isccCommand = Get-Command ISCC.exe -ErrorAction SilentlyContinue
    if ($isccCommand) { $iscc = $isccCommand.Source }
}
if (-not $iscc) {
    throw "Inno Setup compiler (ISCC.exe) not found; install Inno Setup 6 or pass -IsccPath. Refusing to emit a fake installer."
}
if (-not (Test-Path $iscc)) { throw "ISCC.exe not found at $iscc" }

# --- Compile the installer ----------------------------------------------------
$iss = Join-Path $PSScriptRoot "inno\cc-remote.iss"
$outputName = if ($OutputName) { $OutputName } else { "cc-remote-v$DistributionVersion-windows-x64-setup" }
$setupArgs = "-Unattended -AllowInsecureHttp"
if ($NoServices) { $setupArgs += " -NoServices" }

$defines = @(
    "/DStageDir=$stageDir",
    "/DDistVersion=$DistributionVersion",
    "/DProductVersion=$ProductVersion",
    "/DOutputDir=$outputDir",
    "/DOutputName=$outputName"
)
$defines += "/DSetupArgs=$setupArgs"
if ($IsolatedSmokeTest) {
    if (-not $NoServices) { throw 'IsolatedSmokeTest requires NoServices' }
    $defines += '/DIsolatedSmokeTest=1'
}

Write-Step "Compiling installer with ISCC: $iscc"
& $iscc $defines $iss
if ($LASTEXITCODE -ne 0) { throw "ISCC compilation failed (exit $LASTEXITCODE)" }

# --- Verify the produced artifact --------------------------------------------
$exePath = Join-Path $outputDir "$outputName.exe"
if (-not (Test-Path $exePath)) { throw "ISCC reported success but produced no $exePath" }
$bytes = [System.IO.File]::ReadAllBytes($exePath)
if ($bytes.Length -lt 1024) { throw "installer is implausibly small ($($bytes.Length) bytes)" }
if ($bytes[0] -ne 0x4D -or $bytes[1] -ne 0x5A) { throw "installer does not have a valid PE header (missing MZ)" }

$sha = (Get-FileHash -Algorithm SHA256 -Path $exePath).Hash.ToLowerInvariant()
Write-Step "Built $exePath ($($bytes.Length) bytes)"
Write-Step "SHA256 $sha"
Write-Step "Done: $exePath"
exit 0
