; Inno Setup script for the cc-remote Windows installer.
;
; Compiled by build-installer.ps1 (ISCC.exe) with these defines:
;   /DStageDir=<abs path to the staged release root>   (contains setup.ps1,
;                                                       cc_portable_control/, payload/)
;   /DDistVersion=<canonical distribution_version>      from release-metadata.json
;   /DProductVersion=<canonical product_version>        from release-metadata.json
;   /DOutputDir=<abs path for the produced .exe>
;   /DSetupArgs=-NoServices                             optional; a CI smoke
;                                                       build installs without
;                                                       registering scheduled tasks
;
; The produced executable is a genuine installer: it extracts the release
; bundle into the chosen install root and runs setup.ps1, which performs the
; transactional install (verified payload, runtime venv, immutable releases\
; directory, config wizard, supervised scheduled tasks, firewall rule). The
; .exe itself performs no install logic of its own — it bootstraps the shipped
; setup.ps1 flow, so the artifact can never drift from install.ps1.

#ifndef StageDir
  #error "StageDir is not defined; run build-installer.ps1 instead of compiling this file directly"
#endif
#ifndef DistVersion
  #error "DistVersion is not defined; run build-installer.ps1"
#endif
#ifndef ProductVersion
  #define ProductVersion "0.0.0"
#endif
#ifndef OutputDir
  #define OutputDir "..\..\dist"
#endif
#ifndef OutputName
  #define OutputName "cc-remote-v{#DistVersion}-windows-x64-setup"
#endif
#ifndef SetupArgs
  #define SetupArgs "-Unattended -AllowInsecureHttp"
#endif

[Setup]
#ifdef IsolatedSmokeTest
AppId=CC-Remote-Isolated-Lifecycle-Smoke
UsePreviousAppDir=no
#else
AppId={{8F6D9A7E-CCR0-4E21-9B5E-CCREMOTE2026}}
#endif
AppName=CC Remote
AppVersion={#DistVersion}
AppVerName=CC Remote {#DistVersion}
AppPublisher=CC Remote
AppComments=Self-hosted remote control for Claude Code / Codex
DefaultDirName={localappdata}\cc-remote
DefaultGroupName=CC Remote
Compression=lzma2
SolidCompression=yes
OutputDir={#OutputDir}
OutputBaseFilename={#OutputName}
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
CreateUninstallRegKey=yes
Uninstallable=yes
UninstallDisplayName=CC Remote {#DistVersion}
DisableWelcomePage=yes
DisableProgramGroupPage=yes
DisableReadyPage=yes
DisableStartupPrompt=yes
ShowLanguageDialog=no
WizardStyle=modern

[Files]
; The release bundle keeps the archive layout: setup.ps1 at the root, the
; cc_portable_control/windows scripts, and the verified payload tree. The
; Inno-managed
; copy lives at {app}\release and is what setup.ps1 runs from.
Source: "{#StageDir}\setup.ps1"; DestDir: "{app}\release"; Flags: ignoreversion
Source: "{#StageDir}\cc_portable_control\*"; DestDir: "{app}\release\cc_portable_control"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#StageDir}\payload\*"; DestDir: "{app}\release\payload"; Flags: ignoreversion recursesubdirs createallsubdirs

[Run]
Filename: "powershell.exe"; Parameters: "-NoProfile -STA -WindowStyle Hidden -ExecutionPolicy Bypass -File ""{app}\release\cc_portable_control\windows\open-console.ps1"" -InstallRoot ""{app}"""; Description: "Open cc-remote console"; Flags: postinstall nowait skipifsilent

[Icons]
#ifndef IsolatedSmokeTest
Name: "{group}\CC Remote 控制台"; Filename: "powershell.exe"; Parameters: "-NoProfile -STA -WindowStyle Hidden -ExecutionPolicy Bypass -File ""{app}\release\cc_portable_control\windows\open-console.ps1"" -InstallRoot ""{app}"""; WorkingDir: "{app}"
Name: "{autodesktop}\CC Remote 控制台"; Filename: "powershell.exe"; Parameters: "-NoProfile -STA -WindowStyle Hidden -ExecutionPolicy Bypass -File ""{app}\release\cc_portable_control\windows\open-console.ps1"" -InstallRoot ""{app}"""; WorkingDir: "{app}"
Name: "{group}\卸载 CC Remote"; Filename: "{uninstallexe}"
#endif

[Code]
var
  SetupFailed: Boolean;
  SetupFailureMessage: String;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
var
  ResultCode: Integer;
  Succeeded: Boolean;
begin
  if CurUninstallStep <> usUninstall then exit;
  Succeeded := Exec(ExpandConstant('{sys}\WindowsPowerShell\v1.0\powershell.exe'),
    ExpandConstant('-NoProfile -ExecutionPolicy Bypass -File "{app}\release\cc_portable_control\windows\uninstall.ps1" -InstallRoot "{app}"'),
    ExpandConstant('{app}'), SW_HIDE, ewWaitUntilTerminated, ResultCode);
  if Succeeded then Succeeded := ResultCode = 0;
  if not Succeeded then
    RaiseException('Unable to stop or remove cc-remote. Close the console and retry. Your configuration has been kept.');
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ResultCode: Integer;
  PowerShell: String;
  Parameters: String;
  ExtraArgs: String;
begin
  if CurStep <> ssPostInstall then
    exit;

  WizardForm.StatusLabel.Caption := 'Preparing CC Remote and its offline runtime...';
  WizardForm.FilenameLabel.Caption := 'Please wait. No Python installation or manual configuration is needed.';

  { Run the real transactional installer ourselves so a non-zero child exit
    aborts Setup. A plain [Run] entry records the child failure in its log but
    still returns overall success, leaving users with an extracted shell and
    no configured app. }
  PowerShell := ExpandConstant('{sys}\WindowsPowerShell\v1.0\powershell.exe');
  ExtraArgs := '';
  if ExpandConstant('{param:NoFirewall|0}') = '1' then
    ExtraArgs := ' -NoFirewall';
  Parameters := ExpandConstant(
    '-NoProfile -ExecutionPolicy Bypass -Command "' +
    'Start-Transcript -Path ''{app}\installer-setup.log'' -Force | Out-Null; ' +
    '& ''{app}\release\setup.ps1'' -InstallRoot ''{app}'' {#SetupArgs}'
  ) + ExtraArgs + '"';
  if not Exec(
    PowerShell,
    Parameters,
    ExpandConstant('{app}\release'),
    SW_HIDE,
    ewWaitUntilTerminated,
    ResultCode
  ) then begin
    SetupFailed := True;
    SetupFailureMessage := 'Unable to start the cc-remote setup script.';
  end else if ResultCode <> 0 then begin
    SetupFailed := True;
    SetupFailureMessage := Format('cc-remote setup failed with exit code %d.', [ResultCode]);
  end;

  if SetupFailed then
    SuppressibleMsgBox(SetupFailureMessage, mbCriticalError, MB_OK, IDOK);
end;

function GetCustomSetupExitCode: Integer;
begin
  if SetupFailed then
    Result := 20
  else
    Result := 0;
end;

procedure CurPageChanged(CurPageID: Integer);
begin
  if (CurPageID = wpFinished) and SetupFailed then begin
    WizardForm.FinishedHeadingLabel.Caption := 'cc-remote setup failed';
    WizardForm.FinishedLabel.Caption := SetupFailureMessage;
  end;
end;
