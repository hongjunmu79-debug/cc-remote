<# Native, offline-capable control window. No credentials in URLs or reports. #>
[CmdletBinding()]
param([string]$InstallRoot)
$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0
if (-not $InstallRoot) { $InstallRoot = Join-Path $env:LOCALAPPDATA "cc-remote" }
$installRootFull = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
Add-Type -AssemblyName PresentationFramework, System.Net.Http
$reader = New-Object System.Xml.XmlNodeReader ([xml][IO.File]::ReadAllText((Join-Path $PSScriptRoot "console.xaml")))
$window = [Windows.Markup.XamlReader]::Load($reader)
$controls = @{}
foreach ($name in @('Version','Status','Detail','Pair','Browser','Restart','Address','Diagnose','Firewall','Logs','Uninstall','Report')) {
    $controls[$name] = $window.FindName($name)
}
$port = 8765
$envPath = Join-Path $installRootFull "config\.env"
if (Test-Path -LiteralPath $envPath) {
    foreach ($line in [IO.File]::ReadAllLines($envPath)) {
        if ($line -match '^\s*RELAY_PORT\s*=\s*(\d+)\s*$') { $port = [int]$matches[1] }
    }
}
if ($port -lt 1 -or $port -gt 65535) { $port = 8765 }
$consoleUrl = "http://127.0.0.1:$port/"
$controls.Address.Text = "本机网页：$consoleUrl"
$versionPath = Join-Path $installRootFull "releases\current.json"
if (Test-Path -LiteralPath $versionPath) {
    try { $controls.Version.Text = (Get-Content -LiteralPath $versionPath -Raw | ConvertFrom-Json).version } catch { }
}
$script:operation = $null
$script:pendingBrowser = $null
$script:healthTask = $null
$script:ready = $false
$handler = New-Object Net.Http.HttpClientHandler
$handler.UseProxy = $false
$client = New-Object Net.Http.HttpClient($handler)
$client.Timeout = [TimeSpan]::FromSeconds(2)

function Start-ConsoleOperation([string]$Action) {
    if ($script:operation -and -not $script:operation.HasExited) { return }
    $controls.Report.Text = "正在处理，请稍候…"
    $stateDir = Join-Path $installRootFull "state"
    New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
    $script:operationOutput = Join-Path $stateDir "console-result.txt"
    $arguments = '-NoProfile -ExecutionPolicy Bypass -File "{0}" -InstallRoot "{1}" -Action {2}' -f (Join-Path $PSScriptRoot "console-action.ps1"), $installRootFull, $Action
    $script:operation = Start-Process powershell.exe -ArgumentList $arguments -WindowStyle Hidden -PassThru -RedirectStandardOutput $script:operationOutput -RedirectStandardError (Join-Path $stateDir "console-error.txt")
    $null = $script:operation.Handle
}
function Open-ConsolePage([string]$Url) {
    try { Start-Process $Url -ErrorAction Stop | Out-Null }
    catch { $controls.Report.Text = "未能启动默认浏览器。请复制此地址到浏览器：`r`n$Url`r`n也可以在 Windows 设置中指定默认浏览器。" }
}
$controls.Pair.Add_Click({
    if ($script:ready) { Open-ConsolePage "${consoleUrl}?pair=1" }
    else { $script:pendingBrowser = "${consoleUrl}?pair=1"; Start-ConsoleOperation 'start' }
})
$controls.Browser.Add_Click({
    if ($script:ready) { Open-ConsolePage $consoleUrl }
    else { $script:pendingBrowser = $consoleUrl; Start-ConsoleOperation 'start' }
})
$controls.Restart.Add_Click({ Start-ConsoleOperation 'restart' })
$controls.Diagnose.Add_Click({ Start-ConsoleOperation 'diagnose' })
$controls.Firewall.Add_Click({ Start-ConsoleOperation 'firewall' })
$controls.Logs.Add_Click({
    $logDir = Join-Path $installRootFull 'logs'
    if (Test-Path -LiteralPath $logDir) { Start-Process explorer.exe -ArgumentList ('"{0}"' -f $logDir) }
})
$controls.Uninstall.Add_Click({
    $uninstaller = Join-Path $installRootFull 'unins000.exe'
    if (Test-Path -LiteralPath $uninstaller) {
        Start-Process $uninstaller
        $window.Close()
    } else {
        $controls.Report.Text = "此安装没有系统卸载器（可能是旧版或便携版）。请安装新版安装包到同一目录以修复卸载入口，配置会保留。"
    }
})
$timer = New-Object Windows.Threading.DispatcherTimer
$timer.Interval = [TimeSpan]::FromMilliseconds(600)
$timer.Add_Tick({
    try {
        if ($script:operation -and $script:operation.HasExited) {
            $script:operation.WaitForExit()
            $controls.Report.Text = [IO.File]::ReadAllText($script:operationOutput)
            if ($script:operation.ExitCode -ne 0) { $controls.Report.AppendText("`r`n操作未完成。请点「排查故障」查看下一步。") }
            $script:operation.Dispose(); $script:operation = $null
        }
        if ($script:healthTask -and $script:healthTask.IsCompleted) {
            $script:ready = $false
            try {
                $health = $script:healthTask.GetAwaiter().GetResult() | ConvertFrom-Json
                $script:ready = $health.ok -eq $true -and $health.wrapper_connected -eq $true
                if ($script:ready) {
                    $controls.Status.Text = if ($health.clients -gt 0) { "已连接 · $($health.clients) 个客户端在线" } else { "已就绪 · 等待手机扫码" }
                    $controls.Status.Foreground = '#6EE7B7'
                    $controls.Detail.Text = "电脑连接服务运行正常，可随时显示二维码。"
                    if ($script:pendingBrowser) { Open-ConsolePage $script:pendingBrowser; $script:pendingBrowser = $null }
                } else {
                    $controls.Status.Text = '正在连接本机服务…'
                    $controls.Status.Foreground = '#FCD34D'
                    $controls.Detail.Text = '网页已启动，电脑连接服务尚未就绪。持续等待时，请点「排查故障」。'
                }
            } catch {
                $controls.Status.Text = '服务尚未启动'
                $controls.Status.Foreground = '#FCD34D'
                $controls.Detail.Text = '正在启动时请稍候；若持续未就绪，点「启动 / 修复连接」或「排查故障」。'
            }
            $script:healthTask = $null
        }
        if (-not $script:healthTask) { $script:healthTask = $client.GetStringAsync("${consoleUrl}healthz") }
    } catch { $controls.Report.Text = "状态检查失败，请点「排查故障」。" }
})
$window.Add_ContentRendered({ Start-ConsoleOperation 'start'; $timer.Start() })
$window.Add_Closed({ $timer.Stop(); $client.Dispose(); $handler.Dispose() })
$null = $window.ShowDialog()
