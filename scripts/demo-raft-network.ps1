param(
    [string]$NodeId = ""
)

$ErrorActionPreference = "Stop"

$RootDir = Resolve-Path (Join-Path $PSScriptRoot "..")
$JarPath = Join-Path $RootDir "target\RaftViz-0.0.1-SNAPSHOT.jar"
$RuntimeDir = Join-Path $RootDir "demo-runtime"
$JavaProcess = $null

if ([string]::IsNullOrWhiteSpace($NodeId)) {
    $NodeId = if ($env:NODE_ID) { $env:NODE_ID } else { "node-1" }
}

if ($NodeId -ne "node-1" -and $NodeId -ne "node-2") {
    Write-Host "Usage:"
    Write-Host "  .\scripts\demo-raft-network.ps1 node-1"
    Write-Host "  .\scripts\demo-raft-network.ps1 node-2"
    throw "NodeId must be node-1 or node-2."
}

$Port = if ($env:PORT) { $env:PORT } else { "8080" }
$DiscoveryPort = if ($env:RAFT_DISCOVERY_PORT) { $env:RAFT_DISCOVERY_PORT } else { "4446" }
$LanScanIntervalMs = if ($env:RAFT_DISCOVERY_LAN_SCAN_INTERVAL_MS) { $env:RAFT_DISCOVERY_LAN_SCAN_INTERVAL_MS } else { "3000" }
$StaleAfterMs = if ($env:RAFT_DISCOVERY_STALE_AFTER_MS) { $env:RAFT_DISCOVERY_STALE_AFTER_MS } else { "15000" }
$ElectionTimeoutMin = if ($env:ELECTION_TIMEOUT_MIN) { $env:ELECTION_TIMEOUT_MIN } else { "5000" }
$ElectionTimeoutMax = if ($env:ELECTION_TIMEOUT_MAX) { $env:ELECTION_TIMEOUT_MAX } else { "9000" }
$HeartbeatInterval = if ($env:HEARTBEAT_INTERVAL) { $env:HEARTBEAT_INTERVAL } else { "1000" }

function Get-LanIp {
    if ($env:ADVERTISE_ADDR) {
        return ([uri]$env:ADVERTISE_ADDR).Host
    }

    $addresses = Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object {
            $_.IPAddress -notlike "127.*" -and
            $_.IPAddress -notlike "169.254.*" -and
            $_.IPAddress -notmatch "^172\.(1[6-9]|2[0-9]|3[0-1])\."
        } |
        Sort-Object @{ Expression = { if ($_.IPAddress -like "192.168.*") { 0 } elseif ($_.IPAddress -like "10.*") { 1 } else { 2 } } }, InterfaceMetric

    if (-not $addresses) {
        throw "Could not detect a LAN IP. Set ADVERTISE_ADDR=http://YOUR_LAN_IP:$Port and run again."
    }

    return $addresses[0].IPAddress
}

function Wait-ForHttp($Url, $Attempts = 90) {
    for ($i = 1; $i -le $Attempts; $i++) {
        try {
            Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2 | Out-Null
            return
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    throw "Timed out waiting for $Url"
}

function Get-ClusterSnapshot($BaseUrl) {
    try {
        return Invoke-RestMethod -Uri "$BaseUrl/raft/cluster/state" -TimeoutSec 4
    } catch {
        return @()
    }
}

function Wait-ForTwoNodeCluster($BaseUrl) {
    Write-Host "Waiting until node-1 and node-2 are visible on this dashboard..."
    for ($i = 1; $i -le 80; $i++) {
        $snapshot = @(Get-ClusterSnapshot $BaseUrl)
        $ids = @($snapshot | ForEach-Object { $_.node.nodeId })
        if ($ids -contains "node-1" -and $ids -contains "node-2") {
            Write-Host "Both nodes are visible in the dashboard snapshot."
            return
        }
        Start-Sleep -Seconds 2
    }
    throw "Could not see both node-1 and node-2 yet. Open $BaseUrl/raft/cluster/state to inspect the snapshot."
}

function Invoke-Election($BaseUrl) {
    Write-Host "Triggering election from $NodeId..."
    try {
        Invoke-RestMethod -Method Post -Uri "$BaseUrl/raft/simulate/trigger-election" -TimeoutSec 4 | Out-Null
    } catch {
    }
    Start-Sleep -Seconds 4
}

function Send-Log($BaseUrl, $Message) {
    $body = @{ message = $Message } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/log" -Body $body -ContentType "application/json" -MaximumRedirection 5 | Out-Null
}

function Stop-DemoNode {
    if ($script:JavaProcess -and -not $script:JavaProcess.HasExited) {
        Write-Host ""
        Write-Host "Stopping $NodeId..."
        Stop-Process -Id $script:JavaProcess.Id -Force -ErrorAction SilentlyContinue
    }
}

try {
    if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
        throw "Missing required command: java"
    }

    if (-not (Test-Path $JarPath)) {
        Write-Host "Building application jar..."
        Push-Location $RootDir
        try {
            .\mvnw.cmd -q -DskipTests package
        } finally {
            Pop-Location
        }
    }

    New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null

    $LocalIp = Get-LanIp
    $BaseUrl = if ($env:ADVERTISE_ADDR) { $env:ADVERTISE_ADDR } else { "http://${LocalIp}:${Port}" }
    $LogFile = Join-Path $RuntimeDir "$NodeId.log"
    $ErrFile = Join-Path $RuntimeDir "$NodeId.err.log"

    Write-Host ""
    Write-Host "RaftViz two-laptop network demo is live."
    Write-Host ""
    Write-Host "This laptop:"
    Write-Host "  $NodeId -> $BaseUrl"
    Write-Host ""
    Write-Host "Dashboard:"
    Write-Host "  $BaseUrl/dashboard.html"
    Write-Host ""
    Write-Host "Run on the other laptop with the opposite node id:"
    Write-Host "  .\scripts\demo-raft-network.ps1 node-1"
    Write-Host "  .\scripts\demo-raft-network.ps1 node-2"
    Write-Host ""

    $env:PORT = $Port
    $env:NODE_ID = $NodeId
    $env:ADVERTISE_ADDR = $BaseUrl
    $env:RAFT_DISCOVERY_PORT = $DiscoveryPort
    $env:RAFT_DISCOVERY_LAN_SCAN_ENABLED = "true"
    $env:RAFT_DISCOVERY_LAN_SCAN_INTERVAL_MS = $LanScanIntervalMs
    $env:RAFT_DISCOVERY_STALE_AFTER_MS = $StaleAfterMs
    $env:ELECTION_TIMEOUT_MIN = $ElectionTimeoutMin
    $env:ELECTION_TIMEOUT_MAX = $ElectionTimeoutMax
    $env:HEARTBEAT_INTERVAL = $HeartbeatInterval

    Write-Host "Starting $NodeId on $BaseUrl"
    $script:JavaProcess = Start-Process `
        -FilePath "java" `
        -ArgumentList @("-jar", $JarPath) `
        -WorkingDirectory $RootDir `
        -RedirectStandardOutput $LogFile `
        -RedirectStandardError $ErrFile `
        -WindowStyle Hidden `
        -PassThru

    Wait-ForHttp "$BaseUrl/raft/state"
    Wait-ForTwoNodeCluster $BaseUrl

    if ($NodeId -eq "node-1") {
        Invoke-Election $BaseUrl
        Write-Host "Appending demo log entries. They should appear in the dashboard log table."
        Send-Log $BaseUrl "network demo started by node-1"
        Send-Log $BaseUrl "node-1 and node-2 discovered over LAN"
        Send-Log $BaseUrl "leader election simulated from node-1"
    } else {
        Write-Host "node-2 is visible and waiting. node-1 will trigger election and append demo logs."
    }

    Write-Host "Demo is running. Keep $BaseUrl/dashboard.html open to watch topology, roles, and logs."
    while ($true) {
        Start-Sleep -Seconds 5
        if ($script:JavaProcess.HasExited) {
            throw "$NodeId stopped. Check $LogFile and $ErrFile."
        }
    }
} finally {
    Stop-DemoNode
}
