# Fetches a portable Redis 5.0.14 for Windows into tools\redis (ADR-002).
# No installer, no admin rights: tests and dev servers spawn the exe directly.
$ErrorActionPreference = "Stop"
$dest = Join-Path $PSScriptRoot "..\tools\redis"
New-Item -ItemType Directory -Force $dest | Out-Null
$zip = Join-Path $env:TEMP "redis-win.zip"
Invoke-WebRequest -UseBasicParsing `
    -Uri "https://github.com/tporadowski/redis/releases/download/v5.0.14.1/Redis-x64-5.0.14.1.zip" `
    -OutFile $zip
Expand-Archive -Path $zip -DestinationPath $dest -Force
Remove-Item $zip
Write-Host "redis-server.exe ready in $dest"
