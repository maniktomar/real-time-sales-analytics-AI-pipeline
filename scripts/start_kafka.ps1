Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

docker compose -f docker/docker-compose.yml up -d
Write-Host "Kafka, Zookeeper, and Kafka UI are starting."
Write-Host "Kafka UI: http://localhost:8080"

