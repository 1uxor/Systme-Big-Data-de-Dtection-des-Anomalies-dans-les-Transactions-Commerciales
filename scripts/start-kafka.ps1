$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
docker compose up -d
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose a échoué.' }
docker compose ps -a
