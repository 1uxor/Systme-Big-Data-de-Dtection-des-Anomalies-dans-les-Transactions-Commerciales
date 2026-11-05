$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml up -d --build
if ($LASTEXITCODE -ne 0) { throw 'Impossible de démarrer Kafka et Hadoop. Vérifiez Docker Desktop.' }
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml ps
