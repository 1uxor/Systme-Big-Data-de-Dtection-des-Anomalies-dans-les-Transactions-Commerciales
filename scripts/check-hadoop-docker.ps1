$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml exec -T namenode hdfs dfsadmin -report
if ($LASTEXITCODE -ne 0) { throw 'HDFS indisponible.' }
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml exec -T resourcemanager yarn node -list
if ($LASTEXITCODE -ne 0) { throw 'YARN indisponible.' }
