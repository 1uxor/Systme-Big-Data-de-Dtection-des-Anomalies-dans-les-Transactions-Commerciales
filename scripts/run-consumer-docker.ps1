$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$jar = 'target/transaction-producer-1.0.0-all.jar'
if (-not (Test-Path $jar)) { throw "Jar absent: $jar. Lancez mvn clean package." }
$osType = docker info --format '{{.OSType}}'
if ($LASTEXITCODE -ne 0) { throw 'Moteur Docker indisponible. Ouvrez ou redémarrez Docker Desktop, puis relancez start-all.ps1.' }
if ($osType -ne 'linux') { throw 'Docker Desktop utilise les conteneurs Windows. Basculez vers les conteneurs Linux.' }
$running = @(docker compose -f docker-compose.yml -f docker-compose.hadoop.yml ps --status running --services)
if ($LASTEXITCODE -ne 0) { throw 'Impossible de lire les services Docker Compose.' }
if ($running -notcontains 'namenode') { throw 'Le conteneur namenode ne tourne pas. Lancez .\scripts\start-all.ps1.' }
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml exec namenode hadoop jar /workspace/target/transaction-producer-1.0.0-all.jar com.saad.bigdata.consumer.HdfsTransactionConsumer
if ($LASTEXITCODE -ne 0) { throw 'Consumer Kafka/HDFS en échec.' }
