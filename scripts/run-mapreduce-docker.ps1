$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$jar = 'target/transaction-producer-1.0.0-all.jar'
if (-not (Test-Path $jar)) { throw "Jar absent: $jar. Lancez mvn clean package." }
$osType = docker info --format '{{.OSType}}'
if ($LASTEXITCODE -ne 0) { throw 'Moteur Docker indisponible. Ouvrez ou redémarrez Docker Desktop.' }
if ($osType -ne 'linux') { throw 'Docker Desktop utilise les conteneurs Windows. Basculez vers les conteneurs Linux.' }
$running = @(docker compose -f docker-compose.yml -f docker-compose.hadoop.yml ps --status running --services)
if ($LASTEXITCODE -ne 0) { throw 'Impossible de lire les services Docker Compose.' }
if ($running -notcontains 'namenode' -or $running -notcontains 'datanode' -or $running -notcontains 'resourcemanager' -or $running -notcontains 'nodemanager') {
    throw 'Hadoop n est pas entièrement démarré. Lancez .\scripts\start-all.ps1 puis .\scripts\check-hadoop-docker.ps1.'
}
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml exec -T namenode hadoop jar /workspace/target/transaction-producer-1.0.0-all.jar com.saad.bigdata.mapreduce.AnomalyDetectionPipeline /transactions /results /workspace/reports /workspace/data
if ($LASTEXITCODE -ne 0) { throw 'Pipeline MapReduce en échec.' }
