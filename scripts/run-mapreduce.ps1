param(
    [string]$InputPath = '/transactions',
    [string]$ResultsPath = '/results',
    [string]$ReportsPath = 'reports'
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$jar = 'target/transaction-producer-1.0.0-all.jar'
if (-not (Test-Path $jar)) { throw "Jar absent: $jar. Lancez mvn clean package." }
hadoop jar $jar com.saad.bigdata.mapreduce.AnomalyDetectionPipeline $InputPath $ResultsPath $ReportsPath
if ($LASTEXITCODE -ne 0) { throw 'Pipeline MapReduce en échec.' }
