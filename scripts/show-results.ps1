$ErrorActionPreference = 'Stop'
hdfs dfs -ls -R /results
foreach ($name in @('client-stats','category-stats','period-stats','transaction-anomalies','customer-anomalies','period-anomalies')) {
    Write-Host "`n$name"
    hdfs dfs -cat "/results/$name/part-*" | Select-Object -First 10
}
Get-Content reports/summary-report.csv
if (Test-Path reports/powerbi/data/data_quality.csv) {
    Get-Content reports/powerbi/data/data_quality.csv
}
