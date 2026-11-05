$ErrorActionPreference = 'Stop'
hdfs dfs -ls /
if ($LASTEXITCODE -ne 0) { throw 'HDFS inaccessible.' }
hdfs dfs -ls -R /transactions
