$ErrorActionPreference = 'Stop'
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic transactions --partitions 3 --replication-factor 1
if ($LASTEXITCODE -ne 0) { throw 'Création du topic impossible.' }
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions
