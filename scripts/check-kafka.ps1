$ErrorActionPreference = 'Stop'
docker compose ps -a
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic transactions
