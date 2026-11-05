# Lancer Kafka + Hadoop avec Docker Desktop

Cette voie utilise **PowerShell Windows et Docker Desktop seulement**. WSL/Ubuntu n'a pas besoin d'être ouvert, et Hadoop n'a pas besoin d'être installé sur Windows. Docker Desktop exécute les conteneurs Linux en arrière-plan.

Le dépôt utilise l'image officielle `apache/hadoop:3.5.0`. `docker-compose.hadoop.yml` ajoute NameNode, DataNode, ResourceManager et NodeManager au service Kafka existant. Les volumes Docker `namenode-data` et `datanode-data` conservent HDFS lors d'un `docker compose down`.

## Démarrage

Ouvrir Docker Desktop et attendre l'état **Running**. Dans PowerShell :

```powershell
Set-Location 'C:\my things\Projet1_big_data'
.\scripts\start-all.ps1
```

Le premier téléchargement de l'image Hadoop est volumineux. Attendre que les services démarrent, puis contrôler :

```powershell
.\scripts\check-hadoop-docker.ps1
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml ps
```

`hdfs dfsadmin -report` doit indiquer au moins un DataNode vivant; `yarn node -list` doit indiquer un NodeManager. Les interfaces Web sont <http://localhost:9870/> (HDFS) et <http://localhost:8088/> (YARN).

## Compiler et générer les transactions

```powershell
mvn clean package
```

Terminal PowerShell A — Consumer Kafka/HDFS **dans le conteneur Hadoop** :

```powershell
.\scripts\run-consumer-docker.ps1
```

Terminal PowerShell B — Producer **sur Windows**, connecté à Kafka sur `localhost:9092` :

```powershell
mvn exec:java
```

Laisser tourner assez longtemps pour obtenir les transactions voulues. Arrêter le Producer puis le Consumer avec `Ctrl+C`. Le Consumer reçoit automatiquement `KAFKA_BOOTSTRAP_SERVERS=kafka:19092` depuis Docker Compose, alors que le Producer Windows conserve `localhost:9092`.

## Analyser et créer les rapports

```powershell
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml exec -T namenode hdfs dfs -ls -R /transactions
.\scripts\run-mapreduce-docker.ps1
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml exec -T namenode hdfs dfs -ls -R /results
Get-Content reports/summary-report.csv
Get-ChildItem reports/powerbi/data
```

Le script lance les six jobs Hadoop MapReduce sur YARN, puis écrit les rapports et les CSV Power BI dans le dossier Windows `reports/` monté dans le conteneur sous `/workspace/reports`. Les résultats des jobs restent dans HDFS.

Pour consulter un résultat :

```powershell
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml exec -T namenode hdfs dfs -cat /results/category-stats/part-r-00000
```

## Arrêt et données persistantes

```powershell
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml down
```

Cette commande arrête les services mais conserve les volumes HDFS. Ne pas ajouter `-v` si les transactions et résultats doivent être conservés. Pour relancer les jobs, les dossiers de sortie `/results/*` doivent être supprimés explicitement ou un autre chemin de résultats doit être choisi.

## Diagnostic rapide

- Erreur `500 Internal Server Error` sur `dockerDesktopLinuxEngine/.../containers/json` : Docker Desktop ne répond pas correctement avant même l'exécution du Consumer. Redémarrer Docker Desktop, vérifier que `docker version` affiche une section **Server** et que `docker info --format '{{.OSType}}'` affiche `linux`, puis relancer `start-all.ps1`.
- `docker compose ... ps` et `docker compose ... logs namenode datanode resourcemanager nodemanager --tail 100` montrent l'état des services.
- `check-hadoop-docker.ps1` échoue au premier démarrage si le NameNode ou le DataNode ne sont pas encore prêts : attendre puis réessayer.
- Si `/results/client-stats` existe déjà, le pipeline refuse de l'écraser. Utiliser un nouveau chemin ou supprimer uniquement les anciens résultats après vérification.
- Un `hdfs dfsadmin -report` avec zéro DataNode vivant indique un problème de service DataNode; consulter ses logs.
- Le jar doit être reconstruit après tout changement Java avec `mvn clean package`.

Sources : [image officielle Apache Hadoop](https://hub.docker.com/r/apache/hadoop), [exemple Compose Apache](https://raw.githubusercontent.com/apache/hadoop/docker-hadoop-3.5.0/docker-compose.yaml).
