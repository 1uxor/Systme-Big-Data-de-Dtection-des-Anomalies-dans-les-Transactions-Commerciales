# Système Big Data de Détection des Anomalies dans les Transactions Commerciales

## 1. Présentation

Ce projet académique archive des transactions commerciales synthétiques, calcule des statistiques par lots et signale les valeurs inhabituelles. Kafka découple production et archivage; HDFS conserve les fichiers; Hadoop MapReduce exécute les calculs distribués. Une anomalie statistique n'est pas une preuve de fraude.

## 2. Architecture

```text
Synthetic Transactions
        ↓
Kafka Producer → topic transactions (3 partitions)
        ↓
Kafka Consumer (groupe hdfs-archiver)
        ↓
HDFS /transactions/year=YYYY/month=MM/day=DD/*.jsonl
        ↓
MapReduce : ClientStats, CategoryStats, PeriodStats
        ↓
MapReduce : AmountAnomaly, HighVolumeCustomer, PeriodVariation
        ↓
HDFS /results → ReportGenerator → rapports texte/CSV
              → PowerBiExport → CSV Power BI → tableau de bord
```

Le Producer crée des transactions JSON; le Consumer les archive en JSON Lines. Les jobs statistiques agrègent les montants; les jobs de détection lisent ces agrégats. Le rapport local résume les six résultats.

## 3. Technologies

Java 17 est le langage du pipeline; Maven compile et teste; Docker Desktop lance Kafka et, avec le fichier Compose complémentaire, Hadoop; Kafka transporte les événements; Hadoop HDFS stocke les fichiers; Hadoop MapReduce calcule les agrégats et détecte les anomalies; Jackson lit et écrit le JSON; JUnit teste les règles métier. Aucun Spark, Pandas ou Python analytique n'est utilisé.

## 4. Prérequis et installation Windows

Installer JDK 17, Maven, Docker Desktop et VS Code avec les extensions Java. **Voie recommandée sous Windows :** Kafka et Hadoop 3.5.0 tournent dans Docker; aucun Hadoop local ni Ubuntu interactif n'est requis. Voir [docs/hadoop-docker.md](docs/hadoop-docker.md). Pour un cluster externe, le NameNode, le DataNode et YARN/MapReduce doivent être démarrés et leurs fichiers `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml` et `yarn-site.xml` accessibles au client.

`docker-compose.yml` seul lance Kafka. En ajoutant `docker-compose.hadoop.yml`, le dépôt lance aussi Hadoop. L'installation Ubuntu/WSL2 reste disponible dans [docs/hadoop-wsl.md](docs/hadoop-wsl.md). Si l'établissement fournit déjà un cluster, utiliser sa configuration au lieu d'en créer un nouveau.

Les variables Hadoop ci-dessous ne sont nécessaires **que pour une installation Hadoop locale sur Windows**. Avec la voie Docker recommandée, configurer seulement Java 17 et Maven sur Windows.

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-17'  # adapter
$env:HADOOP_HOME = 'C:\hadoop-3.5.0'               # adapter
$env:HADOOP_CONF_DIR = "$env:HADOOP_HOME\etc\hadoop"
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
java -version
mvn -version
docker --version
hadoop version
hdfs version
```

`JAVA_HOME` et `HADOOP_HOME` pointent sur les dossiers racines, sans `bin`. Avec WSL2, utiliser les commandes Hadoop via `wsl` et lancer le code Java dans le même environnement, ou fournir une configuration réseau HDFS joignable depuis Windows. `fs.defaultFS` doit viser le port RPC du NameNode, par exemple `hdfs://localhost:9000`, pas le port Web 9870.

## 5. Configuration Kafka

```powershell
docker compose up -d
docker ps
.\scripts\create-topic.ps1
.\scripts\check-kafka.ps1
```

`docker-compose.yml` utilise Kafka KRaft et `kafka-init` crée automatiquement `transactions` avec 3 partitions et un facteur de réplication de 1. `create-topic.ps1` est idempotent. Le broker écoute sur `localhost:9092`.

### Démarrage simplifié avec Hadoop dans Docker

Sous PowerShell Windows, après ouverture de Docker Desktop :

```powershell
.\scripts\start-all.ps1
.\scripts\check-hadoop-docker.ps1
mvn clean package
# Terminal A : Consumer
.\scripts\run-consumer-docker.ps1
# Terminal B : Producer (arrêter ensuite les deux avec Ctrl+C)
mvn exec:java
# Après l'archivage :
.\scripts\run-mapreduce-docker.ps1
```

Le guide [docs/hadoop-docker.md](docs/hadoop-docker.md) explique les vérifications et les chemins HDFS. Les commandes `hadoop` et `hdfs` données plus bas dans le README concernent un client Hadoop installé localement; avec Docker, les scripts ci-dessus exécutent ces commandes dans le conteneur.

## 6. Phase 1 : génération et envoi

`Transaction.java` définit les champs JSON. `TransactionProducer.java` crée environ 100 transactions/s. Environ 95 % suivent les plages habituelles de prix et quantité; environ 5 % reçoivent un prix ou une quantité beaucoup plus grands. Aucun champ `is_anomaly` n'est écrit : le détecteur doit inférer les anomalies.

Jackson sérialise chaque transaction. Un `ProducerRecord` utilise `customer_id` comme clé Kafka, ce qui stabilise la partition d'un client. `producer.send(...)` est asynchrone; son callback affiche la confirmation, la partition ou l'erreur. Les IDs client et produit correspondent aux CSV de `data/`.

```powershell
mvn clean package
mvn exec:java
```

Arrêter avec `Ctrl+C` après avoir produit les données voulues.

## 7. Phase 2 : archivage HDFS

`HdfsTransactionConsumer.java` appelle `poll`, accumule jusqu'à 500 messages ou 5 secondes, puis écrit des fichiers JSON Lines dans `/transactions/year=YYYY/month=MM/day=DD/`. Il appartient au groupe `hdfs-archiver`. L'auto-commit est désactivé : `commitSync()` ne valide les offsets qu'après l'écriture HDFS. Un échec entre l'écriture et le commit peut provoquer un doublon.

```powershell
mvn exec:java '-Dexec.mainClass=com.saad.bigdata.consumer.HdfsTransactionConsumer'
hdfs dfs -ls -R /transactions
```

Lancer le Consumer dans un terminal distinct du Producer. Vérifier `hdfs dfsadmin -report` avant l'archivage.

## 8. Phase 3 : statistiques MapReduce

Chaque job reçoit un dossier d'entrée HDFS et un dossier de sortie **inexistant**. Les mappers comptent les lignes JSON invalides dans `DATA/INVALID_TRANSACTION`.

| Job | Input | Mapper : clé / valeur | Reducer | Output |
|---|---|---|---|---|
| `ClientStatsJob` | JSONL transactions | `customer_id` / `total_amount` | compte, somme, moyenne | `/results/client-stats/` |
| `CategoryStatsJob` | JSONL transactions | `category` / `(1, x, x²)` | compte, somme, moyenne, écart-type | `/results/category-stats/` |
| `PeriodStatsJob` | JSONL transactions | période / `total_amount` | compte, somme, moyenne | `/results/period-stats/` |

La variance de catégorie est `sum(x²)/n - mean²`, bornée à zéro en cas d'arrondi; `std = sqrt(variance)`. Les sorties sont en JSON Lines. `PeriodStatsJob` accepte `-Dperiod.granularity=hour|day|month`; `day` est le défaut. Les dates ISO viennent de `timestamp`.

## 9. Phase 4 : détection MapReduce

`AmountAnomalyJob` charge les sorties de `CategoryStatsJob` au démarrage du mapper et signale `amount > mean + k × std` (`k=3`). Exemple : `mean=3500`, `std=1200` donnent `threshold=7100`; `25000` est signalé. Il ne lit aucun marqueur du générateur.

`HighVolumeCustomerJob` lit `ClientStatsJob` et signale un client si `count > 500` **ou** `total_amount > 500000`. `PeriodVariationJob` trie les périodes agrégées, compare deux périodes observées successives et calcule `(current - previous) / previous × 100`; il signale `abs(variation) > 50`. Les périodes vides ne sont pas ajoutées artificiellement.

| Job | Entrée | Sortie |
|---|---|---|
| `AmountAnomalyJob` | transactions + category-stats | `/results/transaction-anomalies/` |
| `HighVolumeCustomerJob` | client-stats | `/results/customer-anomalies/` |
| `PeriodVariationJob` | period-stats | `/results/period-anomalies/` |

Les valeurs par défaut sont dans `src/main/resources/application.properties`. Les propriétés Hadoop `anomaly.std.multiplier`, `anomaly.customer.max.transactions`, `anomaly.customer.max.amount`, `anomaly.period.variation.percent` et `period.granularity` se surchargent avec `-D` après le nom de classe.

## 10. Chaînage MapReduce

`AnomalyDetectionPipeline` exécute `ClientStatsJob`, `CategoryStatsJob`, `PeriodStatsJob`, `AmountAnomalyJob`, `HighVolumeCustomerJob`, `PeriodVariationJob`, puis `ReportGenerator` et `PowerBiExport`. `CategoryStatsJob` précède nécessairement `AmountAnomalyJob`, qui utilise sa moyenne et son écart-type. Le pipeline affiche `[START]` et `[SUCCESS]`, s'arrête au premier échec et refuse des sorties déjà présentes.

## 11. Rapports

`ReportGenerator` lit les `part-*` HDFS et écrit `reports/anomaly-report.txt` et `reports/summary-report.csv`. Le texte inclut le total analysé, les nombres d'anomalies, les catégories les plus touchées, les périodes signalées, le top 10 des montants et le top 10 des clients. Le CSV utilise `metric,value`. `PowerBiExport` crée ensuite onze CSV dans `reports/powerbi/data/` pour une analyse interactive.

## 12. Données de référence

`data/Customers.csv` contient 1000 clients avec segment et ville. `data/Products.csv` contient 900 produits de cinq catégories avec prix de référence. Le Producer choisit ses IDs dans ces plages. Le prix effectif est généré indépendamment du prix de référence. `data/sample-transactions.jsonl` fournit 21 lignes pour une démonstration : 20 montants de 100 le premier jour et un montant de 10000 le lendemain.

## 13. Commandes d'exécution

Pour la voie **Docker recommandée**, utiliser directement `start-all.ps1`, `run-consumer-docker.ps1` et `run-mapreduce-docker.ps1` comme indiqué dans [docs/hadoop-docker.md](docs/hadoop-docker.md). Les commandes ci-dessous concernent uniquement une installation Hadoop locale ou un cluster externe avec client `hadoop`/`hdfs` installé.

```powershell
# Services et compilation
.\scripts\start-kafka.ps1
.\scripts\check-hdfs.ps1
mvn clean package

# Dans deux terminaux séparés : Consumer puis Producer; arrêter avec Ctrl+C
mvn exec:java '-Dexec.mainClass=com.saad.bigdata.consumer.HdfsTransactionConsumer'
mvn exec:java '-Dexec.mainClass=com.saad.bigdata.producer.TransactionProducer'

# Vérifier les données puis lancer les six jobs, le rapport et l'export Power BI
hdfs dfs -ls -R /transactions
.\scripts\run-mapreduce.ps1
```

Le jar pour `hadoop jar` est `target/transaction-producer-1.0.0-all.jar`. Il contient les dépendances applicatives; les bibliothèques Hadoop viennent de l'installation Hadoop. Exécution manuelle, dans cet ordre :

```powershell
$jar = 'target/transaction-producer-1.0.0-all.jar'
hadoop jar $jar com.saad.bigdata.mapreduce.ClientStatsJob /transactions /results/client-stats
hadoop jar $jar com.saad.bigdata.mapreduce.CategoryStatsJob /transactions /results/category-stats
hadoop jar $jar com.saad.bigdata.mapreduce.PeriodStatsJob /transactions /results/period-stats
hadoop jar $jar com.saad.bigdata.mapreduce.AmountAnomalyJob /transactions /results/category-stats /results/transaction-anomalies
hadoop jar $jar com.saad.bigdata.mapreduce.HighVolumeCustomerJob /results/client-stats /results/customer-anomalies
hadoop jar $jar com.saad.bigdata.mapreduce.PeriodVariationJob /results/period-stats /results/period-anomalies
hadoop jar $jar com.saad.bigdata.report.ReportGenerator /results reports
hadoop jar $jar com.saad.bigdata.report.PowerBiExport /transactions /results reports/powerbi/data data
```

Exemple de surcharge : `hadoop jar $jar com.saad.bigdata.mapreduce.AmountAnomalyJob -Danomaly.std.multiplier=2.5 /transactions /results/category-stats /results/transaction-anomalies`. Le pipeline accepte `[TRANSACTIONS [RESULTS [REPORTS_LOCAL [REFERENCE_DATA_LOCAL]]]]` après le nom de classe.

## 14. Vérification

```powershell
hdfs dfs -ls -R /results
hdfs dfs -cat /results/client-stats/part-r-00000
hdfs dfs -cat /results/category-stats/part-r-00000
hdfs dfs -cat /results/period-stats/part-r-00000
hdfs dfs -cat '/results/transaction-anomalies/part-*'
hdfs dfs -cat '/results/customer-anomalies/part-*'
hdfs dfs -cat /results/period-anomalies/part-r-00000
Get-Content reports/summary-report.csv
Get-Content reports/powerbi/data/data_quality.csv
.\scripts\show-results.ps1
```

Les jobs sans reducer peuvent produire plusieurs fichiers `part-m-*`; utiliser `part-*` pour eux.

## 15. Exemple chiffré

Une transaction normale `TRX10001` de catégorie Electronics, montant `7000`, reste sous `7100`. Une transaction `TRX50001` de même catégorie, montant `25000`, dépasse ce seuil : `3500 + 3 × 1200 = 7100`. Elle apparaît dans `transaction-anomalies` avec montant, moyenne, écart-type, seuil et motif.

## 16. Structure HDFS

```text
/transactions/year=2026/month=11/day=01/part-....jsonl
/results/client-stats/part-r-00000
/results/category-stats/part-r-00000
/results/period-stats/part-r-00000
/results/transaction-anomalies/part-m-00000
/results/customer-anomalies/part-m-00000
/results/period-anomalies/part-r-00000
```

## 17. Tests

`mvn test` lance JUnit pour la génération, la sérialisation JSON, le seuil statistique et la variation de période. Ces tests ne lancent pas de cluster Hadoop.

## 18. Erreurs fréquentes

| Problème | Solution |
|---|---|
| `mvn` ou `java` non reconnu | Installer Maven/JDK 17 et ajouter leurs `bin` au `PATH`. |
| `JAVA_HOME` incorrect | Pointer vers la racine du JDK, sans `bin`, puis rouvrir PowerShell. |
| Docker arrêté | Démarrer Docker Desktop, puis `docker compose up -d`. |
| Kafka `connection refused` | Vérifier `docker compose ps -a`, les logs Kafka et le port 9092. |
| Topic absent | Exécuter `.\scripts\create-topic.ps1`. |
| HDFS `connection refused` / NameNode arrêté | Démarrer le cluster; vérifier `fs.defaultFS` et `hdfs dfsadmin -report`. |
| `HADOOP_HOME` incorrect | Pointer vers la racine Hadoop et vérifier `HADOOP_CONF_DIR`. |
| `winutils.exe` sous Windows | Utiliser un client Windows compatible ou exécuter Hadoop et Java sous WSL2. |
| Dossier output déjà existant | Supprimer explicitement le résultat concerné ou choisir un autre chemin. |
| DataNode inaccessible | Utiliser une adresse annoncée joignable depuis le client, ou lancer le client dans le même réseau. |

## 19. Nettoyage

```powershell
docker compose down
hdfs dfs -rm -r /results/client-stats
# Pour relancer tout le pipeline après vérification des données à conserver :
hdfs dfs -rm -r /results
```

MapReduce refuse de démarrer si le dossier output existe déjà. L'effacement reste manuel.

## 20. Limites

MapReduce traite des lots : pas de détection instantanée événement par événement. Le seuil moyenne + 3σ peut être influencé par les valeurs extrêmes. Les périodes absentes ne comptent pas comme zéros. Le Consumer est « au moins une fois » : un lot peut être dupliqué après une panne entre HDFS et le commit Kafka. Une anomalie n'est pas nécessairement une fraude.

## 21. Analyse Power BI

La dernière étape exporte les transactions détaillées et toutes les sorties MapReduce vers `reports/powerbi/data/`. Le dossier [powerbi/README.md](powerbi/README.md) décrit l'import dans Power BI Desktop, les onze tables, les relations, les mesures DAX et sept pages de rapport. [theme.json](powerbi/theme.json) fournit les couleurs et la typographie; [design-preview.svg](powerbi/design-preview.svg) montre la maquette. Le fichier `.pbix` doit être enregistré avec Power BI Desktop après import : aucun rapport natif non vérifié n'est présenté comme terminé.

## 22. Améliorations futures

Hors de la version académique : Apache Spark, Spark Streaming, Kafka Streams, Flink, Machine Learning (Isolation Forest, Autoencoder), dashboard, Elasticsearch, Kibana, Grafana et API REST.

## Structure du dépôt

```text
Projet1_big_data/
├── pom.xml, docker-compose.yml, docker-compose.hadoop.yml, README.md
├── data/Customers.csv, data/Products.csv
├── reports/                         # rapports générés
├── powerbi/                         # thème, requêtes M, DAX, maquette
├── scripts/                         # commandes PowerShell
├── src/main/resources/application.properties
├── src/main/java/com/saad/bigdata/
│   ├── producer/, consumer/, mapreduce/, report/, util/
└── src/test/java/com/saad/bigdata/
```
