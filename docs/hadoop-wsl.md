# Créer le cluster Hadoop du projet sous Windows avec WSL2

Le dépôt fournit Kafka via `docker-compose.yml`, mais **ne déploie pas Hadoop**. Pour une démonstration académique sur un seul PC, utiliser Hadoop 3.5.0 en mode pseudo-distribué sous Ubuntu/WSL2 : un NameNode, un DataNode, un ResourceManager et un NodeManager. Ce sont de vrais services HDFS et YARN/MapReduce, même s'ils tournent sur une seule machine.

Si un cluster Hadoop de l'établissement existe déjà, ne suivre **aucune étape d'installation ni de formatage** ci-dessous. Récupérer simplement ses fichiers `core-site.xml`, `hdfs-site.xml`, `mapred-site.xml` et `yarn-site.xml`, puis définir `HADOOP_CONF_DIR` vers leur dossier.

## 1. Installer WSL2 et Ubuntu

Dans **PowerShell en administrateur** :

```powershell
wsl --install -d Ubuntu
```

Redémarrer Windows si demandé. Ouvrir Ubuntu et créer un utilisateur Linux. Vérifier depuis PowerShell :

```powershell
wsl --list --verbose
```

La distribution Ubuntu doit utiliser WSL version 2. Si elle est déjà installée, ne pas la réinstaller.

## 2. Installer Java 17, Maven et SSH dans Ubuntu

Les commandes suivantes sont à lancer **dans le terminal Ubuntu**, pas dans PowerShell :

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk maven openssh-server rsync curl
java -version
mvn -version
sudo service ssh start
```

Hadoop 3.5.0 utilise Java 17 côté serveur. Si `java -version` affiche une autre version, choisir le JDK 17 avec `sudo update-alternatives --config java`.

## 3. Télécharger Hadoop 3.5.0

```bash
cd ~
curl -fL -o hadoop-3.5.0.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz
tar -xzf hadoop-3.5.0.tar.gz
```

L'archive binaire est volumineuse (environ 554 Mo). Dans `~/.bashrc`, ajouter :

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME="$HOME/hadoop-3.5.0"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
```

Puis recharger le fichier :

```bash
source ~/.bashrc
hadoop version
```

Si le chemin de Java diffère, utiliser `readlink -f $(command -v javac)` pour repérer le JDK et ajuster `JAVA_HOME`. Ajouter également `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` dans `$HADOOP_HOME/etc/hadoop/hadoop-env.sh` (chemin à adapter si nécessaire).

## 4. Configurer HDFS

Ces commandes écrivent la configuration du **nouveau** Hadoop installé sous `~/hadoop-3.5.0`.

```bash
mkdir -p "$HOME/hadoop-data/namenode" "$HOME/hadoop-data/datanode"

cat > "$HADOOP_CONF_DIR/core-site.xml" <<'EOF'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
EOF

cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://$HOME/hadoop-data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://$HOME/hadoop-data/datanode</value>
  </property>
</configuration>
EOF
```

Le port `9000` est le port RPC utilisé par le Consumer; `9870` est l'interface Web du NameNode. Les dossiers `hadoop-data` sont dans le système de fichiers Linux de WSL, ce qui évite de stocker les données HDFS sous `/mnt/c`.

## 5. Configurer MapReduce sur YARN

```bash
cat > "$HADOOP_CONF_DIR/mapred-site.xml" <<'EOF'
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
</configuration>
EOF

cat > "$HADOOP_CONF_DIR/yarn-site.xml" <<'EOF'
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
  </property>
</configuration>
EOF
```

## 6. Autoriser SSH local pour les scripts Hadoop

Les scripts `start-dfs.sh` et `start-yarn.sh` utilisent SSH vers `localhost`.

```bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
test -f ~/.ssh/id_rsa || ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa
touch ~/.ssh/authorized_keys
grep -qxF "$(cat ~/.ssh/id_rsa.pub)" ~/.ssh/authorized_keys || cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh -o StrictHostKeyChecking=accept-new localhost 'echo SSH_OK'
```

Si SSH refuse la connexion, exécuter `sudo service ssh start` et réessayer.

## 7. Initialiser et démarrer le cluster

**Uniquement la première fois, sur ce nouveau cluster vide**, formater le NameNode :

```bash
hdfs namenode -format
```

Ne jamais relancer cette commande sur un cluster qui contient déjà des données HDFS. Ensuite, à chaque démarrage d'Ubuntu :

```bash
sudo service ssh start
start-dfs.sh
start-yarn.sh
jps
hdfs dfsadmin -report
yarn node -list
hdfs dfs -mkdir -p "/user/$USER" /transactions
hdfs dfs -ls /
```

`jps` doit montrer au minimum `NameNode`, `DataNode`, `ResourceManager` et `NodeManager`. Les interfaces sont normalement visibles sur <http://localhost:9870/> (HDFS) et <http://localhost:8088/> (YARN). Les services WSL sont généralement joignables depuis le navigateur Windows via `localhost`.

## 8. Exécuter le projet avec ce cluster

Lancer Kafka avec Docker Desktop depuis PowerShell :

```powershell
Set-Location 'C:\my things\Projet1_big_data'
docker compose up -d
```

Dans Ubuntu, vérifier que `localhost:9092` atteint Kafka. Selon la configuration réseau WSL, les services Windows ne sont pas toujours accessibles via `localhost`. Le broker Kafka du dépôt annonce `localhost:9092` : il faut que cette adresse fonctionne **depuis Ubuntu** avant de lancer Producer et Consumer. Si elle ne fonctionne pas, activer l'intégration WSL de Docker Desktop ou adapter ensemble les adresses `kafka.bootstrap.servers` et `KAFKA_ADVERTISED_LISTENERS`; changer seulement la première ne suffit pas.

Ouvrir ensuite plusieurs terminaux Ubuntu. Le dépôt Windows est disponible sous :

```bash
cd "/mnt/c/my things/Projet1_big_data"
mvn clean package
```

Terminal A — Consumer (le laisser tourner) :

```bash
cd "/mnt/c/my things/Projet1_big_data"
mvn exec:java '-Dexec.mainClass=com.saad.bigdata.consumer.HdfsTransactionConsumer'
```

Terminal B — Producer (le laisser tourner le temps de générer les transactions, puis `Ctrl+C`) :

```bash
cd "/mnt/c/my things/Projet1_big_data"
mvn exec:java '-Dexec.mainClass=com.saad.bigdata.producer.TransactionProducer'
```

Après que le Consumer a archivé les derniers lots, l'arrêter avec `Ctrl+C`, puis vérifier et lancer l'analyse :

```bash
hdfs dfs -ls -R /transactions
cd "/mnt/c/my things/Projet1_big_data"
hadoop jar target/transaction-producer-1.0.0-all.jar com.saad.bigdata.mapreduce.AnomalyDetectionPipeline /transactions /results reports data
hdfs dfs -ls -R /results
cat reports/summary-report.csv
ls reports/powerbi/data
```

Le script PowerShell `run-mapreduce.ps1` est destiné à un client Hadoop installé dans PowerShell. Sous Ubuntu/WSL, utiliser la commande `hadoop jar` ci-dessus. Les rapports sont écrits dans le dépôt Windows et restent donc accessibles à Power BI Desktop.

## 9. Arrêt

```bash
stop-yarn.sh
stop-dfs.sh
```

Puis, si souhaité, dans PowerShell : `docker compose down`. Les fichiers HDFS restent dans `~/hadoop-data`; ne pas les supprimer si l'on souhaite conserver les transactions et les résultats.

## Références officielles

- [Installation WSL](https://learn.microsoft.com/windows/wsl/install)
- [Hadoop 3.5.0 : cluster mono-nœud, HDFS et YARN](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Hadoop 3.5.0 : Java 17](https://hadoop.apache.org/docs/r3.5.0/)
