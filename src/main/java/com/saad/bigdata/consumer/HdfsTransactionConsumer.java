package com.saad.bigdata.consumer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saad.bigdata.util.ConfigLoader;

public final class HdfsTransactionConsumer {

    private static final String BOOTSTRAP_SERVERS = ConfigLoader.get("kafka.bootstrap.servers", "localhost:9092");
    private static final String TOPIC = ConfigLoader.get("kafka.topic.transactions", "transactions");
    private static final String GROUP_ID = ConfigLoader.get("kafka.consumer.group", "hdfs-archiver");
    private static final String HDFS_ROOT_DIRECTORY = "/transactions";

    private static final int TARGET_BATCH_SIZE = 500;
    private static final Duration MAX_BATCH_WAIT = Duration.ofSeconds(5);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private HdfsTransactionConsumer() {
    }

    public static void main(String[] args) {
        AtomicBoolean shutdownRequested = new AtomicBoolean(false);
        CountDownLatch resourcesClosed = new CountDownLatch(1);
        int exitCode = 0;

        try {
            Configuration hadoopConfiguration = createHadoopConfiguration();

            try (FileSystem fileSystem = FileSystem.get(hadoopConfiguration);
                    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createConsumerProperties())) {

                verifyHdfsConnection(fileSystem);

                Thread shutdownHook = new Thread(() -> {
                    shutdownRequested.set(true);
                    System.out.println("Arrêt demandé : finalisation du dernier batch...");

                    // wakeup() est la méthode prévue par Kafka pour interrompre poll()
                    // depuis un autre thread. Le thread principal ferme ensuite les ressources.
                    consumer.wakeup();

                    try {
                        resourcesClosed.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                }, "hdfs-consumer-shutdown-hook");

                Runtime.getRuntime().addShutdownHook(shutdownHook);

                try {
                    consumeAndArchive(consumer, fileSystem, shutdownRequested);
                } finally {
                    if (!shutdownRequested.get()) {
                        try {
                            Runtime.getRuntime().removeShutdownHook(shutdownHook);
                        } catch (IllegalStateException ignored) {
                            // La JVM a commencé son arrêt entre le test et cet appel.
                        }
                    }
                }
            }
        } catch (Exception exception) {
            exitCode = 1;
            System.err.printf("Erreur du Consumer Kafka/HDFS : %s%n", exception.getMessage());
            exception.printStackTrace(System.err);
        } finally {
            resourcesClosed.countDown();
        }

        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static void consumeAndArchive(
            KafkaConsumer<String, String> consumer,
            FileSystem fileSystem,
            AtomicBoolean shutdownRequested) throws IOException {

        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        long firstBufferedRecordAt = 0L;

        consumer.subscribe(List.of(TOPIC));
        System.out.printf(
                "Consumer démarré : broker=%s, topic=%s, group=%s, batch=%d ou %d secondes%n",
                BOOTSTRAP_SERVERS,
                TOPIC,
                GROUP_ID,
                TARGET_BATCH_SIZE,
                MAX_BATCH_WAIT.toSeconds());

        try {
            while (!shutdownRequested.get()) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);

                if (!records.isEmpty()) {
                    if (buffer.isEmpty()) {
                        firstBufferedRecordAt = System.nanoTime();
                    }
                    records.forEach(buffer::add);
                }

                boolean targetSizeReached = buffer.size() >= TARGET_BATCH_SIZE;
                boolean maximumWaitReached = !buffer.isEmpty()
                        && System.nanoTime() - firstBufferedRecordAt >= MAX_BATCH_WAIT.toNanos();

                if (targetSizeReached || maximumWaitReached) {
                    archiveAndCommit(buffer, consumer, fileSystem);
                    buffer.clear();
                    firstBufferedRecordAt = 0L;
                }
            }
        } catch (WakeupException exception) {
            if (!shutdownRequested.get()) {
                throw exception;
            }
        } finally {
            if (shutdownRequested.get() && !buffer.isEmpty()) {
                try {
                    archiveAndCommit(buffer, consumer, fileSystem);
                    buffer.clear();
                } catch (Exception exception) {
                    System.err.printf(
                            "Le dernier batch n'a pas été archivé ; ses offsets ne sont pas validés : %s%n",
                            exception.getMessage());
                }
            }
        }
    }

    private static void archiveAndCommit(
            List<ConsumerRecord<String, String>> records,
            KafkaConsumer<String, String> consumer,
            FileSystem fileSystem) throws IOException {

        System.out.printf("Received %d Kafka records%n", records.size());

        Map<LocalDate, List<ConsumerRecord<String, String>>> recordsByDate = groupRecordsByDate(records);

        for (Map.Entry<LocalDate, List<ConsumerRecord<String, String>>> entry : recordsByDate.entrySet()) {
            String archivedPath = writeJsonLinesFile(fileSystem, entry.getKey(), entry.getValue());
            System.out.printf(
                    "Archived %d records to:%n%s%n",
                    entry.getValue().size(),
                    archivedPath);
        }

        // Aucun offset n'est validé si une des écritures HDFS ci-dessus échoue.
        consumer.commitSync();
        System.out.println("Offsets committed successfully");
    }

    private static Map<LocalDate, List<ConsumerRecord<String, String>>> groupRecordsByDate(
            List<ConsumerRecord<String, String>> records) throws IOException {

        Map<LocalDate, List<ConsumerRecord<String, String>>> recordsByDate = new TreeMap<>();

        for (ConsumerRecord<String, String> record : records) {
            LocalDate transactionDate = extractTransactionDate(record);
            recordsByDate.computeIfAbsent(transactionDate, ignored -> new ArrayList<>()).add(record);
        }

        return recordsByDate;
    }

    private static LocalDate extractTransactionDate(ConsumerRecord<String, String> record) throws IOException {
        try {
            JsonNode transaction = OBJECT_MAPPER.readTree(record.value());
            JsonNode timestampNode = transaction == null ? null : transaction.get("timestamp");

            if (timestampNode == null || !timestampNode.isTextual() || timestampNode.asText().isBlank()) {
                throw new IOException(recordError(record, "champ timestamp absent ou invalide"));
            }

            return LocalDateTime.parse(timestampNode.asText()).toLocalDate();
        } catch (DateTimeParseException exception) {
            throw new IOException(recordError(record, "format timestamp invalide"), exception);
        } catch (JsonProcessingException exception) {
            throw new IOException(recordError(record, "JSON invalide"), exception);
        } catch (IOException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IOException(recordError(record, "JSON invalide"), exception);
        }
    }

    private static String writeJsonLinesFile(
            FileSystem fileSystem,
            LocalDate date,
            List<ConsumerRecord<String, String>> records) throws IOException {

        Path directory = new Path(String.format(
                "%s/year=%04d/month=%02d/day=%02d",
                HDFS_ROOT_DIRECTORY,
                date.getYear(),
                date.getMonthValue(),
                date.getDayOfMonth()));

        if (!fileSystem.mkdirs(directory) && !fileSystem.exists(directory)) {
            throw new IOException("Impossible de créer le répertoire HDFS " + directory);
        }

        String uniqueSuffix = UUID.randomUUID().toString().substring(0, 8);
        String fileName = "part-%d-%s.jsonl".formatted(System.currentTimeMillis(), uniqueSuffix);
        Path finalPath = new Path(directory, fileName);
        Path temporaryPath = new Path(directory, "." + fileName + ".inprogress");
        boolean published = false;

        try {
            try (FSDataOutputStream outputStream = fileSystem.create(temporaryPath, false);
                    BufferedWriter writer = new BufferedWriter(
                            new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {

                for (ConsumerRecord<String, String> record : records) {
                    writer.write(record.value());
                    writer.newLine();
                }

                writer.flush();
                outputStream.hflush();
            }

            if (!fileSystem.rename(temporaryPath, finalPath)) {
                throw new IOException("Impossible de publier le fichier HDFS " + finalPath);
            }

            published = true;
            return fileSystem.makeQualified(finalPath).toString();
        } finally {
            if (!published) {
                try {
                    fileSystem.delete(temporaryPath, false);
                } catch (IOException cleanupException) {
                    System.err.printf(
                            "Impossible de supprimer le fichier temporaire %s : %s%n",
                            temporaryPath,
                            cleanupException.getMessage());
                }
            }
        }
    }

    private static Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, TARGET_BATCH_SIZE);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "hdfs-transaction-archiver");
        return properties;
    }

    private static Configuration createHadoopConfiguration() {
        Configuration configuration = new Configuration();
        String configurationDirectory = firstNonBlank(
                System.getenv("HADOOP_CONF_DIR"),
                hadoopHomeConfigurationDirectory());

        if (configurationDirectory == null) {
            return configuration;
        }

        java.nio.file.Path directory = Paths.get(configurationDirectory).toAbsolutePath().normalize();
        if (!Files.isDirectory(directory)) {
            throw new IllegalStateException("Répertoire Hadoop introuvable : " + directory);
        }

        addHadoopResource(configuration, directory.resolve("core-site.xml"));
        addHadoopResource(configuration, directory.resolve("hdfs-site.xml"));
        return configuration;
    }

    private static void addHadoopResource(Configuration configuration, java.nio.file.Path resource) {
        if (Files.isRegularFile(resource)) {
            configuration.addResource(new Path(resource.toUri()));
            System.out.println("Configuration Hadoop chargée : " + resource);
        }
    }

    private static String hadoopHomeConfigurationDirectory() {
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null || hadoopHome.isBlank()) {
            return null;
        }
        return Paths.get(hadoopHome, "etc", "hadoop").toString();
    }

    private static String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return null;
    }

    private static void verifyHdfsConnection(FileSystem fileSystem) throws IOException {
        String scheme = fileSystem.getUri().getScheme();
        if (scheme == null || !scheme.equalsIgnoreCase("hdfs")) {
            throw new IllegalStateException(
                    "fs.defaultFS ne désigne pas HDFS (valeur actuelle : " + fileSystem.getUri()
                            + "). Vérifiez HADOOP_CONF_DIR et core-site.xml.");
        }

        if (!fileSystem.exists(new Path("/"))) {
            throw new IOException("La racine HDFS n'est pas accessible : " + fileSystem.getUri());
        }

        System.out.println("Connected to HDFS: " + fileSystem.getUri());
    }

    private static String recordError(ConsumerRecord<String, String> record, String reason) {
        return "%s (topic=%s, partition=%d, offset=%d)".formatted(
                reason,
                record.topic(),
                record.partition(),
                record.offset());
    }
}
