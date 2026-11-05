package com.saad.bigdata.mapreduce;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import com.saad.bigdata.util.ConfigLoader;

/** Format JSONL commun aux jobs; les fichiers HDFS part-* sont les seules entrées dérivées. */
public final class JobSupport {
    public static final ObjectMapper JSON = new ObjectMapper();
    private JobSupport() {}
    public static JsonNode parse(Text line) throws java.io.IOException { return JSON.readTree(line.toString()); }
    public static ObjectNode object() { return JSON.createObjectNode(); }
    public static String required(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.asText().isBlank()) throw new IllegalArgumentException("Champ absent: " + field);
        return value.asText();
    }
    public static double amount(JsonNode node) {
        JsonNode value = node.get("total_amount");
        if (value == null || !value.isNumber() || !Double.isFinite(value.asDouble()))
            throw new IllegalArgumentException("total_amount invalide");
        return value.asDouble();
    }
    public static Job job(String name, Class<?> jarClass, Configuration conf, String input, String output)
            throws Exception {
        ConfigLoader.applyAnomalyDefaults(conf);
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(jarClass);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job;
    }
    public static boolean run(Job job) throws Exception { return job.waitForCompletion(true); }
    public static void mainExit(boolean result) { if (!result) System.exit(1); }
    public static String[] options(Configuration conf,String[] args) throws Exception {
        return new GenericOptionsParser(conf,args).getRemainingArgs();
    }
    public static List<JsonNode> readParts(Configuration conf, String dir) throws Exception {
        Path path = new Path(dir);
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) throw new java.io.IOException("Résultat absent: " + path);
        List<JsonNode> result = new ArrayList<>();
        for (FileStatus status : fs.listStatus(path)) {
            if (!status.isFile() || !status.getPath().getName().startsWith("part-")) continue;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) if (!line.isBlank()) result.add(JSON.readTree(line));
            }
        }
        return result;
    }
}
