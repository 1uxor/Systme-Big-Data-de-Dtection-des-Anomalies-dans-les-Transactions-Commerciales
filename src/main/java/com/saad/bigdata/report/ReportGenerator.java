package com.saad.bigdata.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.saad.bigdata.mapreduce.JobSupport;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/** Lit les part-* HDFS et écrit deux fichiers locaux faciles à présenter. */
public final class ReportGenerator {
    private ReportGenerator() {}
    public static void generate(Configuration conf,String results,String outputDir) throws Exception {
        List<JsonNode> clients=JobSupport.readParts(conf,results+"/client-stats");
        List<JsonNode> transactions=JobSupport.readParts(conf,results+"/transaction-anomalies");
        List<JsonNode> unusualClients=JobSupport.readParts(conf,results+"/customer-anomalies");
        List<JsonNode> periods=JobSupport.readParts(conf,results+"/period-anomalies");
        long total=clients.stream().mapToLong(r->r.path("count").asLong()).sum();
        Map<String,Long> categories=new HashMap<>();
        for(JsonNode row:transactions) categories.merge(row.path("category").asText(),1L,Long::sum);
        Path dir=Path.of(outputDir);Files.createDirectories(dir);
        List<String> csv=List.of("metric,value","total_transactions,"+total,
                "transaction_anomalies,"+transactions.size(),"customer_anomalies,"+unusualClients.size(),
                "period_anomalies,"+periods.size());
        Files.write(dir.resolve("summary-report.csv"),csv,StandardCharsets.UTF_8);
        List<String> text=new ArrayList<>();
        text.add("RAPPORT DES ANOMALIES");text.add("Transactions analysées: "+total);
        text.add("Anomalies transactionnelles: "+transactions.size());
        text.add("Clients atypiques: "+unusualClients.size());text.add("Variations de période: "+periods.size());
        text.add("");text.add("Catégories les plus touchées:");
        categories.entrySet().stream().sorted(Map.Entry.<String,Long>comparingByValue().reversed())
                .limit(10).forEach(e->text.add(e.getKey()+": "+e.getValue()));
        text.add("");text.add("Périodes avec variation importante:");
        periods.stream().sorted(Comparator.comparing(r->r.path("period").asText()))
                .forEach(r->text.add(r.toString()));
        text.add("");text.add("Top 10 transactions atypiques (montant décroissant):");
        transactions.stream().sorted(Comparator.comparingDouble((JsonNode r)->r.path("amount").asDouble()).reversed())
                .limit(10).forEach(r->text.add(r.toString()));
        text.add("");text.add("Top 10 clients atypiques (montant cumulé décroissant):");
        unusualClients.stream().sorted(Comparator.comparingDouble((JsonNode r)->r.path("total_amount").asDouble()).reversed())
                .limit(10).forEach(r->text.add(r.toString()));
        Files.write(dir.resolve("anomaly-report.txt"),text,StandardCharsets.UTF_8);
    }
    public static void main(String[] args) throws Exception {
        if(args.length>2) throw new IllegalArgumentException("ReportGenerator [RESULTS_HDFS [REPORTS_LOCAL]]");
        generate(new Configuration(),args.length>0?args[0]:"/results",args.length>1?args[1]:"reports");
    }
}
