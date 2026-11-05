package com.saad.bigdata.report;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.saad.bigdata.mapreduce.JobSupport;
import com.saad.bigdata.util.ConfigLoader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

/** Export CSV de présentation: les calculs et signalements restent produits par MapReduce. */
public final class PowerBiExport {
    private PowerBiExport() {}

    public static void generate(Configuration conf,String input,String results,String outputDir,String referenceDir) throws Exception {
        Path dir=Path.of(outputDir);Files.createDirectories(dir);
        List<JsonNode> clientStats=JobSupport.readParts(conf,results+"/client-stats");
        List<JsonNode> categoryStats=JobSupport.readParts(conf,results+"/category-stats");
        List<JsonNode> periodStats=JobSupport.readParts(conf,results+"/period-stats");
        List<JsonNode> amountAnomalies=JobSupport.readParts(conf,results+"/transaction-anomalies");
        List<JsonNode> customerAnomalies=JobSupport.readParts(conf,results+"/customer-anomalies");
        List<JsonNode> periodAnomalies=JobSupport.readParts(conf,results+"/period-anomalies");

        long[] raw=writeTransactions(conf,input,dir.resolve("transactions.csv"));
        writeRows(dir.resolve("client_stats.csv"),clientStats,
                "customer_id,count,total_amount,average_amount");
        writeRows(dir.resolve("category_stats.csv"),categoryStats,
                "category,count,total_amount,mean,std");
        writeRows(dir.resolve("period_stats.csv"),periodStats,
                "period,count,total_amount,average_amount");
        writeRows(dir.resolve("transaction_anomalies.csv"),amountAnomalies,
                "transaction_id,timestamp,transaction_date,customer_id,product_id,category,city,payment_method,amount,category_mean,category_std,threshold,reason");
        writeRows(dir.resolve("customer_anomalies.csv"),customerAnomalies,
                "customer_id,transaction_count,total_amount,reason");
        writeRows(dir.resolve("period_anomalies.csv"),periodAnomalies,
                "previous_period,period,previous_count,current_count,variation_percent,reason");
        List<JsonNode> categories=new ArrayList<>(categoryStats);
        writeRows(dir.resolve("categories.csv"),categories,"category");

        copyReference(Path.of(referenceDir,"Customers.csv"),dir.resolve("customers.csv"));
        copyReference(Path.of(referenceDir,"Products.csv"),dir.resolve("products.csv"));
        long statsCount=clientStats.stream().mapToLong(row->row.path("count").asLong()).sum();
        List<String> quality=List.of("metric,value",
                "raw_transactions,"+raw[0],"invalid_raw_lines,"+raw[1],
                "client_stats_transactions,"+statsCount,
                "count_difference,"+(statsCount-raw[0]),
                "transaction_anomalies,"+amountAnomalies.size(),
                "customer_anomalies,"+customerAnomalies.size(),
                "period_anomalies,"+periodAnomalies.size(),
                "period_granularity,"+conf.get("period.granularity",ConfigLoader.get("period.granularity","day")),
                "std_multiplier,"+conf.get("anomaly.std.multiplier",ConfigLoader.get("anomaly.std.multiplier","3.0")),
                "exported_at,"+LocalDateTime.now());
        Files.write(dir.resolve("data_quality.csv"),quality,StandardCharsets.UTF_8);
        System.out.printf("Power BI: %d transactions, %d lignes invalides; données dans %s%n",raw[0],raw[1],dir.toAbsolutePath());
    }

    private static long[] writeTransactions(Configuration conf,String input,Path output) throws Exception {
        org.apache.hadoop.fs.Path source=new org.apache.hadoop.fs.Path(input);
        FileSystem fs=source.getFileSystem(conf);
        long valid=0,invalid=0;
        try(BufferedWriter writer=Files.newBufferedWriter(output,StandardCharsets.UTF_8)) {
            writer.write("transaction_id,timestamp,transaction_date,year,month,day,hour,customer_id,product_id,category,quantity,unit_price,total_amount,payment_method,city\n");
            RemoteIterator<LocatedFileStatus> files=fs.listFiles(source,true);
            while(files.hasNext()) {
                LocatedFileStatus file=files.next();
                String name=file.getPath().getName();
                if(!name.endsWith(".jsonl") || name.startsWith(".") || name.startsWith("_")) continue;
                try(BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(file.getPath()),StandardCharsets.UTF_8))) {
                    String line;
                    while((line=reader.readLine())!=null) {
                        String[] fields;
                        try {
                            JsonNode row=JobSupport.JSON.readTree(line);
                            String timestamp=JobSupport.required(row,"timestamp");
                            LocalDateTime time=LocalDateTime.parse(timestamp);
                            fields=new String[]{
                                    JobSupport.required(row,"transaction_id"),timestamp,time.toLocalDate().toString(),
                                    Integer.toString(time.getYear()),Integer.toString(time.getMonthValue()),
                                    Integer.toString(time.getDayOfMonth()),Integer.toString(time.getHour()),
                                    JobSupport.required(row,"customer_id"),JobSupport.required(row,"product_id"),
                                    JobSupport.required(row,"category"),JobSupport.required(row,"quantity"),
                                    JobSupport.required(row,"unit_price"),Double.toString(JobSupport.amount(row)),
                                    JobSupport.required(row,"payment_method"),JobSupport.required(row,"city")};
                        } catch(JsonProcessingException | IllegalArgumentException e) { invalid++;continue; }
                        writeLine(writer,fields);valid++;
                    }
                }
            }
        }
        return new long[]{valid,invalid};
    }

    static void writeRows(Path path,List<JsonNode> rows,String header) throws Exception {
        String[] fields=header.split(",");
        try(BufferedWriter writer=Files.newBufferedWriter(path,StandardCharsets.UTF_8)) {
            writer.write(header);writer.newLine();
            for(JsonNode row:rows) {
                String[] values=new String[fields.length];
                for(int i=0;i<fields.length;i++) values[i]=row.path(fields[i]).asText("");
                writeLine(writer,values);
            }
        }
    }
    private static void writeLine(BufferedWriter writer,String[] values) throws java.io.IOException {
        for(int i=0;i<values.length;i++) {
            if(i>0) writer.write(',');
            writer.write('"');writer.write(values[i].replace("\"","\"\""));writer.write('"');
        }
        writer.newLine();
    }
    private static void copyReference(Path source,Path target) throws Exception {
        if(!Files.isRegularFile(source)) throw new java.io.IOException("Référence absente: "+source);
        Files.copy(source,target,StandardCopyOption.REPLACE_EXISTING);
    }
    public static void main(String[] args) throws Exception {
        if(args.length>4) throw new IllegalArgumentException("PowerBiExport [TRANSACTIONS_HDFS [RESULTS_HDFS [OUTPUT_LOCAL [REFERENCE_DATA_LOCAL]]]]");
        generate(new Configuration(),args.length>0?args[0]:"/transactions",args.length>1?args[1]:"/results",
                args.length>2?args[2]:"reports/powerbi/data",args.length>3?args[3]:"data");
    }
}
