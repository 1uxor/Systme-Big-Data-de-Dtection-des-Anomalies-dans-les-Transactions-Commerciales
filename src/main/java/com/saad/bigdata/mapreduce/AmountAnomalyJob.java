package com.saad.bigdata.mapreduce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.saad.bigdata.util.AnomalyMath;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/** Jointure simple: statistiques de catégories chargées au démarrage de chaque mapper. */
public final class AmountAnomalyJob {
    private AmountAnomalyJob() {}
    public static class AmountMapper extends Mapper<LongWritable,Text,NullWritable,Text> {
        private final Map<String,JsonNode> stats=new HashMap<>();
        private double multiplier;
        @Override protected void setup(Context context) throws IOException {
            Configuration conf=context.getConfiguration();
            multiplier=conf.getDouble("anomaly.std.multiplier",3.0);
            if(multiplier<0) throw new IOException("Multiplicateur négatif");
            try {
                for(JsonNode row:JobSupport.readParts(conf,conf.get("category.stats.path")))
                    stats.put(JobSupport.required(row,"category"),row);
            } catch(Exception e) { throw new IOException("Impossible de lire CategoryStatsJob",e); }
            if(stats.isEmpty()) throw new IOException("Statistiques de catégories vides");
        }
        @Override protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
            ObjectNode output=null;
            try {
                JsonNode transaction=JobSupport.parse(value);
                String category=JobSupport.required(transaction,"category");
                JsonNode categoryStats=stats.get(category);
                if(categoryStats==null) {context.getCounter("DATA","UNKNOWN_CATEGORY").increment(1);return;}
                double amount=JobSupport.amount(transaction),mean=categoryStats.path("mean").asDouble(),std=categoryStats.path("std").asDouble();
                double threshold=AnomalyMath.amountThreshold(mean,std,multiplier);
                if(amount>threshold) {
                    ObjectNode row=JobSupport.object();row.put("transaction_id",JobSupport.required(transaction,"transaction_id"));
                    String timestamp=JobSupport.required(transaction,"timestamp");
                    row.put("timestamp",timestamp);row.put("transaction_date",timestamp.substring(0,10));
                    row.put("customer_id",JobSupport.required(transaction,"customer_id"));row.put("category",category);
                    row.put("product_id",JobSupport.required(transaction,"product_id"));
                    row.put("city",JobSupport.required(transaction,"city"));
                    row.put("payment_method",JobSupport.required(transaction,"payment_method"));
                    row.put("amount",amount);row.put("category_mean",mean);row.put("category_std",std);
                    row.put("threshold",threshold);row.put("reason","amount_above_"+multiplier+"_sigma");
                    output=row;
                }
            } catch(Exception e) {context.getCounter("DATA","INVALID_TRANSACTION").increment(1);return;}
            if(output!=null) context.write(NullWritable.get(),new Text(output.toString()));
        }
    }
    public static boolean run(Configuration conf,String input,String categoryStats,String output) throws Exception {
        conf.set("category.stats.path",categoryStats);
        Job job=JobSupport.job("AmountAnomalyJob",AmountAnomalyJob.class,conf,input,output);
        job.setMapperClass(AmountMapper.class);job.setNumReduceTasks(0);return JobSupport.run(job);
    }
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();args=JobSupport.options(conf,args);
        if(args.length!=3) throw new IllegalArgumentException("AmountAnomalyJob TRANSACTIONS CATEGORY_STATS OUTPUT");
        JobSupport.mainExit(run(conf,args[0],args[1],args[2]));
    }
}
