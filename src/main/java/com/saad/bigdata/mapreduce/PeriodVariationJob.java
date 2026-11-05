package com.saad.bigdata.mapreduce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.saad.bigdata.util.AnomalyMath;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/** Un reducer trie les périodes ISO et compare les périodes observées successives. */
public final class PeriodVariationJob {
    private PeriodVariationJob() {}
    public static class VariationMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override protected void map(LongWritable key,Text value,Context context) throws java.io.IOException,InterruptedException {
            String period;long count;
            try {JsonNode row=JobSupport.parse(value);period=JobSupport.required(row,"period");count=row.path("count").asLong();}
            catch(Exception e){context.getCounter("DATA","INVALID_PERIOD_STATS").increment(1);return;}
            context.write(new Text("periods"),new Text(period+"\t"+count));
        }
    }
    public static class VariationReducer extends Reducer<Text,Text,NullWritable,Text> {
        @Override protected void reduce(Text key,Iterable<Text> values,Context context) throws java.io.IOException,InterruptedException {
            Map<String,Long> periods=new TreeMap<>();
            for(Text value:values){String[] parts=value.toString().split("\t",2);periods.put(parts[0],Long.parseLong(parts[1]));}
            String previous=null;long previousCount=0;double threshold=context.getConfiguration().getDouble("anomaly.period.variation.percent",50);
            for(Map.Entry<String,Long> entry:periods.entrySet()) {
                if(previous!=null && previousCount>0) {
                    double variation=AnomalyMath.periodVariation(previousCount,entry.getValue());
                    if(Math.abs(variation)>threshold) {
                        ObjectNode row=JobSupport.object();row.put("previous_period",previous);row.put("period",entry.getKey());
                        row.put("previous_count",previousCount);row.put("current_count",entry.getValue());
                        row.put("variation_percent",variation);row.put("reason","ACTIVITY_VARIATION");
                        context.write(NullWritable.get(),new Text(row.toString()));
                    }
                }
                previous=entry.getKey();previousCount=entry.getValue();
            }
        }
    }
    public static boolean run(Configuration conf,String periodStats,String output) throws Exception {
        Job job=JobSupport.job("PeriodVariationJob",PeriodVariationJob.class,conf,periodStats,output);
        job.setMapperClass(VariationMapper.class);job.setReducerClass(VariationReducer.class);
        job.setMapOutputKeyClass(Text.class);job.setMapOutputValueClass(Text.class);job.setNumReduceTasks(1);return JobSupport.run(job);
    }
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();args=JobSupport.options(conf,args);
        if(args.length!=2) throw new IllegalArgumentException("PeriodVariationJob PERIOD_STATS OUTPUT");
        JobSupport.mainExit(run(conf,args[0],args[1]));
    }
}
