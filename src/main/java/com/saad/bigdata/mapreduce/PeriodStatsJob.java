package com.saad.bigdata.mapreduce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.LocalDateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public final class PeriodStatsJob {
    private PeriodStatsJob() {}
    public static String period(String timestamp,String granularity) {
        LocalDateTime time=LocalDateTime.parse(timestamp);
        return switch(granularity) {
            case "hour" -> time.toLocalDate()+"T"+String.format("%02d",time.getHour());
            case "day" -> time.toLocalDate().toString();
            case "month" -> time.toLocalDate().toString().substring(0,7);
            default -> throw new IllegalArgumentException("period.granularity: hour, day ou month attendu");
        };
    }
    public static class PeriodMapper extends Mapper<LongWritable,Text,Text,DoubleWritable> {
        @Override protected void map(LongWritable key,Text value,Context context) throws java.io.IOException,InterruptedException {
            String period;double amount;
            try {
                JsonNode row=JobSupport.parse(value);
                period=period(JobSupport.required(row,"timestamp"),context.getConfiguration().get("period.granularity","day"));
                amount=JobSupport.amount(row);
            } catch(Exception e) { context.getCounter("DATA","INVALID_TRANSACTION").increment(1); return; }
            context.write(new Text(period),new DoubleWritable(amount));
        }
    }
    public static class PeriodReducer extends Reducer<Text,DoubleWritable,NullWritable,Text> {
        @Override protected void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws java.io.IOException,InterruptedException {
            long count=0; double total=0; for(DoubleWritable v:values){count++;total+=v.get();}
            ObjectNode row=JobSupport.object(); row.put("period",key.toString()); row.put("count",count);
            row.put("total_amount",total); row.put("average_amount",total/count);
            context.write(NullWritable.get(),new Text(row.toString()));
        }
    }
    public static boolean run(Configuration conf,String input,String output) throws Exception {
        Job job=JobSupport.job("PeriodStatsJob",PeriodStatsJob.class,conf,input,output);
        job.setMapperClass(PeriodMapper.class);job.setReducerClass(PeriodReducer.class);
        job.setMapOutputKeyClass(Text.class);job.setMapOutputValueClass(DoubleWritable.class);return JobSupport.run(job);
    }
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();args=JobSupport.options(conf,args);
        if(args.length!=2) throw new IllegalArgumentException("PeriodStatsJob INPUT OUTPUT");
        JobSupport.mainExit(run(conf,args[0],args[1]));
    }
}
