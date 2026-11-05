package com.saad.bigdata.mapreduce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public final class CategoryStatsJob {
    private CategoryStatsJob() {}
    public static class Moments implements Writable {
        long count; double sum; double sumSquares;
        public Moments() {}
        Moments(long count, double sum, double sumSquares) { this.count=count; this.sum=sum; this.sumSquares=sumSquares; }
        @Override public void write(DataOutput out) throws IOException { out.writeLong(count); out.writeDouble(sum); out.writeDouble(sumSquares); }
        @Override public void readFields(DataInput in) throws IOException { count=in.readLong(); sum=in.readDouble(); sumSquares=in.readDouble(); }
    }
    public static class CategoryMapper extends Mapper<LongWritable, Text, Text, Moments> {
        @Override protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String category; double x;
            try {
                JsonNode row=JobSupport.parse(value);x=JobSupport.amount(row);category=JobSupport.required(row,"category");
            } catch (Exception e) { context.getCounter("DATA","INVALID_TRANSACTION").increment(1); return; }
            context.write(new Text(category),new Moments(1,x,x*x));
        }
    }
    public static class MomentsCombiner extends Reducer<Text, Moments, Text, Moments> {
        @Override protected void reduce(Text key, Iterable<Moments> values, Context context) throws IOException, InterruptedException {
            long count=0; double sum=0, squares=0;
            for (Moments v:values) { count+=v.count; sum+=v.sum; squares+=v.sumSquares; }
            context.write(key,new Moments(count,sum,squares));
        }
    }
    public static class CategoryReducer extends Reducer<Text, Moments, NullWritable, Text> {
        @Override protected void reduce(Text key, Iterable<Moments> values, Context context) throws IOException, InterruptedException {
            long count=0; double sum=0, squares=0;
            for (Moments v:values) { count+=v.count; sum+=v.sum; squares+=v.sumSquares; }
            double mean=sum/count, variance=Math.max(0,squares/count-mean*mean);
            ObjectNode row=JobSupport.object(); row.put("category",key.toString()); row.put("count",count);
            row.put("total_amount",sum); row.put("mean",mean); row.put("std",Math.sqrt(variance));
            context.write(NullWritable.get(),new Text(row.toString()));
        }
    }
    public static boolean run(Configuration conf,String input,String output) throws Exception {
        Job job=JobSupport.job("CategoryStatsJob",CategoryStatsJob.class,conf,input,output);
        job.setMapperClass(CategoryMapper.class); job.setCombinerClass(MomentsCombiner.class);
        job.setReducerClass(CategoryReducer.class); job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Moments.class); return JobSupport.run(job);
    }
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();args=JobSupport.options(conf,args);
        if(args.length!=2) throw new IllegalArgumentException("CategoryStatsJob INPUT OUTPUT");
        JobSupport.mainExit(run(conf,args[0],args[1]));
    }
}
