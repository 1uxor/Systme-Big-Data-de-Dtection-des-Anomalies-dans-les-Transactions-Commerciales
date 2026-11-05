package com.saad.bigdata.mapreduce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public final class ClientStatsJob {
    private ClientStatsJob() {}
    public static class ClientMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            String customer; double amount;
            try {
                JsonNode row = JobSupport.parse(value);
                customer=JobSupport.required(row,"customer_id"); amount=JobSupport.amount(row);
            } catch (Exception e) { context.getCounter("DATA", "INVALID_TRANSACTION").increment(1); return; }
            context.write(new Text(customer),new DoubleWritable(amount));
        }
    }
    public static class ClientReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
        @Override protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws java.io.IOException, InterruptedException {
            long count = 0; double total = 0;
            for (DoubleWritable value : values) { count++; total += value.get(); }
            ObjectNode row = JobSupport.object();
            row.put("customer_id", key.toString()); row.put("count", count);
            row.put("total_amount", total); row.put("average_amount", total / count);
            context.write(NullWritable.get(), new Text(row.toString()));
        }
    }
    public static boolean run(Configuration conf, String input, String output) throws Exception {
        Job job = JobSupport.job("ClientStatsJob", ClientStatsJob.class, conf, input, output);
        job.setMapperClass(ClientMapper.class); job.setReducerClass(ClientReducer.class);
        job.setMapOutputKeyClass(Text.class); job.setMapOutputValueClass(DoubleWritable.class);
        return JobSupport.run(job);
    }
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();args=JobSupport.options(conf,args);
        if (args.length != 2) throw new IllegalArgumentException("ClientStatsJob INPUT OUTPUT");
        JobSupport.mainExit(run(conf, args[0], args[1]));
    }
}
