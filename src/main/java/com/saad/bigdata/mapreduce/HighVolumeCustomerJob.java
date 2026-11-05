package com.saad.bigdata.mapreduce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/** Filtre MapReduce sur les agrégats calculés par ClientStatsJob. */
public final class HighVolumeCustomerJob {
    private HighVolumeCustomerJob() {}
    public static class HighVolumeMapper extends Mapper<LongWritable,Text,NullWritable,Text> {
        @Override protected void map(LongWritable key,Text value,Context context) throws java.io.IOException,InterruptedException {
            ObjectNode output=null;
            try {
                JsonNode row=JobSupport.parse(value);
                long count=row.path("count").asLong();double total=row.path("total_amount").asDouble();
                long maxCount=context.getConfiguration().getLong("anomaly.customer.max.transactions",500);
                double maxAmount=context.getConfiguration().getDouble("anomaly.customer.max.amount",500000);
                if(count>maxCount || total>maxAmount) {
                    ObjectNode result=JobSupport.object();result.put("customer_id",JobSupport.required(row,"customer_id"));
                    result.put("transaction_count",count);result.put("total_amount",total);
                    result.put("reason",count>maxCount && total>maxAmount?"HIGH_COUNT_AND_AMOUNT":count>maxCount?"HIGH_TRANSACTION_ACTIVITY":"HIGH_TOTAL_AMOUNT");
                    output=result;
                }
            } catch(Exception e) {context.getCounter("DATA","INVALID_CLIENT_STATS").increment(1);return;}
            if(output!=null) context.write(NullWritable.get(),new Text(output.toString()));
        }
    }
    public static boolean run(Configuration conf,String clientStats,String output) throws Exception {
        Job job=JobSupport.job("HighVolumeCustomerJob",HighVolumeCustomerJob.class,conf,clientStats,output);
        job.setMapperClass(HighVolumeMapper.class);job.setNumReduceTasks(0);return JobSupport.run(job);
    }
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();args=JobSupport.options(conf,args);
        if(args.length!=2) throw new IllegalArgumentException("HighVolumeCustomerJob CLIENT_STATS OUTPUT");
        JobSupport.mainExit(run(conf,args[0],args[1]));
    }
}
