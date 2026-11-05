package com.saad.bigdata.mapreduce;

import com.saad.bigdata.report.ReportGenerator;
import com.saad.bigdata.report.PowerBiExport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Lance une analyse batch complète; aucun résultat existant n'est supprimé implicitement. */
public final class AnomalyDetectionPipeline {
    private AnomalyDetectionPipeline() {}
    @FunctionalInterface private interface Step { boolean run() throws Exception; }
    private static void execute(String name,Step step) throws Exception {
        System.out.println("[START] "+name);
        if(!step.run()) throw new IllegalStateException("[FAILED] "+name);
        System.out.println("[SUCCESS] "+name);
    }
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();args=JobSupport.options(conf,args);
        String input=args.length>0?args[0]:"/transactions";
        String root=args.length>1?args[1]:"/results";
        String report=args.length>2?args[2]:"reports";
        String referenceDir=args.length>3?args[3]:"data";
        if(args.length>4) throw new IllegalArgumentException("Pipeline [TRANSACTIONS [RESULTS [REPORTS_LOCAL [REFERENCE_DATA_LOCAL]]]]");
        FileSystem fs=new Path(root).getFileSystem(conf);
        String[] names={"client-stats","category-stats","period-stats","transaction-anomalies","customer-anomalies","period-anomalies"};
        for(String name:names) if(fs.exists(new Path(root+"/"+name)))
            throw new IllegalStateException("Sortie déjà présente: "+root+"/"+name+". Supprimez-la explicitement avant de relancer.");
        execute("ClientStatsJob",()->ClientStatsJob.run(new Configuration(conf),input,root+"/client-stats"));
        execute("CategoryStatsJob",()->CategoryStatsJob.run(new Configuration(conf),input,root+"/category-stats"));
        execute("PeriodStatsJob",()->PeriodStatsJob.run(new Configuration(conf),input,root+"/period-stats"));
        execute("AmountAnomalyJob",()->AmountAnomalyJob.run(new Configuration(conf),input,root+"/category-stats",root+"/transaction-anomalies"));
        execute("HighVolumeCustomerJob",()->HighVolumeCustomerJob.run(new Configuration(conf),root+"/client-stats",root+"/customer-anomalies"));
        execute("PeriodVariationJob",()->PeriodVariationJob.run(new Configuration(conf),root+"/period-stats",root+"/period-anomalies"));
        System.out.println("[START] ReportGenerator");
        ReportGenerator.generate(conf,root,report);
        System.out.println("[SUCCESS] ReportGenerator");
        System.out.println("[START] PowerBiExport");
        PowerBiExport.generate(conf,input,root,report+"/powerbi/data",referenceDir);
        System.out.println("[SUCCESS] PowerBiExport");
    }
}
