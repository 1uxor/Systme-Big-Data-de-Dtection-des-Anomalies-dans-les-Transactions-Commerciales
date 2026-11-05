package com.saad.bigdata.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.saad.bigdata.mapreduce.JobSupport;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PowerBiExportTest {
    @Test void csvEscapesCommasAndQuotes() throws Exception {
        ObjectNode row=JobSupport.object();
        row.put("customer_id","CUST,\"001\"");row.put("count",2);
        Path output=Path.of("target","test-output","powerbi-export.csv");
        Files.createDirectories(output.getParent());
        PowerBiExport.writeRows(output,List.<JsonNode>of(row),"customer_id,count");
        assertEquals(List.of("customer_id,count","\"CUST,\"\"001\"\"\",\"2\""),Files.readAllLines(output));
    }
}
