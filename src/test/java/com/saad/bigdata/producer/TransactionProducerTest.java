package com.saad.bigdata.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransactionProducerTest {
    @Test void generationAndJson() throws Exception {
        Transaction transaction=TransactionProducer.generateTransaction();
        assertTrue(transaction.getTotalAmount().signum()>0);
        assertEquals(transaction.getUnitPrice().multiply(java.math.BigDecimal.valueOf(transaction.getQuantity())),transaction.getTotalAmount());
        assertTrue(transaction.getCustomerId().matches("CUST[0-9]{4}"));
        ObjectMapper mapper=new ObjectMapper().registerModule(new JavaTimeModule());
        String json=mapper.writeValueAsString(transaction);
        assertEquals(transaction.getTransactionId(),mapper.readTree(json).path("transaction_id").asText());
        assertFalse(json.contains("is_anomaly"));
    }
}
