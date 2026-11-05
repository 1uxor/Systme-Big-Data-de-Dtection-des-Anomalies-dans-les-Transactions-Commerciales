package com.saad.bigdata.producer;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({
        "transaction_id",
        "timestamp",
        "customer_id",
        "product_id",
        "category",
        "quantity",
        "unit_price",
        "total_amount",
        "payment_method",
        "city"
})
public class Transaction {

    @JsonProperty("transaction_id")
    private String transactionId;

    private LocalDateTime timestamp;

    @JsonProperty("customer_id")
    private String customerId;

    @JsonProperty("product_id")
    private String productId;

    private String category;
    private int quantity;

    @JsonProperty("unit_price")
    private BigDecimal unitPrice;

    @JsonProperty("total_amount")
    private BigDecimal totalAmount;

    @JsonProperty("payment_method")
    private String paymentMethod;

    private String city;

    public Transaction() {
        // Constructeur sans argument requis par les bibliothèques de sérialisation.
    }

    public Transaction(
            String transactionId,
            LocalDateTime timestamp,
            String customerId,
            String productId,
            String category,
            int quantity,
            BigDecimal unitPrice,
            BigDecimal totalAmount,
            String paymentMethod,
            String city) {
        this.transactionId = transactionId;
        this.timestamp = timestamp;
        this.customerId = customerId;
        this.productId = productId;
        this.category = category;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.totalAmount = totalAmount;
        this.paymentMethod = paymentMethod;
        this.city = city;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getPaymentMethod() {
        return paymentMethod;
    }

    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
