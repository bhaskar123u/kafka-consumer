package com.bsharan.kafka_consumer.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private String orderId;
    private String customerId;
    private String productId;
    @JsonIgnore
    private Integer quantity;
    @JsonIgnore
    private Double totalAmount;
    @JsonIgnore
    private String status;
}
