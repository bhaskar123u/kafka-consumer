package com.bsharan.kafka_consumer.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentStatus {

    private String orderId;
    private String paymentId;
    private String status;
}