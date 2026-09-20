package com.bsharan.kafka_consumer.services;

import com.bsharan.kafka_consumer.models.Order;
import com.bsharan.kafka_consumer.models.PaymentStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class PaymentProcessorService {

    private final KafkaTemplate<String, PaymentStatus> kafkaTemplate;

    public PaymentProcessorService(KafkaTemplate<String, PaymentStatus> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
    }

    public void process(Order order) {
        System.out.println("PAYMENT PROCESSOR | Processing order=" + order.getOrderId());
        PaymentStatus paymentStatus = new PaymentStatus(order.getOrderId(), "PAY-" + UUID.randomUUID(), "SUCCESS");
        kafkaTemplate.send("order-payments", order.getOrderId(), paymentStatus);
    }
}
