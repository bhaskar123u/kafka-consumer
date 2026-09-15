package com.bsharan.kafka_consumer.components;

import com.bsharan.kafka_consumer.models.PaymentStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventListener {

    // containerFactory tells spring don't use the default consumer configuration. Use the configuration specifically designed for PaymentStatus.
    @KafkaListener(
            topics = "order-payments",
            groupId = "order-success-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(PaymentStatus paymentStatus) {
        System.out.println("\u001B[31mPAYMENT CONSUMER | Received Payment Status | " + paymentStatus + "\u001B[0m");

        if ("SUCCESS".equals(paymentStatus.getStatus())) {
            System.out.println("\u001B[31m📢 ORDER SUCCESSFUL | orderId=" + paymentStatus.getOrderId() + "\u001B[0m");
        }
    }
}
