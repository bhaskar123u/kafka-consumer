package com.bsharan.kafka_consumer.components;

import com.bsharan.kafka_consumer.models.Order;
import com.bsharan.kafka_consumer.services.PaymentProcessorService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {

    private final PaymentProcessorService paymentProcessorService;

    public OrderEventListener(PaymentProcessorService paymentProcessorService) {
        this.paymentProcessorService = paymentProcessorService;
    }

    @KafkaListener(
            topics = "order-events",
            groupId = "payment-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(Order order) {
        System.out.println("\u001B[31mORDER CONSUMER | Received Order Event | " + order + "\u001B[0m");
        paymentProcessorService.process(order);
    }
}
