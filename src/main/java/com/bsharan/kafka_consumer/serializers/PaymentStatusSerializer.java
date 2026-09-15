package com.bsharan.kafka_consumer.serializers;

import com.bsharan.kafka_consumer.models.PaymentStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.LinkedHashMap;
import java.util.Map;

public class PaymentStatusSerializer implements Serializer<PaymentStatus> {

    private final ObjectMapper objectMapper;
    public PaymentStatusSerializer() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, PaymentStatus paymentStatus) {
        if (paymentStatus == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(paymentStatus);
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to serialize payment status", ex);
        }
    }
}