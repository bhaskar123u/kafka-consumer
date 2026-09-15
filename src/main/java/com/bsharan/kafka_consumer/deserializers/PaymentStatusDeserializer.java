package com.bsharan.kafka_consumer.deserializers;

import com.bsharan.kafka_consumer.models.PaymentStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class PaymentStatusDeserializer
        implements Deserializer<PaymentStatus> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public PaymentStatus deserialize(String topic, byte[] data) {
        if (data == null)
            return null;
        try {
            return objectMapper.readValue(data, PaymentStatus.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize PaymentStatus", e);
        }
    }
}
