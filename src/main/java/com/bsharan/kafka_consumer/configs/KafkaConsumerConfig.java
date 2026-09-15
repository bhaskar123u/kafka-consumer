package com.bsharan.kafka_consumer.configs;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.JacksonJsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public NewTopic orderPaymentsTopic() {
        return TopicBuilder.name("order-payments")
                .partitions(3)
                .replicas(2)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, "delete")
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
                .build();
    }

    @Bean
    public ConsumerFactory<String, byte[]> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
                new StringDeserializer(),
                new ByteArrayDeserializer()
        );
    }

    /*
        This method returns a factory capable of creating listener containers for messages whose key is String and value is byte[]. The bean name will be - "kafkaListenerContainerFactory"
    */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, byte[]>
    kafkaListenerContainerFactory(ConsumerFactory<String, byte[]> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        // when spring will eventually create a ListenerContainer, it will use this ConsumerFactory to create the actual KafkaConsumer
        factory.setConsumerFactory(consumerFactory);
        // When the KafkaConsumer gives a record, it will use this converter to convert the payload before calling @KafkaListener
        factory.setRecordMessageConverter(kafkaMessageConverter());
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }
//    Jackson itself doesn't know from the bytes alone whether they represent Order or PaymentStatus.The listener method's parameter type tells Spring what the target type is. For example:
//    @KafkaListener(topics = "order-events")
//    public void consume(Order order) {}
//    tells Jackson to convert into Order object

    @Bean
    public RecordMessageConverter kafkaMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, 2));
    }
}

//     ConcurrentKafkaListenerContainerFactory
//             │ creates
//             ▼
//     ListenerContainer
//             │
//             │ asks ConsumerFactory to create Consumer
//             ▼
//     ConsumerFactory<String, byte[]>
//             │
//             │ provides KafkaConsumer configuration
//             ▼
//     KafkaConsumer<String, byte[]>
//             │
//             │ poll() → gets records
//             ▼
//     ListenerContainer
//             │
//             │ JacksonJsonMessageConverter
//             ▼
//     byte[] → Order / PaymentStatus
//             │
//             ├── ✅ Valid record (deserialization SUCCESSFUL)
//             │       ↓
//             │   @KafkaListener
//             │       ↓
//             │   Business Logic
//             │
//             └── ❌❌ Poison Pill ❌❌ (deserialization FAILS)
//                     ↓
//                Deserialization Exception
//                     ↓
//                DefaultErrorHandler
//                     ├── Retry
//                     └── Recover → DLT
//                              ↓
//                       Continue to next record
//                              ↓
//                       Commit appropriate offset


//Think of the ListenerContainer as the Spring Kafka manager sitting between your KafkaConsumer and your @KafkaListener method. KafkaConsumer knows how to poll, but it's ListenerContainer which decides what to do with actual result. It's main responsibility are :

//    ListenerContainer
//        │
//        ├── 1. Consumer lifecycle
//        │      ├── Create/start KafkaConsumer
//        │      ├── Stop/close KafkaConsumer
//        │      └── Restart when required
//        │
//        ├── 2. Polling
//        │      └── Repeatedly calls consumer.poll()
//        │
//        ├── 3. Listener invocation
//        │      └── Takes ConsumerRecords → calls @KafkaListener method
//        │
//        ├── 4. Message conversion
//        │      └── byte[] → Order / PaymentStatus
//        │
//        ├── 5. Error handling
//        │      ├── Listener exceptions
//        │      ├── Deserialization/conversion errors
//        │      └── ErrorHandler / retry mechanisms
//        │
//        ├── 6. Offset management
//        │      └── Commits offsets according to configuration
//        │
//        ├── 7. Consumer threading
//        │      └── Runs consumer/poll loop on its consumer thread
//        │
//        ├── 8. Concurrency
//        │      └── concurrency=N → manages N KafkaConsumers
//        │
//        ├── 9. Rebalancing participation
//        │      └── Coordinates consumer lifecycle around
//        │          partition assignment/revocation
//        │
//        ├── 10. Pause / Resume
//        │       └── Can pause/resume consumption
//        │
//        └── 11. Consumer/container events
//                └── Publishes events such as partitions assigned/revoked, consumer started/stopped
