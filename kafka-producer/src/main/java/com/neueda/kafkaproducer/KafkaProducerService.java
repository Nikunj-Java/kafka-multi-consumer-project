package com.neueda.kafkaproducer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {
    public static final String TOPIC = "settlement-events";
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTrade(String accountId, String trade) {
        kafkaTemplate.send(TOPIC, accountId, trade).whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Kafka send failed: " + ex.getMessage());
                return;
            }
            RecordMetadata m = result.getRecordMetadata();
            System.out.printf("PRODUCED -> topic=%s partition=%d offset=%d key=%s value=%s%n",
                    m.topic(), m.partition(), m.offset(), accountId, trade);
        });
    }
}
