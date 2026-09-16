package com.neueda.notificationconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {
    @KafkaListener(topics = "settlement-events", groupId = "notification-group")
    public void consume(ConsumerRecord<String, String> record) {
        System.out.println("==========================================");
        System.out.println("Notification Service RECEIVED MESSAGE");
        System.out.println("Topic     : " + record.topic());
        System.out.println("Partition : " + record.partition());
        System.out.println("Offset    : " + record.offset());
        System.out.println("Key       : " + record.key());
        System.out.println("Value     : " + record.value());
        System.out.println("Consumer Group: notification-group");
        System.out.println("==========================================");
    }
}
