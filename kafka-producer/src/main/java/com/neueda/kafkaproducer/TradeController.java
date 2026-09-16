package com.neueda.kafkaproducer;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/trades")
public class TradeController {
    private final KafkaProducerService producerService;

    public TradeController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<String> publish(@RequestParam String accountId,
                                         @RequestParam String trade) {
        producerService.sendTrade(accountId, trade);
        return ResponseEntity.ok("Trade sent to Kafka topic: " + KafkaProducerService.TOPIC);
    }
}
