package com.example.customerapi.service;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Getter
@Service
public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final List<String> kafkaLogs = new ArrayList<>();

    @KafkaListener(topics = "customer_topic", groupId = "customer_group")
    public void consume(Object message) {
        String logMessage = "Consumed message: " + message;
        logger.info(logMessage);
        kafkaLogs.add(logMessage);
    }

}
