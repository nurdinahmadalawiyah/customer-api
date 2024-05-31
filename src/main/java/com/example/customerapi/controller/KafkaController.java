package com.example.customerapi.controller;

import com.example.customerapi.service.KafkaConsumerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaConsumerService kafkaConsumerService;

    @GetMapping("/logs")
    public List<String> getKafkaLogs() {
        return kafkaConsumerService.getKafkaLogs();
    }
}
