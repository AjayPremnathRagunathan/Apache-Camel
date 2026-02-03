package com.example.privateapi.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class HealthController {

    // The message you want to show
    private static final String MESSAGE = "Camel Kafka using Private API connectivity is working";

    // GET /internal/health
    @GetMapping("/internal/health")
    public Map<String, String> getHealth() {
        Map<String, String> response = new HashMap<>();
        response.put("message", MESSAGE); // Always returns this message
        return response;
    }
}
