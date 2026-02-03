package com.example.privateapi.service;

import com.example.privateapi.model.HealthResponse;
import org.springframework.stereotype.Service;

@Service
public class HealthService {

    public HealthResponse getHealthStatus() {
        return new HealthResponse("Good", "Private API is running");
    }
}
