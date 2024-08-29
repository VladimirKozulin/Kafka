package com.example.kafka_example_app.model;

import lombok.Data;

@Data
public class KafkaMessage {

    private Long id;

    private String message;
}
