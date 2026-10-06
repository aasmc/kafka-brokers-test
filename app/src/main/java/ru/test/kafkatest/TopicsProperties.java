package ru.test.kafkatest;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.topics")
public record TopicsProperties(String producer, String consumer, String consumer2) {
}
