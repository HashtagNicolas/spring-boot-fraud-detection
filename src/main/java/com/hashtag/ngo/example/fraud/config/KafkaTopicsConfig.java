package com.hashtag.ngo.example.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Déclare les topics Kafka nécessaires à l'application. Spring Kafka détecte
 * automatiquement les beans NewTopic et les crée au démarrage via le
 * KafkaAdmin auto-configuré (création idempotente : ignorée si déjà existant).
 */
@Configuration
public class KafkaTopicsConfig {

    private final KafkaTopicsProperties topicsProperties;

    public KafkaTopicsConfig(KafkaTopicsProperties topicsProperties) {
        this.topicsProperties = topicsProperties;
    }

    @Bean
    public NewTopic transactionsTopic() {
        return TopicBuilder.name(topicsProperties.getTransactions())
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name(topicsProperties.getFraudAlerts())
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic fraudAlertsDltTopic() {
        return TopicBuilder.name(topicsProperties.getFraudAlertsDlt())
                .partitions(1)
                .replicas(1)
                .build();
    }
}
