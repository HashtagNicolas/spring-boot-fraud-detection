package com.hashtag.ngo.example.fraud.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Vérifie que l'application démarre avec un broker Kafka embarqué et que les
 * topics déclarés dans KafkaTopicsConfig sont bien créés avec le nombre de
 * partitions attendu.
 */
@SpringBootTest
@EmbeddedKafka
class KafkaTopicsIntegrationTest {

    @Autowired
    private KafkaAdmin kafkaAdmin;

    @Test
    void applicationStartsAndCreatesExpectedTopics() throws Exception {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> topicNames = adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(topicNames).contains("transactions", "fraud-alerts", "fraud-alerts.DLT");

            Map<String, TopicDescription> descriptions = adminClient
                    .describeTopics(List.of("transactions", "fraud-alerts", "fraud-alerts.DLT"))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS);

            assertThat(descriptions.get("transactions").partitions()).hasSize(3);
            assertThat(descriptions.get("fraud-alerts").partitions()).hasSize(3);
            assertThat(descriptions.get("fraud-alerts.DLT").partitions()).hasSize(1);
        }
    }
}
