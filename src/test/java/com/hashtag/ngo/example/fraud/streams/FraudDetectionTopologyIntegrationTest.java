package com.hashtag.ngo.example.fraud.streams;

import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import com.hashtag.ngo.example.fraud.entity.FraudRuleType;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Vérifie de bout en bout, via un broker Kafka embarqué réel, que
 * FraudDetectionTopology détecte le montant élevé et la rafale et publie les
 * alertes correspondantes sur "fraud-alerts".
 *
 * Piège d'isolation : accountId unique par test (le broker embarqué et la
 * topologie Kafka Streams sont partagés entre les méthodes de test de cette
 * classe), et recherche de l'alerte par accountId + type de raison plutôt
 * que par position dans le flux.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "fraud-alerts")
class FraudDetectionTopologyIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<String, FraudAlert> alertConsumer;

    @BeforeEach
    void setUpAlertConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "topology-test-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.hashtag.ngo.example.fraud.entity");

        alertConsumer = new DefaultKafkaConsumerFactory<String, FraudAlert>(consumerProps).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(alertConsumer, "fraud-alerts");
    }

    @AfterEach
    void tearDownAlertConsumer() {
        alertConsumer.close();
    }

    @Test
    void highAmountTransactionProducesAMontantEleveAlert() {
        String accountId = "acc-" + UUID.randomUUID();
        Transaction transaction = new Transaction(
                UUID.randomUUID().toString(), accountId, new BigDecimal("15000.00"), "EUR", Instant.now());

        kafkaTemplate.send("transactions", accountId, transaction);

        FraudAlert alert = awaitAlert(accountId, alert2 -> alert2.getReasons().contains(FraudRuleType.MONTANT_ELEVE),
                Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("Aucune alerte MONTANT_ELEVE reçue pour le compte " + accountId));

        assertThat(alert.getTransactionId()).isEqualTo(transaction.id());
        assertThat(alert.getScore()).isEqualTo(50);
    }

    @Test
    void threeCloseTransactionsProduceARafaleAlert() {
        String accountId = "acc-" + UUID.randomUUID();

        for (int i = 0; i < 3; i++) {
            Transaction transaction = new Transaction(
                    UUID.randomUUID().toString(), accountId, new BigDecimal("10.00"), "EUR", Instant.now());
            kafkaTemplate.send("transactions", accountId, transaction);
        }

        FraudAlert alert = awaitAlert(accountId, alert2 -> alert2.getReasons().contains(FraudRuleType.RAFALE),
                Duration.ofSeconds(15))
                .orElseThrow(() -> new AssertionError("Aucune alerte RAFALE reçue pour le compte " + accountId));

        assertThat(alert.getAccountId()).isEqualTo(accountId);
        assertThat(alert.getScore()).isEqualTo(40);
    }

    private Optional<FraudAlert> awaitAlert(String accountId, Predicate<FraudAlert> matches, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, FraudAlert> records = KafkaTestUtils.getRecords(alertConsumer, Duration.ofMillis(500));
            for (ConsumerRecord<String, FraudAlert> record : records) {
                if (accountId.equals(record.key()) && matches.test(record.value())) {
                    return Optional.of(record.value());
                }
            }
        }
        return Optional.empty();
    }
}
