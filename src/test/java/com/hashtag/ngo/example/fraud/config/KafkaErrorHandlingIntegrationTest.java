package com.hashtag.ngo.example.fraud.config;

import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import com.hashtag.ngo.example.fraud.entity.FraudRuleType;
import com.hashtag.ngo.example.fraud.repository.FraudAlertRepository;
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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Vérifie que KafkaErrorHandlingConfig retente puis écarte vers la
 * dead-letter (fraud-alerts.DLT) un message dont le traitement échoue
 * systématiquement (ici : une alerte avec un accountId manquant, colonne
 * NOT NULL en base, donc la persistance échoue à chaque tentative).
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "fraud-alerts.DLT")
class KafkaErrorHandlingIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private FraudAlertRepository fraudAlertRepository;

    private Consumer<String, FraudAlert> dlqConsumer;

    @BeforeEach
    void setUpDlqConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("dlq-test-group", "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.hashtag.ngo.example.fraud.entity");

        dlqConsumer = new DefaultKafkaConsumerFactory<String, FraudAlert>(consumerProps).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dlqConsumer, "fraud-alerts.DLT");
    }

    @AfterEach
    void tearDownDlqConsumer() {
        dlqConsumer.close();
    }

    @Test
    void permanentlyInvalidAlertEndsUpOnTheDeadLetterTopicAfterRetriesAreExhausted() {
        String transactionId = UUID.randomUUID().toString();

        // accountId volontairement null : colonne NOT NULL en base, la
        // persistance échoue donc systématiquement à chaque tentative, ce qui
        // doit épuiser les 3 retries puis publier le message sur la DLQ.
        FraudAlert invalidAlert = new FraudAlert(
                transactionId, null, List.of(FraudRuleType.MONTANT_ELEVE), 50, Instant.now());

        kafkaTemplate.send("fraud-alerts", transactionId, invalidAlert);

        FraudAlert deadLettered = awaitDeadLetterForTransaction(transactionId, Duration.ofSeconds(20))
                .orElseThrow(() -> new AssertionError("Aucun message reçu en DLQ pour la transaction " + transactionId));

        assertThat(deadLettered.getTransactionId()).isEqualTo(transactionId);

        // La persistance ayant échoué à chaque tentative, aucune ligne ne
        // doit exister en base pour cette transaction.
        assertThat(fraudAlertRepository.findByTransactionId(transactionId)).isEmpty();
    }

    private Optional<FraudAlert> awaitDeadLetterForTransaction(String transactionId, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, FraudAlert> records = KafkaTestUtils.getRecords(dlqConsumer, Duration.ofMillis(500));
            for (ConsumerRecord<String, FraudAlert> record : records) {
                if (transactionId.equals(record.value().getTransactionId())) {
                    return Optional.of(record.value());
                }
            }
        }
        return Optional.empty();
    }
}
