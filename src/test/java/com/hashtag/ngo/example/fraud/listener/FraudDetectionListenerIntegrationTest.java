package com.hashtag.ngo.example.fraud.listener;

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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Vérifie de bout en bout que FraudDetectionListener consomme "transactions",
 * évalue chaque transaction via le moteur de règles, et publie (ou non) une
 * FraudAlert sur "fraud-alerts" en conséquence.
 *
 * Piège d'isolation évité : le contexte Spring (et donc le bean singleton
 * FraudDetectionListener, avec son historique en mémoire) ainsi que le broker
 * Kafka embarqué sont partagés entre toutes les méthodes de test de cette
 * classe. On utilise donc un accountId unique par test (aucune contamination
 * de l'historique de rafale entre scénarios), et on filtre les enregistrements
 * lus sur "fraud-alerts" par ce même accountId plutôt que de supposer le
 * topic globalement vide (des alertes d'un autre test peuvent déjà y figurer).
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = "fraud-alerts")
class FraudDetectionListenerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<String, FraudAlert> alertConsumer;

    @BeforeEach
    void setUpAlertConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("fraud-alerts-test-group", "true", embeddedKafkaBroker);
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
    void highAmountTransactionProducesAFraudAlert() {
        String accountId = uniqueAccountId();
        Transaction fraudulent = new Transaction(
                UUID.randomUUID().toString(), accountId, new BigDecimal("15000.00"), "EUR", Instant.now());

        kafkaTemplate.send("transactions", accountId, fraudulent);

        FraudAlert alert = awaitAlertForAccount(accountId, Duration.ofSeconds(10))
                .orElseThrow(() -> new AssertionError("Aucune alerte reçue pour le compte " + accountId));

        assertThat(alert.getTransactionId()).isEqualTo(fraudulent.id());
        assertThat(alert.getAccountId()).isEqualTo(accountId);
        assertThat(alert.getScore()).isGreaterThanOrEqualTo(50);
        assertThat(alert.getReasons()).contains(FraudRuleType.MONTANT_ELEVE);
    }

    @Test
    void normalTransactionDoesNotProduceAFraudAlert() {
        String accountId = uniqueAccountId();
        Transaction normal = new Transaction(
                UUID.randomUUID().toString(), accountId, new BigDecimal("42.00"), "EUR", Instant.now());

        kafkaTemplate.send("transactions", accountId, normal);

        Optional<FraudAlert> alert = awaitAlertForAccount(accountId, Duration.ofSeconds(3));

        assertThat(alert).isEmpty();
    }

    private String uniqueAccountId() {
        return "acc-" + UUID.randomUUID();
    }

    private Optional<FraudAlert> awaitAlertForAccount(String accountId, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            ConsumerRecords<String, FraudAlert> records = KafkaTestUtils.getRecords(alertConsumer, Duration.ofMillis(500));
            for (ConsumerRecord<String, FraudAlert> record : records) {
                if (accountId.equals(record.key())) {
                    return Optional.of(record.value());
                }
            }
        }
        return Optional.empty();
    }
}
