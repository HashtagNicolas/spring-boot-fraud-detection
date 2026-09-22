package com.hashtag.ngo.example.fraud.streams;

import com.hashtag.ngo.example.fraud.bean.impl.FraudRuleEngineImpl;
import com.hashtag.ngo.example.fraud.config.KafkaTopicsProperties;
import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import com.hashtag.ngo.example.fraud.entity.FraudRuleType;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Teste FraudDetectionTopology sans broker Kafka, via TopologyTestDriver :
 * traitement synchrone et déterministe, contrôle total des horodatages
 * (essentiel pour tester le fenêtrage de la règle de rafale).
 */
class FraudDetectionTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Transaction> inputTopic;
    private TestOutputTopic<String, FraudAlert> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaTopicsProperties topicsProperties = new KafkaTopicsProperties();
        topicsProperties.setTransactions("transactions");
        topicsProperties.setFraudAlerts("fraud-alerts");
        topicsProperties.setFraudAlertsDlt("fraud-alerts.DLT");

        new FraudDetectionTopology(new FraudRuleEngineImpl(), topicsProperties)
                .fraudDetectionStream(streamsBuilder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        // Émission immédiate de chaque mise à jour de l'agrégat, pour un test
        // déterministe (pas de mise en cache retardant l'émission).
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);

        JsonSerde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class).ignoreTypeHeaders();
        JsonSerde<FraudAlert> fraudAlertSerde = new JsonSerde<>(FraudAlert.class).ignoreTypeHeaders();

        inputTopic = testDriver.createInputTopic(
                "transactions", Serdes.String().serializer(), transactionSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
                "fraud-alerts", Serdes.String().deserializer(), fraudAlertSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void highAmountTransactionProducesAMontantEleveAlert() {
        Transaction transaction = transaction("acc-1", "15000.00", Instant.parse("2026-01-01T10:00:00Z"));

        inputTopic.pipeInput(transaction.accountId(), transaction, transaction.timestamp());

        List<FraudAlert> alerts = outputTopic.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getTransactionId()).isEqualTo(transaction.id());
        assertThat(alerts.get(0).getReasons()).containsExactly(FraudRuleType.MONTANT_ELEVE);
        assertThat(alerts.get(0).getScore()).isEqualTo(50);
    }

    @Test
    void normalAmountTransactionProducesNoAlert() {
        Transaction transaction = transaction("acc-1", "100.00", Instant.parse("2026-01-01T10:00:00Z"));

        inputTopic.pipeInput(transaction.accountId(), transaction, transaction.timestamp());

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void threeTransactionsInTheSameFiveMinuteWindowProduceARafaleAlert() {
        String accountId = "acc-burst";
        Instant windowStart = Instant.parse("2026-01-01T10:00:00Z"); // aligné sur une fenêtre tumbling de 5 min

        inputTopic.pipeInput(accountId, transaction(accountId, "10.00", windowStart), windowStart);
        inputTopic.pipeInput(accountId, transaction(accountId, "10.00", windowStart.plusSeconds(30)), windowStart.plusSeconds(30));
        Transaction third = transaction(accountId, "10.00", windowStart.plusSeconds(60));
        inputTopic.pipeInput(accountId, third, windowStart.plusSeconds(60));

        List<FraudAlert> alerts = outputTopic.readValuesToList();
        assertThat(alerts).hasSize(1);
        assertThat(alerts.get(0).getTransactionId()).isEqualTo(third.id());
        assertThat(alerts.get(0).getAccountId()).isEqualTo(accountId);
        assertThat(alerts.get(0).getReasons()).containsExactly(FraudRuleType.RAFALE);
        assertThat(alerts.get(0).getScore()).isEqualTo(40);
    }

    @Test
    void lessThanThreeTransactionsInTheWindowProduceNoRafaleAlert() {
        String accountId = "acc-quiet";
        Instant windowStart = Instant.parse("2026-01-01T10:00:00Z");

        inputTopic.pipeInput(accountId, transaction(accountId, "10.00", windowStart), windowStart);
        inputTopic.pipeInput(accountId, transaction(accountId, "10.00", windowStart.plusSeconds(30)), windowStart.plusSeconds(30));

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void transactionsSpanningTwoTumblingWindowsDoNotCombineIntoARafaleAlert() {
        // Limite connue des fenêtres tumbling (par opposition à une fenêtre
        // glissante) : 3 transactions en 6 secondes, mais à cheval sur deux
        // fenêtres de 5 minutes non chevauchantes, ne sont pas regroupées.
        String accountId = "acc-boundary";
        Instant windowStart = Instant.parse("2026-01-01T10:00:00Z");

        inputTopic.pipeInput(accountId, transaction(accountId, "10.00", windowStart.plusSeconds(295)), windowStart.plusSeconds(295));
        inputTopic.pipeInput(accountId, transaction(accountId, "10.00", windowStart.plusSeconds(299)), windowStart.plusSeconds(299));
        inputTopic.pipeInput(accountId, transaction(accountId, "10.00", windowStart.plusSeconds(301)), windowStart.plusSeconds(301));

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void burstOnOneAccountDoesNotAffectAnother() {
        Instant windowStart = Instant.parse("2026-01-01T10:00:00Z");

        inputTopic.pipeInput("acc-a", transaction("acc-a", "10.00", windowStart), windowStart);
        inputTopic.pipeInput("acc-b", transaction("acc-b", "10.00", windowStart.plusSeconds(1)), windowStart.plusSeconds(1));
        inputTopic.pipeInput("acc-a", transaction("acc-a", "10.00", windowStart.plusSeconds(2)), windowStart.plusSeconds(2));

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    private Transaction transaction(String accountId, String amount, Instant timestamp) {
        return new Transaction(UUID.randomUUID().toString(), accountId, new BigDecimal(amount), "EUR", timestamp);
    }
}
