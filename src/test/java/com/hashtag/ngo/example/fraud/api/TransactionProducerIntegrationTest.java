package com.hashtag.ngo.example.fraud.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Vérifie de bout en bout qu'une transaction soumise via l'API REST est
 * effectivement publiée sur le topic Kafka "transactions", avec accountId
 * comme clé de partition et un JSON conforme à l'événement Transaction.
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 3, topics = "transactions")
class TransactionProducerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    // ObjectMapper Jackson 2 dédié au test : l'ObjectMapper auto-configuré par
    // Spring Boot 4 pour la couche web utilise Jackson 3 (tools.jackson), alors
    // que ce test ne fait que produire un JSON simple pour le corps de la requête.
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Consumer<String, Transaction> consumer;

    @BeforeEach
    void setUpConsumer() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                "transactions-test-group-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.hashtag.ngo.example.fraud.entity");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Transaction.class.getName());
        consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        consumer = new DefaultKafkaConsumerFactory<String, Transaction>(consumerProps).createConsumer();
        // Positionnement au DÉBUT du topic (assignation de toutes les partitions
        // puis seekToBeginning). Chaque test lit donc l'intégralité du topic.
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "transactions");
    }

    @AfterEach
    void tearDownConsumer() {
        consumer.close();
    }

    @Test
    void submittingATransactionPublishesItOnTheTransactionsTopicKeyedByAccountId() throws Exception {
        String accountId = "acc-42";
        String requestBody = objectMapper.writeValueAsString(
                new TransactionRequest(accountId, new BigDecimal("125.50"), "EUR"));

        mockMvc.perform(post("/api/v1/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + obtainToken())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isAccepted());

        ConsumerRecord<String, Transaction> record = KafkaTestUtils.getSingleRecord(consumer, "transactions");

        assertThat(record.key()).isEqualTo(accountId);

        Transaction transaction = record.value();
        assertThat(transaction.id()).isNotBlank();
        assertThat(transaction.accountId()).isEqualTo(accountId);
        assertThat(transaction.amount()).isEqualByComparingTo("125.50");
        assertThat(transaction.currency()).isEqualTo("EUR");
        assertThat(transaction.timestamp()).isNotNull();
    }

    @Test
    void submittingAnInvalidTransactionIsRejectedWithoutPublishing() throws Exception {
        // Vider les enregistrements des tests précédents : le broker et le topic
        // "transactions" sont partagés entre les méthodes de test, et ce consommateur
        // démarre au début du topic. On draine donc les messages antérieurs afin que
        // l'assertion suivante ne porte que sur ce qui est produit DURANT ce test.
        consumer.poll(Duration.ofSeconds(1));

        String requestBody = objectMapper.writeValueAsString(
                new TransactionRequest("", new BigDecimal("-5"), ""));

        mockMvc.perform(post("/api/v1/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + obtainToken())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isBadRequest());

        assertThat(KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(2)).isEmpty()).isTrue();
    }

    private String obtainToken() throws Exception {
        String requestBody = objectMapper.writeValueAsString(new AuthRequest("demo", "demo123"));

        MvcResult result = mockMvc.perform(post("/auth/token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andReturn();

        return objectMapper.readTree(result.getResponse().getContentAsString()).get("token").asText();
    }
}
