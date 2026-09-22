package com.hashtag.ngo.example.fraud.bean.impl;

import com.hashtag.ngo.example.fraud.bean.TransactionPublishingException;
import com.hashtag.ngo.example.fraud.bean.TransactionService;
import com.hashtag.ngo.example.fraud.config.KafkaTopicsProperties;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class TransactionServiceImpl implements TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionServiceImpl.class);

    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(5);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topicsProperties;

    public TransactionServiceImpl(KafkaTemplate<String, Object> kafkaTemplate,
                                   KafkaTopicsProperties topicsProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicsProperties = topicsProperties;
    }

    @Override
    public Transaction submit(String accountId, BigDecimal amount, String currency) {
        Transaction transaction = new Transaction(
                UUID.randomUUID().toString(),
                accountId,
                amount,
                currency,
                Instant.now());

        // Clé de partition = accountId (et non l'id de la transaction) afin que
        // toutes les transactions d'un même compte atterrissent sur la même
        // partition : Kafka garantit l'ordre des messages au sein d'une
        // partition, ce qui est indispensable pour analyser correctement la
        // séquence des opérations d'un compte (ex. détection de rafales).
        try {
            kafkaTemplate.send(topicsProperties.getTransactions(), transaction.accountId(), transaction)
                    .get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransactionPublishingException(
                    "Publication interrompue pour la transaction " + transaction.id(), e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Échec de la publication de la transaction {} sur le topic {}",
                    transaction.id(), topicsProperties.getTransactions(), e);
            throw new TransactionPublishingException(
                    "Échec de la publication de la transaction " + transaction.id(), e);
        }

        return transaction;
    }
}
