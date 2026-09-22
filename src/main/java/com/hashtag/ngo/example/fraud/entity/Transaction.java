package com.hashtag.ngo.example.fraud.entity;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Représente une transaction bancaire, telle qu'elle circule sur le topic
 * Kafka "transactions". Ce n'est pas une entité JPA : ce message n'est pas
 * persisté en tant que tel, seules les alertes de fraude le sont.
 */
public record Transaction(
        String id,
        String accountId,
        BigDecimal amount,
        String currency,
        Instant timestamp
) {
}
