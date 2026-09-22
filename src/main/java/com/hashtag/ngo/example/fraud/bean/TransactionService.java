package com.hashtag.ngo.example.fraud.bean;

import com.hashtag.ngo.example.fraud.entity.Transaction;

import java.math.BigDecimal;

/**
 * Service métier responsable de la réception des transactions par l'API REST
 * et de leur publication sur Kafka pour analyse par les consommateurs.
 *
 * Le service ne dépend pas du DTO de la couche api (TransactionRequest) : il
 * reçoit des paramètres primitifs pour rester indépendant du contrat REST.
 */
public interface TransactionService {

    /**
     * Construit l'événement Transaction (id et timestamp générés côté
     * serveur) et le publie sur Kafka.
     *
     * @throws TransactionPublishingException si la publication échoue
     */
    Transaction submit(String accountId, BigDecimal amount, String currency);
}
