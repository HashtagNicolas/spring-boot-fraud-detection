package com.hashtag.ngo.example.fraud.bean;

import com.hashtag.ngo.example.fraud.entity.Transaction;

/**
 * Service métier responsable de la réception des transactions par l'API REST
 * et de leur publication sur Kafka pour analyse par les consommateurs.
 */
public interface TransactionService {

    void submit(Transaction transaction);
}
