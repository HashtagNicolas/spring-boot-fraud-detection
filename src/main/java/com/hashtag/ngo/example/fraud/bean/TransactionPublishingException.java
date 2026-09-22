package com.hashtag.ngo.example.fraud.bean;

/**
 * Erreur technique levée lorsqu'une transaction n'a pas pu être publiée sur
 * Kafka (broker injoignable, timeout...). Distincte des erreurs fonctionnelles
 * de validation, qui sont rejetées avant même d'atteindre le service.
 */
public class TransactionPublishingException extends RuntimeException {

    public TransactionPublishingException(String message, Throwable cause) {
        super(message, cause);
    }
}
