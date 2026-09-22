package com.hashtag.ngo.example.fraud.bean;

/**
 * Erreur technique levée lorsqu'un cas de fraude n'a pas pu être persisté
 * (base de données indisponible, contrainte violée...).
 */
public class FraudCasePersistenceException extends RuntimeException {

    public FraudCasePersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
