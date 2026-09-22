package com.hashtag.ngo.example.fraud.api;

/**
 * Corps de la réponse 202 Accepted : identifiant généré côté serveur, à
 * conserver par l'appelant pour retrouver la transaction (ex. dans une
 * future alerte de fraude qui la référencerait).
 */
public record TransactionAccepted(String transactionId) {
}
