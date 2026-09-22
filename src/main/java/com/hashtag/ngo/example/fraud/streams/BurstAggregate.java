package com.hashtag.ngo.example.fraud.streams;

/**
 * État accumulé par fenêtre pour la règle de rafale : nombre de transactions
 * vues et identifiant de la dernière d'entre elles (utilisé comme
 * transactionId de l'alerte, puisque l'agrégation ne conserve pas
 * l'historique complet des transactions, seulement un compteur).
 */
public record BurstAggregate(long count, String lastTransactionId) {

    public static BurstAggregate empty() {
        return new BurstAggregate(0, null);
    }

    public BurstAggregate withTransaction(String transactionId) {
        return new BurstAggregate(count + 1, transactionId);
    }
}
