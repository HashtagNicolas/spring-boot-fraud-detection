package com.hashtag.ngo.example.fraud.api;

import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import com.hashtag.ngo.example.fraud.entity.FraudRuleType;

import java.time.Instant;
import java.util.List;

/**
 * Vue REST d'un cas de fraude, distincte de l'entité JPA FraudAlert pour ne
 * pas exposer directement le modèle de persistance sur l'API.
 */
public record FraudCaseView(
        Long id,
        String transactionId,
        String accountId,
        int score,
        List<FraudRuleType> reasons,
        Instant detectedAt
) {

    public static FraudCaseView from(FraudAlert alert) {
        return new FraudCaseView(
                alert.getId(),
                alert.getTransactionId(),
                alert.getAccountId(),
                alert.getScore(),
                alert.getReasons(),
                alert.getDetectedAt());
    }
}
