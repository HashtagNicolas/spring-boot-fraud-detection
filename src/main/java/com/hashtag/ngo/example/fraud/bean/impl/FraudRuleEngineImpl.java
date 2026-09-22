package com.hashtag.ngo.example.fraud.bean.impl;

import com.hashtag.ngo.example.fraud.bean.FraudRuleEngine;
import com.hashtag.ngo.example.fraud.entity.FraudDecision;
import com.hashtag.ngo.example.fraud.entity.FraudRuleType;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Moteur de règles de détection de fraude (v1).
 *
 * Logique pure : ne dépend que du modèle de domaine (Transaction,
 * FraudDecision), aucune dépendance Kafka/JPA. Elle peut donc être testée
 * unitairement ou via Cucumber sans démarrer de contexte Spring ni de
 * broker Kafka. L'annotation @Component sert uniquement à l'enregistrer
 * comme bean pour un futur consommateur Kafka ; elle n'introduit aucun
 * couplage dans la logique elle-même.
 */
@Component
public class FraudRuleEngineImpl implements FraudRuleEngine {

    // Règle "montant élevé" : un montant strictement supérieur à ce seuil
    // est considéré comme suspect.
    static final BigDecimal HIGH_AMOUNT_THRESHOLD = new BigDecimal("10000");
    static final int HIGH_AMOUNT_SCORE = 50;

    // Règle "rafale" : au moins ce nombre de transactions (candidate incluse)
    // dans la fenêtre glissante ci-dessous, sur le même compte.
    static final int BURST_MIN_COUNT = 3;
    static final Duration BURST_WINDOW = Duration.ofMinutes(5);
    static final int BURST_SCORE = 40;

    // Score à partir duquel la transaction est considérée comme frauduleuse
    // (seuil inclusif). Le seuil est volontairement fixé à 50 : un signal fort
    // (montant élevé, +50) déclenche une fraude à lui seul, tandis qu'un signal
    // modéré (rafale, +40) est suspect mais insuffisant seul — il doit se
    // combiner avec un autre signal (ex. montant élevé) pour franchir le seuil.
    // C'est ce mécanisme d'accumulation de points qui fait l'intérêt du moteur
    // de scoring.
    static final int FRAUD_THRESHOLD = 50;

    @Override
    public FraudDecision evaluate(List<Transaction> accountHistory, Transaction candidate) {
        int score = 0;
        List<FraudRuleType> triggeredRules = new ArrayList<>();

        if (isHighAmount(candidate)) {
            score += HIGH_AMOUNT_SCORE;
            triggeredRules.add(FraudRuleType.MONTANT_ELEVE);
        }

        if (isBurst(accountHistory, candidate)) {
            score += BURST_SCORE;
            triggeredRules.add(FraudRuleType.RAFALE);
        }

        boolean fraud = score >= FRAUD_THRESHOLD;
        return new FraudDecision(score, fraud, List.copyOf(triggeredRules));
    }

    private boolean isHighAmount(Transaction candidate) {
        return candidate.amount().compareTo(HIGH_AMOUNT_THRESHOLD) > 0;
    }

    private boolean isBurst(List<Transaction> accountHistory, Transaction candidate) {
        // Fenêtre fermée [candidate.timestamp - 5min, candidate.timestamp] :
        // une transaction vieille d'exactement 5 minutes compte encore dans
        // la rafale (borne inférieure incluse) ; le candidat lui-même est
        // toujours dans la fenêtre (écart nul, borne supérieure incluse).
        Instant windowStart = candidate.timestamp().minus(BURST_WINDOW);

        long countInWindow = accountHistory.stream()
                .filter(transaction -> transaction.accountId().equals(candidate.accountId()))
                .filter(transaction -> !transaction.timestamp().isBefore(windowStart))
                .filter(transaction -> !transaction.timestamp().isAfter(candidate.timestamp()))
                .count();

        long totalCount = countInWindow + 1; // + le candidat lui-même

        return totalCount >= BURST_MIN_COUNT;
    }
}
