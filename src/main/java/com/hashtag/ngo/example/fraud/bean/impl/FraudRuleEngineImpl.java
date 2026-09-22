package com.hashtag.ngo.example.fraud.bean.impl;

import com.hashtag.ngo.example.fraud.bean.FraudRuleEngine;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Seuils et prédicats de détection de fraude (v2, Kafka Streams).
 *
 * Logique pure : ne dépend que du modèle de domaine (Transaction), aucune
 * dépendance Kafka/JPA. Utilisée directement par FraudDetectionTopology pour
 * construire les deux détecteurs indépendants (montant élevé, rafale) -
 * l'agrégation de la rafale elle-même (comptage fenêtré) est désormais
 * assurée par Kafka Streams, plus par cette classe.
 */
@Component
public class FraudRuleEngineImpl implements FraudRuleEngine {

    // Règle "montant élevé" : un montant strictement supérieur à ce seuil
    // est considéré comme suspect.
    private static final BigDecimal HIGH_AMOUNT_THRESHOLD = new BigDecimal("10000");
    private static final int HIGH_AMOUNT_SCORE = 50;

    // Règle "rafale" : au moins ce nombre de transactions (candidate incluse)
    // dans la fenêtre ci-dessous, sur le même compte.
    private static final int BURST_MIN_COUNT = 3;
    private static final Duration BURST_WINDOW = Duration.ofMinutes(5);
    private static final int BURST_SCORE = 40;

    @Override
    public boolean isHighAmount(Transaction transaction) {
        return transaction.amount().compareTo(HIGH_AMOUNT_THRESHOLD) > 0;
    }

    @Override
    public BigDecimal highAmountThreshold() {
        return HIGH_AMOUNT_THRESHOLD;
    }

    @Override
    public int highAmountScore() {
        return HIGH_AMOUNT_SCORE;
    }

    @Override
    public int burstMinCount() {
        return BURST_MIN_COUNT;
    }

    @Override
    public Duration burstWindow() {
        return BURST_WINDOW;
    }

    @Override
    public int burstScore() {
        return BURST_SCORE;
    }
}
