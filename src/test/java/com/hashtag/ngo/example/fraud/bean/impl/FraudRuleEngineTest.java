package com.hashtag.ngo.example.fraud.bean.impl;

import com.hashtag.ngo.example.fraud.entity.FraudDecision;
import com.hashtag.ngo.example.fraud.entity.FraudRuleType;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Complète les scénarios Cucumber avec les cas limites du moteur de règles :
 * seuil de montant exact, seuil de score exact, et bornes de la fenêtre de
 * rafale de 5 minutes.
 */
class FraudRuleEngineTest {

    private final FraudRuleEngineImpl engine = new FraudRuleEngineImpl();
    private final Instant now = Instant.parse("2026-01-01T10:00:00Z");

    @Test
    void amountExactlyAtThresholdDoesNotTriggerHighAmountRule() {
        // La règle est "montant strictement supérieur à 10 000" : pile 10 000
        // ne doit donc pas déclencher la règle.
        Transaction candidate = transaction("10000.00", now);

        FraudDecision decision = engine.evaluate(List.of(), candidate);

        assertThat(decision.triggeredRules()).doesNotContain(FraudRuleType.MONTANT_ELEVE);
        assertThat(decision.fraud()).isFalse();
    }

    @Test
    void amountJustAboveThresholdTriggersHighAmountRuleAndReachesFraudThreshold() {
        Transaction candidate = transaction("10000.01", now);

        FraudDecision decision = engine.evaluate(List.of(), candidate);

        assertThat(decision.triggeredRules()).containsExactly(FraudRuleType.MONTANT_ELEVE);
        assertThat(decision.score()).isEqualTo(50);
        // Le seuil de fraude (50) est inclusif : un score de pile 50 est déjà une fraude.
        assertThat(decision.fraud()).isTrue();
    }

    @Test
    void transactionExactlyFiveMinutesOldCountsInTheBurstWindow() {
        // Fenêtre fermée [candidate - 5min, candidate] : une transaction vieille
        // d'exactement 5 minutes compte encore, ce qui porte le total à 3
        // (candidate + les 2 transactions historiques) et déclenche la rafale.
        Transaction candidate = transaction("50.00", now);
        List<Transaction> history = List.of(
                transaction("50.00", now.minusSeconds(60)),
                transaction("50.00", now.minus(Duration.ofMinutes(5))));

        FraudDecision decision = engine.evaluate(history, candidate);

        assertThat(decision.triggeredRules()).containsExactly(FraudRuleType.RAFALE);
        // Une rafale seule (+40) reste sous le seuil de fraude (50) : suspecte
        // mais pas frauduleuse à elle seule.
        assertThat(decision.fraud()).isFalse();
    }

    @Test
    void transactionJustOverFiveMinutesOldIsExcludedFromTheBurstWindow() {
        // Une transaction vieille de 5 minutes et 1 seconde tombe hors fenêtre :
        // il ne reste que 2 transactions (candidate + 1), la rafale n'est pas
        // déclenchée.
        Transaction candidate = transaction("50.00", now);
        List<Transaction> history = List.of(
                transaction("50.00", now.minusSeconds(60)),
                transaction("50.00", now.minus(Duration.ofMinutes(5)).minusSeconds(1)));

        FraudDecision decision = engine.evaluate(history, candidate);

        assertThat(decision.triggeredRules()).doesNotContain(FraudRuleType.RAFALE);
        assertThat(decision.fraud()).isFalse();
    }

    @Test
    void historyFromAnotherAccountIsIgnoredForTheBurstRule() {
        Transaction candidate = transaction("50.00", now);
        List<Transaction> otherAccountHistory = List.of(
                new Transaction(UUID.randomUUID().toString(), "acc-other", new BigDecimal("50.00"), "EUR", now.minusSeconds(30)),
                new Transaction(UUID.randomUUID().toString(), "acc-other", new BigDecimal("50.00"), "EUR", now.minusSeconds(60)));

        FraudDecision decision = engine.evaluate(otherAccountHistory, candidate);

        assertThat(decision.triggeredRules()).doesNotContain(FraudRuleType.RAFALE);
        assertThat(decision.fraud()).isFalse();
    }

    private Transaction transaction(String amount, Instant timestamp) {
        return new Transaction(UUID.randomUUID().toString(), "acc-1", new BigDecimal(amount), "EUR", timestamp);
    }
}
