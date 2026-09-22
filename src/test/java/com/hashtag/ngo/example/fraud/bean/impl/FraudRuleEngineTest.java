package com.hashtag.ngo.example.fraud.bean.impl;

import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cas limites du prédicat "montant élevé" (seuil exact). La règle de rafale
 * n'est plus testée ici : son agrégation fenêtrée est désormais assurée par
 * Kafka Streams et testée via FraudDetectionTopologyTest (TopologyTestDriver)
 * et FraudDetectionTopologyIntegrationTest (@EmbeddedKafka).
 */
class FraudRuleEngineTest {

    private final FraudRuleEngineImpl engine = new FraudRuleEngineImpl();
    private final Instant now = Instant.parse("2026-01-01T10:00:00Z");

    @Test
    void amountExactlyAtThresholdDoesNotTriggerHighAmountRule() {
        // La règle est "montant strictement supérieur à 10 000" : pile 10 000
        // ne doit donc pas déclencher la règle.
        Transaction candidate = transaction("10000.00");

        assertThat(engine.isHighAmount(candidate)).isFalse();
    }

    @Test
    void amountJustAboveThresholdTriggersHighAmountRule() {
        Transaction candidate = transaction("10000.01");

        assertThat(engine.isHighAmount(candidate)).isTrue();
    }

    @Test
    void normalAmountDoesNotTriggerHighAmountRule() {
        Transaction candidate = transaction("100.00");

        assertThat(engine.isHighAmount(candidate)).isFalse();
    }

    private Transaction transaction(String amount) {
        return new Transaction(UUID.randomUUID().toString(), "acc-1", new BigDecimal(amount), "EUR", now);
    }
}
