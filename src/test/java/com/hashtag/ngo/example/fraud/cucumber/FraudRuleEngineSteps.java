package com.hashtag.ngo.example.fraud.cucumber;

import com.hashtag.ngo.example.fraud.bean.FraudRuleEngine;
import com.hashtag.ngo.example.fraud.bean.impl.FraudRuleEngineImpl;
import com.hashtag.ngo.example.fraud.entity.FraudDecision;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import io.cucumber.java.Before;
import io.cucumber.java.fr.Alors;
import io.cucumber.java.fr.Etantdonné;
import io.cucumber.java.fr.Quand;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Définitions des étapes du scénario fraud_rules.feature.
 *
 * Les montants sont capturés en {string} (et non {double}) : le type
 * intégré {double} de Cucumber convertit selon un format numérique qui
 * dépend de la locale de la machine (virgule vs point décimal), ce qui rend
 * les tests non déterministes selon l'environnement d'exécution. En passant
 * par {string} + `new BigDecimal(String)`, l'analyse est toujours faite avec
 * le point comme séparateur décimal, indépendamment de la locale.
 */
public class FraudRuleEngineSteps {

    private final FraudRuleEngine engine = new FraudRuleEngineImpl();

    // Instant de référence commun à tous les horodatages du scénario, pour que
    // l'historique et le candidat restent cohérents entre eux dans le temps.
    private final Instant now = Instant.now();

    private String accountId;
    private final List<Transaction> history = new ArrayList<>();
    private Transaction candidate;
    private FraudDecision decision;

    @Before
    public void resetState() {
        history.clear();
        candidate = null;
        decision = null;
    }

    @Etantdonné("le compte {string}")
    public void leCompte(String accountId) {
        this.accountId = accountId;
    }

    @Etantdonné("un historique de {int} transactions de {string} EUR dans les 5 dernières minutes sur le compte")
    public void unHistoriqueDeTransactionsDansLesCinqDernieresMinutes(int count, String amount) {
        for (int i = 0; i < count; i++) {
            // Espacées d'une minute en remontant dans le temps : toutes restent
            // dans la fenêtre de rafale de 5 minutes tant que count reste petit.
            Instant timestamp = now.minus(Duration.ofMinutes(i + 1L));
            history.add(newTransaction(amount, timestamp));
        }
    }

    @Etantdonné("une transaction de {string} EUR sur le compte")
    public void uneTransactionSurLeCompte(String amount) {
        candidate = newTransaction(amount, now);
    }

    @Quand("j'évalue la transaction")
    public void jEvalueLaTransaction() {
        decision = engine.evaluate(history, candidate);
    }

    @Alors("le score doit être au moins {int}")
    public void leScoreDoitEtreAuMoins(int minimumScore) {
        assertThat(decision.score()).isGreaterThanOrEqualTo(minimumScore);
    }

    @Alors("la décision doit être une fraude")
    public void laDecisionDoitEtreUneFraude() {
        assertThat(decision.fraud()).isTrue();
    }

    @Alors("la décision ne doit pas être une fraude")
    public void laDecisionNeDoitPasEtreUneFraude() {
        assertThat(decision.fraud()).isFalse();
    }

    @Alors("la raison {string} doit être présente")
    public void laRaisonDoitEtrePresente(String reason) {
        assertThat(decision.triggeredRules())
                .extracting(Enum::name)
                .contains(reason);
    }

    private Transaction newTransaction(String amount, Instant timestamp) {
        return new Transaction(UUID.randomUUID().toString(), accountId, new BigDecimal(amount), "EUR", timestamp);
    }
}
