package com.hashtag.ngo.example.fraud.cucumber;

import com.hashtag.ngo.example.fraud.bean.FraudRuleEngine;
import com.hashtag.ngo.example.fraud.bean.impl.FraudRuleEngineImpl;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import io.cucumber.java.fr.Alors;
import io.cucumber.java.fr.Etantdonné;
import io.cucumber.java.fr.Quand;

import java.math.BigDecimal;
import java.time.Instant;
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

    private String accountId;
    private Transaction candidate;
    private boolean highAmount;

    @Etantdonné("le compte {string}")
    public void leCompte(String accountId) {
        this.accountId = accountId;
    }

    @Etantdonné("une transaction de {string} EUR sur le compte")
    public void uneTransactionSurLeCompte(String amount) {
        candidate = new Transaction(UUID.randomUUID().toString(), accountId, new BigDecimal(amount), "EUR", Instant.now());
    }

    @Quand("j'évalue la transaction")
    public void jEvalueLaTransaction() {
        highAmount = engine.isHighAmount(candidate);
    }

    @Alors("le montant est jugé élevé")
    public void leMontantEstJugeEleve() {
        assertThat(highAmount).isTrue();
    }

    @Alors("le montant n'est pas jugé élevé")
    public void leMontantNestPasJugeEleve() {
        assertThat(highAmount).isFalse();
    }
}
