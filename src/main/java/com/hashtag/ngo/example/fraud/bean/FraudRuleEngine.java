package com.hashtag.ngo.example.fraud.bean;

import com.hashtag.ngo.example.fraud.entity.Transaction;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Prédicats et seuils purs de détection de fraude. Depuis l'introduction de
 * la topologie Kafka Streams (FraudDetectionTopology), ce moteur n'évalue
 * plus lui-même de décision globale (pas d'historique en mémoire) : il
 * expose uniquement les règles élémentaires que la topologie assemble en
 * deux détecteurs indépendants (montant élevé, rafale).
 */
public interface FraudRuleEngine {

    /**
     * Règle "montant élevé" (stateless) : vrai si le montant est strictement
     * supérieur au seuil.
     */
    boolean isHighAmount(Transaction transaction);

    BigDecimal highAmountThreshold();

    int highAmountScore();

    /**
     * Nombre minimal de transactions (candidate comprise) déclenchant la
     * règle de rafale au sein de la fenêtre {@link #burstWindow()}.
     */
    int burstMinCount();

    Duration burstWindow();

    int burstScore();
}
