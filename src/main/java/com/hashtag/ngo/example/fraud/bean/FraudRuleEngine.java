package com.hashtag.ngo.example.fraud.bean;

import com.hashtag.ngo.example.fraud.entity.FraudDecision;
import com.hashtag.ngo.example.fraud.entity.Transaction;

import java.util.List;

/**
 * Évalue une transaction candidate au regard de l'historique récent du même
 * compte, et produit une décision de fraude explicable (score + règles
 * déclenchées).
 */
public interface FraudRuleEngine {

    /**
     * @param accountHistory transactions récentes du même compte que candidate
     *                       (candidate elle-même n'y figure pas), utilisées
     *                       pour la détection de rafale
     * @param candidate      transaction à évaluer
     */
    FraudDecision evaluate(List<Transaction> accountHistory, Transaction candidate);
}
