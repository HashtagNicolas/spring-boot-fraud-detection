package com.hashtag.ngo.example.fraud.entity;

import java.util.List;

/**
 * Résultat de l'évaluation d'une transaction par le moteur de règles :
 * score explicite (somme des points des règles déclenchées), décision
 * binaire de fraude, et liste des règles ayant contribué (pour traçabilité
 * et pour renseigner une future alerte de fraude).
 */
public record FraudDecision(int score, boolean fraud, List<FraudRuleType> triggeredRules) {
}
