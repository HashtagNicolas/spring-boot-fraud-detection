package com.hashtag.ngo.example.fraud.listener;

import com.hashtag.ngo.example.fraud.bean.FraudRuleEngine;
import com.hashtag.ngo.example.fraud.config.KafkaTopicsProperties;
import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import com.hashtag.ngo.example.fraud.entity.FraudDecision;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Consomme le flux de transactions ("transactions"), évalue chacune via le
 * moteur de règles et publie une alerte sur "fraud-alerts" pour toute
 * transaction jugée frauduleuse.
 */
@Component
public class FraudDetectionListener {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionListener.class);

    // Fenêtre de conservation de l'historique par compte, alignée sur la
    // fenêtre de la règle de rafale du moteur de règles (cf. FraudRuleEngineImpl).
    private static final Duration HISTORY_RETENTION = Duration.ofMinutes(5);

    private final FraudRuleEngine fraudRuleEngine;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topicsProperties;

    // Historique récent des transactions par compte, utilisé pour la règle de
    // rafale. Ce listener est un bean singleton et le conteneur d'écoute
    // Kafka peut être configuré avec plusieurs threads consommateurs pour
    // paralléliser la lecture des 3 partitions du topic "transactions" ;
    // ConcurrentHashMap (accès par clé sans verrou global) et
    // ConcurrentLinkedDeque (ajout/retrait thread-safe en tête/queue) évitent
    // toute corruption ou perte de mise à jour si deux threads traitent
    // concurremment des transactions de comptes différents - voire du même
    // compte si une réaffectation de partition change la clé de partitionnement.
    //
    // Limite connue (acceptée pour cette v1) : les entrées d'un compte ne
    // sont purgées qu'à l'arrivée d'une nouvelle transaction de ce même
    // compte ; un compte resté inactif garde donc sa dernière entrée en
    // mémoire indéfiniment. Un nettoyage périodique global serait nécessaire
    // en production.
    private final ConcurrentHashMap<String, Deque<Transaction>> historyByAccount = new ConcurrentHashMap<>();

    public FraudDetectionListener(FraudRuleEngine fraudRuleEngine,
                                   KafkaTemplate<String, Object> kafkaTemplate,
                                   KafkaTopicsProperties topicsProperties) {
        this.fraudRuleEngine = fraudRuleEngine;
        this.kafkaTemplate = kafkaTemplate;
        this.topicsProperties = topicsProperties;
    }

    @KafkaListener(topics = "${app.kafka.topics.transactions}", groupId = "fraud-detector")
    public void onTransaction(Transaction transaction) {
        List<Transaction> recentHistory = recentHistory(transaction.accountId(), transaction.timestamp());

        FraudDecision decision = fraudRuleEngine.evaluate(recentHistory, transaction);

        recordHistory(transaction);

        if (decision.fraud()) {
            log.warn("Transaction {} du compte {} détectée frauduleuse (score={}, raisons={})",
                    transaction.id(), transaction.accountId(), decision.score(), decision.triggeredRules());
            publishAlert(transaction, decision);
        } else {
            log.debug("Transaction {} du compte {} jugée légitime (score={})",
                    transaction.id(), transaction.accountId(), decision.score());
        }
    }

    private void publishAlert(Transaction transaction, FraudDecision decision) {
        FraudAlert alert = new FraudAlert(
                transaction.id(),
                transaction.accountId(),
                decision.triggeredRules(),
                decision.score(),
                Instant.now());

        // Clé = accountId, comme pour "transactions" : les alertes d'un même
        // compte restent ordonnées et colocalisées sur une même partition.
        kafkaTemplate.send(topicsProperties.getFraudAlerts(), transaction.accountId(), alert);
    }

    private List<Transaction> recentHistory(String accountId, Instant reference) {
        Deque<Transaction> history = historyByAccount.get(accountId);
        if (history == null) {
            return List.of();
        }

        Instant windowStart = reference.minus(HISTORY_RETENTION);
        // L'itérateur de ConcurrentLinkedDeque est "weakly consistent" : il ne
        // lève jamais ConcurrentModificationException même si un autre thread
        // modifie la deque pendant le parcours, ce qui rend ce flux sûr sans
        // verrou explicite (au prix d'une vue éventuellement légèrement obsolète).
        return history.stream()
                .filter(candidate -> !candidate.timestamp().isBefore(windowStart))
                .toList();
    }

    private void recordHistory(Transaction transaction) {
        Deque<Transaction> history = historyByAccount.computeIfAbsent(
                transaction.accountId(), accountId -> new ConcurrentLinkedDeque<>());
        history.addLast(transaction);
        evictExpiredEntries(history, transaction.timestamp());
    }

    private void evictExpiredEntries(Deque<Transaction> history, Instant reference) {
        Instant windowStart = reference.minus(HISTORY_RETENTION);
        // peekFirst/remove sont des opérations atomiques de ConcurrentLinkedDeque :
        // si un autre thread a déjà retiré l'élément entre le peek et le remove,
        // ce dernier est un no-op sûr, sans exception ni double suppression.
        Transaction oldest;
        while ((oldest = history.peekFirst()) != null && oldest.timestamp().isBefore(windowStart)) {
            history.remove(oldest);
        }
    }
}
