package com.hashtag.ngo.example.fraud.listener;

import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Simule l'envoi d'une notification (email/SMS/push en production) pour
 * chaque alerte de fraude reçue. Aucune intégration réelle n'est implémentée
 * à ce stade : un simple log structuré en tient lieu.
 *
 * Fan-out Kafka : le groupe "notifier" est indépendant du groupe
 * "case-manager" (CaseManagementListener) - Kafka délivre à chaque groupe de
 * consommateurs sa propre copie complète du flux "fraud-alerts", avec un
 * offset de consommation propre à chaque groupe. Les deux traitements
 * (notification, persistance) sont donc totalement découplés : l'échec ou le
 * retard de l'un n'affecte pas l'autre.
 */
@Component
public class NotificationListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);

    @KafkaListener(topics = "${app.kafka.topics.fraud-alerts}", groupId = "notifier")
    public void onFraudAlert(FraudAlert alert) {
        log.info("Notification envoyée : transaction {} du compte {} signalée comme frauduleuse (score={}, raisons={})",
                alert.getTransactionId(), alert.getAccountId(), alert.getScore(), alert.getReasons());
    }
}
