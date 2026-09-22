package com.hashtag.ngo.example.fraud.listener;

import com.hashtag.ngo.example.fraud.bean.FraudCaseService;
import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Persiste chaque alerte de fraude reçue, pour constituer le dossier des cas
 * traités (consultable via GET /api/v1/fraud-cases).
 *
 * Fan-out Kafka : le groupe "case-manager" est indépendant du groupe
 * "notifier" (NotificationListener) - un groupe de consommateurs Kafka
 * correspond à une copie du flux "fraud-alerts" par service ; chaque service
 * avance à son propre rythme, avec son propre offset.
 */
@Component
public class CaseManagementListener {

    private static final Logger log = LoggerFactory.getLogger(CaseManagementListener.class);

    private final FraudCaseService fraudCaseService;

    public CaseManagementListener(FraudCaseService fraudCaseService) {
        this.fraudCaseService = fraudCaseService;
    }

    @KafkaListener(topics = "${app.kafka.topics.fraud-alerts}", groupId = "case-manager")
    public void onFraudAlert(FraudAlert alert) {
        // La persistance (log + exception) est gérée dans FraudCaseServiceImpl ;
        // laisser l'exception se propager ici permet au conteneur d'écoute
        // Kafka d'appliquer sa gestion d'erreur par défaut (log, retries selon
        // configuration) plutôt que de masquer l'échec.
        fraudCaseService.recordCase(alert);
        log.info("Cas de fraude persisté : transaction {} (compte {}, score {})",
                alert.getTransactionId(), alert.getAccountId(), alert.getScore());
    }
}
