package com.hashtag.ngo.example.fraud.listener;

import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import com.hashtag.ngo.example.fraud.entity.FraudRuleType;
import com.hashtag.ngo.example.fraud.repository.FraudAlertRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Vérifie que CaseManagementListener persiste bien, via FraudAlertRepository,
 * une FraudAlert produite sur "fraud-alerts".
 *
 * Piège d'isolation évité : la base H2 en mémoire est partagée par le
 * contexte Spring, lui-même potentiellement réutilisé entre classes de test
 * (cache de contexte) - d'autres cas de fraude peuvent donc déjà s'y trouver
 * (y compris produits par NotificationListener/CaseManagementListener lors
 * d'autres tests). On identifie donc notre propre alerte par un
 * transactionId unique plutôt que de vérifier un compte ou un contenu global
 * de la table.
 */
@SpringBootTest
@EmbeddedKafka
class CaseManagementListenerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private FraudAlertRepository fraudAlertRepository;

    @Test
    void fraudAlertIsPersistedAfterConsumption() {
        String transactionId = UUID.randomUUID().toString();
        String accountId = "acc-" + UUID.randomUUID();

        FraudAlert alert = new FraudAlert(
                transactionId,
                accountId,
                List.of(FraudRuleType.MONTANT_ELEVE),
                50,
                Instant.now());

        kafkaTemplate.send("fraud-alerts", accountId, alert);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Optional<FraudAlert> persisted = findByTransactionId(transactionId);

            assertThat(persisted).isPresent();
            assertThat(persisted.get().getAccountId()).isEqualTo(accountId);
            assertThat(persisted.get().getScore()).isEqualTo(50);
            assertThat(persisted.get().getReasons()).containsExactly(FraudRuleType.MONTANT_ELEVE);
        });
    }

    @Test
    void duplicateDeliveryOfTheSameAlertIsPersistedOnlyOnce() {
        // Simule la sémantique "au moins une fois" de Kafka : la même alerte
        // métier (même transactionId) est livrée deux fois au consommateur
        // (ex. redémarrage du consommateur avant validation de l'offset).
        String transactionId = UUID.randomUUID().toString();
        String accountId = "acc-" + UUID.randomUUID();

        FraudAlert alert = new FraudAlert(
                transactionId, accountId, List.of(FraudRuleType.MONTANT_ELEVE), 50, Instant.now());

        kafkaTemplate.send("fraud-alerts", accountId, alert);
        kafkaTemplate.send("fraud-alerts", accountId, alert);

        // .during(...) vérifie que le compte reste stable à 1 pendant toute
        // cette fenêtre, et pas seulement au premier instant où il l'atteint :
        // sans cela, un test qui vérifierait juste "== 1" pourrait réussir
        // simplement parce que la seconde livraison n'a pas encore été traitée.
        await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(200))
                .during(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(countByTransactionId(transactionId)).isEqualTo(1));
    }

    private Optional<FraudAlert> findByTransactionId(String transactionId) {
        return fraudAlertRepository.findAll().stream()
                .filter(candidate -> transactionId.equals(candidate.getTransactionId()))
                .findFirst();
    }

    private long countByTransactionId(String transactionId) {
        return fraudAlertRepository.findAll().stream()
                .filter(candidate -> transactionId.equals(candidate.getTransactionId()))
                .count();
    }
}
