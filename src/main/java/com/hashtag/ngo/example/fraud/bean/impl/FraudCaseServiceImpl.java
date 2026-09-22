package com.hashtag.ngo.example.fraud.bean.impl;

import com.hashtag.ngo.example.fraud.bean.FraudCasePersistenceException;
import com.hashtag.ngo.example.fraud.bean.FraudCaseService;
import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import com.hashtag.ngo.example.fraud.repository.FraudAlertRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class FraudCaseServiceImpl implements FraudCaseService {

    private static final Logger log = LoggerFactory.getLogger(FraudCaseServiceImpl.class);

    private final FraudAlertRepository fraudAlertRepository;

    public FraudCaseServiceImpl(FraudAlertRepository fraudAlertRepository) {
        this.fraudAlertRepository = fraudAlertRepository;
    }

    @Override
    public FraudAlert recordCase(FraudAlert alert) {
        // Idempotence : Kafka garantit une sémantique "au moins une fois". Un
        // rebalance ou un redémarrage du consommateur juste avant la validation
        // de l'offset peut donc redélivrer une alerte déjà traitée. On vérifie
        // son existence par transactionId avant d'insérer, pour qu'un
        // retraitement n'insère jamais de doublon.
        //
        // Limite connue (acceptée pour cette v1) : cette vérification puis
        // insertion n'est pas atomique - une fenêtre de course existe si deux
        // instances traitaient concurremment la même alerte au même instant.
        // En pratique, une redélivraison Kafka pour un même groupe de
        // consommateurs est séquentielle (une seule instance traite une
        // partition donnée à la fois), ce qui rend ce scénario improbable ici ;
        // une contrainte d'unicité en base fermerait cette fenêtre si besoin.
        Optional<FraudAlert> existing = findExisting(alert.getTransactionId());
        if (existing.isPresent()) {
            log.info("Alerte déjà traitée pour la transaction {} (redélivraison Kafka) : ignorée",
                    alert.getTransactionId());
            return existing.get();
        }

        try {
            return fraudAlertRepository.save(alert);
        } catch (DataAccessException e) {
            log.error("Échec de la persistance du cas de fraude pour la transaction {}",
                    alert.getTransactionId(), e);
            throw new FraudCasePersistenceException(
                    "Échec de la persistance du cas de fraude " + alert.getTransactionId(), e);
        }
    }

    private Optional<FraudAlert> findExisting(String transactionId) {
        try {
            return fraudAlertRepository.findByTransactionId(transactionId);
        } catch (DataAccessException e) {
            log.error("Échec de la vérification d'idempotence pour la transaction {}", transactionId, e);
            throw new FraudCasePersistenceException(
                    "Échec de la vérification d'idempotence pour la transaction " + transactionId, e);
        }
    }

    @Override
    public List<FraudAlert> findAll() {
        return fraudAlertRepository.findAll();
    }
}
