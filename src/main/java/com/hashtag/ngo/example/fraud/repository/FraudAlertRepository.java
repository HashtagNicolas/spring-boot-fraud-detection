package com.hashtag.ngo.example.fraud.repository;

import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/**
 * Accès Spring Data JPA aux cas de fraude persistés en base H2.
 */
public interface FraudAlertRepository extends JpaRepository<FraudAlert, Long> {

    // Support de l'idempotence du case-manager (cf. FraudCaseServiceImpl) :
    // permet de vérifier si une alerte a déjà été traitée avant de la persister.
    Optional<FraudAlert> findByTransactionId(String transactionId);
}
