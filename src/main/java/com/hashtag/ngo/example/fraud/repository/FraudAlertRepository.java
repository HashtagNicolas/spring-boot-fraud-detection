package com.hashtag.ngo.example.fraud.repository;

import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Accès Spring Data JPA aux cas de fraude persistés en base H2.
 */
public interface FraudAlertRepository extends JpaRepository<FraudAlert, Long> {
}
