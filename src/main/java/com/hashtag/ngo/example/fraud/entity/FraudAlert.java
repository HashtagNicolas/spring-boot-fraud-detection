package com.hashtag.ngo.example.fraud.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import java.time.Instant;

/**
 * Entité JPA représentant un cas de fraude détecté, persisté en base H2.
 * La logique de détection elle-même sera ajoutée dans une itération future.
 */
@Entity
public class FraudAlert {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String transactionId;

    private String reason;

    private Instant detectedAt;

    protected FraudAlert() {
        // constructeur requis par JPA
    }

    public FraudAlert(String transactionId, String reason, Instant detectedAt) {
        this.transactionId = transactionId;
        this.reason = reason;
        this.detectedAt = detectedAt;
    }

    public Long getId() {
        return id;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getReason() {
        return reason;
    }

    public Instant getDetectedAt() {
        return detectedAt;
    }
}
