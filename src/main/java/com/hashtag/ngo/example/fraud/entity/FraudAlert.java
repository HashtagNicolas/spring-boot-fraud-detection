package com.hashtag.ngo.example.fraud.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.CollectionTable;
import jakarta.persistence.Column;
import jakarta.persistence.ElementCollection;
import jakarta.persistence.Entity;
import jakarta.persistence.Enumerated;
import jakarta.persistence.EnumType;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Entité JPA représentant un cas de fraude détecté, persistable en base H2,
 * et servant également de message JSON publié sur le topic Kafka
 * "fraud-alerts" (via JsonSerializer/JsonDeserializer).
 *
 * Le constructeur porte @JsonCreator/@JsonProperty : sans ces annotations,
 * Jackson (utilisé par le (dé)sérialiseur JSON de Spring Kafka) ne saurait
 * pas reconstruire l'objet à la réception, faute de setters ou de record
 * canonique - FraudAlert doit rester une classe mutable pour respecter les
 * contraintes de JPA (identifiant généré après insertion), contrairement à
 * Transaction qui est un simple record.
 */
@Entity
public class FraudAlert {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String transactionId;

    @Column(nullable = false)
    private String accountId;

    // Règles ayant contribué à la décision de fraude, pour traçabilité.
    @ElementCollection(fetch = FetchType.EAGER)
    @Enumerated(EnumType.STRING)
    @CollectionTable(name = "fraud_alert_reason", joinColumns = @JoinColumn(name = "fraud_alert_id"))
    @Column(name = "reason")
    private List<FraudRuleType> reasons = new ArrayList<>();

    private int score;

    private Instant detectedAt;

    protected FraudAlert() {
        // constructeur requis par JPA
    }

    @JsonCreator
    public FraudAlert(
            @JsonProperty("transactionId") String transactionId,
            @JsonProperty("accountId") String accountId,
            @JsonProperty("reasons") List<FraudRuleType> reasons,
            @JsonProperty("score") int score,
            @JsonProperty("detectedAt") Instant detectedAt) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.reasons = new ArrayList<>(reasons);
        this.score = score;
        this.detectedAt = detectedAt;
    }

    public Long getId() {
        return id;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public List<FraudRuleType> getReasons() {
        return reasons;
    }

    public int getScore() {
        return score;
    }

    public Instant getDetectedAt() {
        return detectedAt;
    }
}
