package com.hashtag.ngo.example.fraud.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Liaison typée des noms de topics Kafka déclarés sous "app.kafka.topics"
 * dans application.yml, pour éviter de coder ces noms en dur.
 */
@ConfigurationProperties(prefix = "app.kafka.topics")
public class KafkaTopicsProperties {

    private String transactions;
    private String fraudAlerts;
    private String fraudAlertsDlt;

    public String getTransactions() {
        return transactions;
    }

    public void setTransactions(String transactions) {
        this.transactions = transactions;
    }

    public String getFraudAlerts() {
        return fraudAlerts;
    }

    public void setFraudAlerts(String fraudAlerts) {
        this.fraudAlerts = fraudAlerts;
    }

    public String getFraudAlertsDlt() {
        return fraudAlertsDlt;
    }

    public void setFraudAlertsDlt(String fraudAlertsDlt) {
        this.fraudAlertsDlt = fraudAlertsDlt;
    }
}
