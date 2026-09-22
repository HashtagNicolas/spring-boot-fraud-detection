package com.hashtag.ngo.example.fraud.bean;

import com.hashtag.ngo.example.fraud.entity.FraudAlert;

import java.util.List;

/**
 * Gère le cycle de vie des cas de fraude persistés (issus des alertes reçues
 * sur "fraud-alerts").
 */
public interface FraudCaseService {

    /**
     * @throws FraudCasePersistenceException si la persistance échoue
     */
    FraudAlert recordCase(FraudAlert alert);

    List<FraudAlert> findAll();
}
