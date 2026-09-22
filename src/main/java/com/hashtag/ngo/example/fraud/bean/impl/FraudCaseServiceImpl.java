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

@Service
public class FraudCaseServiceImpl implements FraudCaseService {

    private static final Logger log = LoggerFactory.getLogger(FraudCaseServiceImpl.class);

    private final FraudAlertRepository fraudAlertRepository;

    public FraudCaseServiceImpl(FraudAlertRepository fraudAlertRepository) {
        this.fraudAlertRepository = fraudAlertRepository;
    }

    @Override
    public FraudAlert recordCase(FraudAlert alert) {
        try {
            return fraudAlertRepository.save(alert);
        } catch (DataAccessException e) {
            log.error("Échec de la persistance du cas de fraude pour la transaction {}",
                    alert.getTransactionId(), e);
            throw new FraudCasePersistenceException(
                    "Échec de la persistance du cas de fraude " + alert.getTransactionId(), e);
        }
    }

    @Override
    public List<FraudAlert> findAll() {
        return fraudAlertRepository.findAll();
    }
}
