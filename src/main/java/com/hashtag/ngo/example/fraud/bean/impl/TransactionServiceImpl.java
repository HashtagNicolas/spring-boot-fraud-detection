package com.hashtag.ngo.example.fraud.bean.impl;

import com.hashtag.ngo.example.fraud.bean.TransactionService;
import com.hashtag.ngo.example.fraud.config.KafkaTopicsProperties;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TransactionServiceImpl implements TransactionService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTopicsProperties topicsProperties;

    public TransactionServiceImpl(KafkaTemplate<String, Object> kafkaTemplate,
                                   KafkaTopicsProperties topicsProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicsProperties = topicsProperties;
    }

    @Override
    public void submit(Transaction transaction) {
        kafkaTemplate.send(topicsProperties.getTransactions(), transaction.id(), transaction);
    }
}
