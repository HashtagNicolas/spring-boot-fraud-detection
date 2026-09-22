package com.hashtag.ngo.example.fraud.api;

import com.hashtag.ngo.example.fraud.bean.TransactionService;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Point d'entrée REST pour l'enregistrement des transactions bancaires.
 * Chaque transaction reçue est publiée sur Kafka pour analyse asynchrone ;
 * aucune détection de fraude n'est encore implémentée à ce stade.
 */
@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final TransactionService transactionService;

    public TransactionController(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    @PostMapping
    public ResponseEntity<Void> registerTransaction(@RequestBody Transaction transaction) {
        transactionService.submit(transaction);
        return ResponseEntity.accepted().build();
    }
}
