package com.hashtag.ngo.example.fraud.api;

import com.hashtag.ngo.example.fraud.bean.FraudCaseService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Expose les cas de fraude persistés, à des fins de visualisation de la
 * détection.
 *
 * Aucune authentification n'est appliquée à ce stade : cet endpoint est prévu
 * pour la démonstration/le développement uniquement. Une vraie mise en
 * production nécessiterait a minima une authentification et une autorisation
 * (ex. rôle "analyste fraude") avant exposition de ces données sensibles.
 */
@RestController
@RequestMapping("/api/v1/fraud-cases")
public class FraudCaseController {

    private final FraudCaseService fraudCaseService;

    public FraudCaseController(FraudCaseService fraudCaseService) {
        this.fraudCaseService = fraudCaseService;
    }

    @GetMapping
    public List<FraudCaseView> listFraudCases() {
        return fraudCaseService.findAll().stream()
                .map(FraudCaseView::from)
                .toList();
    }
}
