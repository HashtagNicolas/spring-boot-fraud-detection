package com.hashtag.ngo.example.fraud.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/**
 * Corps de la requête REST d'enregistrement d'une transaction.
 *
 * Ce DTO est volontairement distinct de l'événement métier Transaction :
 * le DTO ne reflète que ce que le client est autorisé à fournir (pas d'id,
 * pas de timestamp - ces champs sont générés côté serveur pour ne pas faire
 * confiance à l'appelant), alors que l'événement Kafka représente le fait
 * métier complet et immuable qui sera consommé par les analyseurs de fraude.
 * Coupler les deux forcerait à exposer sur l'API des champs qui n'ont de
 * sens que côté événement, et empêcherait de faire évoluer l'un sans casser
 * l'autre (versionnage de l'API vs évolution du contrat Kafka).
 */
public record TransactionRequest(

        @NotBlank
        String accountId,

        @NotNull
        @Positive
        BigDecimal amount,

        @NotBlank
        String currency
) {
}
