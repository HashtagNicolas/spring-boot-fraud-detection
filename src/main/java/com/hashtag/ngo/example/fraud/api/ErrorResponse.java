package com.hashtag.ngo.example.fraud.api;

import java.time.Instant;
import java.util.List;

/**
 * Corps de réponse uniforme pour les erreurs de l'API, qu'elles soient
 * fonctionnelles (validation) ou techniques (dépendance indisponible).
 */
public record ErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String message,
        List<String> details
) {
}
