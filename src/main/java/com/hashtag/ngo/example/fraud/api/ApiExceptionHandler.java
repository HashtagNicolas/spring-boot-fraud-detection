package com.hashtag.ngo.example.fraud.api;

import com.hashtag.ngo.example.fraud.bean.TransactionPublishingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.List;

/**
 * Centralise la traduction des erreurs en réponses HTTP :
 * - erreurs fonctionnelles (requête invalide) -> 400 ;
 * - erreurs techniques (dépendance Kafka indisponible) -> 503.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationError(MethodArgumentNotValidException ex) {
        List<String> details = ex.getBindingResult().getFieldErrors().stream()
                .map(fieldError -> fieldError.getField() + ": " + fieldError.getDefaultMessage())
                .toList();

        ErrorResponse body = new ErrorResponse(
                Instant.now(),
                HttpStatus.BAD_REQUEST.value(),
                "Requête invalide",
                "La transaction soumise ne respecte pas les contraintes de validation",
                details);

        return ResponseEntity.badRequest().body(body);
    }

    @ExceptionHandler(TransactionPublishingException.class)
    public ResponseEntity<ErrorResponse> handlePublishingFailure(TransactionPublishingException ex) {
        log.error("Erreur technique lors de la publication d'une transaction", ex);

        ErrorResponse body = new ErrorResponse(
                Instant.now(),
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                "Service indisponible",
                "La transaction n'a pas pu être publiée, veuillez réessayer ultérieurement",
                List.of());

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }
}
