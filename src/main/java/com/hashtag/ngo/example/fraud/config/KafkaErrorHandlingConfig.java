package com.hashtag.ngo.example.fraud.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Politique d'erreur appliquée à tous les @KafkaListener de l'application.
 *
 * Spring Boot détecte automatiquement un bean DefaultErrorHandler
 * (implémentant CommonErrorHandler) et l'applique au
 * ConcurrentKafkaListenerContainerFactory qu'il auto-configure : il n'est pas
 * nécessaire de redéfinir cette factory pour brancher cette politique.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    // Nombre de nouvelles tentatives après l'échec initial (donc 4 tentatives
    // au total). Au-delà, le message est écarté vers le dead-letter topic
    // plutôt que de bloquer indéfiniment la partition : un message "poison"
    // (donnée invalide, erreur systématique) empêcherait sinon tous les
    // messages suivants de cette même partition d'être traités.
    private static final int MAX_RETRY_ATTEMPTS = 3;

    // Backoff fixe d'1 seconde entre deux tentatives : suffisant pour
    // absorber une indisponibilité transitoire courte (ex. verrou base de
    // données momentané) sans retarder excessivement le traitement du flux
    // en cas d'erreur systématique (le message part en DLQ après ~3 secondes).
    private static final long RETRY_BACKOFF_MS = 1000L;

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate,
                                                  KafkaTopicsProperties topicsProperties) {
        // Un seul dead-letter topic partagé par tous les listeners de
        // l'application : "fraud-alerts.DLT", déjà déclaré par
        // KafkaTopicsConfig. On ne reprend pas le comportement par défaut de
        // DeadLetterPublishingRecoverer (topic d'origine suffixé de ".DLT",
        // ex. "transactions.DLT"), qui exigerait de déclarer un dead-letter
        // topic distinct par topic source.
        //
        // Partition forcée à -1 (laissée au partitioner du producteur) : le
        // comportement par défaut réutilise la partition du message
        // d'origine, ce qui échouerait ici puisque "fraud-alerts.DLT" n'a
        // qu'une seule partition alors que "transactions" et "fraud-alerts"
        // en ont chacun 3 - un message issu de la partition 1 ou 2 viserait
        // une partition inexistante sur le topic de dead-letter.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (consumerRecord, exception) -> new TopicPartition(topicsProperties.getFraudAlertsDlt(), -1));

        return new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_BACKOFF_MS, MAX_RETRY_ATTEMPTS));
    }
}
