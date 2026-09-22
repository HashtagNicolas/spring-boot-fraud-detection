package com.hashtag.ngo.example.fraud.streams;

import com.hashtag.ngo.example.fraud.bean.FraudRuleEngine;
import com.hashtag.ngo.example.fraud.config.KafkaTopicsProperties;
import com.hashtag.ngo.example.fraud.entity.FraudAlert;
import com.hashtag.ngo.example.fraud.entity.FraudRuleType;
import com.hashtag.ngo.example.fraud.entity.Transaction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Instant;
import java.util.List;

/**
 * Topologie Kafka Streams de détection de fraude : deux détecteurs
 * indépendants lisant "transactions" et publiant chacun leurs propres
 * alertes sur "fraud-alerts" (option "détecteurs indépendants" - une
 * transaction à la fois frauduleuse par le montant ET par la rafale produit
 * donc deux alertes distinctes, plutôt qu'une alerte combinée à score cumulé
 * comme le faisait l'ancien FraudDetectionListener).
 *
 * Remplace l'historique en mémoire (ConcurrentHashMap par compte) de l'ancien
 * consommateur : le comptage par fenêtre est désormais assuré par Kafka
 * Streams lui-même (état distribué, tolérant aux pannes via les changelogs
 * de topics internes), au lieu d'un état local perdu au redémarrage de
 * l'instance.
 */
@Configuration
@EnableKafkaStreams
public class FraudDetectionTopology {

    private final FraudRuleEngine fraudRuleEngine;
    private final KafkaTopicsProperties topicsProperties;

    public FraudDetectionTopology(FraudRuleEngine fraudRuleEngine, KafkaTopicsProperties topicsProperties) {
        this.fraudRuleEngine = fraudRuleEngine;
        this.topicsProperties = topicsProperties;
    }

    @Bean
    public KStream<String, Transaction> fraudDetectionStream(StreamsBuilder streamsBuilder) {
        // ignoreTypeHeaders() côté Transaction : ce Serde ne sert qu'à LIRE
        // "transactions" dans la topologie, toujours du même type - inutile
        // de dépendre de l'en-tête __TypeId__ pour cela. Le Serde FraudAlert,
        // lui, garde son comportement par défaut (écrit __TypeId__ à la
        // production) : NotificationListener et CaseManagementListener en
        // ont besoin pour désambiguïser Transaction/FraudAlert sur la JVM.
        JsonSerde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class).ignoreTypeHeaders();
        JsonSerde<FraudAlert> fraudAlertSerde = new JsonSerde<>(FraudAlert.class);
        JsonSerde<BurstAggregate> burstAggregateSerde = new JsonSerde<>(BurstAggregate.class).ignoreTypeHeaders();

        KStream<String, Transaction> transactions = streamsBuilder.stream(
                topicsProperties.getTransactions(),
                Consumed.with(Serdes.String(), transactionSerde));

        detectHighAmount(transactions, fraudAlertSerde);
        detectBurst(transactions, transactionSerde, burstAggregateSerde, fraudAlertSerde);

        return transactions;
    }

    // Détecteur "montant élevé" (stateless) : évalue chaque transaction
    // indépendamment, aucun état à conserver entre les messages.
    private void detectHighAmount(KStream<String, Transaction> transactions, JsonSerde<FraudAlert> fraudAlertSerde) {
        transactions
                .filter((accountId, transaction) -> fraudRuleEngine.isHighAmount(transaction))
                .mapValues(this::toHighAmountAlert)
                .to(topicsProperties.getFraudAlerts(), Produced.with(Serdes.String(), fraudAlertSerde));
    }

    // Détecteur "rafale" (stateful) : fenêtres tumbling (non chevauchantes)
    // de 5 minutes. Différence assumée avec l'ancienne fenêtre glissante en
    // mémoire : une rafale à cheval sur deux fenêtres tumbling successives
    // (ex. 2 transactions juste avant xx:05:00, 1 juste après) peut ne pas
    // être détectée alors qu'elle l'aurait été avec une fenêtre glissante -
    // compromis documenté dans le README, au bénéfice de la simplicité de
    // l'API TimeWindows (vs SlidingWindows).
    private void detectBurst(KStream<String, Transaction> transactions,
                              JsonSerde<Transaction> transactionSerde,
                              JsonSerde<BurstAggregate> burstAggregateSerde,
                              JsonSerde<FraudAlert> fraudAlertSerde) {
        transactions
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(fraudRuleEngine.burstWindow()))
                .aggregate(
                        BurstAggregate::empty,
                        (accountId, transaction, aggregate) -> aggregate.withTransaction(transaction.id()),
                        Materialized.<String, BurstAggregate, WindowStore<Bytes, byte[]>>as("burst-window-counts")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(burstAggregateSerde))
                .toStream()
                .filter((windowedKey, aggregate) -> aggregate.count() >= fraudRuleEngine.burstMinCount())
                .map((windowedKey, aggregate) -> KeyValue.pair(windowedKey.key(), toBurstAlert(windowedKey.key(), aggregate)))
                .to(topicsProperties.getFraudAlerts(), Produced.with(Serdes.String(), fraudAlertSerde));
    }

    private FraudAlert toHighAmountAlert(Transaction transaction) {
        return new FraudAlert(
                transaction.id(),
                transaction.accountId(),
                List.of(FraudRuleType.MONTANT_ELEVE),
                fraudRuleEngine.highAmountScore(),
                Instant.now());
    }

    private FraudAlert toBurstAlert(String accountId, BurstAggregate aggregate) {
        return new FraudAlert(
                aggregate.lastTransactionId(),
                accountId,
                List.of(FraudRuleType.RAFALE),
                fraudRuleEngine.burstScore(),
                Instant.now());
    }
}
