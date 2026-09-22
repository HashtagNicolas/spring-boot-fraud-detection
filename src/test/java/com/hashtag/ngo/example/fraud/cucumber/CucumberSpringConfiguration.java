package com.hashtag.ngo.example.fraud.cucumber;

import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.context.EmbeddedKafka;

/**
 * Point de bootstrap Spring pour les step definitions Cucumber qui ont
 * besoin du contexte applicatif complet (scénario E2E).
 *
 * Cucumber n'autorise qu'une seule classe @CucumberContextConfiguration par
 * exécution : elle s'applique donc à TOUTES les step definitions du module,
 * y compris FraudRuleEngineSteps (P3) qui n'a pourtant besoin ni de Spring ni
 * de Kafka. Conséquence acceptée : le contexte Spring (avec Kafka embarqué)
 * démarre une fois pour l'ensemble de la suite Cucumber, mis en cache par le
 * TestContext framework de Spring au même titre que pour les tests JUnit
 * classiques du module.
 */
@CucumberContextConfiguration
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka
public class CucumberSpringConfiguration {
}
