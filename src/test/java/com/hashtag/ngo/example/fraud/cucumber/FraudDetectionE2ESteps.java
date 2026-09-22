package com.hashtag.ngo.example.fraud.cucumber;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hashtag.ngo.example.fraud.api.AuthRequest;
import com.hashtag.ngo.example.fraud.api.FraudCaseView;
import com.hashtag.ngo.example.fraud.api.TransactionAccepted;
import com.hashtag.ngo.example.fraud.api.TransactionRequest;
import io.cucumber.java.fr.Alors;
import io.cucumber.java.fr.Etantdonné;
import io.cucumber.java.fr.Quand;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Définitions des étapes du scénario fraud_detection_e2e.feature. Instanciée
 * par le SpringFactory de cucumber-spring grâce à CucumberSpringConfiguration,
 * ce qui permet l'injection de MockMvc.
 */
public class FraudDetectionE2ESteps {

    @Autowired
    private MockMvc mockMvc;

    // ObjectMapper Jackson 2 dédié : la couche web de Spring Boot 4 sérialise
    // ses réponses via Jackson 3 (tools.jackson), mais le JSON produit reste
    // du JSON standard, lisible par un ObjectMapper Jackson 2 classique -
    // même choix que pour les tests producteur (P2). JavaTimeModule est
    // nécessaire pour désérialiser le champ Instant "detectedAt" de FraudCaseView.
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private String token;
    private String accountId;
    private String transactionId;

    @Etantdonné("je suis authentifié")
    public void jeSuisAuthentifie() throws Exception {
        String requestBody = objectMapper.writeValueAsString(new AuthRequest("demo", "demo123"));

        MvcResult result = mockMvc.perform(post("/auth/token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andReturn();

        token = objectMapper.readTree(result.getResponse().getContentAsString()).get("token").asText();
    }

    @Etantdonné("un nouveau compte bancaire")
    public void unNouveauCompteBancaire() {
        // accountId unique par scénario : le contexte Spring (Kafka embarqué,
        // base H2) est partagé entre exécutions de tests dans la même JVM, un
        // identifiant fixe risquerait de croiser l'historique ou les cas
        // d'un autre test.
        accountId = "acc-e2e-" + UUID.randomUUID();
    }

    @Quand("je soumets une transaction de {string} EUR sur ce compte via l'API REST")
    public void jeSoumetsUneTransaction(String amount) throws Exception {
        String requestBody = objectMapper.writeValueAsString(
                new TransactionRequest(accountId, new BigDecimal(amount), "EUR"));

        MvcResult result = mockMvc.perform(post("/api/v1/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isAccepted())
                .andReturn();

        TransactionAccepted accepted = objectMapper.readValue(
                result.getResponse().getContentAsString(), TransactionAccepted.class);
        transactionId = accepted.transactionId();
    }

    @Alors("la transaction est acceptée")
    public void laTransactionEstAcceptee() {
        assertThat(transactionId).isNotBlank();
    }

    @Alors("un cas de fraude apparaît pour cette transaction dans la liste des cas de fraude")
    public void unCasDeFraudeApparait() {
        // Attente bornée : le message doit traverser "transactions" ->
        // topologie Kafka Streams -> "fraud-alerts" -> case-manager -> base H2
        // avant d'être visible via l'API. Délai généreux (60s) : lors de
        // l'exécution de la suite complète, plusieurs contextes Spring
        // restent actifs simultanément (cache de contexte de test), chacun
        // avec son propre broker Kafka embarqué ET sa propre instance Kafka
        // Streams (RocksDB, threads dédiés) - la contention induite peut
        // largement ralentir ce test par rapport à une exécution isolée.
        // Fenêtre de stabilité (during) : on s'assure que le cas reste
        // visible, pas seulement qu'il apparaît un court instant.
        await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(300))
                .during(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(findCase()).isPresent());
    }

    @Alors("ce cas mentionne la raison {string}")
    public void ceCasMentionneLaRaison(String reason) throws Exception {
        FraudCaseView view = findCase()
                .orElseThrow(() -> new AssertionError("Cas de fraude introuvable pour la transaction " + transactionId));

        assertThat(view.reasons()).extracting(Enum::name).contains(reason);
    }

    private Optional<FraudCaseView> findCase() throws Exception {
        MvcResult result = mockMvc.perform(get("/api/v1/fraud-cases")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token))
                .andExpect(status().isOk())
                .andReturn();

        List<FraudCaseView> cases = objectMapper.readValue(
                result.getResponse().getContentAsString(),
                objectMapper.getTypeFactory().constructCollectionType(List.class, FraudCaseView.class));

        return cases.stream()
                .filter(candidate -> transactionId.equals(candidate.transactionId()))
                .findFirst();
    }
}
