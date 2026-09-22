package com.hashtag.ngo.example.fraud.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hashtag.ngo.example.fraud.api.AuthRequest;
import com.hashtag.ngo.example.fraud.api.TransactionRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Vérifie la politique de sécurité stateless JWT : émission de jeton, accès
 * refusé (401, pas 403) sans jeton sur les endpoints métier, accès autorisé
 * avec un jeton valide.
 */
@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka
class SecurityIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void obtainingATokenWithDemoCredentialsSucceeds() throws Exception {
        mockMvc.perform(post("/auth/token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AuthRequest("demo", "demo123"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token").isNotEmpty());
    }

    @Test
    void submittingATransactionWithoutATokenIsUnauthorized() throws Exception {
        mockMvc.perform(post("/api/v1/transactions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validTransactionRequestJson()))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void submittingATransactionWithAValidTokenIsAccepted() throws Exception {
        mockMvc.perform(post("/api/v1/transactions")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + obtainToken())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validTransactionRequestJson()))
                .andExpect(status().isAccepted());
    }

    @Test
    void listingFraudCasesWithoutATokenIsUnauthorized() throws Exception {
        mockMvc.perform(get("/api/v1/fraud-cases"))
                .andExpect(status().isUnauthorized());
    }

    private String obtainToken() throws Exception {
        MvcResult result = mockMvc.perform(post("/auth/token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(new AuthRequest("demo", "demo123"))))
                .andExpect(status().isOk())
                .andReturn();

        String token = objectMapper.readTree(result.getResponse().getContentAsString()).get("token").asText();
        assertThat(token).isNotBlank();
        return token;
    }

    private String validTransactionRequestJson() throws Exception {
        return objectMapper.writeValueAsString(
                new TransactionRequest("acc-security-" + System.nanoTime(), new BigDecimal("42.00"), "EUR"));
    }
}
