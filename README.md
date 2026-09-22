# Détection de fraude en temps réel

Application Spring Boot / Apache Kafka de démonstration : une API REST
enregistre des transactions bancaires, qui transitent par Kafka et sont
analysées en temps réel par un moteur de règles pour détecter des fraudes.

## Sommaire

- [Présentation](#présentation)
- [Architecture](#architecture)
- [Règles de fraude](#règles-de-fraude)
- [Stack technique](#stack-technique)
- [Démarrer le projet](#démarrer-le-projet)
- [Tester](#tester)
- [Endpoints REST](#endpoints-rest)
- [Sécurité](#sécurité)
- [Robustesse](#robustesse)
- [Étapes de construction du projet](#étapes-de-construction-du-projet)
- [Intégration continue](#intégration-continue)

## Présentation

Dans un système bancaire, chaque transaction (paiement, virement, retrait...)
est un candidat potentiel à la fraude : montant anormalement élevé, rafale de
petites opérations suspectes sur un même compte, etc. Détecter ces signaux
**en temps réel**, au fil de l'eau, plutôt qu'en traitement batch nocturne,
permet de réagir avant que le préjudice ne s'aggrave (bloquer un compte,
alerter un analyste...).

Ce projet illustre une architecture **event-driven** typique pour ce besoin :
une API encaisse les transactions, les publie sur un flux Kafka, et des
consommateurs indépendants analysent ce flux, publient des alertes, notifient
et archivent les cas détectés - le tout de façon découplée et résiliente aux
pannes transitoires.

## Architecture

```
                    POST /auth/token (demo/demo123) → JWT
                                    │
                         POST /api/v1/transactions
                         Authorization: Bearer <JWT>
                                    │
                                    ▼
                        ┌───────────────────────┐
                        │  TransactionController │  (api)
                        └───────────┬───────────┘
                                    │ TransactionService (bean/bean.impl)
                                    ▼
                    ┌───────────────────────────────┐
                    │   topic "transactions"         │  3 partitions
                    │   clé = accountId               │
                    └───────────────┬───────────────┘
                                    │ groupId = fraud-detector
                                    ▼
                        ┌───────────────────────┐
                        │ FraudDetectionListener │  (listener)
                        │  → FraudRuleEngine     │  montant élevé, rafale
                        └───────────┬───────────┘
                                    │ si score de fraude atteint
                                    ▼
                    ┌───────────────────────────────┐
                    │   topic "fraud-alerts"         │  3 partitions
                    │   clé = accountId               │
                    └───────┬───────────────┬───────┘
                 groupId=notifier      groupId=case-manager
                            │                 │
                            ▼                 ▼
               ┌────────────────────┐  ┌─────────────────────────┐
               │ NotificationListener│  │  CaseManagementListener  │
               │  (log simulé :      │  │  → FraudCaseService      │
               │   email/SMS/push)   │  │  → FraudAlertRepository  │
               └────────────────────┘  └────────────┬─────────────┘
                                                      ▼
                                              Base H2 (fraudDb)
                                                      ▲
                                                      │ FraudCaseService
                                       GET /api/v1/fraud-cases
                                        (FraudCaseController)

  Échec de traitement (ex. donnée invalide) : 3 tentatives, backoff 1 s,
  puis dead-letter → topic "fraud-alerts.DLT" (1 partition, partagé par
  tous les listeners).
```

### Composants

| Composant                  | Rôle                                                                 |
|-----------------------------|-----------------------------------------------------------------------|
| `TransactionController`     | Reçoit les transactions via REST, valide, délègue au service          |
| `TransactionService`        | Génère id/horodatage, publie sur `transactions` (clé = accountId)     |
| `FraudRuleEngine`           | Logique pure de scoring (aucune dépendance Kafka/JPA)                 |
| `FraudDetectionListener`    | Consomme `transactions`, évalue via le moteur, publie une alerte       |
| `NotificationListener`      | Consomme `fraud-alerts`, simule une notification (log)                |
| `CaseManagementListener`    | Consomme `fraud-alerts`, persiste le cas (idempotent)                 |
| `FraudCaseController`       | Expose les cas de fraude persistés en lecture (authentification requise)   |
| `AuthController`            | `POST /auth/token` : émet un JWT pour le compte de démonstration          |
| `JwtService`                | Génère et valide les JWT (HMAC)                                        |
| `JwtAuthenticationFilter` / `SecurityConfig` | Vérifient le jeton sur chaque requête `/api/v1/**` (stateless) |

Les groupes de consommateurs `notifier` et `case-manager` réalisent un
**fan-out** : chaque alerte de fraude est livrée intégralement aux deux
services, indépendamment l'un de l'autre (chacun a son propre offset de
consommation).

### Topics Kafka

| Topic                | Partitions | Clé         | Producteur              | Consommateurs (groupId)              |
|-----------------------|-----------:|-------------|--------------------------|----------------------------------------|
| `transactions`        | 3          | accountId   | API REST                 | `fraud-detector`                       |
| `fraud-alerts`        | 3          | accountId   | `FraudDetectionListener` | `notifier`, `case-manager`             |
| `fraud-alerts.DLT`    | 1          | (aucune, -1)| gestion d'erreur Kafka   | -                                       |

La clé de partition des transactions est **`accountId`** (et non l'id de la
transaction) afin que Kafka garantisse l'ordre des événements d'un même
compte au sein d'une partition - indispensable pour la règle de rafale.

## Règles de fraude

Le moteur de règles (`FraudRuleEngineImpl`) calcule un score **explicite**
(somme des points des règles déclenchées) à partir de la transaction évaluée
et de l'historique récent du compte :

| Règle          | Condition                                                        | Points |
|-----------------|-------------------------------------------------------------------|-------:|
| Montant élevé   | Montant strictement supérieur à 10 000 €                          | +50    |
| Rafale          | Au moins 3 transactions (candidate comprise) en 5 minutes glissantes, même compte | +40    |

**Seuil de fraude : score ≥ 50.**

Ce seuil est volontairement fixé à 50 et non à 40 ou 90 : un signal fort
(montant élevé) suffit **à lui seul** à qualifier une fraude, tandis qu'un
signal modéré (rafale) est *suspect mais insuffisant seul* - il doit se
combiner avec un autre signal pour franchir le seuil. C'est ce mécanisme
d'**accumulation de points** qui distingue ce moteur d'une simple liste de
règles booléennes indépendantes.

La fenêtre de rafale est fermée `[candidate - 5min, candidate]` : une
transaction vieille d'exactement 5 minutes compte encore.

## Stack technique

- **Java 25**
- **Spring Boot 4.1** (`spring-boot-starter-web`, `-validation`, `-data-jpa`, `-actuator`, `-security`)
- **Spring Kafka 4.1** / **Apache Kafka clients 4.2**
- **JJWT 0.12** (génération/validation des JWT, HMAC)
- **H2** (base embarquée, persistance des cas de fraude)
- **Jackson** (JSON, avec le module `jsr310` pour les types `java.time`)
- **JUnit 5**, **AssertJ**, **spring-kafka-test** (`@EmbeddedKafka`)
- **Cucumber 7** (`cucumber-java`, `cucumber-spring`) pour les tests BDD
- **ArchUnit 1.4** pour les règles d'architecture
- **Maven** (build), **GitHub Actions** (CI)

## Démarrer le projet

### 1. Démarrer Kafka (Docker, mode KRaft)

Un unique broker Kafka en mode KRaft (sans ZooKeeper) est fourni via
`docker-compose.yml`, exposé sur `localhost:9092` :

```bash
docker compose up -d
```

### 2. Démarrer l'application

```bash
mvn clean package -DskipTests
java -jar target/fraud-detection-0.1.0-SNAPSHOT.jar
```

ou directement avec le plugin Maven :

```bash
mvn spring-boot:run
```

Au démarrage, l'application crée automatiquement les topics `transactions`,
`fraud-alerts` et `fraud-alerts.DLT` s'ils n'existent pas déjà. Vérifier que
tout est démarré via l'endpoint de santé :

```bash
curl http://localhost:8080/actuator/health
```

### 3. Arrêter Kafka

```bash
docker compose down
```

## Tester

```bash
mvn clean test
```

Le projet **ne nécessite pas Docker pour ses tests** : chaque test
d'intégration Kafka démarre son propre broker embarqué dans la JVM de test
via `@EmbeddedKafka` (spring-kafka-test), qui crée un cluster Kafka
éphémère en mémoire pour la durée du test.

La suite couvre :

- des tests unitaires purs (moteur de règles, sans Spring ni Kafka) ;
- des tests d'intégration `@EmbeddedKafka` par composant (producteur,
  détecteur, notification, gestion des cas, dead-letter) ;
- des scénarios **Cucumber** (BDD, en français) : règles de fraude
  (`fraud_rules.feature`) et un scénario **bout en bout**
  (`fraud_detection_e2e.feature`) qui exerce la chaîne complète API → Kafka →
  base de données → API, via `@CucumberContextConfiguration` ;
- des règles **ArchUnit** vérifiant le respect des couches
  (`api`/`bean`/`entity`/`repository`/`listener`/`config`/`security`) et la
  convention interface/implémentation (`*Impl` dans un package `.impl`).

## Endpoints REST

Tous les endpoints `/api/v1/**` nécessitent un jeton JWT (voir
[Sécurité](#sécurité)). Récupérer d'abord un jeton :

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/auth/token \
  -H "Content-Type: application/json" \
  -d '{ "username": "demo", "password": "demo123" }' \
  | jq -r .token)
```

### Soumettre une transaction

```bash
curl -X POST http://localhost:8080/api/v1/transactions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "accountId": "acc-123",
        "amount": 15000.00,
        "currency": "EUR"
      }'
```

Réponse `202 Accepted` (id et horodatage générés côté serveur) :

```json
{ "transactionId": "5f2a9e2e-1234-4b8e-9c1a-abcdef123456" }
```

Une requête invalide (`accountId` vide, `amount` négatif ou nul...) renvoie
`400 Bad Request` avec le détail des champs en erreur. Une requête sans
jeton (ou avec un jeton invalide/expiré) renvoie `401 Unauthorized`.

### Consulter les cas de fraude détectés

```bash
curl http://localhost:8080/api/v1/fraud-cases \
  -H "Authorization: Bearer $TOKEN"
```

```json
[
  {
    "id": 1,
    "transactionId": "5f2a9e2e-1234-4b8e-9c1a-abcdef123456",
    "accountId": "acc-123",
    "score": 50,
    "reasons": ["MONTANT_ELEVE"],
    "detectedAt": "2026-09-22T20:12:00Z"
  }
]
```

> Cet endpoint reste sans autorisation fine (tout utilisateur authentifié y
> a accès, quel que soit son rôle) : suffisant pour cette démonstration, mais
> une vraie mise en production distinguerait les rôles (ex. "analyste
> fraude") avant d'exposer ces données sensibles.

## Sécurité

L'API est protégée par une authentification **JWT stateless** : aucune
session HTTP, aucun cookie - chaque requête doit porter son propre jeton, ce
qui permet de faire passer à l'échelle horizontalement l'application sans
partager d'état de session entre instances.

- `POST /auth/token` est **ouvert** : il échange des identifiants de
  démonstration (`demo` / `demo123` - il n'y a pas de base d'utilisateurs
  derrière) contre un JWT signé (HMAC), valable 1 heure par défaut.
- Toute requête vers `/api/v1/**` doit porter l'en-tête
  `Authorization: Bearer <jeton>`. `JwtAuthenticationFilter` vérifie la
  signature et l'expiration du jeton avant de peupler le contexte de
  sécurité ; en son absence ou s'il est invalide, la requête est rejetée
  avec **`401 Unauthorized`** (et non `403 Forbidden`, le comportement par
  défaut de Spring Security en l'absence d'authentification) - configuré via
  `HttpStatusEntryPoint(UNAUTHORIZED)` dans `SecurityConfig`, pour rester
  cohérent avec la sémantique HTTP ("non authentifié" vs "authentifié mais
  non autorisé").
- CSRF est désactivé : cette protection n'a de sens que pour une
  authentification par cookie de session, pas pour un jeton porté
  explicitement dans un en-tête par un client non-navigateur.

Le secret HMAC de signature est externalisé dans `application.yml`
(`app.security.jwt.secret`) :

```yaml
app:
  security:
    jwt:
      secret: "changeit-ceci-est-un-secret-de-demonstration-a-remplacer-en-production-..."
      expiration-minutes: 60
```

> **À changer en production** : ce secret est un exemple en clair dans le
> dépôt, acceptable uniquement pour une démonstration locale. En production,
> il doit provenir d'une variable d'environnement ou d'un coffre-fort de
> secrets (Vault, AWS Secrets Manager...), jamais être committé en clair.

## Robustesse

- **Retries + dead-letter** : un `DefaultErrorHandler` (Spring Kafka) est
  appliqué à tous les `@KafkaListener` de l'application. Un message dont le
  traitement échoue est retenté 3 fois avec un backoff fixe d'1 seconde ;
  au-delà, il est publié sur le topic de dead-letter partagé
  `fraud-alerts.DLT` (`DeadLetterPublishingRecoverer`) plutôt que de bloquer
  indéfiniment sa partition d'origine.
- **Idempotence** : Kafka garantit une sémantique **au moins une fois** - un
  redémarrage de consommateur avant validation d'offset peut redélivrer un
  message déjà traité. `CaseManagementListener` vérifie donc l'existence
  préalable d'une alerte par `transactionId` avant de la persister, pour
  qu'une redélivraison n'insère jamais de doublon.

## Étapes de construction du projet

Le projet a été construit incrémentalement, chaque étape restant verte
(`mvn clean test`) avant de passer à la suivante :

| Étape | Contenu |
|-------|---------|
| **P1** | Squelette Spring Boot 4.1 + Kafka : topics, config producteur/consommateur minimale, test `@EmbeddedKafka` de démarrage |
| **P2** | Producteur : API REST → topic `transactions`, validation, clé de partition = accountId, gestion d'erreur |
| **P3** | Moteur de règles de fraude (logique pure), développé en TDD avec des scénarios Cucumber |
| **P4** | Consommateur détecteur : `FraudDetectionListener`, historique par compte thread-safe, publication d'alertes |
| **P5** | Notification + gestion des cas : consommateurs `fraud-alerts` en fan-out, persistance H2, endpoint de consultation |
| **P6** | Robustesse : retries, dead-letter topic, idempotence du case-manager |
| **P7** | Tests E2E Cucumber (bout en bout) et règles d'architecture ArchUnit |
| **P8** | Documentation (ce README) et `docker-compose.yml` pour Kafka en local |
| **P9** | Sécurité : authentification JWT stateless sur `/api/v1/**`, `POST /auth/token` |

## Intégration continue

Un workflow GitHub Actions (`.github/workflows/ci.yml`) exécute `mvn clean
test` sur chaque push et pull request vers `main`, avec JDK 25 (Temurin).
Aucun service Docker n'est nécessaire en CI : les tests d'intégration Kafka
utilisent un broker embarqué (`@EmbeddedKafka`), pas le `docker-compose.yml`
(celui-ci est réservé au développement local).
