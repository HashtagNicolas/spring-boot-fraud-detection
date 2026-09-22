# language: fr
Fonctionnalité: Détection de fraude bout en bout

  Une transaction frauduleuse soumise via l'API REST doit, après avoir
  traversé Kafka (détecteur puis gestion des cas), apparaître dans la liste
  des cas de fraude exposée par l'API : transactions -> détecteur ->
  fraud-alerts -> case-manager -> base H2 -> GET /api/v1/fraud-cases.

  Scénario: Une transaction à montant élevé devient un cas de fraude visible
    Etant donné un nouveau compte bancaire
    Quand je soumets une transaction de "15000.00" EUR sur ce compte via l'API REST
    Alors la transaction est acceptée
    Et un cas de fraude apparaît pour cette transaction dans la liste des cas de fraude
    Et ce cas mentionne la raison "MONTANT_ELEVE"
