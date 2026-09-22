# language: fr
Fonctionnalité: Règle de fraude "montant élevé"

  Le prédicat isHighAmount de FraudRuleEngine signale un montant strictement
  supérieur à 10 000 €. C'est la seule règle exprimée comme un prédicat pur
  sur une transaction isolée : la règle de rafale nécessite un état partagé
  entre transactions et est désormais assurée par une agrégation fenêtrée
  Kafka Streams (voir FraudDetectionTopologyTest et
  FraudDetectionTopologyIntegrationTest), pas par ce moteur de règles.

  Contexte:
    Etant donné le compte "acc-1"

  Scénario: Un montant élevé est signalé
    Etant donné une transaction de "15000.00" EUR sur le compte
    Quand j'évalue la transaction
    Alors le montant est jugé élevé

  Scénario: Un montant normal n'est pas signalé
    Etant donné une transaction de "100.00" EUR sur le compte
    Quand j'évalue la transaction
    Alors le montant n'est pas jugé élevé
