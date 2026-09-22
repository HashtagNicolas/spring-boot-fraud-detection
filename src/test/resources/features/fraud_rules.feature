# language: fr
Fonctionnalité: Détection de fraude par règles

  Le moteur de règles évalue une transaction candidate au regard de
  l'historique récent du même compte et calcule un score explicite :
  - Montant élevé (> 10 000 €) : +50 points
  - Rafale (au moins 3 transactions en 5 minutes, candidate comprise) : +40 points
  Une transaction est frauduleuse si le score atteint 50 : un signal fort
  (montant élevé) suffit à lui seul, mais un signal modéré (rafale) est
  suspect sans suffire — il doit se combiner avec un autre pour franchir le seuil.

  Contexte:
    Etant donné le compte "acc-1"

  Scénario: Un montant élevé déclenche une fraude
    Etant donné une transaction de "15000.00" EUR sur le compte
    Quand j'évalue la transaction
    Alors le score doit être au moins 50
    Et la décision doit être une fraude
    Et la raison "MONTANT_ELEVE" doit être présente

  Scénario: Un montant normal ne déclenche pas de fraude
    Etant donné une transaction de "100.00" EUR sur le compte
    Quand j'évalue la transaction
    Alors la décision ne doit pas être une fraude

  Scénario: Une rafale seule est suspecte mais ne déclenche pas de fraude
    Etant donné un historique de 2 transactions de "50.00" EUR dans les 5 dernières minutes sur le compte
    Et une transaction de "50.00" EUR sur le compte
    Quand j'évalue la transaction
    Alors la raison "RAFALE" doit être présente
    Et la décision ne doit pas être une fraude

  Scénario: Moins de trois transactions ne déclenche pas de rafale
    Etant donné un historique de 1 transactions de "50.00" EUR dans les 5 dernières minutes sur le compte
    Et une transaction de "50.00" EUR sur le compte
    Quand j'évalue la transaction
    Alors la décision ne doit pas être une fraude

  Scénario: Montant élevé et rafale combinés déclenchent une fraude avec les deux raisons
    Etant donné un historique de 2 transactions de "50.00" EUR dans les 5 dernières minutes sur le compte
    Et une transaction de "15000.00" EUR sur le compte
    Quand j'évalue la transaction
    Alors la décision doit être une fraude
    Et la raison "MONTANT_ELEVE" doit être présente
    Et la raison "RAFALE" doit être présente
