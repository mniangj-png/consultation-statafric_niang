# Recette avant diffusion des liens

**Toutes les données d’essai doivent être fictives et enregistrées avec `TEST_MODE=true`.** Un test logiciel ne constitue ni une consultation réelle ni un test d’utilité des futurs outils stratégiques auprès d’un INS.

Pour chaque ligne, consigner date, environnement, version du code, exécutant, résultat observé et correction éventuelle. Ne pas précocher une réussite sur la seule présence du test dans ce document.

| N° | Essai | Résultat attendu |
|---|---|---|
| 1 | Exécuter `pytest` avec Streamlit installé. | Aucune erreur ; les huit ouvertures de formulaire/langue et le refus de l’administration en prévisualisation sont exécutés, pas ignorés. |
| 2 | Exécuter les scripts SQL 03 et 04. | Droits restrictifs ; recette transactionnelle sans erreur et sans réponse persistante. |
| 3 | Ouvrir chacun des huit liens de formulaire/langue. | Bon titre, bonne langue, aucune réponse substantielle précochée. |
| 4 | Lire le parcours sur téléphone et ordinateur. | Textes et choix lisibles ; rubriques et boutons accessibles. |
| 5 | Répondre « Je ne participe pas ». | Fin du parcours ; aucun enregistrement dans la base. |
| 6 | INS : QI02a = 0. | QI03–QI07 sautées, QI08 conservée. |
| 7 | INS : QI03 = 2. | QI04 posée ; QI05–QI07 sautées. |
| 8 | INS : QI03 = 3 ou 98. | Passage à QI08 ; aucune donnée résiduelle de la demande. |
| 9 | Utilisateurs : QU02a = 0 ou QU03 = 0/98. | Passage à QU07 conformément à la source. |
| 10 | Remplir un cas puis changer la réponse qui le rend inapplicable. | Les anciennes réponses filtrées ne sont pas envoyées. |
| 11 | Laisser une question commandant un filtre vide. | Aucun faux code 0 ou NF ; état de filtre indéterminé dans l’export. |
| 12 | Mélanger « aucun » et une option de fond, choisir quatre besoins ou deux priorités identiques. | Messages de contrôle ; envoi bloqué tant que l’erreur persiste. |
| 13 | Dépasser une limite de mots, fournir un mois mal formaté ou un courriel invalide. | Contrôle explicite. Un mois hors période mais valide déclenche une alerte distincte. |
| 14 | Remplir partiellement, télécharger un brouillon, fermer puis réimporter. | Questionnaire, langue, réponses et identifiant retrouvés ; contacts protégés dans le fichier. |
| 15 | Passer de FR à EN en cours de réponse. | Codes et réponses conservés ; libellés traduits. |
| 16 | Laisser l’attribution vide puis choisir 1/2/3. | Défaut anonymisé sans réponse fabriquée ; habilitation demandée pour 1/2 ; accord préalable non présumé pour 3. |
| 17 | Envoyer une réponse abrégée. | Indicateur de réponse partielle ; aucune imputation des questions du formulaire complet. |
| 18 | Envoyer une contribution fictive complète avec contact. | Accusé avec référence et date ; données dans réponses et contact dans une table distincte. |
| 19 | Réessayer un même envoi ou double-cliquer. | Une seule contribution ; même référence. Un contenu modifié avec le même identifiant est refusé. |
| 20 | Redémarrer l’application de collecte et actualiser l’administration. | Réponses distantes conservées. |
| 21 | Simuler une indisponibilité du service sur un environnement de recette. | Pas de faux succès, envoi conservé pour réessai, aucun stockage local de repli. |
| 22 | Essayer une lecture des tables avec la seule clé publique. | Refus ; aucune réponse ni liste de contacts renvoyée. |
| 23 | Se connecter à l’administration avec un compte non habilité. | Accès refusé avant lecture des données. |
| 24 | Utiliser un compte habilité et examiner une réponse. | Statut « À examiner » initial ; ajout d’une décision sans modification de l’original. |
| 25 | Qualifier une réponse comme doublon. | Justification et lien requis ; les deux originaux demeurent traçables. |
| 26 | Exporter les données puis les contacts. | Contacts structurés absents de l’export analytique ; export de contacts séparé, volontaire et limité au filtre courant. |
| 27 | Importer les CSV dans Excel. | Accents et colonnes corrects, codes préservés, aucune formule reçue exécutée. |
| 28 | Fermer la campagne et tenter une nouvelle réponse. | Nouvelle réponse refusée ; un réessai strictement identique d’un envoi reçu retrouve son accusé. |
| 29 | Relire les deux langues avec les instruments français. | Sens, codes et filtres cohérents ; corrections consignées avant publication. |
| 30 | Vérifier notices, contacts et propriété des comptes. | Informations complétées, accès approuvés, sauvegarde et clôture définies. |

## Passage en production

N’autoriser la diffusion qu’après clôture des défauts bloquants, confirmation de l’hébergement et de la notice, enregistrement des résultats de recette et changement de `TEST_MODE` à `false`. Les tests restent marqués comme tels et masqués par défaut dans le suivi de collecte réelle.

Une réception technique ne constitue ni une validation des réponses ni une approbation institutionnelle. Toute publication reste soumise à un examen des permissions et du risque d’identification indirecte.
