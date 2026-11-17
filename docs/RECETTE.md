# Recette avant diffusion

## Résultat des contrôles effectués dans l'environnement de préparation

Le 16 novembre 2026, la commande `python -m pytest -q` a exécuté **140 tests réussis** de logique, de validation, de sérialisation, d'exports, de concurrence SQLite et d'organisation des fichiers. **Un module a été ignoré**, car Streamlit n'a pas pu être installé dans cet environnement. Ce module contient les essais réels Streamlit AppTest, dont 25 parcours formulaire–langue.

Les fichiers `app.py`, `admin.py` et `relais_ins.py` ont été compilés ; aucun import local `diagnostic` ou `src` n'est nécessaire à leur lancement. Les cinq langues sont présentes dans le schéma et l'interface. Les dictionnaires, 25 formulaires de repli et huit modèles de registre ont été générés. Le classeur vierge a été calculé, contrôlé et rendu ; une ligne de test de formule a été retirée avant remise.

Ces contrôles ne constituent pas : un lancement effectif des applications Streamlit dans cet environnement ; une connexion Supabase ; un essai OIDC ; une recette sur téléphone ; une validation native de l'arabe ; ni une relecture linguistique indépendante. Aucune de ces opérations n'est déclarée accomplie.

## Tests automatisés fournis

- Conservation des codes, absence de valeurs présélectionnées, unicité des champs et cohérence des dépendances de filtres.
- Brouillons des 25 combinaisons, valeurs non latines, identifiant et langue.
- Consentement, refus, éligibilité des producteurs et absence de contacts dans l'export analytique.
- QI/QP sans nouvelle demande mais avec correction ; parcours sans chiffres ; origines administratives et unité comptée.
- QR03bis/QR04bis, priorités distinctes, sélections exclusives, dates arabes, longueurs de texte et données manquantes.
- Refus des brouillons d'une autre version ; contrôle d'un envoi en attente ; conservation du même reçu lors des réessais.
- Concurrence SQLite (même envoi en parallèle), conflit d'identifiant et interdiction des données réelles en local.
- Versions des registres, contrôle des dates, preuves de clôture, séparation L2/consultation, contrôle préalable d'autorisation de diffusion.
- Neutralisation des formules potentielles en CSV et signalement de la vague de réception.
- Vérifications statiques des droits SQL et absence de commande de suppression des tables.

Les essais Streamlit figurent dans `tests/test_streamlit.py` et sont déclenchés par GitHub Actions après installation des dépendances. Le résultat distant n'est pas disponible à la livraison. Toute erreur trouvée doit être corrigée et faire l'objet d'une nouvelle exécution ; ne pas remplacer un échec par un statut manuel « conforme ».

## Recette distante requise

### Collecte

Pour chaque combinaison questionnaire–langue : vérifier l'ouverture, le choix volontaire, les principaux sauts, les négations, l'absence de réponse par défaut, les priorités, une réponse partielle et une complète. Passer de FR à AR puis PT/ES/EN, vérifier la conservation des valeurs et de l'identifiant. Contrôler boutons, messages d'erreur, notices, aide et accusé dans la langue choisie.

Télécharger puis recharger un brouillon. Vérifier qu'aucune donnée de fond ne subsiste après refus. Vérifier QP non producteur, QI02=0/QI08, QP02=0/QP02d/QP08 et le filtre de QP05c. Relever la version exacte du code, du schéma et du catalogue.

### Arabe et téléphone

Tester sur ordinateur et téléphone, sans supposer que le CSS suffit. Vérifier ordre de lecture, curseur, liens et codes latins, dates, chiffres, retours à la ligne et exports UTF-8. Faire relire concepts et autorisations par une personne compétente. Les contrôles automatisés de présence ne prouvent pas l'équivalence linguistique.

### Stockage

Dans la campagne de test, soumettre une contribution, retrouver son reçu et la ligne de base. Recommencer le même envoi : une seule réponse. Interrompre le réseau et tester le fichier d'envoi en attente ; le message ne doit pas prétendre avoir enregistré sans reçu. Redémarrer l'application puis vérifier la persistance distante. Tester campagne fermée, mauvais jeton et discordance de TEST_MODE. Exécuter le SQL transactionnel fourni, sans données réelles.

### Administration

Un compte non connecté ou hors liste doit être refusé. Un compte autorisé doit voir seulement la campagne configurée. Vérifier contacts séparés, historique des contrôles, lien de doublon, version des registres et export complet. Tester une modification concurrente : la seconde version obsolète doit être refusée.

### Relais

Préparer une invitation producteur et une invitation utilisateur ; vérifier respectivement `form=producteurs` et `form=utilisateurs`. Le message est préparé, pas envoyé. La date réelle d'envoi reste à saisir. Sauvegarder et réimporter le CSV. Aucun lien de relais ne doit ouvrir les réponses du destinataire.

## Décision d'ouverture

Dans `Recette_25_combinaisons.csv` ou le registre, documenter date, version, vérificateur, résultat, anomalie, correction et preuve. Les 25 lignes livrées sont « À vérifier ». Un statut « Autorisation de diffusion donnée » nécessite la clôture des vérifications correspondantes. La liste `APPROVED_LANGUAGES` est mise à jour après ces décisions, pas par la seule génération du code.

Le consultant confirme les notices d'hébergement, l'administration, les campagnes séparées, les sauvegardes et la disponibilité. STATAFRIC relaie les liens dans le circuit convenu. Une version linguistique non prête requiert un formulaire de repli relu dans la même langue ; ne pas imposer FR/EN ni prétendre une disponibilité complète non obtenue.
