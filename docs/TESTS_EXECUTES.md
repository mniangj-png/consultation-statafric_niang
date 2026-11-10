# Vérifications effectuées et contrôles restant à exécuter

## Résultat de la construction

Version du dossier : **1.0.0**, instruments `2026-11-08.web-1`.

Environnement utilisé pour les vérifications exécutées : **Python 3.13.5**, sans bibliothèque Streamlit installée et sans accès à un projet Supabase de la mission.

Commandes réellement exécutées :

```text
python -m compileall -q .
python -m pytest -q --junitxml=...
```

Résultat : **126 tests réussis, 1 module ignoré**. Aucune réponse réelle n’a été utilisée. Le module ignoré est `tests/test_app_streamlit.py` : il contient les tests `AppTest` à exécuter avec Streamlit installé. Son absence ne permet pas de déclarer la navigation réelle vérifiée.

## Couverture des tests exécutés

| Domaine | Contrôles exécutés |
|---|---|
| Schéma | Rubriques, unicité des champs, codes, libellés dans les deux langues, dépendances des filtres et séparation des contacts. |
| Parcours | QI02 sans demande, QI03 sans figures ou sans cas connu, QU02/QU03 sans discours/cas, maintien de QI08. |
| Manquants | Zéro, NSP, NA, blanc à contrôler et filtre indéterminé distingués. |
| Contrôles | Choix exclusifs, limites de sélection, priorités dupliquées ou discontinues, limites de mots, mois et courriels. |
| Brouillons | Aller-retour des quatre formulaires dans les deux langues, formats invalides, taille excessive, champs inconnus et identifiants invalides. |
| Confidentialité | Contacts exclus du contenu analytique et des accusés ; permission par défaut distincte d’un choix explicite. |
| Stockage local | Enregistrement, refus de données non marquées comme tests, réessai identique, refus de modification et conservation des décisions. |
| Concurrence locale | Seize essais simultanés/rapprochés du même envoi par huit fils d’exécution : une contribution enregistrée. |
| Accès distant simulé | Erreur réseau, erreur distante expurgée, absence d’accusé valide, interdiction de lecture publique, pagination et absence de redirection. |
| Exports | En-têtes compatibles avec les registres sources, format long des choix multiples, codes préservés, texte original dans JSON, protection des CSV contre les formules. |
| Configuration | Mode de prévisualisation par défaut, secrets manquants refusés, clé d’administration interdite dans la collecte, URL Supabase restreinte et approbation explicite. |
| Scripts | Génération privée des jetons, concordance jeton/empreinte, absence de jeton dans la sortie console, refus d’écrasement et validité TOML. |
| SQL statique | Présence des restrictions de droits, sécurités par lignes, fonctions limitées et transactions prévues. Aucun test sur moteur PostgreSQL n’est revendiqué. |

## Vérifications non exécutées dans cet environnement

- Installation et lancement effectif de Streamlit 1.62.0.
- Neuf cas `AppTest` : huit combinaisons formulaire/langue et un refus d’accès administrateur.
- Exécution des scripts SQL dans Supabase et appel réel des fonctions de collecte.
- Authentification OIDC réelle et contrôle du fournisseur d’identité.
- Navigation sur navigateur et téléphone ; interruptions et reprises réelles de session.
- Persistance après redémarrage de Streamlit Community Cloud.
- Sauvegarde/restauration distante et contrôle des droits depuis un navigateur non habilité.

Le workflow GitHub Actions exécute les tests avec les dépendances installées sur Python 3.11 et 3.12 ; il est fourni, mais son exécution sur un dépôt distant n’est pas présumée. La recette SQL et la liste de vérification web sont fournies pour fermer ces points avant diffusion.

**Conclusion :** le code de logique et ses contrôles locaux ont passé les vérifications décrites. Le dossier doit encore être configuré et réceptionné dans l’environnement réel. Il ne constitue ni un déploiement déjà effectué ni un audit de sécurité externe.
