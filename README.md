# Diagnostic HISWACA — version 2.0.0

Dossier de code pour la méthodologie française du **15 novembre 2026**, fichier source `Rapport_methodologique_HISWACA_FR_revise_inclusif_20261115(3).docx`. Collecte prévue du **19 au 26 novembre 2026 inclus** ; période des pratiques **1er novembre 2025–31 octobre 2026**.

## Trois applications

| Fichier | Public | Fonction |
|---|---|---|
| `app.py` | Répondants | Cinq questionnaires, cinq langues, filtres, brouillon, confirmation d'enregistrement. |
| `admin.py` | Consultant et personnes explicitement habilitées | Contributions, décisions de contrôle, huit registres, exports et liens. Authentification Google OIDC et liste d'adresses autorisées en production. |
| `relais_ins.py` | Relais INS, facultatif | Préparer les invitations QP et QU et un relevé CSV. Aucun accès aux réponses ; aucun courriel envoyé par l'application. |

**Les trois fichiers sont autonomes : aucun dossier `diagnostic` n'est nécessaire au démarrage.** Le dossier `src` contient les sources éditables et les catalogues, pas une dépendance des fichiers déployés. Remplacer le contenu de l'ancien `app.py`, ne pas ajouter le nouveau code à sa fin.

## Premier essai

Avec Python 3.12, installer `requirements.txt`, puis lancer :

```bash
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

Sans secrets, l'application est en **prévisualisation**, sans envoi. Pour les essais locaux, copier `config/local_test.example.toml` vers `.streamlit/secrets.toml`. Toute réponse locale est alors fictive. Ne jamais déployer cette configuration sur un site public.

Pour l'administration locale :
```bash
python -m streamlit run admin.py --server.port 8502
```

## Déploiement GitHub

Lire **DEPLOIEMENT.md**. Le chemin Streamlit est `app.py` pour la collecte et `admin.py` pour l'administration, dans deux applications distinctes. Conserver `requirements.txt` à la racine. La troisième application peut être lancée localement ou déployée séparément. Ne créer aucun dossier `pages/` exposant l'administration dans la collecte publique.

Stockage retenu : Supabase/PostgreSQL. Le code et les secrets restent distincts ; les réponses ne sont pas dans GitHub. Les scripts SQL sont dans `sql/`. Commencer par une campagne de test distincte ; la campagne réelle est fermée par défaut. Aucun compte, projet distant ni lien public n'est créé par ce dossier.

## Outils fournis

`outils/` contient les 25 formulaires de repli UTF-8, le dictionnaire des cinq langues, les modèles CSV des huit registres, la matrice de préparation des 25 combinaisons et le journal de traduction. Le classeur vierge fourni sert à la tenue hors ligne ; les exports CSV de l'application restent la source de transfert. Aucun résultat de consultation réelle ou fictive n'est préchargé.

Les opérations GitHub et Streamlit ne mettent pas automatiquement les 25 cases de recette au statut « vérifié ». Les traductions sont rédigées ; leur relecture indépendante et les tests distants restent à documenter avant autorisation de diffusion.

## Périmètre et garde-fous

- QI pour les INS ; QP pour les autres producteurs ; QU pour les utilisateurs préparant les discours ; QR pour les partenaires ; RA pour les réponses abrégées.
- Français, anglais, arabe, portugais et espagnol pour les questionnaires. Kiswahili uniquement dans le registre de recherche documentaire.
- Chaque INS relaie QP à 3–8 producteurs distincts et QU à 4–6 équipes utilisatrices. Ce sont des cibles d'invitations, pas des quotas de réponses.
- Participation volontaire, sans réponse présélectionnée. Coordonnées facultatives séparées. Réponses brutes non écrasées.
- L2 exclusivement documentaire ; réponses réelles destinées à L3 ; tests et revue documentés pour L4.
- Pas de classement des pays, pas de score de maturité, pas d'extrapolation statistique ni de taux de réponse sans liste connue.
- Les brouillons ne sont pas sauvegardés automatiquement dans la base : les télécharger avant de quitter. Les reçus sont générés seulement après confirmation du stockage.

## Vérifications locales et reconstruction

```bash
python scripts/preflight.py
python -m pytest -q
python scripts/compile_catalogue.py
python scripts/build_standalone.py
python scripts/export_tools.py
```

Après modification du catalogue ou des sources, reconstruire les trois applications et exécuter les tests avant dépôt. Les changements d'instrument exigent une nouvelle version et un nouveau contrôle de traduction ; ne pas changer le sens d'une variable au milieu d'une collecte sans traçabilité.

Voir `docs/CONCORDANCE.md`, `docs/SECURITE.md`, `docs/RECETTE.md` et `docs/EXPLOITATION.md`.
