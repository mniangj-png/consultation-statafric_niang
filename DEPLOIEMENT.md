# Déploiement et ouverture de la consultation

**Version 2.0.0 — 16 novembre 2026.** Le dossier ne constitue pas une preuve de déploiement. Il faut un dépôt GitHub accessible au compte Streamlit, un projet Supabase autorisé et un fournisseur d'identité pour l'administration.

## 1. Sauvegarder l'existant

Exporter la base et noter les paramètres de l'application actuellement déployée. Fermer l'ancienne collecte pendant la migration. Ne supprimer aucune table ni aucune réponse. Conserver l'ancien code dans une branche ou une étiquette Git. Le script `sql/01_install_or_upgrade.sql` est prévu pour les tables `diag_*` de la version antérieure fournie ; une autre structure de base exige une vérification préalable.

Les réponses anciennes ne sont pas transformées en nouvelles réponses et ne reçoivent pas les réponses aux nouvelles questions. Les brouillons v1 sont refusés par la collecte v2. `scripts/migrate_legacy_draft.py ancien.json rapprochement.json` produit, hors ligne, un relevé de champs à relire et reporter manuellement ; il ne reconduit pas le consentement et ne soumet rien.

## 2. Déposer les fichiers dans GitHub

Extraire l'archive. À la racine du dépôt utilisé par Streamlit, placer au minimum :

```text
app.py
admin.py
relais_ins.py
requirements.txt
.streamlit/config.toml
```

Pour disposer des tests et outils, déposer également `src/`, `scripts/`, `tests/`, `sql/`, `config/`, `outils/`, `docs/`, `.github/`, `.gitignore`, les guides et `pytest.ini`.

**Ne pas déposer le ZIP à la place des fichiers.** Dans la branche effectivement déployée, remplacer intégralement l'ancien `app.py`. Les fichiers autonomes ne requièrent aucun module local `diagnostic`. Éviter un chemin supplémentaire involontaire tel que `mon_dossier/app.py` si le point d'entrée Streamlit est configuré sur `app.py`.

Ne jamais déposer `.streamlit/secrets.toml`, `private_config/`, les bases SQLite, les réponses, les contacts ou les exports réels. Le dépôt privé est préférable ; il ne remplace pas la protection des secrets. Une clé déjà publiée doit être révoquée, pas seulement supprimée du dernier commit.

## 3. Préparer la base Supabase

Utiliser un projet dédié dont le propriétaire, la région d'hébergement, les personnes autorisées et la conservation sont convenus avec STATAFRIC. Dans l'éditeur SQL :

1. Exécuter `sql/01_install_or_upgrade.sql` après sauvegarde. Il crée ou adapte les tables, ajoute le profil producteurs et les registres, protège les tables et limite l'écriture publique à des fonctions contrôlées.
2. Exécuter `sql/03_security_checks.sql`. Les droits de lecture directe des rôles `anon` et `authenticated` sur les réponses et contacts doivent être absents.
3. Exécuter `sql/04_transactional_test.sql` pour une recette technique : les données fictives sont créées dans une transaction terminée par `ROLLBACK`. Ce script doit réussir sur le projet de test avant la collecte réelle.

Ne pas ajouter de règle RLS autorisant « SELECT pour tous » pour résoudre une erreur. Ne pas donner la clé `service_role` à l'application de collecte.

### Générer le jeton de campagne

Sur votre poste, à la racine du dossier :

```bash
python scripts/generate_secrets.py --mode test
```

Le dossier privé `private_config/discours-2026-v2-test` contient un modèle de secrets, un SQL de création de campagne et un secret de cookie d'administration. Le jeton aléatoire est enregistré localement ; seule son empreinte est conservée dans la table des campagnes. Aucune base n'est modifiée par ce script.

Exécuter le fichier `campagne.sql` généré dans Supabase. La campagne de test est **fermée** par défaut. Ses bornes couvrent le 16 novembre–31 décembre 2026 ; ses réponses ont obligatoirement `is_test=true`.

Pour le réel, générer séparément :

```bash
python scripts/generate_secrets.py --mode reel
```

Créer ensuite cette campagne avec son **autre jeton**. Ses bornes sont le **19 novembre 00:00 UTC–26 novembre 23:59:59.999999 UTC**, limite technique choisie et à confirmer avant diffusion. Le SQL impose `is_test=false`. Ne pas transformer une campagne de test en campagne réelle.

## 4. Déployer la collecte

Dans Streamlit Community Cloud, créer ou modifier l'application avec : dépôt réel, branche réelle, fichier `app.py`, Python **3.12**. Les dépendances de `requirements.txt` sont installées par la plateforme.

Dans les secrets de cette application, copier le modèle généré ou `config/collecte_test.example.toml` et renseigner :

- `SUPABASE_URL` : URL HTTPS du projet, sans `/rest/v1` ;
- `SUPABASE_PUBLIC_KEY` : **clé anon publique historique JWT** du projet ;
- `WRITER_TOKEN` : jeton aléatoire correspondant à l'empreinte de la campagne ;
- `CAMPAIGN_ID`, `TEST_MODE` : identiques au mode de la campagne SQL ;
- `CONTACT_EMAIL`, `NOTICE_CONTROLLER`, `RETENTION_NOTICE` : valeurs autorisées ;
- `DEPLOYMENT_APPROVED` : `true` seulement après vérification de la configuration.

Cette version utilise la clé anon JWT dans `apikey` et `Authorization`. Ne pas substituer une nouvelle clé `sb_publishable_*` sans adapter et tester le mode d'authentification REST.

En mode réel, remplir `[notice]` dans toutes les langues autorisées : responsable de traitement opérationnel, hébergeur, localisation, accès, durée de conservation et contact. Après recette, renseigner `APPROVED_LANGUAGES` avec les seules versions linguistiques effectivement autorisées. La cible reste les cinq langues. Une langue non autorisée ne peut pas envoyer une réponse réelle ; son repli dans la même langue doit être prêt et relu.

**Ne jamais mettre `SUPABASE_ADMIN_KEY` dans ces secrets.** L'application le refuse. Sans secrets, elle reste en prévisualisation sans envoi.

## 5. Déployer l'administration privée

Créer une seconde application Streamlit pointant sur `admin.py`, même dépôt et même branche. Ne pas l'ajouter comme page publique du questionnaire. Utiliser `config/admin.example.toml` :

- `SUPABASE_ADMIN_KEY` : clé JWT **service_role**, uniquement ici ;
- `CAMPAIGN_ID` : campagne à administrer ;
- `TEST_MODE` : `true` pour la campagne de test, `false` pour la campagne réelle ;
- `COLLECTION_URL` : URL effective de la collecte ;
- `ADMIN_ALLOWED_EMAILS` : adresses Google précises des personnes habilitées.

Configurer un client OAuth/OIDC de type application Web chez Google Identity. Le fournisseur doit autoriser l'adresse de redirection exacte `https://VOTRE-ADMIN.streamlit.app/oauth2callback`. Compléter `[auth]` : `client_id`, `client_secret`, `cookie_secret` aléatoire, `redirect_uri` et `server_metadata_url`.

La connexion ne suffit pas : l'adresse doit être vérifiée et appartenir à la liste d'autorisation. Un compte différent doit être refusé avant lecture de la base. Les paramètres généraux TOML sont placés **avant** `[auth]`. L'exemple est prévu pour Google ; un autre fournisseur doit être testé, notamment sur `email_verified`.

L'application d'administration est en français, pour le consultant. Les textes des répondants restent dans leur langue d'origine. Restreindre également sa visibilité côté hébergeur quand cette fonction est disponible. Aucune valeur de secret ne doit apparaître dans les logs publics ou les captures partagées.

## 6. Ouvrir d'abord la campagne de test

Dans l'administration, « Préparation et campagne » permet d'afficher et d'ouvrir la campagne identifiée dans les secrets. Ou exécuter explicitement :

```sql
UPDATE public.diag_campaigns SET active=true WHERE id='discours-2026-v2-test';
```

Exécuter la recette décrite dans `docs/RECETTE.md`. Renseigner les 25 combinaisons ; tester l'accès non autorisé à l'administration, les envois, les réessais et les exports. Un accusé doit correspondre à une seule ligne en base. Redémarrer l'application et vérifier que les réponses transmises sont toujours présentes. Les brouillons non transmis ne sont pas sauvegardés automatiquement.

Les tests GitHub Actions s'exécutent à chaque commit. Vérifier l'onglet **Actions** ; la suite Streamlit s'y exécute seulement si les dépendances ont été installées. Un test vert de logique ne prouve ni l'accès Supabase ni la qualité de traduction.

## 7. Passer à la collecte réelle

Après autorisation et recette : créer la campagne réelle avec jeton distinct, renseigner les secrets réels de collecte et d'administration, autoriser les versions linguistiques relues, activer la campagne réelle et vérifier les bornes. Ne pas envoyer de réponse fictive dans cette campagne.

L'ouverture SQL n'annule pas les dates : une campagne active reste fermée avant le 19 novembre et après le 26 novembre. Une reprise identique d'un envoi déjà enregistré conserve son reçu, même après fermeture ; un nouvel envoi tardif est refusé sans réouverture explicite. Toute réception de complément par e-mail est horodatée et gérée séparément pour L3.

À partir de l'URL réelle de la collecte, les suffixes sont :

```text
?form=ins&lang=fr
?form=producteurs&lang=ar
?form=utilisateurs&lang=pt
?form=partenaires&lang=es
?form=abrege&lang=en
```

Le menu permet de choisir les cinq langues ; aucun pays ne détermine automatiquement la langue. Les identifiants `relay` et `inv` sont facultatifs, non secrets, sans droit d'accès ni réponse préremplie.

## 8. Relais INS et exploitation

`relais_ins.py` peut être une troisième application ou un outil local. Elle ne nécessite pas de base ni de clé Supabase. L'INS prépare séparément QP pour les producteurs et QU pour les utilisateurs, copie les messages dans sa messagerie et enregistre la **date réelle** d'envoi. Le CSV peut être sauvegardé, réimporté et transmis au consultant. La préparation d'un message ne vaut pas envoi.

Le consultant importe le relevé dans « Invitations et relais ». Le relais ne permet pas de lire les réponses. Les statuts sont rapprochés par le consultant ; aucune validation des réponses par l'INS n'est créée. Relances coordonnées les 23 et 25 ; contrôle interne de couverture le 24. Aucune réunion périodique STATAFRIC ajoutée.

## 9. Disponibilité et sauvegarde

La persistance Supabase est distincte de la disponibilité de Streamlit. Streamlit Community Cloud peut mettre en veille une application sans trafic ; la documentation indique 12 heures d'inactivité. Un projet Supabase gratuit peut également être suspendu en cas de faible activité sur sept jours. Aucun contournement artificiel de ces limites ni garantie de disponibilité continue n'est fourni.

Pour une exigence de service sans mise en veille, retenir un hébergement explicitement sans veille et un service de base adapté, après décision du Client. Le `Dockerfile` fourni permet un hébergement permanent choisi par l'administrateur ; il faut y ajouter HTTPS, supervision, gestion des secrets et stratégie de sauvegarde. Ce choix n'est pas un déploiement effectué.

Exporter et sauvegarder les réponses, contacts autorisés et registres à chaque journée de collecte selon les règles approuvées. Tester la restauration avant diffusion. Un redémarrage Streamlit ne doit pas servir de sauvegarde. Ne pas publier les archives analytiques : retirer les coordonnées ne rend pas les récits anonymes.

## Références techniques consultées

Documentation officielle, vérification technique du 16 novembre 2026 :
- https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/file-organization
- https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management
- https://docs.streamlit.io/develop/api-reference/user/st.login
- https://docs.streamlit.io/develop/concepts/architecture/widget-behavior
- https://docs.streamlit.io/deploy/streamlit-community-cloud/manage-your-app/app-settings
- https://supabase.com/docs/guides/database/postgres/row-level-security
- https://supabase.com/docs/guides/database/functions
- https://supabase.com/docs/guides/deployment/going-into-prod

Les procédures et options d'hébergement peuvent évoluer. Les exemples utilisent une architecture précise ; contrôler les intitulés dans les comptes effectifs.
