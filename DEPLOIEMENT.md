# Déployer les applications HISWACA

## 1. Résultat attendu

À l’issue des opérations, vous disposerez d’une URL de collecte pour les répondants et d’une URL d’administration distincte, réservée aux personnes autorisées. Une seule application de collecte distribue les quatre questionnaires et leurs deux langues. Il n’est pas nécessaire de créer huit applications.

Les comptes GitHub, Streamlit et Supabase ainsi que la configuration d’identité restent à créer ou à choisir par les personnes habilitées. Utiliser des comptes institutionnels lorsque cela est possible. Les conditions d’hébergement, d’accès et de conservation doivent être convenues avec STATAFRIC avant toute réponse réelle.

## 2. Mettre le code dans GitHub

1. Décompresser l’archive et créer un dépôt, de préférence privé si les habilitations et le service retenu le permettent.
2. Déposer le **contenu** du dossier à la racine : `app.py`, `admin.py`, `requirements.txt`, `diagnostic/`, `sql/`, `docs/`, `scripts/`, `tests/`, `.streamlit/config.toml` et `.github/workflows/tests.yml`.
3. Vérifier la présence des dossiers commençant par un point, parfois masqués par l’explorateur de fichiers.
4. Ne déposer ni `.streamlit/secrets.toml`, ni fichier `.private.*`, ni contrat, questionnaire rempli, export, base SQLite ou fichier de contacts. Le `.gitignore` est un garde-fou ; il ne retire pas un secret déjà publié.
5. Laisser GitHub Actions exécuter les tests. Examiner toute erreur avant de poursuivre. L’installation des dépendances requiert un accès réseau aux paquets Python.

Le dossier ne contient aucun secret réel. Les modèles de secrets portent des valeurs `A_COMPLETER` ou descriptives, à remplacer uniquement dans un fichier local privé ou dans l’interface des secrets du service d’hébergement.

## 3. Préparer la base Supabase

### 3.1 Projet dédié

Créer un projet Supabase dédié à la mission. Choisir sa région et ses modalités de sauvegarde selon les décisions institutionnelles. Noter dans un espace sécurisé : l’URL HTTPS du projet, la clé publique `publishable` ou `anon`, et la clé serveur `secret` ou `service_role`.

La clé serveur donne des pouvoirs importants. **Elle est réservée à l’application d’administration.** L’application de collecte utilise uniquement une clé publique plus un jeton serveur limité aux fonctions de dépôt.

### 3.2 Installer les tables et les fonctions

Dans le SQL Editor du projet, exécuter intégralement `sql/01_schema.sql`. Il crée quatre tables : campagne, réponses, contacts et décisions de contrôle. Il active la sécurité par lignes, retire les accès directs aux rôles publics et crée deux fonctions limitées : lire l’état de campagne et déposer une réponse.

Exécuter ensuite `sql/03_security_checks.sql`. Résultats attendus : quatre sécurités par lignes activées, aucun droit de table ni règle permissive pour `anon` ou `authenticated`, et deux fonctions avec un chemin de recherche fixé à vide.

Exécuter `sql/04_recette_transactionnelle.sql` en entier. Il utilise des données fictives et termine par `ROLLBACK`. Il doit produire le message `RECETTE SQL RÉUSSIE`. En cas d’erreur, exécuter `ROLLBACK`, corriger et rejouer. Ce script n’a pas été exécuté sur un serveur distant lors de la préparation du dossier.

### 3.3 Créer les secrets de campagne

Sur un ordinateur de confiance, depuis la racine du projet :

```bash
python scripts/generate_secrets.py
```

Pour ajouter un code d’accès facultatif à communiquer séparément aux destinataires :

```bash
python scripts/generate_secrets.py --access-code
```

Choisir une seule des deux commandes. Le script refuse de remplacer des fichiers existants. Il crée, dans `.local/` :

- `collecte.secrets.private.toml`, avec le jeton serveur généré ;
- `campaign.private.sql`, avec l’empreinte du même jeton ;
- `oidc_cookie.private.txt`, secret distinct pour l’authentification ;
- éventuellement `invitation_code.private.txt`, code destiné aux répondants.

Ne jamais transmettre le jeton serveur aux répondants. Ne pas le mettre dans une URL. Ne pas déposer ces fichiers dans GitHub. Sous Windows, vérifier les permissions du dossier ; une restriction Unix `chmod` ne suffit pas à garantir les permissions Windows.

Exécuter `campaign.private.sql` dans le SQL Editor. La campagne `discours-2026` est créée **fermée**. La borne de début est le 10 novembre 2026 et la borne de fin proposée le 14 décembre 2026 à 23:59:59 UTC. Cette dernière est une fermeture technique modifiable, **pas** la date souhaitée de retour des questionnaires, fixée au 17 novembre. Les compléments du 24 et du 25 novembre restent possibles.

Ne pas régénérer le jeton sans mettre à jour simultanément les secrets de collecte et l’empreinte en base. Ne pas réexécuter le SQL de création sur une campagne existante sans examiner son état.

## 4. Déployer l’application de collecte

Dans Streamlit Community Cloud, créer une application reliée au dépôt GitHub. Sélectionner la branche voulue et **`app.py` comme fichier d’entrée**. Choisir Python **3.12** dans les paramètres avancés. Les bibliothèques sont définies dans `requirements.txt`.

Dans les secrets de cette application, coller le contenu de `collecte.secrets.private.toml`, puis compléter :

| Paramètre | Valeur à renseigner |
|---|---|
| `SUPABASE_URL` | URL réelle du projet, sous la forme `https://...supabase.co`. |
| `SUPABASE_PUBLIC_KEY` | Clé publique Supabase, pas la clé serveur. |
| `WRITER_TOKEN` | Jeton généré correspondant à l’empreinte enregistrée en base. |
| `CONTACT_EMAIL` | Adresse institutionnelle/générique réellement suivie pour la mission. |
| `RETENTION_NOTICE` | Information convenue sur hébergement, accès, conservation, suppression et contact. Prévoir aussi l’anglais. |
| `DEPLOYMENT_APPROVED` | `true` seulement après accord sur les dispositions de collecte. |
| `TEST_MODE` | Conserver `true` pendant toute la recette. |

**Ne jamais ajouter `SUPABASE_ADMIN_KEY` ici.** Le programme refuse une configuration de collecte de production contenant cette clé.

L’application peut être publique pour permettre aux parties prenantes de répondre sans compte. Un code de campagne facultatif réduit les réponses hors cible ; ce code ne constitue ni une identification individuelle ni une preuve de mandat.

Noter l’URL réelle attribuée par Streamlit. Aucun lien proposé dans les exemples du dossier n’est une URL déployée.

## 5. Déployer l’administration

Créer une seconde application Streamlit depuis le même dépôt et la même branche, avec **`admin.py` comme fichier d’entrée**. Utiliser Python 3.12. Restreindre en plus les visiteurs dans les paramètres de partage lorsque le service le permet.

### 5.1 Configurer l’identité

L’administration utilise OpenID Connect, puis une liste explicite de courriels autorisés. L’exemple fourni utilise Google Identity. Dans le compte d’administration du fournisseur :

1. Créer un client OAuth/OIDC de type application web.
2. Enregistrer exactement l’URL de retour de l’administration : `https://VOTRE-ADMIN.streamlit.app/oauth2callback`.
3. Configurer les utilisateurs autorisés et l’écran de consentement selon le mode retenu ; en mode de test du fournisseur, inscrire les personnes habilitées comme testeurs.
4. Obtenir le `client_id` et le `client_secret`.

Le code exige les attributs `email`, `email_verified` et `exp`. Il refuse l’accès si l’adresse n’est pas dans la liste, si elle n’est pas vérifiée ou si le jeton est expiré. Un autre fournisseur peut être utilisé, mais sa correspondance d’attributs doit être vérifiée avant déploiement.

### 5.2 Secrets séparés

Copier `docs/admin.secrets.example.toml` dans les secrets **de l’administration uniquement**, puis remplacer les valeurs : URL Supabase, clé serveur, liste des courriels habilités, URL réelle de collecte et paramètres `[auth]`. Utiliser le secret de `oidc_cookie.private.txt` pour `cookie_secret`.

Aucune clé serveur n’apparaît dans un formulaire ou dans un lien transmis aux répondants. Activer l’authentification multifacteur du compte administrateur auprès du fournisseur lorsque disponible.

Se connecter avec un compte autorisé, puis vérifier qu’un autre compte est refusé. Une connexion Google réussie ne suffit pas à autoriser l’accès aux réponses.

## 6. Faire la recette avant diffusion

Dans le SQL Editor, ouvrir la campagne tout en gardant `TEST_MODE=true` dans les secrets de collecte :

```sql
UPDATE public.diag_campaigns
SET active=true
WHERE id='discours-2026';
```

Les bornes de dates restent applicables. Pour un essai hors période, les modifier temporairement dans un projet de recette et rétablir les dates convenues avant diffusion ; ne pas changer la période statistique des questionnaires.

Exécuter la liste complète de `docs/RECETTE.md`. Vérifier les quatre parcours, les deux langues, les filtres, les retours en arrière, les brouillons, les accusés, l’administration, les droits d’accès et les exports. Redémarrer l’application puis retrouver les réponses fictives dans la base.

Les tests locaux fournis ne dispensent pas de cette recette. Ne pas envoyer les liens aux parties prenantes tant que ces opérations n’ont pas abouti.

## 7. Passer à la collecte réelle

Une fois la recette consignée et les dispositions approuvées :

1. Conserver les réponses fictives avec `is_test=true`, sans les intégrer au diagnostic.
2. Dans les secrets de collecte, mettre `TEST_MODE=false`, sans changer les identifiants de campagne.
3. Vérifier que la campagne est ouverte, que la notice et le contact sont corrects et que le bandeau de test a disparu.
4. Ne pas utiliser une vraie réponse pour tester un envoi. Conserver les tests techniques identifiés comme tels.
5. Dans l’administration, ouvrir « Liens à transmettre aux parties prenantes » et copier les liens générés à partir de l’URL réelle.

Exemples de suffixes :

```text
/?form=ins&lang=fr
/?form=ins&lang=en
/?form=utilisateurs&lang=fr
/?form=utilisateurs&lang=en
/?form=partenaires&lang=fr
/?form=partenaires&lang=en
/?form=abrege&lang=fr
/?form=abrege&lang=en
```

Ne jamais envoyer l’URL d’administration ou une clé d’accès serveur dans les invitations. Le formulaire abrégé est un outil de relance ; il ne remplace pas automatiquement un formulaire complet déjà reçu.

## 8. Suivre et exporter

Dans l’administration, cliquer sur « Charger ou actualiser les réponses ». Les tests sont masqués par défaut. Les compteurs indiquent des contributions, et non des INS distincts.

Qualifier les contributions et les doublons en conservant les originaux. Télécharger régulièrement un export analytique dans un espace de travail restreint. Télécharger les contacts séparément uniquement lorsque le suivi l’exige. Les sauvegardes de la base et leur restauration relèvent également de la procédure d’hébergement retenue ; les exports ponctuels ne la remplacent pas.

Importer les CSV dans Excel en UTF-8, séparateur point-virgule. Les rubriques `Contributions` et `Réponses` correspondent au classeur source. Aucun compteur ne remplace l’examen des réponses manquantes, des périmètres ni des permissions de citation.

## 9. Fermer et remettre

Fermer la campagne dans l’administration ou par SQL (`active=false`). Conserver une trace datée de cette décision. Une réémission strictement identique d’une réponse déjà reçue peut encore retrouver son accusé ; une nouvelle réponse est refusée.

Remettre au Client les données, contacts séparés, décisions de contrôle, dictionnaire, code, versions et procédure d’accès dans un canal sécurisé. Appliquer la décision de conservation/suppression, y compris pour les exports locaux et les sauvegardes. Retirer les accès devenus inutiles et faire tourner les clés lors d’un transfert de responsabilité.

## 10. Aide au diagnostic technique

| Symptôme | Vérification |
|---|---|
| Bandeau « prévisualisation » | Secrets absents ou `APP_MODE` non configuré pour la production. |
| Bouton d’envoi inactif | Consentement, relecture, erreurs, confirmation finale, autorisation de citation ou état de campagne. |
| Campagne fermée | `active`, bornes UTC, identifiant de campagne et date système. |
| Erreur de jeton | Correspondance entre `WRITER_TOKEN` et son empreinte en base ; ne pas publier le jeton pour demander de l’aide. |
| Envoi incertain | Réessayer le même envoi sans changer de contenu ; ne pas créer immédiatement une autre réponse. |
| Administration refusée | Compte habilité, adresse vérifiée, expiration, secrets OIDC et URL exacte de retour. |
| Aucune réponse visible | Actualiser, sélectionner le bon parcours et, pour la recette, inclure les tests. |
| Échec de lecture ou quotas | Vérifier le projet Supabase, les journaux expurgés et les conditions du service ; aucun repli silencieux vers SQLite n’est prévu. |

## Références techniques consultées

Documentation officielle, consultée pour cette version. Les interfaces et conditions des services peuvent évoluer ; rejouer les tests après toute mise à niveau.

- Streamlit — déploiement : https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app
- Streamlit — secrets : https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management
- Streamlit — authentification : https://docs.streamlit.io/develop/concepts/connections/authentication
- Streamlit — tests : https://docs.streamlit.io/develop/api-reference/app-testing/st.testing.v1.apptest
- Supabase — fonctions : https://supabase.com/docs/guides/database/functions
- Supabase — sécurité par lignes : https://supabase.com/docs/guides/database/postgres/row-level-security
