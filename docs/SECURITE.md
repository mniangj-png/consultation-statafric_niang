# Protection et limites techniques

## Séparation des accès

La collecte utilise une clé anon Supabase et un jeton d'écriture serveur ; les tables de réponses, contacts et registres n'ont aucun droit public de lecture. L'administration possède la clé service_role uniquement après une authentification OIDC et une vérification d'adresse. La sécurité des comptes GitHub, Streamlit, Supabase et Google reste indispensable. Activer l'authentification multifacteur des comptes propriétaires selon les politiques de l'organisation.

Les fonctions SQL d'écriture sont `SECURITY DEFINER` avec `search_path=''` et droits explicites. Elles vérifient campagne, jeton, mode test/réel, dates, quota, consentement et forme générale du contenu. Les contrôles détaillés de codes et de parcours sont exécutés côté serveur Python ; ils ne sont pas dupliqués intégralement en SQL. Le jeton d'écriture n'est pas un secret à distribuer aux répondants.

Un code d'accès de campagne facultatif peut être défini par une empreinte SHA-256 dans `access_code_hash`. Le transmettre séparément des secrets serveur. Un quota global limite la saturation ; il ne remplace pas un dispositif complet contre les abus. Aucun CAPTCHA ni filtrage IP permanent n'est installé. Une consultation ouverte peut nécessiter un contrôle d'accès supplémentaire décidé par le Client.

## Intégrité et reprise d'envoi

Un identifiant UUID est attribué à la contribution. Une nouvelle tentative du même envoi restitue le même reçu ; un contenu différent avec le même identifiant est refusé. En cas d'incertitude réseau, l'envoi est figé, téléchargeable puis réimportable. Ne pas modifier ce JSON d'envoi en attente. Un reçu n'est affiché qu'après confirmation du stockage. Une contribution corrigée est un complément référencé, pas un écrasement de l'original.

Un changement de langue préserve les valeurs et l'identifiant. Un retrait de consentement avant envoi purge les réponses de fond de la session. Aucun retrait de réponse déjà enregistrée n'est prétendu automatique : les demandes éventuelles sont traitées selon les règles de conservation approuvées.

## Coordonnées et exports

Les champs de contact structurés sont conservés séparément, avec export explicite réservé à l'administration. Les formulaires ne demandent pas de nom personnel, numéro privé, opinion politique ou brouillon confidentiel. Les récits et noms d'institutions peuvent néanmoins permettre une identification ; les exports analytiques ne sont pas automatiquement anonymes.

Les cellules CSV commençant comme une formule sont neutralisées par apostrophe pour l'ouverture dans un tableur ; le JSON conserve l'original. L'application ne télécharge pas automatiquement les liens proposés par les répondants. Aucune information réelle n'est envoyée à un service de traduction. Les traductions et décisions de codage sont enregistrées séparément, avec relecteur et version.

## Stockage et sauvegardes

SQLite est exclusivement un mode de test local. Aucun repli local n'intervient si Supabase échoue. Les sauvegardes distantes, leur fréquence, leur localisation et la restauration sont à configurer selon l'offre et les règles du Client. Le code n'applique pas une date arbitraire de suppression des données. Les modalités de transfert et de conservation doivent être confirmées.

Le SQL ne supprime pas les réponses existantes. Une migration doit néanmoins être précédée d'une sauvegarde vérifiée. Les mises à jour et contrôles de dépendances peuvent exiger une recette ultérieure. La version fournie n'est pas un audit de sécurité externe.

## Avant diffusion

Vérifier l'accès public, l'interdiction de lecture anonyme, l'accès administrateur autorisé/refusé, les secrets séparés, la notice effective dans chaque langue, les dates en UTC, la campagne de test distincte et une restauration réelle. Ne pas partager le lien d'administration avec les répondants. Ne pas envoyer les contacts et le classeur complet aux INS relais.
