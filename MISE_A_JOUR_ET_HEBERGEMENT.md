# Mise à jour et hébergement continu — HISWACA 2.1.0

Date de préparation : 17 novembre 2026. Ce document concerne l’administrateur, pas les répondants.

## 1. Ce qui est livré et ce qui ne l’est pas

L’application de collecte a été corrigée à partir du fichier app(1).py fourni. Le schéma 2026-11-15.web-2, les cinq questionnaires, les cinq langues, les codes, filtres et règles de confidentialité sont conservés. L’application porte désormais le numéro 2.1.0. Les brouillons v2.0 restent compatibles. Les réponses déjà reçues ne sont ni supprimées ni modifiées.

Le dossier contient du code et des configurations. Aucun serveur, nom de domaine, compte payant, certificat ou campagne Supabase n’a été créé à distance. Les secrets existants ne sont pas connus du consultant technique. Il ne faut pas inventer leurs valeurs.

## 2. Corriger immédiatement le déploiement Streamlit existant

1. Sauvegarder l’ancien app.py et noter la branche et le chemin réellement déployés.
2. Remplacer entièrement app.py par le fichier 2.1.0, sans le coller sous l’ancien code. Le fichier doit s’appeler app.py, non app(1).py ni app.py.txt.
3. Remplacer .streamlit/config.toml ; la rubrique [runner] contient magicEnabled = false. Le code autonome a aussi été nettoyé : cette protection ne dépend donc pas seulement de la configuration.
4. Conserver requirements.txt fourni, sans ajouter de paquet diagnostic. Les trois points d’entrée restent autonomes. Déposer également docs/Guide_remplissage_HISWACA_FR.docx pour proposer son téléchargement dans la barre latérale ; son absence n’empêche pas le questionnaire de fonctionner.
5. Conserver vos secrets existants. Vérifier leur validité avant d’ouvrir une collecte réelle ; ne jamais copier les valeurs d’exemple comme si elles étaient opérationnelles.
6. Enregistrer dans la branche déployée, puis redémarrer l’application. Vérifier le repère v2.1.0 sous le titre.
7. Ouvrir une fenêtre privée et tester les cinq liens avec form=ins, producteurs, utilisateurs, partenaires et abrege. Utiliser lang=fr/en/ar/pt/es pour choisir la langue. La traduction automatique du navigateur peut modifier des libellés : la désactiver pendant la recette.

Le dépôt GitHub stocke le code. Il n’exécute pas, à lui seul, le serveur de collecte.

## 3. Pourquoi le bouton d’envoi peut ne pas conduire à un enregistrement

Dans la version 2.1.0, le bouton « Envoyer ma contribution » reste cliquable sur la page de relecture. Il vérifie les conditions à chaque clic et affiche les motifs précis d’un refus. Cela ne signifie pas que les protections ont été supprimées.

| Situation | Action de l’administrateur |
|---|---|
| APP_MODE absent ou preview | L’application n’a pas de stockage actif. Configurer un mode de test explicite ou la production. |
| APP_MODE = local | Essais fictifs uniquement ; toutes les réponses portent is_test=true. Ne pas utiliser pour une collecte réelle sur Streamlit Cloud. |
| APP_MODE = production | Renseigner les secrets Supabase, la notice et les validations de déploiement. |
| Campagne inactive dans diag_campaigns | Vérifier et activer explicitement la campagne autorisée. |
| Date antérieure à opens_at ou postérieure à closes_at | Ne pas contourner la fenêtre de collecte. Utiliser une campagne de test indépendante. |
| Langue absente d’APPROVED_LANGUAGES | Contrôler la traduction et autoriser la langue après relecture ; ne pas déclarer une validation non réalisée. |
| Code d’accès manquant ou erroné | Vérifier le code communiqué dans l’invitation ; ne pas le confondre avec WRITER_TOKEN. |
| TEST_MODE différent de la campagne en base | Corriger le mode ou l’identifiant de campagne ; ne pas convertir une campagne fictive en campagne réelle contenant déjà des essais. |
| Erreur réseau ou service non joignable | Conserver le même envoi en attente et réessayer sans changer son identifiant. |

La collecte réelle est prévue du 19 au 26 novembre 2026. Le 17 ou le 18 novembre, un refus de la campagne réelle est donc normal si ces dates sont configurées. Cette correction ne décale pas silencieusement l’ouverture.

## 4. Secrets pour un essai immédiat

Dans une copie de test distincte, les paramètres suivants autorisent un enregistrement LOCAL FICTIF, sans Supabase :

```toml
APP_MODE = "local"
TEST_MODE = true
LOCAL_DB_PATH = ".local/recette_v21.sqlite3"
```

Ne pas mettre cette configuration sur le lien diffusé aux répondants. Sur un hébergement éphémère, le fichier local peut être perdu ; ce mode sert à la recette, pas à la conservation des contributions réelles.

Pour un essai distant, utiliser config/collecte_test.example.toml avec une campagne Supabase distincte, test_mode=true, un jeton de test distinct et des dates de test ouvertes. Déclarer DEPLOYMENT_APPROVED selon les contrôles effectivement réalisés, même si les réponses sont fictives. Ne jamais exposer la clé d’administration dans la collecte publique.

## 5. Configuration de production Supabase

Vérifier les paramètres de config/collecte_reelle.example.toml et remplacer chaque valeur de substitution dans un fichier privé. Les points indispensables sont APP_MODE, TEST_MODE, CAMPAIGN_ID, SUPABASE_URL, SUPABASE_PUBLIC_KEY, WRITER_TOKEN, CONTACT_EMAIL, RETENTION_NOTICE, DEPLOYMENT_APPROVED, APPROVED_LANGUAGES et les notices par langue.

Le jeton WRITER_TOKEN reste côté serveur, dans les secrets. Il n’est pas transmis dans les liens aux participants. La base conserve son empreinte SHA-256, pas le jeton en clair. SUPABASE_ADMIN_KEY est réservé à l’application admin.py.

Ne pas publier les secrets dans GitHub. La clé publique Supabase n’est pas un substitut à l’installation des fonctions SQL sécurisées. Les droits de lecture des réponses restent fermés aux accès publics.

Pour une installation existante v2.0, les corrections de l’interface ne nécessitent pas de nouvelle table. Le fichier sql/05_campaign_status_v21.sql est facultatif : il enrichit seulement le diagnostic de l’état de campagne avec son mode et la date du serveur. Il ne supprime aucune donnée. Pour une nouvelle installation, suivre d’abord DEPLOIEMENT.md et sql/01_install_or_upgrade.sql.

Le serveur reste l’autorité d’enregistrement. Le bouton ne produit un accusé qu’après confirmation du stockage. L’interface ne bascule jamais silencieusement vers SQLite si Supabase est indisponible.

## 6. Ce que « sans veille » signifie réellement

Un Dockerfile construit l’environnement d’exécution ; il ne peut pas annuler la politique de suspension d’un hébergeur. Pour une disponibilité continue, il faut un serveur maintenu allumé ou un service de conteneurs dont la suspension automatique et la réduction à zéro sont désactivées. La base de données doit elle aussi rester disponible.

Le scénario fourni utilise un serveur Linux permanent avec Docker Engine et Docker Compose, une instance de collecte et Caddy pour HTTPS. Il ne génère pas de trafic artificiel pour contourner une offre gratuite. Il n’ajoute aucun service de surveillance payant sans accord.

La politique restart: unless-stopped relance un conteneur qui se termine et permet son retour après un redémarrage de Docker, sauf arrêt volontaire. Elle ne remet pas en marche un serveur éteint. Un HEALTHCHECK déclare un conteneur sain ou défaillant ; à lui seul il ne redémarre pas un processus bloqué encore vivant. Une supervision et une intervention restent nécessaires.

## 7. Contenu du Dockerfile fourni

L’image utilise Python 3.12 et installe les dépendances de requirements.txt. Elle fonctionne sous un compte non privilégié (UID/GID 10001), lance Streamlit sur toutes les interfaces internes, respecte PORT et contrôle APP_FILE dans une liste limitée. Les protections CORS et XSRF restent actives. L’affichage automatique des chaînes est désactivé, comme la collecte de statistiques d’usage de Streamlit.

Les fichiers de secrets ne sont jamais copiés dans l’image. La collecte de production monte un fichier TOML privé en lecture seule. Le contrôle de santé appelle seulement /_stcore/health ; il ne vérifie pas l’écriture Supabase et ne doit pas être présenté comme une preuve que les réponses sont enregistrées.

## 8. Recette Docker locale, sans domaine

Prérequis : Docker et Compose installés sur le poste ou le serveur. Extraire le dossier et ouvrir un terminal à sa racine.

```bash
docker compose -f compose.test.yaml config
docker compose -f compose.test.yaml up -d --build
docker compose -f compose.test.yaml ps
```

Ouvrir http://localhost:8501 sur ce poste. La bannière MODE TEST doit apparaître. Réaliser une contribution fictive pour chaque parcours. Le fichier SQLite de test se trouve dans le volume test_data, et non dans GitHub.

```bash
docker compose -f compose.test.yaml logs --tail=100 collecte_test
docker compose -f compose.test.yaml down
```

La commande down n’efface pas le volume nommé, sauf ajout de --volumes. Ne pas employer --volumes sans intention explicite de supprimer les essais.

## 9. Hébergement continu HTTPS sur un serveur

Préparer un serveur permanent, un nom DNS contrôlé pointant vers son adresse publique, Docker Engine, Docker Compose et l’accès réseau HTTPS. Le serveur ne doit pas se mettre en veille. Les frais et les accès doivent être approuvés par le responsable de la mission.

Sur ce serveur, cloner le dépôt GitHub ou transférer le dossier, puis :

```bash
cp .env.example .env
mkdir -p secrets
cp config/collecte_reelle.example.toml secrets/collecte.toml
```

Modifier .env : DOMAIN doit être le nom DNS réel, sans https://, et ACME_EMAIL une adresse réelle de gestion du certificat. Modifier secrets/collecte.toml avec les valeurs réelles et les validations obtenues. Le domaine d’exemple ne peut pas recevoir de certificat public.

Limiter la lecture du fichier secret. Sur un serveur Linux utilisant les UID sans remappage :

```bash
sudo chown root:10001 secrets/collecte.toml
sudo chmod 640 secrets/collecte.toml
sudo chmod 750 secrets
```

Adapter les droits avec l’administrateur si Docker fonctionne en mode rootless ou avec un remappage d’utilisateurs. Le compte du conteneur doit pouvoir lire le fichier, sans droit d’écriture. Éviter un chmod 777.

Vérifier que le pare-feu et le groupe de sécurité autorisent 80/tcp et 443/tcp pour le certificat et HTTPS ; 443/udp est facultatif pour HTTP/3. Ne pas exposer le port interne 8501 à Internet. Conserver l’accès d’administration SSH selon les règles de sécurité du serveur.

```bash
docker compose config
docker compose up -d --build
docker compose ps
docker compose logs --tail=100 collecte caddy
```

Caddy obtient le certificat si le DNS et l’accès aux ports sont corrects. Le lien de collecte devient https://VOTRE_DOMAINE ; ajouter les paramètres form et lang pour les liens spécialisés. Les WebSockets sont transmis par Caddy. La base reste Supabase et ne dépend pas du disque du conteneur.

## 10. Administration séparée

Le compose.yaml de production n’expose volontairement que la collecte. Ne pas démarrer admin.py avec les secrets publics et ne pas insérer SUPABASE_ADMIN_KEY dans le conteneur collecte.

L’image peut lancer admin.py avec APP_FILE=admin.py, mais son déploiement doit être distinct, avec ses propres secrets, son adresse HTTPS, l’authentification OpenID Connect et sa liste d’adresses autorisées. Les paramètres de redirection doivent correspondre à l’adresse réelle de cette administration. Le dossier v2.0 comporte le modèle de configuration administrative et le guide correspondant, toujours applicable.

## 11. Mise à jour, sauvegarde et incidents

Avant un redéploiement, prévenir les testeurs ou les personnes en cours de saisie : une interruption peut faire perdre un brouillon resté uniquement en session. Les réponses déjà confirmées dans Supabase ne sont pas effacées par le remplacement du conteneur. Ne pas mettre deux versions concurrentes en production sans gestion des sessions.

```bash
git pull --ff-only
docker compose up -d --build
docker compose ps
```

Vérifier ensuite les cinq titres, la langue, une reprise de brouillon et un enregistrement dans la campagne de test. Conserver l’ancien code ou son commit pour pouvoir revenir à la version précédente sans restaurer une vieille base sur la nouvelle.

Prévoir une sauvegarde restreinte et une procédure de restauration pour Supabase. Les exports sans contacts ne sont pas automatiquement anonymes. Ne pas stocker les exports, contacts, brouillons ou journaux sensibles dans GitHub. Ne pas arrêter une base active pour provoquer un test de panne : utiliser une campagne et un environnement de recette séparés.

## 12. Conditions avant diffusion des liens

- Vérifier les 25 combinaisons des cinq parcours et cinq langues ; documenter séparément la relecture linguistique.
- Vérifier une réponse partielle, un refus, les filtres, les priorités, les choix exclusifs, la navigation et la reprise d’un brouillon.
- Confirmer le parcours complet : envoi distant, reçu, ligne dans l’administration et export lisible.
- Vérifier qu’aucune réponse fictive n’est mélangée aux résultats réels et que les contacts restent séparés.
- Vérifier le mode réel, les dates de la campagne et les langues autorisées ; publier seulement après autorisation.

## 13. Sources techniques consultées le 17 novembre 2026

Streamlit — Magic : https://docs.streamlit.io/develop/api-reference/write-magic/magic
Streamlit — Widget behavior : https://docs.streamlit.io/develop/concepts/architecture/widget-behavior
Streamlit — Docker : https://docs.streamlit.io/deploy/tutorials/docker
Docker — Restart policies : https://docs.docker.com/engine/containers/start-containers-automatically/
Docker — Compose services : https://docs.docker.com/reference/compose-file/services/
Caddy — Reverse proxy : https://caddyserver.com/docs/caddyfile/directives/reverse_proxy

Les fichiers sont syntaxiquement contrôlés. Ni une construction Docker ni une recette du service distant n’a pu être exécutée dans l’environnement de préparation ; elles doivent être réalisées sur le serveur cible.
