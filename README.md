# Diagnostic HISWACA — applications de collecte et d’administration

**Version 1.0.0 — 10 novembre 2026**  
Mission : appui statistique aux discours des décideurs. HISWACA – SOP 1, P178497, ET-AUC-478525-CS-INDV. Consultant : Mamadou Niang.

## Ce que contient le dépôt

Deux applications Python distinctes, alimentées par le même schéma de questionnaires :

| Application | Fichier d’entrée | Utilisateurs | Fonction |
|---|---|---|---|
| Collecte | `app.py` | Parties prenantes | Quatre parcours, interface et questionnaires en français et en anglais, filtres, relecture, brouillon, envoi et accusé. |
| Administration | `admin.py` | Consultant et personnes habilitées | Authentification, contrôle des contributions, qualification des doublons, exports, contacts séparés et ouverture/fermeture. |

| Parcours | Codes conservés | Paramètre du lien | Durée indicative de l’instrument source |
|---|---|---|---|
| Instituts nationaux de la statistique | QI01–QI12 | `?form=ins&lang=fr` | 15–20 minutes |
| Équipes préparant les discours | QU01–QU08 | `?form=utilisateurs&lang=fr` | 10–15 minutes |
| CER et organisations internationales | QR01–QR05 | `?form=partenaires&lang=fr` | Environ 10 minutes |
| Réponse abrégée | RA01–RA05 | `?form=abrege&lang=fr` | 5–10 minutes |

Remplacer `fr` par `en` pour l’anglais. Ces durées n’ont pas été remesurées auprès de répondants. Les liens ne vérifient ni l’identité du répondant ni son mandat institutionnel.

## Architecture retenue

```text
GitHub : code et versions, SANS réponses, contacts ni secrets
  ├── Streamlit / app.py : collecte publique, clé publique + jeton serveur limité
  └── Streamlit / admin.py : OIDC + liste des personnes autorisées, clé d’administration
                  │
                  └── Supabase / PostgreSQL : réponses et coordonnées dans des tables distinctes
```

**GitHub n’héberge pas le processus Python.** Le scénario documenté utilise Streamlit Community Cloud, connecté au dépôt GitHub. La base externe assure la persistance indépendamment des redémarrages de l’application. Supabase est une proposition technique de ce dossier, pas une décision institutionnelle déjà acquise.

Avant toute collecte réelle, confirmer avec STATAFRIC les comptes propriétaires, les personnes habilitées, les lieux d’hébergement, la durée de conservation et la procédure de remise/suppression. Vérifier les conditions et quotas des services choisis. Le dossier n’atteste pas une homologation de sécurité ou une conformité réglementaire.

## Démarrer

**Pour un déploiement réel, suivre `DEPLOIEMENT.md`.** Il couvre l’installation SQL, les secrets séparés, les deux applications, l’authentification et la recette.

Pour prévisualiser localement, depuis le dossier du projet :

```bash
python -m venv .venv
# Windows PowerShell : .venv\Scripts\Activate.ps1
# macOS / Linux : source .venv/bin/activate
python -m pip install -r requirements.txt
streamlit run app.py
```

Sans secrets, l’application s’ouvre en **prévisualisation** et ne permet aucun enregistrement.

Pour des essais locaux exclusivement fictifs : copier `docs/local.secrets.example.toml` vers `.streamlit/secrets.toml`, puis lancer la collecte et, dans un second terminal, l’administration :

```bash
streamlit run app.py --server.address 127.0.0.1 --server.port 8501
streamlit run admin.py --server.address 127.0.0.1 --server.port 8502
```

Ne pas partager ces URL locales ni publier cette configuration. SQLite local n’est pas le stockage de production. Toutes les réponses locales sont marquées comme tests.

## Fonctionnalités de collecte

Les 30 rubriques et 150 champs/sous-champs reprennent les quatre instruments français. Les champs de contact sont facultatifs. Les codes de réponse, dont `0`, `97`, `98`, `99`, restent distincts des blancs et des sauts par filtre. Une réponse abrégée est toujours qualifiée de partielle.

La navigation est organisée par rubrique. Une réponse rendue inapplicable par un changement de parcours est retirée ; elle n’est pas réutilisée si le répondant revient ensuite au parcours initial. Si un filtre dépend d’une question non renseignée, l’export porte un état distinct : `FILTRE_INDETERMINE`, et non un faux `NF`.

Les contrôles portent sur les choix exclusifs, les limites de choix, les priorités classées, les longueurs, le format des mois et des courriels, ainsi que quelques contradictions. Les incohérences bloquantes sont signalées ; les situations à clarifier restent dans une liste d’alertes. Le logiciel ne certifie pas la véracité des déclarations.

Une réponse partielle est recevable, sous réserve du consentement et d’au moins une information autre que le consentement. Un profil seul reste « À examiner », jamais automatiquement « Exploitable ».

Le brouillon JSON est téléchargé par le répondant et peut être réimporté. **Il n’existe pas de sauvegarde automatique permanente du brouillon sur le serveur.** Un rafraîchissement ou une rupture de session peut faire perdre une saisie non téléchargée. Le fichier de brouillon peut contenir les coordonnées facultatives ; il doit être protégé.

L’envoi utilise un identifiant stable. Un nouvel essai avec le même identifiant et le même contenu renvoie le même accusé, sans nouvelle réponse. Un contenu différent ne remplace pas une réponse reçue. Un complément doit rappeler la référence de la contribution précédente.

## Données, administration et exports

Les données de collecte sont enregistrées en JSON versionné avec l’empreinte du questionnaire, la langue, les états des questions et l’indicateur de test. Les contacts structurés sont séparés. Les données brutes ne sont pas réécrites par l’administration ; les décisions de contrôle sont ajoutées à un historique.

L’export analytique ZIP comprend `Contributions.csv`, `Reponses.csv`, `Metadonnees.csv`, `Dictionnaire.csv` et les originaux analytiques JSON. Les en-têtes des deux premiers CSV reprennent les onglets correspondants du classeur source. Le dossier ne modifie pas automatiquement ce classeur et ne reconstitue pas les autres registres documentaires.

Les CSV utilisent UTF-8 avec BOM et le séparateur point-virgule. Les textes susceptibles d’être interprétés comme des formules sont neutralisés dans les CSV ; le texte original reste dans le JSON. Pour Excel : importer les identifiants, les codes et les références comme du texte.

**Sans contacts ne signifie pas anonyme.** Les noms d’institutions, récits et liens peuvent être identifiants. Les permissions de citation et une revue humaine s’appliquent avant toute diffusion externe. Aucun score de maturité, classement d’INS, taux de représentativité ou décompte automatique d’institutions dédoublonnées n’est produit.

## Tests et limites de validation

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

À la construction du dossier : **126 tests exécutés avec succès** ; le module de tests Streamlit est ignoré faute de bibliothèque Streamlit disponible dans l’environnement d’exécution. La compilation Python a réussi. Les tests de logique, stockage SQLite, concurrence locale, export et erreurs HTTP simulées ne constituent pas un test en ligne.

Les tests `AppTest` sont fournis pour GitHub Actions et un poste équipé. Les scripts SQL ont été contrôlés statiquement, **pas exécutés sur un projet Supabase réel**. L’authentification réelle, la navigation web et la persistance distante doivent être validées selon `docs/RECETTE.md` avant transmission des liens.

## Repères du dépôt

- `diagnostic/schema.py` : libellés FR/EN, options, filtres et références des instruments.
- `diagnostic/engine.py` : validation, normalisation, brouillons et accusés.
- `diagnostic/storage.py` : accès Supabase et mode local de test.
- `diagnostic/exports.py` : exports analytiques et dictionnaire.
- `sql/` : installation, campagne et scripts de recette.
- `scripts/generate_secrets.py` : préparation locale des secrets et du SQL de campagne.
- `docs/FIDELITE_ET_ADAPTATIONS.md` : sources et adaptations explicites.
- `docs/SECURITE.md` : accès, sauvegardes, limites et clôture.
- `docs/EMAIL_INVITATION_FR_EN.md` : messages à adapter.
- `.github/workflows/tests.yml` : tests automatiques à chaque poussée et demande de fusion.

Aucun compte, dépôt distant, application hébergée ou URL de production n’a été créé par la génération de ce dossier. Aucun questionnaire n’a été envoyé et aucune réponse réelle n’a été collectée.
