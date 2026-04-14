# Dashboards Admin et Superadmin

Ce paquet contient deux tableaux de bord Streamlit distincts pour exploiter les réponses enregistrées dans le dépôt GitHub `mniangj-png/consultation-statafric_niang`.

## Fichiers

- `admin_dashboard.py` : exploration et téléchargement des données en JSON, CSV et XLSX.
- `superadmin_dashboard.py` : génération d'un classeur d'analyse Excel et d'un rapport DOCX.
- `dashboard_common.py` : fonctions partagées de lecture GitHub, transformation, export et génération de rapport.
- `requirements.txt` : dépendances Python.

## Secrets Streamlit recommandés

```toml
[github]
owner = "mniangj-png"
repo = "consultation-statafric_niang"
branch = "main"
token = "VOTRE_TOKEN_GITHUB"

ADMIN_PASSWORD = "votre_mot_de_passe_admin"
SUPERADMIN_PASSWORD = "votre_mot_de_passe_superadmin"
```

## Lancement local

```bash
streamlit run admin_dashboard.py
streamlit run superadmin_dashboard.py
```

## Ce que fait le dashboard Admin

- charge les `drafts` ou `submissions` depuis `data/validation_doc/...` dans GitHub ;
- filtre les données par statut, langue, type d'institution, pays / CER et date ;
- permet de télécharger les données filtrées en JSON, CSV et XLSX ;
- affiche un aperçu tabulaire et le détail d'un enregistrement.

## Ce que fait le dashboard Superadmin

- charge les soumissions finales ;
- construit un classeur d'analyse avec plusieurs feuilles ;
- génère un rapport DOCX de synthèse ;
- permet de télécharger le classeur, le rapport et un paquet ZIP.

## Structure de sortie du classeur d'analyse

- `synthese`
- `soumissions_brutes`
- `langues`
- `types_institution`
- `validation_globale`
- `position_finale`
- `choix_methodo`
- `domaines`
- `justifications`
- `metadata`
