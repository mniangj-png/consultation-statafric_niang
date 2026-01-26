# Application Streamlit – Questionnaire STATAFRIC (statistiques socio-économiques prioritaires)

Cette application remplace la synchronisation Q1/Q2 en proposant **une seule application** respectant l'ordre des rubriques et des questions.

## Fonctionnalités couvertes
- Navigation par rubriques (1 → 12 + relecture / soumission)
- Bascule de langue **FR/EN** (menu latéral)
- Notation multicritères : High/Med/Low/UK et UK clarifié
- Contrôles qualité intégrés (blocage si contraintes non respectées) :
  - Top 5 sans doublon, max 5 domaines
  - Indicateurs uniquement dans les domaines du Top 5
  - 3 indicateurs max par domaine, au moins 1 par domaine
  - Total indicateurs : 5 à 15
  - Pas de duplication de statistique
  - Toutes les statistiques sélectionnées doivent être notées

## Lancement local
```bash
python -m venv .venv
source .venv/bin/activate   # (Windows : .venv\Scripts\activate)
pip install -r requirements.txt
streamlit run app.py
```

## Déploiement GitHub + Streamlit Community Cloud
1. Créez un dépôt GitHub et poussez ce dossier (app.py, requirements.txt, data/, .streamlit/)
2. Allez sur https://share.streamlit.io/
3. Connectez le repo GitHub, sélectionnez `app.py`
4. Déployez

## Données
- La liste longue des indicateurs est dans `data/indicator_longlist.csv`
- Si vous disposez d'une version anglaise des libellés, remplissez la colonne `stat_label_en`


---

## Mode admin (exports, tableaux de bord, rapport Word)

### Activer le mode admin
Le mode admin est caché pour les répondants. Pour y accéder :
- ouvrez l’application avec le paramètre : `?admin=1`
  - Exemple : `https://votre-app.streamlit.app/?admin=1`

### Définir le mot de passe admin
Définissez la variable **ADMIN_PASSWORD** (recommandé via Streamlit secrets).

**Option A (Streamlit Cloud – recommandé)** : Settings → Secrets
```toml
ADMIN_PASSWORD = "change_me"
```

**Option B (local)** :
```bash
export ADMIN_PASSWORD="change_me"
```

### Fonctions disponibles
- **Tableau de bord** : volumes, pays, types d’acteurs, top domaines / statistiques
- **Exports** : CSV, Excel (multi-feuilles), JSONL, base SQLite
- **Rapport Word** : génération d’un rapport .docx prêt à partager (synthèse + tableaux)

