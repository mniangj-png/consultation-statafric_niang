# consultation-stat_niang (Streamlit)

Application Streamlit pour collecter des priorités statistiques (FR/EN), avec contrôles qualité, sauvegarde SQLite, exports Excel et options de stockage Google Sheets / Dropbox.

## Lancer en local
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Déploiement Streamlit Cloud
- Repo GitHub public
- Main file : app.py
- Ajouter les secrets (Settings → Secrets)

### Secrets (optionnels)
```toml
ADMIN_PASSWORD="ChangezMoi"

# Google Sheets (service account)
GOOGLE_SHEET_ID="xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
GOOGLE_SERVICE_ACCOUNT = { ...json du service account... }

# Dropbox
DROPBOX_ACCESS_TOKEN="sl.B...token..."
DROPBOX_FOLDER="/consultation_stat_niang"  # optionnel
```
