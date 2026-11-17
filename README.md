# Diagnostic HISWACA — correction 2.1.0

Commencer par **MISE_A_JOUR_ET_HEBERGEMENT.md**.

- `app.py` : collecte autonome corrigée, cinq questionnaires et cinq langues.
- `admin.py`, `relais_ins.py` : applications distinctes ; chaînes techniques retirées des fichiers autonomes.
- `docs/Guide_remplissage_HISWACA_FR.docx` : guide détaillé des répondants.
- `Dockerfile`, `compose.yaml`, `deploy/` : hébergement HTTPS sur un serveur maintenu allumé.
- `compose.test.yaml` : essais Docker locaux exclusivement fictifs.
- `config/` et `sql/` : modèles et fonctions ; aucune valeur secrète n’est fournie.
- `tests/` : tests de logique, interface simulée et tests Streamlit AppTest à exécuter dans GitHub Actions.

Le bouton d’envoi est cliquable ; ses contrôles restent obligatoires. Une campagne fermée, un stockage non configuré, une langue non autorisée ou un consentement manquant ne produisent pas de faux accusé.

Les questions et le schéma restent identiques à la version jointe 2.0.0. Les réponses déjà reçues ne sont pas modifiées. Les brouillons 2026-11-15.web-2 restent compatibles.

Les documents de la v2.0 conservés décrivent le dispositif initial ; les présentes consignes et le journal des modifications décrivent le correctif 2.1.0.
