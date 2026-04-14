# Streamlit validation app v7

Cette version intègre notamment :
- enregistrement automatique du brouillon dès qu'une adresse email valide a été confirmée lors du passage de l'étape 1 ;
- affichage d'une alerte d'information avec code de reprise ;
- suppression de l'ancienne section 2 (questions 6 et 7) ;
- 4 étapes au total ;
- barre de langue principale avec les 4 langues visibles simultanément ;
- titre du formulaire sans le mot « stratégique » ;
- ouverture immédiate d'un champ de justification en cas de réserve ou de non-validation ;
- question sur les trois révisions majeures conservée comme facultative ;
- correction du paramétrage GitHub pour utiliser par défaut la branche `data`, afin d'éviter l'alerte d'échec d'enregistrement en ligne lorsque le secret `branch` n'est pas défini.

## Déploiement

Remplacer `streamlit_app.py` dans le dépôt par cette version, conserver `requirements.txt`, puis redéployer l'application.

## GitHub

Par défaut :
- owner : `mniangj-png`
- repo : `consultation-statafric_niang`
- branch : `data`
- dossier de stockage : `validation_doc`

Ajouter un secret Streamlit `GITHUB_TOKEN` valide, ou `github.token` dans `secrets.toml`.


## Secrets pris en charge
Le script accepte désormais plusieurs formats de secrets Streamlit pour GitHub, par exemple :

```toml
GITHUB_TOKEN = "..."
```

ou

```toml
[github]
owner = "mniangj-png"
repo = "consultation-statafric_niang"
branch = "data"
token = "..."
```
