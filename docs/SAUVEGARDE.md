# Sauvegardes et remise des fichiers

La sauvegarde de la base PostgreSQL et sa restauration relèvent de l'administrateur du projet. Vérifier les options réellement disponibles dans l'offre retenue. Une archive analytique de l'application ne remplace pas une sauvegarde transactionnelle de base.

En complément, `scripts/backup_restricted.py` lit les réponses, l'historique des contrôles et tous les événements des registres. La lecture échouée ne produit pas d'archive partielle annoncée complète. Les lectures sont successives, non un instantané simultané des tables.

```bash
python scripts/backup_restricted.py --secrets private_config/admin.toml --sortie exports
```

Les contacts facultatifs sont exclus, sauf ajout explicite de `--contacts` lorsque le besoin est autorisé. Le TOML administrateur doit rester dans un dossier privé, jamais dans GitHub. La possession de la clé service_role donne les droits techniques nécessaires à ce script local ; elle ne doit pas être donnée à un répondant ou à un INS relais.

Conserver date, campagne, version et droits d'accès avec chaque export. Pour L3, travailler sur les réponses réelles retenues, avec doublons et hors champ exclus, sans modifier les fichiers originaux. La remise finale suit les autorisations de la mission ; elle n'implique pas publication des archives internes.
