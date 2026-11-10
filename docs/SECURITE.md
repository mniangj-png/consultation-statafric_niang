# Accès, protection et conservation

## Séparation des accès

Le dépôt GitHub contient le code, le schéma des questionnaires et les tests fictifs, pas les réponses. L’application publique ne possède pas la clé d’administration Supabase. Elle appelle deux fonctions limitées avec un jeton serveur : état de campagne et dépôt atomique d’une contribution. La clé publique seule ne permet aucune lecture directe des tables du diagnostic.

L’administration exige une identité OIDC, une adresse vérifiée, un jeton non expiré et l’appartenance à la liste des personnes autorisées. Une protection supplémentaire de l’application par les paramètres de partage de Streamlit est recommandée. L’administration ne doit pas être remplacée par un simple paramètre `?admin=...` ni par un mot de passe dans une URL.

Les secrets de collecte et d’administration sont distincts. Les administrateurs du compte d’hébergement et du projet de base restent des acteurs privilégiés ; limiter leur nombre et consigner les habilitations.

## Ce qui est conservé

`diag_responses` conserve l’original analytique reçu, sa version, sa langue, ses permissions, son horodatage et son indicateur de test. `diag_contacts` conserve les champs de contact structurés séparément. `diag_reviews` conserve les décisions de contrôle ajoutées, sans modifier l’original. `diag_campaigns` conserve les paramètres de campagne et des empreintes de secrets.

Les textes libres peuvent contenir des informations identifiantes malgré la consigne. Il n’existe pas d’anonymisation automatique fiable dans cette version. Un export analytique est un fichier de travail restreint, non une publication.

Les réponses ne sont pas enregistrées automatiquement avant l’envoi final. Les brouillons téléchargés peuvent contenir des contacts : avertir les répondants et ne pas les placer dans un répertoire partagé.

L’application ne collecte pas volontairement d’adresse IP, de géolocalisation ni d’empreinte de navigateur. Les prestataires d’hébergement peuvent produire leurs propres journaux techniques ; leurs politiques et accès doivent être examinés séparément.

## Contrôles intégrés et limites

- Validation des codes, filtres, types, tailles et choix exclusifs côté serveur Python.
- Limites de taille, consentement, jeton, dates, quota et absence de contacts structurés dans les réponses vérifiés aussi par la fonction SQL.
- Enregistrement transactionnel des réponses et contacts ; protection contre les réessais identiques et les écrasements.
- Messages d’erreur expurgés : pas d’affichage du contenu SQL, des clés ou des réponses distantes brutes.
- Neutralisation des formules dans les exports CSV, sans modifier le texte original du JSON.
- Accès HTTPS à la base ; configuration Streamlit conservant les protections CORS et XSRF.

Le dispositif n’est pas un audit de sécurité externe. Il n’intègre pas de CAPTCHA ni de limitation distribuée par adresse IP. La consultation ouverte peut recevoir des contributions hors cible. Un code de campagne facultatif et un quota global sont disponibles, mais ne vérifient pas l’identité. Pour une exposition plus large ou des données sensibles, renforcer l’hébergement, l’identité des répondants et la protection anti-abus avec l’équipe compétente.

Le code n’effectue pas de chiffrement applicatif supplémentaire des réponses en base. Les garanties de chiffrement au repos, de sauvegarde et de résidence dépendent du service choisi et doivent être vérifiées, non présumées.

## Sauvegardes et incidents

Convenir d’une fréquence d’export pendant la collecte et d’une procédure de sauvegarde/restauration de la base. Conserver les exports dans un espace restreint et vérifier effectivement une restauration de données fictives. Les fichiers de contacts restent séparés des exports analytiques.

En cas d’erreur réseau, l’application ne prétend pas que l’envoi a réussi. Elle conserve en session l’envoi en attente et permet de réessayer le même identifiant. Aucun repli vers un fichier local non surveillé n’est effectué. Si la session est perdue, le répondant peut transmettre à l’équipe son fichier d’envoi en attente par le canal autorisé ; ce fichier n’est pas un accusé de réception ni un brouillon réimportable.

En cas de secret publié accidentellement : fermer la campagne, révoquer/renouveler le secret concerné auprès du fournisseur, examiner les accès et prévenir les responsables désignés. Effacer le fichier du dernier commit ne suffit pas à retirer un secret de l’historique Git.

## Clôture et transfert

Documenter les décisions d’hébergement et de conservation dans la notice. À la clôture : fermer la campagne ; remettre les originaux, les contacts séparés, les décisions, le dictionnaire et le code ; transférer les comptes ou les accès selon le circuit autorisé ; retirer les droits inutiles ; appliquer la décision de suppression à la base, aux copies et aux sauvegardes.

Ne pas conserver une copie des productions en dehors des modalités convenues avec le Client. Le dossier technique n’ajoute pas une autorisation de conservation ou de publication.
