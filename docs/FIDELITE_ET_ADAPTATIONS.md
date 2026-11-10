# Fidélité aux instruments et adaptations de la version web

## Sources utilisées

Les instruments français du 8 novembre 2026, retrouvés dans les fichiers de la mission, constituent la base de la transposition :

| Source | Périmètre transposé |
|---|---|
| `Questionnaire_diagnostic_INS_HISWACA_FR.docx` | QI01–QI12, cinq pages. |
| `Questionnaire_utilisateurs_discours_HISWACA_FR.docx` | QU01–QU08, trois pages. |
| `Guide_diagnostic_ecrit_HISWACA_FR.docx` | QR01–QR05, § 2, pages 1–2 ; RA01–RA05, § 3, page 3 ; codage et protection, § 6 et 9. |
| `Rapport_methodologique_HISWACA_FR_20261108.docx` | Unités, consultation, distinction déclaration/preuve, traitement des manquants et absence d’extrapolation. |
| `Classeur_diagnostic_HISWACA_FR.xlsx` | En-têtes des registres Contributions et Réponses, conventions de traçabilité. |

Les empreintes des fichiers effectivement utilisés figurent dans `sources.manifest.json`. Ces fichiers sources ne sont pas copiés dans le dépôt destiné à GitHub. Le présent code n’affirme pas qu’une version ultérieure du rapport ou des instruments a été approuvée.

## Éléments conservés

- Période statistique du 1er novembre 2025 au 31 octobre 2026 et distinction des pratiques nouvelles.
- Codes et ordre des rubriques ; suffixes explicites pour les sous-questions.
- Statut institutionnel, de service ou individuel de la contribution.
- Dernier cas clôturé/préparé, sans sélection du seul exemple réussi.
- Filtres de parcours, notamment QI08 accessible même sans nouvelle demande.
- Codes 0, 97, 98 et 99 ; non-réponse distincte de l’absence déclarée.
- Matrices de métadonnées et de dispositifs, transformées en choix par ligne pour une lecture plus simple sur petit écran.
- Limites de trois choix ou priorités lorsque prévues et limites de 40, 60 ou 80 mots des champs concernés.
- Attribution et citation autorisées par le répondant ; en l’absence de choix, permission effective limitée à la synthèse anonymisée, sans fabriquer une réponse explicite 0.
- Contacts facultatifs séparés de l’analyse ; refus de participation sans enregistrement d’une contribution.
- Réponse abrégée toujours partielle, sans imputation des questions du questionnaire complet.

## Adaptations techniques explicites

**Navigation et langues.** Les rubriques deviennent des étapes web avec possibilité de revenir et relire. La version anglaise est une traduction intégrée au code, à relire dans la recette linguistique avant diffusion. Elle n’est pas présentée comme une traduction déjà validée par STATAFRIC. Le contact et la notice d’hébergement sont paramétrables et doivent comporter les informations utiles dans les deux langues.

**Champs libres.** Les espaces papier sont remplacés par des champs de texte, avec plafonds techniques de caractères pour prévenir les saisies accidentelles excessives. Les trois lignes demandées par la version abrégée restent une consigne, sans conversion arbitraire en limite de mots.

**Absence de fichier joint.** Le web accepte les références, liens publics et extraits autorisés saisis en texte. Il n’accepte pas de pièces binaires : le partage de fichiers, s’il est nécessaire, passe par le canal institutionnel autorisé. Cela ne retire pas la possibilité de décrire l’exemple prévu par la source.

**QR03.** Les deux cases « aucune pratique » et « ne sait pas » sont séparées du texte de description. L’absence de choix à cette question ne bloque pas la description d’une pratique. Aucun nouveau code substantiel « oui » n’a été ajouté.

**QR04.** Le champ « contribution envisageable » est un choix unique, conformément à la règle par défaut d’un choix hors mention de réponses multiples. La sélection des besoins demeure multiple, avec au plus trois codes.

**RA05.** Le contact institutionnel facultatif est détaillé en fonction/service et courriel pour séparer les coordonnées des réponses analytiques. Aucun nom ou téléphone personnel n’est demandé.

**Attribution.** Pour les choix 1 ou 2, une confirmation de l’habilitation à autoriser cette attribution est demandée avant l’envoi. Elle matérialise la condition de la notice ; elle ne constitue pas une vérification externe du mandat. Le code 3 reste soumis à un accord préalable ultérieur, que l’application ne présume pas obtenu.

**Brouillons.** Un téléchargement/import JSON est ajouté. Aucun brouillon permanent n’est enregistré en base. Les fichiers téléchargés restent sous la responsabilité du répondant.

**Horodatage et identifiants.** L’application attribue un UUID et une référence `REP-<UUID>`. Ce format remplace la numérotation manuelle `REP001` à des fins techniques et évite les collisions entre envois simultanés. Il n’identifie pas une personne et ne remplace pas la déduplication institutionnelle.

**Manquants.** Un blanc est marqué `NR_A_CONTROLER` avant revue, et non transformé automatiquement en NR définitif. Un filtre amont non renseigné porte `FILTRE_INDETERMINE`, distinct de NF. Ces états techniques sont documentés dans les exports.

**Dates de collecte.** La réponse souhaitée reste le 17 novembre. La fermeture technique du 14 décembre est proposée pour permettre les compléments et la revue ; elle est modifiable dans la campagne. Ce choix ne modifie pas la période statistique des réponses.

**Stockage et hébergement.** Supabase, les deux applications et l’authentification OIDC sont des choix de mise en œuvre de ce dossier. Ils ne sont pas décrits comme des exigences contractuelles ni comme une autorisation institutionnelle acquise.

## Limites fonctionnelles volontaires

Cette livraison déploie les instruments de diagnostic écrit et leur réception/contrôle. Elle ne crée pas les outils stratégiques O1–O8, ne simule pas des résultats de consultation, n’anime pas une formation et n’implémente pas encore une application d’évaluation des futurs prototypes. Les registres documentaires, d’extraits de discours, de constats et de tests restent dans le classeur de travail existant.

Les décisions de contrôle sont traçables, mais le logiciel ne qualifie pas automatiquement une réponse d’institutionnelle, ne décide pas de sa véracité et ne fusionne pas des réponses individuelles en réponse officielle d’un INS.
