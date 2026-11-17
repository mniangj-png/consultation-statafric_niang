# Concordance avec la méthodologie et décisions de transposition

## Base retenue

Source : `Rapport_methodologique_HISWACA_FR_revise_inclusif_20261115(3).docx`, 35 pages, version du 15 novembre 2026. Application de départ : `app (2).py`, quatre parcours FR/EN, imports `diagnostic.*`, ancienne échéance du 17 novembre. La structure et les codes antérieurs ont été repris autant que possible ; les fichiers de lancement sont désormais autonomes.

| Exigence de la source | Mise en œuvre | Repère source |
|---|---|---|
| Cinq parcours | QI, QP, QU, QR et RA ; QP01–QP12 et QR03bis/QR04bis ajoutés | § 4.1–4.2 ; annexes 1, 1bis, 2, 3, 4 |
| Cinq langues | Catalogue complet fr/en/ar/pt/es ; codes communs, sélecteur libre, CSS RTL arabe | § 4.5 ; annexe 5 |
| Collecte 19–26 novembre | Notice fixe et bornes SQL UTC ; campagne réelle fermée par défaut | § 4.3 et 8 |
| Réponses vers L3 seulement | Métadonnée L3 ; export séparé des registres documentaires L2 | § 1.1 et 5.2 |
| Relais 3–8 producteurs, 4–6 utilisateurs | Application relais et registre d'invitations, liens QP/QU distincts | § 4.3–4.4 et 8 |
| Réponse volontaire et partielle | Consentement vierge, arrêt après refus, champs substantiels facultatifs | Notices annexées |
| Conservation des codes | 0, 97, 98, 99 préservés ; codes techniques non traduits | § 5.1 |
| Corrections malgré absence de demandes nouvelles | QI02=0 et QP02=0 conduisent à QI08/QP08 ; pas de suppression du module de corrections | QI02/QI08 ; QP02/QP08 |
| Séparation des décisions sans discours | QP02d et QP11 ; aucune addition au volume des demandes de discours | Annexe 1bis |
| Contrôles et originaux | Réponses immuables, décisions et registres versionnés ; coordonnées structurées séparées | § 5 et 9 |
| Recherche multilingue | Registre documents, versions originales/traductions, langue sw possible | § 3.1–3.4 |
| Huit critères d'examen des discours | Registre extraits avec justification de chaque appréciation | § 3.2 |
| Cinq tâches d'utilisation | Registre T1–T5, aide, version, langue, résultat et contre-test | § 7 |
| Absence de points périodiques STATAFRIC | Aucun module de réunion ou courriel automatique ajouté | § 1.1 et 9.1 |

## Clarifications explicites de transposition

1. **QP et QU.** Certaines phrases du § 4.3 et du § 8 emploient « questionnaire QP » pour producteurs et utilisateurs. Les annexes distinguent pourtant QP (producteur) et QU (utilisateur). Le générateur de liens applique cette distinction : QP vers les producteurs, QU vers les équipes utilisatrices. Il ne demande pas aux utilisateurs de remplir un questionnaire de producteur.
2. **Annexes.** Les renvois anciens « annexe 7 » ou « annexe 5 pour QP » ne servent pas de clés de navigation. La référence par code prévaut : QP figure dans l'annexe 1bis ; le relais multilingue dans l'annexe 5. Le rapport fourni n'est pas modifié par ce dossier.
3. **Langues.** Les questionnaires comportent cinq langues. La mention du kiswahili dans la recherche documentaire ne crée pas un sixième questionnaire.
4. **Métadonnées ajoutées.** Service, domaine, origine du lien, relais et langue des réponses libres sont recueillis comme champs facultatifs lorsque la source les exige pour les exports mais ne les réplique pas dans chaque questionnaire. Aucun contenu politique ni nom personnel n'est ajouté.
5. **Longueur des réponses.** Les nombres de mots figurant dans les instruments sont présentés comme repères et déclenchent un avertissement, non une coupure silencieuse. La limite technique de caractères empêche les envois excessifs. Cette modalité préserve l'original et évite une pénalisation mécanique d'une langue ; elle doit être confirmée lors de la recette.
6. **Filtres incomplets.** Une condition non déterminée par une question manquante est notée `FILTRE_INDETERMINE`, non `NF`. Un saut explicitement décidé est `NF`. L'absence de volume peut laisser décrire un cas connu, comme le prévoit l'instrument.
7. **Non-réponse.** Une question attendue vide est exportée `NR_A_CONTROLER`. Le consultant peut la qualifier `NR` dans le registre de codage ; aucune valeur négative ou nulle n'est imputée.
8. **Brouillon.** La conservation est par téléchargement local JSON, pas par enregistrement permanent automatique. Les brouillons sont confidentiels et distincts des réponses reçues. Les anciens brouillons ne sont pas assimilés à des consentements valables sur la nouvelle notice.
9. **Références nouvelles.** QR03bis et QR04bis regroupent dans un champ de texte structuré les renseignements demandés pour chacune des deux références ou recommandations ; leurs statuts restent des codes séparés.
10. **Réponse abrégée.** Elle conserve son code de formulaire RA et son profil d'origine. Elle n'est pas transformée en questionnaire complet et les questions omises ne sont pas remplies.
11. **O1–O8.** Le dossier ne prétend pas fournir ou valider le document stratégique L3. Il fournit les registres et le dispositif nécessaires à la construction, aux essais et à la revue de ces outils.

## Historique et dictionnaire

Version applicative : `2.0.0`. Schéma : `2026-11-15.web-2`. Les instruments QI/QU/QR/RA suivent la référence 1.2 ; QP suit la référence 1.0. Le catalogue de traduction est daté `2026-11-16.v2` et conserve un état de relecture à documenter. `outils/Concordance_methodologie.csv` indique chaque rubrique et ses champs ; `Dictionnaire_5_langues.csv` donne chaque modalité et ses filtres.

La version anglaise antérieure ne remplace pas la source française jointe. Les traductions additionnelles sont une réalisation de ce dossier, non une traduction approuvée par STATAFRIC ou certifiée par un relecteur externe.
