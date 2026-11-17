# Corrections 2.1.0 — 17 novembre 2026

Source exacte : fichier app(1).py fourni par l’utilisateur, autonome v2.0.0. Le schéma JSON des instruments est strictement identique ; les cinq parcours, 44 rubriques, 276 champs, codes, modalités, filtres et dates restent inchangés. Les nouveaux traitements d’interface retirent les choix déjà sélectionnés et rendent les conditions d’envoi explicites.

## Modifications demandées

1. Suppression des chaînes documentaires de modules qui étaient interprétées par l’affichage automatique de Streamlit. Le générateur retire toutes les expressions textuelles isolées au niveau des modules ; magicEnabled est aussi désactivé dans la configuration.
2. Titre commun « Appui statistique aux discours des décideurs » placé avant tout autre contenu principal. Pour une langue autre que le français, le titre traduit suit en sous-titre. Les écrans de relecture, d’envoi en attente et de reçu conservent le titre commun.
3. Bouton « Envoyer ma contribution » cliquable sur la relecture ; conditions affichées une par une et contrôlées à nouveau au clic avec une information de campagne actualisée. Aucune fausse réception, aucun contournement de campagne ni de consentement.
4. Choix multiples gérés par ajout d’une modalité à la fois ; les choix déjà retenus disparaissent de la liste restante. Retrait possible. Les options exclusives et le nombre maximal sont préservés. Les priorités précédentes sont retirées des rangs suivants ; une modification amont remet à vide les rangs devenus incompatibles.
5. Sélection du parcours synchronisée avec les liens et l’état de navigation ; titre propre aux INS, producteurs, utilisateurs, partenaires ou réponse abrégée. Réponses conservées séparément par parcours.
6. Bouton « Précédent » en haut et en bas de la page de relecture finale. Les boutons « Modifier cette rubrique » restent présents. Après un reçu, la réponse enregistrée n’est jamais réécrite ; un complément porte sa référence.

## Autres livraisons

Guide de remplissage français : 55 pages Word, 15 chapitres, 44 rubriques, 276 champs, 50 tableaux. Version HTML avec sommaire et catalogue dépliable des modalités françaises exactes. Exemples pédagogiques fictifs explicitement identifiés.

Dockerfile non privilégié, point d’entrée contrôlé, contrôle de santé, Compose de production avec HTTPS Caddy et redémarrage unless-stopped, Compose de test local. L’hôte doit fonctionner en permanence et la base rester disponible ; le Dockerfile ne neutralise pas la veille d’un hébergeur.

## Compatibilité

Pas de suppression des réponses existantes. Pas de clé secrète incluse. Brouillons du schéma 2026-11-15.web-2 compatibles. SQL 05 facultatif pour enrichir le diagnostic de campagne, sans modification des tables. Les sources, admin.py et relais_ins.py ont été régénérés pour éviter le retour des chaînes techniques lors d’une reconstruction.

## Vérification

220 tests locaux réussis ; groupe Streamlit AppTest non exécuté dans cet environnement faute de bibliothèque disponible. Les 25 scénarios d’envoi local sont des tests automatisés avec interface simulée et stockage SQLite, pas des essais navigateur ou Supabase. Construction Docker et recette distante à effectuer sur l’hôte cible. Voir VERIFICATION_2_1.txt.
