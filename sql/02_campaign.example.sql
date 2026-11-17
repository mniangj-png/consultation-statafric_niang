-- Remplacer uniquement les valeurs signalées. Ne pas publier de jeton en clair.
-- Les campagnes v1 et leurs réponses ne sont pas supprimées.
-- TEST : utiliser un identifiant et un jeton distincts de la collecte réelle.
INSERT INTO public.diag_campaigns(id,writer_token_hash,access_code_hash,active,opens_at,closes_at,max_responses,test_mode)
VALUES ('discours-2026-v2-test','REMPLACER_PAR_EMPREINTE_SHA256_64_CARACTERES',NULL,false,
 '2026-11-16 00:00:00+00','2026-12-31 23:59:59+00',10000,true);
-- Réel : créer seulement après recette. Utiliser un autre jeton.
-- INSERT INTO public.diag_campaigns(id,writer_token_hash,access_code_hash,active,opens_at,closes_at,max_responses,test_mode)
-- VALUES ('discours-2026-v2','AUTRE_EMPREINTE_SHA256_64_CARACTERES',NULL,false,
-- '2026-11-19 00:00:00+00','2026-11-26 23:59:59.999999+00',10000,false);
-- Pour ouvrir après autorisation : administration ou SQL explicite.
-- UPDATE public.diag_campaigns SET active=true WHERE id='discours-2026-v2';
-- Les compléments après le 26 nécessitent une décision explicite et un journal.
-- Ne pas prolonger la collecte automatiquement ni changer le mode d'une campagne existante.
