-- Ne PAS publier une version complétée dans GitHub.
-- Remplacer le SHA-256 ci-dessous par celui de WRITER_TOKEN.
-- scripts/generate_secrets.py prépare un fichier local privé prêt à adapter.
-- Campagne fermée au départ. Ouvrir après la recette et l'accord d'hébergement.
INSERT INTO public.diag_campaigns(id,writer_token_hash,access_code_hash,active,opens_at,closes_at,max_responses)
VALUES (
  'discours-2026',
  'REMPLACER_PAR_SHA256_64_CARACTERES',
  NULL, -- NULL = consultation ouverte ; sinon SHA-256 d'un code d'accès diffusé séparément.
  false,
  '2026-11-10T00:00:00Z',
  '2026-12-14T23:59:59Z', -- Clôture technique proposée, MODIFIABLE ; distincte de la réponse souhaitée le 17/11.
  3000
);
-- Après validation de la configuration et une recette complète :
-- UPDATE public.diag_campaigns SET active=true WHERE id='discours-2026';
