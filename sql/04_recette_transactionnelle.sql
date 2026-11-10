-- RECETTE SUR DONNÉES FICTIVES. À exécuter après 01_schema.sql dans SQL Editor.
-- Transaction annulée intégralement : aucune contribution de test n'est conservée.
-- Exécuter le script entier, ROLLBACK compris. En cas d'erreur, lancer ROLLBACK.
BEGIN;
DO $$
DECLARE
  campaign text := 'recette-hiswaca-' || gen_random_uuid()::text;
  token text := 'JETON_DE_TEST_FICTIF_NON_UTILISABLE_EN_PRODUCTION_2026';
  uid uuid := gen_random_uuid();
  p jsonb;
  c jsonb := jsonb_build_object('QI12_Contact_courriel','test-fictif@example.org');
  first_receipt jsonb;
  second_receipt jsonb;
  info jsonb;
  failed boolean;
  n integer;
BEGIN
  IF has_table_privilege('anon','public.diag_responses','SELECT')
     OR has_table_privilege('authenticated','public.diag_contacts','SELECT')
     OR has_table_privilege('anon','public.diag_responses','INSERT') THEN
    RAISE EXCEPTION 'RECETTE_ECHEC : accès direct public aux tables';
  END IF;
  INSERT INTO public.diag_campaigns(id,writer_token_hash,active,opens_at,closes_at,max_responses)
    VALUES(campaign,encode(sha256(convert_to(token,'UTF8')),'hex'),true,now()-interval '1 hour',now()+interval '1 hour',2);
  info := public.diag_campaign_info(campaign,token);
  IF (info->>'active') IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'RECETTE_ECHEC : campagne'; END IF;
  p := jsonb_build_object('submission_id',uid::text,'form_id','ins','language','fr',
    'schema_version','2026-11-08.web-1','instrument_hash',repeat('0',64),'app_version','test',
    'is_test',true,'citation_permission',0,'citation_explicit',false,'short_form',false,'warnings','[]'::jsonb,
    'answers',jsonb_build_object('QI01_Participation',1,'QI01_INS','INS fictif de recette'),
    'field_states',jsonb_build_object('QI01_Participation','RENSEIGNE','QI01_INS','RENSEIGNE'));
  first_receipt := public.diag_submit(campaign,token,'',p,c);
  second_receipt := public.diag_submit(campaign,token,'',p,c);
  IF first_receipt->>'receipt' IS DISTINCT FROM second_receipt->>'receipt'
     OR (second_receipt->>'duplicate') IS DISTINCT FROM 'true' THEN
    RAISE EXCEPTION 'RECETTE_ECHEC : idempotence';
  END IF;
  SELECT count(*) INTO n FROM public.diag_responses WHERE campaign_id=campaign;
  IF n<>1 THEN RAISE EXCEPTION 'RECETTE_ECHEC : double enregistrement'; END IF;
  IF NOT EXISTS(SELECT 1 FROM public.diag_contacts WHERE response_id=uid) THEN
    RAISE EXCEPTION 'RECETTE_ECHEC : contact séparé'; END IF;
  failed:=false;
  BEGIN
    PERFORM public.diag_submit(campaign,token,'',p || jsonb_build_object('language','en'),c);
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM='SUBMISSION_CONFLICT' THEN failed:=true; ELSE RAISE; END IF;
  END;
  IF NOT failed THEN RAISE EXCEPTION 'RECETTE_ECHEC : modification silencieuse'; END IF;
  failed:=false;
  BEGIN
    PERFORM public.diag_submit(campaign,'mauvais-jeton','',p,c);
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM='INVALID_WRITER' THEN failed:=true; ELSE RAISE; END IF;
  END;
  IF NOT failed THEN RAISE EXCEPTION 'RECETTE_ECHEC : jeton'; END IF;
  UPDATE public.diag_campaigns SET active=false WHERE id=campaign;
  second_receipt := public.diag_submit(campaign,token,'',p,c); -- réessai identique autorisé
  failed:=false;
  BEGIN
    PERFORM public.diag_submit(campaign,token,'',p || jsonb_build_object('submission_id',gen_random_uuid()::text),c);
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM='COLLECTION_CLOSED' THEN failed:=true; ELSE RAISE; END IF;
  END;
  IF NOT failed THEN RAISE EXCEPTION 'RECETTE_ECHEC : fermeture'; END IF;
  RAISE NOTICE 'RECETTE SQL RÉUSSIE : droits de table, campagne, séparation des contacts, idempotence, conflit, jeton et fermeture.';
END $$;
ROLLBACK;
