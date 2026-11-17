-- HISWACA v2 — Installation ou migration non destructive sur un projet dédié.
-- Sauvegarder et fermer la campagne précédente avant migration. Aucune réponse n’est supprimée.
-- Les réponses ne sont JAMAIS lisibles avec la clé publique.
-- Ne pas substituer une policy permissive à cette configuration.
BEGIN;
CREATE TABLE IF NOT EXISTS public.diag_campaigns (
  id text PRIMARY KEY,
  writer_token_hash text NOT NULL CHECK (length(writer_token_hash)=64),
  access_code_hash text,
  active boolean NOT NULL DEFAULT false,
  opens_at timestamptz NOT NULL,
  closes_at timestamptz NOT NULL,
  max_responses integer NOT NULL DEFAULT 3000 CHECK(max_responses BETWEEN 1 AND 50000),
  CHECK (closes_at > opens_at)
);
CREATE TABLE IF NOT EXISTS public.diag_responses (
  id uuid PRIMARY KEY,
  campaign_id text NOT NULL REFERENCES public.diag_campaigns(id),
  receipt text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now(),
  content_hash text NOT NULL,
  form_id text NOT NULL CHECK(form_id IN ('ins','producteurs','utilisateurs','partenaires','abrege')),
  is_test boolean NOT NULL,
  payload jsonb NOT NULL,
  UNIQUE(id,campaign_id)
);
ALTER TABLE public.diag_responses DROP CONSTRAINT IF EXISTS diag_responses_form_id_check;
ALTER TABLE public.diag_responses ADD CONSTRAINT diag_responses_form_id_check
  CHECK(form_id IN ('ins','producteurs','utilisateurs','partenaires','abrege'));
-- NULL is retained for legacy campaigns; v2 campaigns must explicitly set test_mode.
ALTER TABLE public.diag_campaigns ADD COLUMN IF NOT EXISTS test_mode boolean;
CREATE TABLE IF NOT EXISTS public.diag_contacts (
  id uuid PRIMARY KEY,
  response_id uuid NOT NULL UNIQUE,
  campaign_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  contact jsonb NOT NULL,
  FOREIGN KEY(response_id,campaign_id) REFERENCES public.diag_responses(id,campaign_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS public.diag_reviews (
  id uuid PRIMARY KEY,
  campaign_id text NOT NULL,
  response_id uuid NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  actor text NOT NULL,
  status text NOT NULL CHECK(status IN ('À examiner','Exploitable','À clarifier','Doublon','Hors champ')),
  note text NOT NULL CHECK(length(note) BETWEEN 1 AND 5000),
  linked_to uuid REFERENCES public.diag_responses(id),
  FOREIGN KEY(response_id,campaign_id) REFERENCES public.diag_responses(id,campaign_id),
  CHECK (status <> 'Doublon' OR linked_to IS NOT NULL),
  CHECK (linked_to IS NULL OR linked_to <> response_id)
);
CREATE INDEX IF NOT EXISTS diag_responses_campaign_date ON public.diag_responses(campaign_id,created_at,id);
CREATE INDEX IF NOT EXISTS diag_reviews_response_date ON public.diag_reviews(response_id,created_at,id);
CREATE INDEX IF NOT EXISTS diag_contacts_campaign ON public.diag_contacts(campaign_id);

ALTER TABLE public.diag_campaigns ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.diag_responses ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.diag_contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.diag_reviews ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.diag_campaigns,public.diag_responses,public.diag_contacts,public.diag_reviews FROM PUBLIC,anon,authenticated;
REVOKE ALL ON TABLE public.diag_campaigns,public.diag_responses,public.diag_contacts,public.diag_reviews FROM service_role;
GRANT SELECT,UPDATE ON public.diag_campaigns TO service_role;
GRANT SELECT ON public.diag_responses,public.diag_contacts TO service_role;
GRANT SELECT,INSERT ON public.diag_reviews TO service_role;

CREATE OR REPLACE FUNCTION public.diag_campaign_info(p_campaign text,p_writer_token text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $$
DECLARE c public.diag_campaigns%ROWTYPE;
BEGIN
  SELECT * INTO c FROM public.diag_campaigns WHERE id=p_campaign;
  IF c.id IS NULL OR p_writer_token IS NULL OR length(p_writer_token)<32
      OR c.writer_token_hash <> encode(sha256(convert_to(p_writer_token,'UTF8')),'hex') THEN
    RAISE EXCEPTION 'INVALID_WRITER';
  END IF;
  RETURN jsonb_build_object('active', c.active AND now()>=c.opens_at AND now()<=c.closes_at,
    'requires_access_code',c.access_code_hash IS NOT NULL,'opens_at',c.opens_at,'closes_at',c.closes_at);
END;
$$;

CREATE OR REPLACE FUNCTION public.diag_submit(p_campaign text,p_writer_token text,p_access_code text,p_payload jsonb,p_contacts jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = '' AS $$
DECLARE
  c public.diag_campaigns%ROWTYPE;
  previous public.diag_responses%ROWTYPE;
  uid uuid;
  form text;
  consent_field text;
  receipt_id text;
  fingerprint text;
  created timestamptz;
  key_name text;
BEGIN
  -- Sérialise les soumissions par campagne : contrôle du quota et double clic atomiques.
  SELECT * INTO c FROM public.diag_campaigns WHERE id=p_campaign FOR UPDATE;
  IF c.id IS NULL OR p_writer_token IS NULL OR length(p_writer_token)<32
      OR c.writer_token_hash <> encode(sha256(convert_to(p_writer_token,'UTF8')),'hex') THEN
    RAISE EXCEPTION 'INVALID_WRITER';
  END IF;
  IF c.access_code_hash IS NOT NULL AND (p_access_code IS NULL OR
      c.access_code_hash <> encode(sha256(convert_to(p_access_code,'UTF8')),'hex')) THEN
    RAISE EXCEPTION 'INVALID_ACCESS_CODE';
  END IF;
  IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object' OR octet_length(p_payload::text)>500000
      OR jsonb_typeof(p_contacts) IS DISTINCT FROM 'object' OR octet_length(p_contacts::text)>10000
      OR jsonb_typeof(p_payload->'answers') IS DISTINCT FROM 'object'
      OR jsonb_typeof(p_payload->'field_states') IS DISTINCT FROM 'object'
      OR coalesce(p_payload->>'language','') NOT IN ('fr','en','ar','pt','es')
      OR coalesce(p_payload->>'schema_version','') NOT IN ('2026-11-08.web-1','2026-11-15.web-2')
      OR jsonb_typeof(p_payload->'is_test') IS DISTINCT FROM 'boolean'
      OR jsonb_typeof(p_payload->'citation_explicit') IS DISTINCT FROM 'boolean'
      OR coalesce(p_payload->>'citation_permission','') NOT IN ('0','1','2','3')
      OR coalesce(p_payload->>'instrument_hash','') !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION 'INVALID_SUBMISSION';
  END IF;
  form := p_payload->>'form_id';
  consent_field := CASE form WHEN 'ins' THEN 'QI01_Participation' WHEN 'utilisateurs' THEN 'QU01_Participation'
    WHEN 'producteurs' THEN 'QP01_Participation' WHEN 'partenaires' THEN 'QR01_Participation' WHEN 'abrege' THEN 'RA01_Participation' ELSE NULL END;
  IF consent_field IS NULL OR (p_payload->'answers'->>consent_field) IS DISTINCT FROM '1' THEN
    RAISE EXCEPTION 'INVALID_SUBMISSION';
  END IF;
  IF form='producteurs' AND p_payload->'answers'->>'QP01_Produit'='0' THEN
    RAISE EXCEPTION 'INVALID_SUBMISSION';
  END IF;
  IF p_payload->>'citation_permission' IN ('1','2') AND
     coalesce(p_payload->>'attribution_authority_confirmed','false') <> 'true' THEN
    RAISE EXCEPTION 'INVALID_SUBMISSION';
  END IF;
  -- Défense en profondeur : aucun champ de contact structuré dans les réponses.
  FOR key_name IN SELECT jsonb_object_keys(p_payload->'answers') LOOP
    IF key_name ILIKE '%contact%' THEN RAISE EXCEPTION 'INVALID_SUBMISSION'; END IF;
  END LOOP;
  FOR key_name IN SELECT jsonb_object_keys(p_contacts) LOOP
    IF key_name NOT LIKE '%_Contact_%' THEN RAISE EXCEPTION 'INVALID_SUBMISSION'; END IF;
  END LOOP;
  BEGIN uid := (p_payload->>'submission_id')::uuid;
  EXCEPTION WHEN invalid_text_representation THEN RAISE EXCEPTION 'INVALID_SUBMISSION'; END;
  IF uid IS NULL THEN RAISE EXCEPTION 'INVALID_SUBMISSION'; END IF;
  fingerprint := encode(sha256(convert_to(jsonb_build_array(p_payload,p_contacts)::text,'UTF8')),'hex');
  SELECT * INTO previous FROM public.diag_responses WHERE id=uid;
  IF previous.id IS NOT NULL THEN
    IF previous.campaign_id <> p_campaign OR previous.content_hash <> fingerprint THEN
      RAISE EXCEPTION 'SUBMISSION_CONFLICT';
    END IF;
    -- Un nouvel essai identique reste recevable après fermeture, sans nouvelle ligne.
    RETURN jsonb_build_object('receipt',previous.receipt,'received_at',previous.created_at,'duplicate',true);
  END IF;
  IF p_payload->>'schema_version'='2026-11-15.web-2' AND c.test_mode IS NULL THEN
    RAISE EXCEPTION 'TEST_MODE_MISMATCH';
  END IF;
  IF c.test_mode IS NOT NULL AND (p_payload->>'is_test')::boolean <> c.test_mode THEN
    RAISE EXCEPTION 'TEST_MODE_MISMATCH';
  END IF;
  IF NOT c.active OR now()<c.opens_at OR now()>c.closes_at THEN RAISE EXCEPTION 'COLLECTION_CLOSED'; END IF;
  IF (SELECT count(*) FROM public.diag_responses WHERE campaign_id=p_campaign)>=c.max_responses THEN
    RAISE EXCEPTION 'QUOTA_REACHED';
  END IF;
  receipt_id := 'REP-' || uid::text;
  created := clock_timestamp();
  INSERT INTO public.diag_responses(id,campaign_id,receipt,created_at,content_hash,form_id,is_test,payload)
    VALUES(uid,p_campaign,receipt_id,created,fingerprint,form,(p_payload->>'is_test')::boolean,p_payload);
  IF p_contacts<>'{}'::jsonb THEN
    INSERT INTO public.diag_contacts(id,response_id,campaign_id,created_at,contact) VALUES(uid,uid,p_campaign,created,p_contacts);
  END IF;
  RETURN jsonb_build_object('receipt',receipt_id,'received_at',created,'duplicate',false);
END;
$$;
REVOKE ALL ON FUNCTION public.diag_campaign_info(text,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.diag_submit(text,text,text,jsonb,jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.diag_campaign_info(text,text) TO anon;
GRANT EXECUTE ON FUNCTION public.diag_submit(text,text,text,jsonb,jsonb) TO anon;

-- Registers are accessible only to the authenticated administrator server.
CREATE TABLE IF NOT EXISTS public.diag_registry_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  campaign_id text NOT NULL REFERENCES public.diag_campaigns(id),
  record_id uuid NOT NULL,
  kind text NOT NULL CHECK (kind IN ('invitations','documents','discours','constats','traductions','tests','observations','recette')),
  version integer NOT NULL CHECK(version>0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  actor text NOT NULL,
  is_test boolean NOT NULL,
  data jsonb NOT NULL CHECK(jsonb_typeof(data)='object'),
  UNIQUE(campaign_id,record_id,version)
);
CREATE INDEX IF NOT EXISTS diag_registry_campaign ON public.diag_registry_events(campaign_id,kind,created_at,id);
ALTER TABLE public.diag_registry_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.diag_registry_events FROM PUBLIC,anon,authenticated,service_role;
GRANT SELECT ON public.diag_registry_events TO service_role;

CREATE OR REPLACE FUNCTION public.diag_registry_save(
 p_campaign text,p_kind text,p_record_id uuid,p_expected_version integer,
 p_data jsonb,p_actor text,p_is_test boolean)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path='' AS $$
DECLARE v integer; previous_kind text; eid uuid; created timestamptz;
BEGIN
 IF p_kind NOT IN ('invitations','documents','discours','constats','traductions','tests','observations','recette')
    OR p_record_id IS NULL OR p_expected_version IS NULL OR p_expected_version<0
    OR jsonb_typeof(p_data) IS DISTINCT FROM 'object' OR octet_length(p_data::text)>200000
    OR p_actor IS NULL OR length(p_actor)<1 OR length(p_actor)>254 OR p_is_test IS NULL THEN
   RAISE EXCEPTION 'INVALID_SUBMISSION';
 END IF;
 IF NOT EXISTS (SELECT 1 FROM public.diag_campaigns WHERE id=p_campaign) THEN RAISE EXCEPTION 'INVALID_SUBMISSION'; END IF;
 IF (SELECT test_mode FROM public.diag_campaigns WHERE id=p_campaign) IS TRUE AND p_is_test IS NOT TRUE THEN RAISE EXCEPTION 'TEST_MODE_MISMATCH'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(p_campaign || p_record_id::text,0));
 SELECT max(version) INTO v FROM public.diag_registry_events WHERE campaign_id=p_campaign AND record_id=p_record_id;
 v:=coalesce(v,0);
 IF v<>p_expected_version THEN RAISE EXCEPTION 'VERSION_CONFLICT'; END IF;
 SELECT kind INTO previous_kind FROM public.diag_registry_events WHERE campaign_id=p_campaign AND record_id=p_record_id LIMIT 1;
 IF previous_kind IS NOT NULL AND previous_kind<>p_kind THEN RAISE EXCEPTION 'INVALID_SUBMISSION'; END IF;
 eid:=gen_random_uuid();created:=clock_timestamp();
 INSERT INTO public.diag_registry_events(id,campaign_id,record_id,kind,version,created_at,actor,is_test,data)
 VALUES(eid,p_campaign,p_record_id,p_kind,v+1,created,p_actor,p_is_test,p_data);
 RETURN jsonb_build_object('id',eid,'record_id',p_record_id,'version',v+1,'created_at',created);
END; $$;
REVOKE ALL ON FUNCTION public.diag_registry_save(text,text,uuid,integer,jsonb,text,boolean) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.diag_registry_save(text,text,uuid,integer,jsonb,text,boolean) TO service_role;
COMMIT;
