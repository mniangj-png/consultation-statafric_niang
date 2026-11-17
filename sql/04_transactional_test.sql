-- RECETTE SQL dans un projet de test. Aucune donnée réelle. ROLLBACK final.
BEGIN;
INSERT INTO public.diag_campaigns VALUES('recette-v2',encode(sha256(convert_to(repeat('x',40),'UTF8')),'hex'),NULL,true,now()-interval '1 day',now()+interval '1 day',20,true);
DO $$
DECLARE p jsonb; c jsonb:='{}'; first_receipt jsonb; second_receipt jsonb; uid uuid:=gen_random_uuid(); n integer;
BEGIN
 p:=jsonb_build_object('submission_id',uid,'form_id','producteurs','language','ar','schema_version','2026-11-15.web-2','instrument_hash',repeat('a',64),'is_test',true,'citation_explicit',false,'citation_permission',0,'answers',jsonb_build_object('QP01_Participation',1,'QP01_Produit',1,'QP01_Institution','TEST مؤسّسة'),'field_states','{}'::jsonb);
 first_receipt:=public.diag_submit('recette-v2',repeat('x',40),'',p,c);
 second_receipt:=public.diag_submit('recette-v2',repeat('x',40),'',p,c);
 IF first_receipt->>'receipt' <> second_receipt->>'receipt' OR second_receipt->>'duplicate'<>'true' THEN RAISE EXCEPTION 'RETRY_FAILED'; END IF;
 SELECT count(*) INTO n FROM public.diag_responses WHERE campaign_id='recette-v2';
 IF n<>1 THEN RAISE EXCEPTION 'DUPLICATE_ROW'; END IF;
 UPDATE public.diag_campaigns SET active=false WHERE id='recette-v2';
 PERFORM public.diag_submit('recette-v2',repeat('x',40),'',p,c); -- identical retry after close
 BEGIN
   p:=jsonb_set(p,'{submission_id}',to_jsonb(gen_random_uuid()));
   PERFORM public.diag_submit('recette-v2',repeat('x',40),'',p,c);
   RAISE EXCEPTION 'CLOSED_NOT_ENFORCED';
 EXCEPTION WHEN raise_exception THEN
   IF SQLERRM<>'COLLECTION_CLOSED' THEN RAISE; END IF;
 END;
END $$;
ROLLBACK;
