-- Lecture seule : aucun droit SELECT/INSERT public sur les tables de données.
SELECT tablename,rowsecurity FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'diag_%';
SELECT table_name,grantee,privilege_type FROM information_schema.role_table_grants
 WHERE table_schema='public' AND table_name LIKE 'diag_%' ORDER BY table_name,grantee;
SELECT p.proname,p.prosecdef,p.proconfig FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid
 WHERE n.nspname='public' AND p.proname LIKE 'diag_%';
SELECT has_table_privilege('anon','public.diag_responses','select') AS anon_can_read_responses,
 has_table_privilege('authenticated','public.diag_contacts','select') AS authenticated_can_read_contacts,
 has_table_privilege('anon','public.diag_registry_events','select') AS anon_can_read_registries;
-- Attendu : les trois valeurs ci-dessus sont false.
SELECT has_function_privilege('anon','public.diag_submit(text,text,text,jsonb,jsonb)','execute') AS anon_submit,
 has_function_privilege('anon','public.diag_registry_save(text,text,uuid,integer,jsonb,text,boolean)','execute') AS anon_registry_write;
-- Attendu : true (RPC protégée par jeton), false (administration uniquement).
