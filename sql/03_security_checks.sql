-- Contrôles à exécuter dans le SQL Editor après installation.
-- expected : quatre RLS activées ; aucune permission anon/authenticated sur les tables.
SELECT tablename, rowsecurity FROM pg_tables
WHERE schemaname='public' AND tablename IN ('diag_campaigns','diag_responses','diag_contacts','diag_reviews');
SELECT table_name, grantee, privilege_type FROM information_schema.table_privileges
WHERE table_schema='public' AND table_name LIKE 'diag_%' AND grantee IN ('anon','authenticated');
-- expected : zéro policy permissive sur ces quatre tables.
SELECT tablename,policyname,roles,cmd FROM pg_policies
WHERE schemaname='public' AND tablename LIKE 'diag_%';
-- expected : search_path vide pour les deux fonctions SECURITY DEFINER.
SELECT proname,prosecdef,proconfig FROM pg_proc
JOIN pg_namespace ON pg_namespace.oid=pg_proc.pronamespace
WHERE nspname='public' AND proname IN ('diag_submit','diag_campaign_info');
