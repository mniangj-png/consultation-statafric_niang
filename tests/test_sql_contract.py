"""Contrôles statiques UNIQUEMENT : ne remplacent pas l'exécution sur Supabase."""
from pathlib import Path
SQL=(Path(__file__).parents[1]/'sql/01_schema.sql').read_text()

def test_rls_every_table():
    for table in ['diag_campaigns','diag_responses','diag_contacts','diag_reviews']:
        assert f'ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY' in SQL

def test_privilege_and_functions():
    assert 'FROM PUBLIC,anon,authenticated' in SQL
    assert SQL.count("SECURITY DEFINER SET search_path = ''")==2
    assert 'FOR UPDATE' in SQL and 'SUBMISSION_CONFLICT' in SQL
    assert 'content_hash' in SQL and 'INVALID_WRITER' in SQL
    assert 'CREATE POLICY' not in SQL

def test_atomic_records_and_separate_contacts():
    assert 'INSERT INTO public.diag_responses' in SQL and 'INSERT INTO public.diag_contacts' in SQL
    assert "IF key_name ILIKE '%contact%'" in SQL
    assert "FOREIGN KEY(response_id,campaign_id)" in SQL
