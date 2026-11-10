import csv,io,json,zipfile
import pytest
from diagnostic.engine import prepare_submission
from diagnostic.exports import (csv_safe,csv_bytes,export_bundle,response_rows,contribution_rows,dictionary_rows,CONTRIBUTIONS,REPONSES)

@pytest.mark.parametrize('value',['=1+1','+CMD','-1+2','@SUM(A1)','  =SUM(A1)','\t=1','\n=1'])
def test_csv_injection(value):assert csv_safe(value).startswith("'")

@pytest.mark.parametrize('value',[0,98,99,'Texte normal','https://example.org'])
def test_safe_values_preserved(value):assert csv_safe(value)==str(value)

def record(ins_answers):
    p,c=prepare_submission('ins',ins_answers,is_test=True)
    return {'id':p['submission_id'],'receipt':'REP-'+p['submission_id'],'received_at':'2026-11-10T12:00:00+00:00','payload':p},c

def test_export_contact_exclusion_and_raw_preserved(ins_answers):
    ins_answers['QI12_Contact_courriel']='service@example.org';ins_answers['QI01_INS']='=1+1'
    r,c=record(ins_answers)
    with zipfile.ZipFile(io.BytesIO(export_bundle([r]))) as z:
        assert 'Contributions.csv' in z.namelist() and 'Reponses.csv' in z.namelist()
        for n in z.namelist():assert b'service@example.org' not in z.read(n)
        original=json.loads(z.read(r['receipt']+'.json'))
        assert original['payload']['answers']['QI01_INS']=='=1+1'
        assert "'=1+1" in z.read('Contributions.csv').decode('utf-8-sig')

def test_columns_and_long_format(ins_answers):
    r,_=record(ins_answers)
    cr=contribution_rows([r])[0];assert list(cr)==CONTRIBUTIONS
    rows=response_rows([r]);assert list(rows[0])==REPONSES
    choices=[x for x in rows if x['Code question']=='QI06a']
    assert {x['Code analytique'] for x in choices}=={1,3}
    assert all('_Contact_' not in x['Code question'] for x in rows)
    assert cr['Exploitabilité']=='À examiner'

def test_dictionary_bilingual():
    rows=dictionary_rows();assert len(rows)>500
    assert all(r['Libellé français'] and r['Libellé anglais'] for r in rows)

def test_export_blanks_not_zero(ins_answers):
    ins_answers.update(QI05_S1=0,QI05_S2=98,QI05_S3=99)
    r,_=record(ins_answers);rows={x['Code question']:x for x in response_rows([r])}
    assert rows['QI05_S1']['Code analytique']==0
    assert rows['QI05_S2']['Statut information']=='NSP'
    assert rows['QI05_S3']['Statut information']=='NA'
    assert rows['QI05_S4']['Code analytique']=='' and rows['QI05_S4']['Statut information']=='Blanc à contrôler'
