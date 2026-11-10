import json
from uuid import uuid4
import pytest
from diagnostic.engine import (ValidationError,field_states,validation,prepare_submission,draft_bytes,load_draft,
                               active_sections,answer_label,receipt_text)
from diagnostic.schema import FORMS,iter_fields

@pytest.mark.parametrize('form_id',list(FORMS))
def test_submission_with_partial_data(form_id,minimal):
    payload,contacts=prepare_submission(form_id,minimal[form_id],is_test=True)
    assert payload['citation_permission']==0 and payload['citation_explicit'] is False
    assert payload['short_form']==(form_id=='abrege') and not contacts
    assert payload['is_test'] is True

@pytest.mark.parametrize('form_id',list(FORMS))
def test_no_consent_no_submission(form_id,minimal):
    for code in [None,0]:
        minimal[form_id][FORMS[form_id]['consent']]=code
        with pytest.raises(ValidationError): prepare_submission(form_id,minimal[form_id])

@pytest.mark.parametrize('form_id',list(FORMS))
def test_consent_only_rejected(form_id):
    with pytest.raises(ValidationError): prepare_submission(form_id,{FORMS[form_id]['consent']:1})

def test_ins_no_request_still_correction(ins_answers):
    ins_answers.update(QI02a=0,QI03_Situation=1,QI08_Situation=1,QI08_Actions=[2,3])
    effective,states=field_states('ins',ins_answers)
    assert 'QI03_Situation' not in effective
    for section,field in iter_fields('ins'):
        if section['id'] in ['QI03','QI04','QI05','QI06','QI07']: assert states[field['id']]=='NF'
    assert effective['QI08_Actions']==[2,3] and states['QI08_Situation']=='RENSEIGNE'
    ins_answers['QI02a']=1
    # Une réponse filtrée est supprimée, pas réutilisée au changement de parcours.
    effective['QI02a']=1
    assert 'QI03_Situation' not in field_states('ins',effective)[0]

@pytest.mark.parametrize('case,expected',[(2,'NR_A_CONTROLER'),(3,'NF'),(98,'NF')])
def test_ins_closed_without_figures(case,expected,ins_answers):
    ins_answers['QI03_Situation']=case
    ins_answers.pop('QI04a',None)
    _,states=field_states('ins',ins_answers)
    assert states['QI04a']==expected
    assert states['QI05_Origine']=='NF' and states['QI07a']=='NF'

@pytest.mark.parametrize('volume,case',[(0,1),(1,0),(1,98)])
def test_users_skip_to_priorities(volume,case):
    a={'QU01_Participation':1,'QU02a':volume,'QU03_Cas':case,'QU04_M1':1,'QU06a':1,'QU07_P1':3}
    e,s=field_states('utilisateurs',a)
    assert s['QU04_M1']=='NF' and s['QU06a']=='NF'
    assert e['QU07_P1']==3

def test_unknown_filter_is_not_nonresponse():
    _,s=field_states('ins',{'QI01_Participation':1,'QI02a':98})
    assert s['QI03_Situation']=='NR_A_CONTROLER'
    assert s['QI05_S1']=='FILTRE_INDETERMINE'

def test_zero_unknown_na_missing(ins_answers):
    ins_answers.update(QI05_S1=0,QI05_S2=98,QI05_S3=99)
    e,s=field_states('ins',ins_answers)
    assert e['QI05_S1']==0 and s['QI05_S1']=='RENSEIGNE'
    assert s['QI05_S2']=='NSP' and s['QI05_S3']=='NA' and s['QI05_S4']=='NR_A_CONTROLER'

@pytest.mark.parametrize('value',[[0,1],[98,3],[1,1],[1,2,3,4]])
def test_multiple_choices_rejected(value,ins_answers):
    ins_answers['QI07a']=value
    assert validation('ins',ins_answers)[0]

@pytest.mark.parametrize('values',[(1,1,None),(1,2,2),(1,None,3)])
def test_priorities_rank_check(values,ins_answers):
    ins_answers.update({f'QI10_P{i}':v for i,v in enumerate(values,1)})
    assert validation('ins',ins_answers)[0]

def test_exclusive_priority_cleans_stale_ranks(ins_answers):
    ins_answers.update(QI10_P1=0,QI10_P2=2,QI10_P3=3)
    e,s=field_states('ins',ins_answers)
    assert 'QI10_P2' not in e and s['QI10_P2']=='NF'

@pytest.mark.parametrize('field,limit',[('QI10_Difficulte',40),('QI11_Apport',60)])
def test_word_limits(field,limit,ins_answers):
    ins_answers['QI11_Type']=1
    ins_answers[field]=' '.join(['mot']*(limit+1))
    assert validation('ins',ins_answers)[0]
    ins_answers[field]=' '.join(['mot']*limit)
    assert not validation('ins',ins_answers)[0]

def test_partner_word_limit(minimal):
    a=minimal['partenaires'];a['QR03_Pratiques']=' '.join(['mot']*81)
    assert validation('partenaires',a)[0]

@pytest.mark.parametrize('month,errors,warnings',[('13/2026',True,False),('2026-10',True,False),('10/2025',False,True),('10/2026',False,False)])
def test_reference_month(month,errors,warnings,ins_answers):
    ins_answers['QI03_Mois']=month
    e,w=validation('ins',ins_answers)
    assert bool(e)==errors and bool(w)==warnings

def test_contacts_separate_and_email_checked(ins_answers):
    ins_answers['QI12_Contact_courriel']='service@example.org'
    ins_answers['QI12_Contact_fonction']='Service fictif'
    p,c=prepare_submission('ins',ins_answers,is_test=True)
    assert c['QI12_Contact_courriel']=='service@example.org'
    assert 'QI12_Contact_courriel' not in p['answers']
    assert not any('_Contact_' in k for k in p['field_states'])
    ins_answers['QI12_Contact_courriel']='pas un courriel'
    assert validation('ins',ins_answers)[0]

@pytest.mark.parametrize('lang',['fr','en'])
@pytest.mark.parametrize('form_id',list(FORMS))
def test_draft_round_trip(form_id,lang,minimal):
    uid=str(uuid4());b=draft_bytes(form_id,minimal[form_id],lang,uid)
    obj=load_draft(b)
    assert obj['submission_id']==uid and obj['language']==lang and obj['answers']==minimal[form_id]

@pytest.mark.parametrize('content',[b'no json',b'[]',b'{}',b'x'*150001,b'\xff'])
def test_invalid_drafts(content):
    with pytest.raises(ValidationError):load_draft(content)

@pytest.mark.parametrize('mutation',[{'form_id':[]},{'language':'xx'},{'submission_id':'bad'},{'answers':{'INCONNU':'test'}}])
def test_invalid_draft_shape(mutation,minimal):
    obj=json.loads(draft_bytes('ins',minimal['ins'],'fr',str(uuid4())))
    obj.update(mutation)
    with pytest.raises(ValidationError):load_draft(json.dumps(obj).encode())

def test_malformed_nested_answer(minimal):
    a=minimal['ins'];a['QI10_P1']={'danger':'not code'}
    obj=json.loads(draft_bytes('ins',a,'fr',str(uuid4())))
    with pytest.raises(ValidationError):load_draft(json.dumps(obj).encode())

def test_citation_explicit_distinction(ins_answers):
    ins_answers.pop('QI12_Attribution')
    p,_=prepare_submission('ins',ins_answers);assert p['citation_permission']==0 and not p['citation_explicit']
    ins_answers['QI12_Attribution']=0
    p,_=prepare_submission('ins',ins_answers);assert p['citation_permission']==0 and p['citation_explicit']

def test_no_new_request_with_correction_warning_not_error(ins_answers):
    ins_answers.update(QI02a=0,QI02b=99,QI08_Situation=1,QI09_D1=1)
    e,w=validation('ins',ins_answers);assert not e and w

def test_receipt_excludes_structured_contact(ins_answers):
    ins_answers['QI12_Contact_courriel']='service@example.org'
    p,c=prepare_submission('ins',ins_answers,is_test=True)
    receipt={'receipt':'REP-'+p['submission_id'],'received_at':'2026-11-10T13:00:00+00:00'}
    rendered=receipt_text(p,receipt)
    assert 'service@example.org' not in rendered and 'TEST' in rendered

def test_rank_other_precision_not_falsely_missing(ins_answers):
    ins_answers.update(QI10_P1=97,QI10_Autre='Priorité fictive de test')
    errors,warnings=validation('ins',ins_answers)
    assert not errors and not any('QI10_P1' in w for w in warnings)

@pytest.mark.parametrize('lang',['pt','ar','FR',None])
def test_unconfigured_language_refused(lang,ins_answers):
    with pytest.raises(ValidationError):prepare_submission('ins',ins_answers,lang=lang)
