import json,csv,io,zipfile,uuid,hashlib,copy
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest
from src.runtime import *

def minimal(fid):
    f=FORMS[fid];a={f['consent']:1,f['institution']:'Institution fictive',f['country']:'Pays TEST'}
    if fid=='producteurs':a['QP01_Produit']=1
    return a

@pytest.mark.parametrize('fid',list(FORMS))
@pytest.mark.parametrize('lang',LANGUAGES)
def test_language_roundtrip(fid,lang):
    a=minimal(fid);a[FORMS[fid]['response_language']]=lang;uid=str(uuid4())
    d=load_draft(draft_bytes(fid,a,lang,uid))
    assert d['answers']==a and d['submission_id']==uid and d['language']==lang
    p,c=prepare_submission(fid,a,lang,uid,True)
    assert not c and p['is_test'] and p['language']==lang and p['intended_deliverable']=='L3'
    assert p['citation_permission']==0 and p['citation_explicit'] is False

@pytest.mark.parametrize('fid',list(FORMS))
def test_complete_schema(fid):
    ids=[f['id'] for _,f in iter_fields(fid)];assert len(ids)==len(set(ids))
    known=[]
    def deps(r):
        if not r:return []
        if 'all' in r:return sum((deps(x) for x in r['all']),[])
        if 'any' in r:return sum((deps(x) for x in r['any']),[])
        return [r['field']]
    for s,f in iter_fields(fid):
        for k in deps(s.get('gate'))+deps(f.get('gate')):assert k in known,(f['id'],k)
        known.append(f['id'])
        for obj in [s['title'],s['note'],f['label']]+[o['label'] for o in f['options']]:
            assert set(LANGUAGES)<=set(obj)
            for l in LANGUAGES:assert isinstance(text(obj,l),str)
        assert len(f['options'])==len({o['code'] for o in f['options']})
        assert all(k not in f for k in ('default','prefill'))

@pytest.mark.parametrize('fid',list(FORMS))
@pytest.mark.parametrize('value',[None,0])
def test_consent_stops(fid,value):
    a=minimal(fid);a[FORMS[fid]['consent']]=value
    effective,states=field_states(fid,a)
    assert not any(not blank(v) for k,v in effective.items() if k!=FORMS[fid]['consent'])
    with pytest.raises(ValidationError):prepare_submission(fid,a)

@pytest.mark.parametrize('prefix,fid',[('QI','ins'),('QP','producteurs')])
def test_no_requests_preserves_corrections(prefix,fid):
    a=minimal(fid);a.update({prefix+'02a':0,prefix+'08_Situation':1,prefix+'08_Actions':[1,3]})
    effective,states=field_states(fid,a)
    assert effective[prefix+'08_Situation']==1 and states[prefix+'03_Situation']=='NF'
    assert states[prefix+'05_S1']=='NF'
    assert not validation(fid,a)[0]

@pytest.mark.parametrize('prefix,fid',[('QI','ins'),('QP','producteurs')])
@pytest.mark.parametrize('situation',[1,2,3,98])
def test_case_routes(prefix,fid,situation):
    a=minimal(fid);a.update({prefix+'02a':1,prefix+'03_Situation':situation})
    _,states=field_states(fid,a)
    assert (states[prefix+'04a']!='NF')==(situation in (1,2))
    assert (states[prefix+'05_S1']!='NF')==(situation==1)

@pytest.mark.parametrize('fid',list(FORMS))
def test_no_contacts_in_payload(fid):
    a=minimal(fid);field=next(f for _,f in iter_fields(fid) if f.get('private') and f['type']=='email')
    a[field['id']]='test@example.invalid';p,c=prepare_submission(fid,a,is_test=True)
    assert field['id'] in c and field['id'] not in p['answers'] and field['id'] not in p['field_states']

def test_qp_eligibility():
    a=minimal('producteurs');a['QP01_Produit']=0
    e,s=field_states('producteurs',a)
    assert 'QP01_Institution' not in e
    with pytest.raises(ValidationError):prepare_submission('producteurs',a)

def test_qp_decision_without_speech():
    a=minimal('producteurs');a.update(QP02a=0,QP02d=1,QP02d_Theme='Note de décision TEST')
    e,s=field_states('producteurs',a);assert e['QP02d_Theme'] and s['QP03_Situation']=='NF'

def test_qp_admin_unit_gate():
    a=minimal('producteurs');a.update(QP02a=1,QP03_Situation=1,QP05_Origine=2,QP05c=1)
    e,s=field_states('producteurs',a);assert e['QP05c']==1
    a['QP05_Origine']=4;e,s=field_states('producteurs',a);assert s['QP05c']=='NF' and 'QP05c' not in e

@pytest.mark.parametrize('section',['QR03bis','QR04bis'])
@pytest.mark.parametrize('choice',[0,98,99])
def test_bis_filters(section,choice):
    a=minimal('partenaires');a[section+'_Presence']=choice;a[section+'_R1']='Should disappear'
    e,s=field_states('partenaires',a);assert s[section+'_R1']=='NF' and section+'_R1' not in e

@pytest.mark.parametrize('fid,prefix',[('ins','QI10'),('producteurs','QP10'),('utilisateurs','QU07')])
def test_duplicate_priorities(fid,prefix):
    a=minimal(fid);a.update({prefix+'_P1':1,prefix+'_P2':1})
    assert validation(fid,a)[0]
    a[prefix+'_P2']=2;assert not validation(fid,a)[0]

def test_blank_is_not_no():
    a=minimal('ins');a['QI02a']=0;e,s=field_states('ins',a)
    assert s['QI02a']=='RENSEIGNE' and s['QI02c']=='NR_A_CONTROLER'

def test_multiselect_missing_states():
    a=minimal('partenaires');a['QR04_Besoins']=[98];assert field_states('partenaires',a)[1]['QR04_Besoins']=='NSP'
    a['QR04_Besoins']=[1,98];assert validation('partenaires',a)[0]

@pytest.mark.parametrize('lang',LANGUAGES)
def test_validation_translated(lang):
    a=minimal('ins');a['QI01_Statut']=777
    assert ui('bad_code',lang) in validation('ins',a,lang)[0][0]
    a['QI01_Statut']=None;a['QI12_Contact_courriel']='bad'
    assert any(ui('email_error',lang) in x for x in validation('ins',a,lang)[0])

def test_arabic_month_and_unicode():
    a=minimal('ins');a.update(QI02a=1,QI03_Situation=1,QI03_Mois='١٠/٢٠٢٦',QI03_Theme='خطاب إحصائي')
    assert not validation('ins',a,'ar')[0]
    p,_=prepare_submission('ins',a,'ar',is_test=True);assert p['answers']['QI03_Mois']=='١٠/٢٠٢٦'

def test_soft_word_limit():
    a=minimal('ins');a.update(QI10_P1=1,QI10_Difficulte='mot '*45)
    errors,warnings=validation('ins',a);assert not errors and any(ui('word_warning') in w for w in warnings)

def test_old_draft_rejected():
    d=json.loads(draft_bytes('ins',minimal('ins'),'fr',str(uuid4())));d['schema_version']='2026-11-08.web-1'
    with pytest.raises(ValidationError):load_draft(json.dumps(d).encode())

@pytest.mark.parametrize('value',[b'not json',b'[]',b'null',b'{"kind":"other"}',b'x'*(MAX_DRAFT_BYTES+1)])
def test_malicious_draft(value):
    with pytest.raises(ValidationError):load_draft(value)

@pytest.mark.parametrize('fid',list(FORMS))
@pytest.mark.parametrize('lang',LANGUAGES)
def test_fallback_contains_all_fields(fid,lang):
    output=fallback_text(fid,lang).decode('utf-8-sig')
    assert all(f['id'] in output for _,f in iter_fields(fid))
    assert text(NOTICE,lang) in output
    if lang=='ar':assert 'المشاركة' in output

@pytest.mark.parametrize('fid',list(FORMS))
def test_local_idempotence_and_export(tmp_path,fid):
    store=LocalStore(tmp_path/'test.sqlite3');a=minimal(fid);p,c=prepare_submission(fid,a,is_test=True)
    first=store.submit(p,c);second=store.submit(p,c)
    assert first['receipt']==second['receipt'] and second['duplicate']
    assert len(store.records())==1
    p2=copy.deepcopy(p);p2['answers'][FORMS[fid]['institution']]='Changed'
    with pytest.raises(StorageError):store.submit(p2,c)
    b=export_bundle(store.records())
    with zipfile.ZipFile(io.BytesIO(b)) as z:
        assert 'Reponses.csv' in z.namelist() and 'Contributions.csv' in z.namelist()
        assert len([x for x in z.namelist() if x.startswith('originaux/')])==1

def test_no_real_data_local(tmp_path):
    p,c=prepare_submission('ins',minimal('ins'),is_test=False)
    with pytest.raises(StorageError):LocalStore(tmp_path/'test.sqlite3').submit(p,c)

def test_local_concurrency(tmp_path):
    path=tmp_path/'test.sqlite3';store=LocalStore(path);p,c=prepare_submission('ins',minimal('ins'),is_test=True)
    with ThreadPoolExecutor(max_workers=5) as pool:receipts=list(pool.map(lambda _:store.submit(p,c),range(10)))
    assert len(store.records())==1 and len({r['receipt'] for r in receipts})==1

def test_registry_history_conflict(tmp_path):
    store=LocalStore(tmp_path/'test.sqlite3');uid=str(uuid4());d={'ID document':'DOC-TEST','Titre original':'TEST'}
    validate_registry('documents',d);store.save_registry('documents',uid,d,'tester',0,True)
    store.save_registry('documents',uid,{**d,'Titre original':'TEST v2'},'tester',1,True)
    with pytest.raises(StorageError):store.save_registry('documents',uid,d,'tester',1,True)
    events=store.registry_events();assert len(events)==2
    assert not latest_registry(events)
    assert latest_registry(events,include_tests=True)[0]['version']==2

def test_registry_checks():
    with pytest.raises(ValidationError):validate_registry('invitations',{'Statut':'Envoyé'})
    with pytest.raises(ValidationError):validate_registry('discours',{'Source':'Satisfait'})
    with pytest.raises(ValidationError):validate_registry('observations',{'Statut':'Clôturée'})
    with pytest.raises(ValidationError):validate_registry('documents',{'Password':'secret'})

@pytest.mark.parametrize('v',['=CMD()','+cmd','-cmd','@SUM(A1)','\tformula','  =1+1'])
def test_csv_injection(v):assert csv_safe(v).startswith("'")

def test_pending_exact(tmp_path):
    p,c=prepare_submission('ins',minimal('ins'),'fr',is_test=True);p['attribution_authority_confirmed']=False
    raw=json.dumps({'kind':'hiswaca_pending','payload':p,'contacts':c}).encode()
    pending=load_pending(raw,True);assert pending['payload']==p
    with pytest.raises(ValidationError):load_pending(raw,False)

def test_settings_and_secret_separation():
    assert Settings.from_mapping({}).mode=='preview'
    assert Settings.from_mapping({'APP_MODE':'local','TEST_MODE':False}).test_mode is True
    with pytest.raises(ConfigurationError):Settings.from_mapping({'TEST_MODE':'false'})
    with pytest.raises(ConfigurationError):Settings.from_mapping({'APP_MODE':'production','SUPABASE_URL':'http://evil.invalid','SUPABASE_ADMIN_KEY':'secret'})

@pytest.mark.parametrize('app',['app.py','admin.py','relais_ins.py'])
def test_standalone_has_no_local_import(app):
    code=(Path(__file__).parents[1]/app).read_text();compile(code,app,'exec')
    import ast
    nodes=ast.walk(ast.parse(code))
    for node in nodes:
        if isinstance(node,ast.ImportFrom):assert (node.module or '').split('.')[0] not in ('diagnostic','src')
        if isinstance(node,ast.Import):assert all(x.name.split('.')[0] not in ('diagnostic','src') for x in node.names)
    assert "APP_VERSION='2.0.0'" in code

def test_source_contract():
    sql=(Path(__file__).parents[1]/'sql/01_install_or_upgrade.sql').read_text()
    assert "'producteurs'" in sql and "'fr','en','ar','pt','es'" in sql
    for t in ['diag_campaigns','diag_responses','diag_contacts','diag_reviews','diag_registry_events']:assert f'public.{t} ENABLE ROW LEVEL SECURITY' in sql
    assert 'TEST_MODE_MISMATCH' in sql and 'FOR UPDATE' in sql and 'pg_advisory_xact_lock' in sql
    assert 'DROP TABLE' not in sql

@pytest.mark.parametrize('dt,expected',[
 ('2026-11-18T23:59:59Z','Avant ouverture'),('2026-11-19T00:00:00Z','Vague principale'),
 ('2026-11-26T23:59:59.999999Z','Vague principale'),('2026-11-27T00:00:00Z','Tardive / complément'),
 ('invalid','Date à vérifier')])
def test_collection_wave(dt,expected):assert collection_wave(dt)==expected

def test_register_reclassification_hides_previous_real():
    events=[{'record_id':'r','kind':'documents','version':1,'is_test':False,'data':{}},
      {'record_id':'r','kind':'documents','version':2,'is_test':True,'data':{}}]
    assert not latest_registry(events) and latest_registry(events,include_tests=True)[0]['version']==2

def test_documentary_finding_excludes_consultation():
    with pytest.raises(ValidationError):validate_registry('constats',{'Destination':'L2 documentaire','IDs réponses / documents / extraits':'REP-TEST'})

def test_publication_requires_verified_recipe():
    with pytest.raises(ValidationError):validate_registry('recette',{'Parcours':'ins','Langue':'fr','Statut final':'Autorisation de diffusion donnée'})

def test_pending_permission_cannot_be_changed():
    p,c=prepare_submission('ins',minimal('ins'),'fr',is_test=True)
    p['citation_permission']=2;p['attribution_authority_confirmed']=True
    with pytest.raises(ValidationError):load_pending(json.dumps({'kind':'hiswaca_pending','payload':p,'contacts':c}).encode(),True)

def test_response_original_version_not_relabelled():
    p,c=prepare_submission('ins',minimal('ins'),is_test=True);p['schema_version']='legacy'
    rows=response_rows([{'id':p['submission_id'],'receipt':'REP-'+p['submission_id'],'payload':p,'received_at':'2026-11-19T00:00:00Z'}])
    assert all(x['Libellé FR'].startswith('Ancien champ') for x in rows)

def test_coverage_separates_versions_and_excludes_tests():
    records=[]
    for is_test,version in [(True,SCHEMA_VERSION),(False,SCHEMA_VERSION),(False,'legacy')]:
        p,c=prepare_submission('ins',minimal('ins'),'ar',is_test=is_test);p['schema_version']=version
        records.append({'payload':p,'review':{'status':'Exploitable'}})
    rows=coverage_rows(records)
    assert len(rows)==2 and sum(x['Contributions réelles reçues'] for x in rows)==2
    assert all(x['Langue interface']=='ar' for x in rows)

def test_production_notices_required():
    cfg={'APP_MODE':'production','TEST_MODE':False,'SUPABASE_URL':'https://test.supabase.co','SUPABASE_PUBLIC_KEY':'TEST',
      'WRITER_TOKEN':'x'*40,'CONTACT_EMAIL':'test@example.invalid','RETENTION_NOTICE':'TEST','DEPLOYMENT_APPROVED':True,'APPROVED_LANGUAGES':['fr']}
    with pytest.raises(ConfigurationError):Settings.from_mapping(cfg)
    cfg['notice']={'fr':'Notice approuvée pour ce test de configuration.'}
    assert Settings.from_mapping(cfg).approved_languages==('fr',)
