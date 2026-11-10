import copy
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4
import pytest
import requests
from diagnostic.engine import prepare_submission
from diagnostic.storage import Settings,ConfigurationError,StorageError,LocalStore,SupabaseStore,make_store

@pytest.fixture
def production_config():
    return {'APP_MODE':'production','SUPABASE_URL':'https://project.supabase.co','SUPABASE_PUBLIC_KEY':'public_key',
            'WRITER_TOKEN':'x'*64,'CONTACT_EMAIL':'consultation@example.org','RETENTION_NOTICE':'Notice fictive de test',
            'DEPLOYMENT_APPROVED':True,'TEST_MODE':True}

def test_preview_default():
    assert make_store(Settings.from_mapping({})) is None

@pytest.mark.parametrize('key',['SUPABASE_URL','SUPABASE_PUBLIC_KEY','WRITER_TOKEN','CONTACT_EMAIL','RETENTION_NOTICE','DEPLOYMENT_APPROVED'])
def test_public_configuration_fails_closed(key,production_config):
    production_config.pop(key)
    with pytest.raises(ConfigurationError):Settings.from_mapping(production_config)

def test_admin_key_never_public(production_config):
    production_config['SUPABASE_ADMIN_KEY']='SECRET'
    with pytest.raises(ConfigurationError):Settings.from_mapping(production_config)
    assert Settings.from_mapping(production_config,admin=True).admin_key=='SECRET'

@pytest.mark.parametrize('url',['http://project.supabase.co','https://project.supabase.co.evil.example','https://user@project.supabase.co','https://project.supabase.co/?q=secret','https://localhost'])
def test_url_allowlist(url,production_config):
    production_config['SUPABASE_URL']=url
    with pytest.raises(ConfigurationError):Settings.from_mapping(production_config)

def test_local_forces_test():
    assert Settings.from_mapping({'APP_MODE':'local','TEST_MODE':False}).test_mode is True

def test_local_idempotence_and_conflict(tmp_path,ins_answers):
    store=LocalStore(tmp_path/'test.sqlite3');p,c=prepare_submission('ins',ins_answers,is_test=True)
    a=store.submit(p,c);b=store.submit(p,c)
    assert a['receipt']==b['receipt'] and b['duplicate']
    assert len(store.records())==1
    changed=copy.deepcopy(p);changed['answers']['QI01_INS']='Autre institution fictive'
    with pytest.raises(StorageError):store.submit(changed,c)
    assert store.records()[0]['payload']==p

def test_local_no_real_data(tmp_path,ins_answers):
    store=LocalStore(tmp_path/'test.sqlite3');p,c=prepare_submission('ins',ins_answers,is_test=False)
    with pytest.raises(StorageError):store.submit(p,c)
    assert not store.records()

def test_local_concurrency(tmp_path,ins_answers):
    store=LocalStore(tmp_path/'test.sqlite3');p,c=prepare_submission('ins',ins_answers,is_test=True)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results=list(pool.map(lambda _:store.submit(p,c),range(16)))
    assert len(store.records())==1 and len({r['receipt'] for r in results})==1
    assert sum(not r['duplicate'] for r in results)==1

def test_local_contacts_and_append_review(tmp_path,ins_answers):
    store=LocalStore(tmp_path/'test.sqlite3');ins_answers['QI12_Contact_courriel']='service@example.org'
    p,c=prepare_submission('ins',ins_answers,is_test=True);store.submit(p,c)
    assert store.all_contacts()[0]['contact']==c
    store.add_review(p['submission_id'],'test-local','À examiner','Contrôle fictif')
    store.add_review(p['submission_id'],'test-local','À clarifier','Second contrôle fictif')
    assert len(store.reviews())==2 and store.records()[0]['review']['status']=='À clarifier'
    assert store.records()[0]['payload']==p

def test_network_failure_no_fallback(monkeypatch,production_config,ins_answers):
    store=SupabaseStore(Settings.from_mapping(production_config));p,c=prepare_submission('ins',ins_answers,is_test=True)
    def fail(*a,**kw):raise requests.Timeout('SECRET must not reach user')
    monkeypatch.setattr(requests,'request',fail)
    with pytest.raises(StorageError,match='^NETWORK_UNCERTAIN$'):store.submit(p,c)

def test_remote_error_sanitized(monkeypatch,production_config):
    response=SimpleNamespace(status_code=500,content=b'x',json=lambda:{'message':'SQL password SECRET'})
    monkeypatch.setattr(requests,'request',lambda *a,**k:response)
    store=SupabaseStore(Settings.from_mapping(production_config))
    with pytest.raises(StorageError,match='^REMOTE_ERROR$'):store.campaign_info()

def test_no_receipt_no_success(monkeypatch,production_config,ins_answers):
    response=SimpleNamespace(status_code=200,content=b'x',json=lambda:{'status':'ok'})
    monkeypatch.setattr(requests,'request',lambda *a,**k:response)
    store=SupabaseStore(Settings.from_mapping(production_config));p,c=prepare_submission('ins',ins_answers,is_test=True)
    with pytest.raises(StorageError,match='^NETWORK_UNCERTAIN$'):store.submit(p,c)

def test_public_no_read(production_config):
    store=SupabaseStore(Settings.from_mapping(production_config))
    with pytest.raises(StorageError):store.records()
    with pytest.raises(StorageError):store.all_contacts()

def test_pagination_complete(monkeypatch,production_config):
    production_config['SUPABASE_ADMIN_KEY']='secret'
    store=SupabaseStore(Settings.from_mapping(production_config,admin=True),admin=True)
    offsets=[]
    def request(method,path,body=None,params=None):
        offsets.append(params['offset'])
        return [{'id':i} for i in range(500)] if params['offset']==0 else [{'id':501}]
    monkeypatch.setattr(store,'_request',request)
    assert len(store._all('diag_responses'))==501 and offsets==[0,500]

def test_remote_request_no_redirect_and_no_admin_key(monkeypatch,production_config):
    captured={}
    def req(*args,**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(status_code=200,content=b'x',json=lambda:{'active':True})
    monkeypatch.setattr(requests,'request',req)
    SupabaseStore(Settings.from_mapping(production_config)).campaign_info()
    assert captured['allow_redirects'] is False and captured['timeout']==(5,25)
    assert captured['headers']['apikey']=='public_key' and 'Authorization' not in captured['headers']

def test_text_false_is_not_approval(production_config):
    production_config['DEPLOYMENT_APPROVED']='false'
    with pytest.raises(ConfigurationError):Settings.from_mapping(production_config)
