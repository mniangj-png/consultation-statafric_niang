"""Real Streamlit AppTest. Runs in GitHub Actions; skipped only if library unavailable locally."""
from pathlib import Path
import pytest
pytest.importorskip('streamlit')
from streamlit.testing.v1 import AppTest
from src.runtime import FORMS,LANGUAGES,ui
ROOT=Path(__file__).parents[1]
@pytest.mark.parametrize('fid',list(FORMS))
@pytest.mark.parametrize('lang',LANGUAGES)
def test_each_public_route(fid,lang):
    at=AppTest.from_file(str(ROOT/'app.py'),default_timeout=30)
    at.query_params['form']=fid;at.query_params['lang']=lang;at.run();assert not at.exception
    at.selectbox(key=f'w|{fid}|{lang}|{FORMS[fid]["consent"]}').select(1).run();assert not at.exception
    if fid=='producteurs':at.selectbox(key='w|producteurs|'+lang+'|QP01_Produit').select(1).run()
    at.text_input(key=f'w|{fid}|{lang}|{FORMS[fid]["institution"]}').set_value('Institution TEST').run()
    at.button(key='review').click().run();assert not at.exception
    assert at.button(key='submit').disabled

def test_language_keeps_values():
    at=AppTest.from_file(str(ROOT/'app.py'),default_timeout=30).run()
    at.selectbox(key='w|ins|fr|QI01_Participation').select(1).run()
    at.text_input(key='w|ins|fr|QI01_INS').set_value('TEST محفوظ').run()
    uid=at.session_state['id_ins']
    for lang in ['ar','pt','es','en','fr']:
        at.selectbox(key='ui_language').select(lang).run();assert not at.exception
        assert at.text_input(key=f'w|ins|{lang}|QI01_INS').value=='TEST محفوظ'
        assert at.session_state['id_ins']==uid

def test_admin_denies_preview():
    at=AppTest.from_file(str(ROOT/'admin.py'),default_timeout=30).run()
    assert not at.exception and not at.dataframe

def test_relay_opens():
    at=AppTest.from_file(str(ROOT/'relais_ins.py'),default_timeout=30).run();assert not at.exception

def test_local_submission(tmp_path):
    at=AppTest.from_file(str(ROOT/'app.py'),default_timeout=30)
    at.secrets['APP_MODE']='local';at.secrets['LOCAL_DB_PATH']=str(tmp_path/'app.sqlite3');at.run()
    at.selectbox(key='w|ins|fr|QI01_Participation').select(1).run()
    at.text_input(key='w|ins|fr|QI01_INS').set_value('Institution fictive').run()
    at.button(key='review').click().run();at.checkbox(key='confirm_ins').check().run()
    at.button(key='submit').click().run();assert not at.exception and at.success
