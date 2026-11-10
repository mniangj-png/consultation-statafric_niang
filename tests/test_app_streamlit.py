"""Tests de parcours Streamlit ; exécutables en CI avec requirements-dev.txt.
Ils sont explicitement ignorés si Streamlit est absent de l'environnement.
"""
from pathlib import Path
import pytest
pytest.importorskip('streamlit')
from streamlit.testing.v1 import AppTest
from diagnostic.schema import FORMS
ROOT=Path(__file__).parents[1]

@pytest.mark.parametrize('lang',['fr','en'])
@pytest.mark.parametrize('form_id',list(FORMS))
def test_preview_opens_each_form(form_id,lang):
    at=AppTest.from_file(str(ROOT/'app.py'),default_timeout=20)
    at.query_params['form']=form_id;at.query_params['lang']=lang
    at.run()
    assert not at.exception
    consent=at.selectbox(key=f'w_{form_id}_{lang}_{FORMS[form_id]["consent"]}')
    assert consent.value is None
    consent.select(1).run()
    assert not at.exception
    institution=at.text_input(key=f'w_{form_id}_{lang}_{FORMS[form_id]["institution"]}')
    institution.set_value('Institution fictive').run()
    review_text='Relire et envoyer' if lang=='fr' else 'Review and submit'
    next(b for b in at.button if b.label==review_text).click().run()
    assert not at.exception
    submit_text='Envoyer ma contribution' if lang=='fr' else 'Submit my contribution'
    assert next(b for b in at.button if b.label==submit_text).disabled

def test_admin_preview_denies_access():
    at=AppTest.from_file(str(ROOT/'admin.py'),default_timeout=20).run()
    assert not at.exception and len(at.info)>0
    assert not at.dataframe
