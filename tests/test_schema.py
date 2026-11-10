import pytest
from diagnostic.schema import FORMS,iter_fields
from diagnostic.engine import schema_hash

@pytest.mark.parametrize('form_id,count', [('ins',12),('utilisateurs',8),('partenaires',5),('abrege',5)])
def test_source_sections(form_id,count):
    assert len(FORMS[form_id]['sections'])==count

@pytest.mark.parametrize('form_id',list(FORMS))
def test_schema_integrity(form_id):
    form=FORMS[form_id]; fields=list(iter_fields(form_id)); ids=[f['id'] for _,f in fields]
    assert len(ids)==len(set(ids))
    assert form['consent'] in ids and form['attribution'] in ids
    for section,field in fields:
        assert section['source']
        assert all(field['label'].get(lang) for lang in ['fr','en'])
        assert field['type'] in ['single','multi','text','textarea','email','month']
        opts=[o['code'] for o in field['options']]
        assert len(opts)==len(set(opts))
        for option in field['options']:
            assert type(option['code']) is int
            assert all(option['label'].get(lang) for lang in ['fr','en'])
        if field.get('private'): assert '_Contact_' in field['id']
    assert len(schema_hash(form_id))==64

def dependencies(rule):
    if not rule: return []
    if 'all' in rule or 'any' in rule:
        return [f for r in rule.get('all',rule.get('any',[])) for f in dependencies(r)]
    return [rule['field']]

@pytest.mark.parametrize('form_id',list(FORMS))
def test_gate_dependencies_are_prior(form_id):
    seen=set()
    for section,field in iter_fields(form_id):
        assert set(dependencies(section.get('gate'))+dependencies(field.get('gate')))<=seen
        seen.add(field['id'])

@pytest.mark.parametrize('form_id',list(FORMS))
def test_contact_fields_distinct(form_id):
    assert any(f.get('private') for _,f in iter_fields(form_id))
    assert not next(f for _,f in iter_fields(form_id) if f['id']==FORMS[form_id]['attribution']).get('private')
