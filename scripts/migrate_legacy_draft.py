"""Convert an unsent v1 draft OFFLINE. Consent and attribution must be given again.
No database access, no modification of already submitted contributions.
"""
import sys,json,argparse
from pathlib import Path
from uuid import uuid4
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from src.runtime import FORMS,SCHEMA_VERSION,LANGUAGES,iter_fields

def main():
    p=argparse.ArgumentParser(description='Convertir un brouillon v1 non envoyé ; exige une nouvelle lecture et un nouveau consentement.')
    p.add_argument('source');p.add_argument('sortie');a=p.parse_args();src=Path(a.source);dst=Path(a.sortie)
    if dst.exists():p.error('La sortie existe déjà.')
    raw=src.read_bytes()
    if len(raw)>1_000_000:p.error('Fichier trop volumineux.')
    d=json.loads(raw.decode('utf-8-sig'));fid=d.get('form_id')
    if d.get('schema_version')!='2026-11-08.web-1' or fid not in FORMS or not isinstance(d.get('answers'),dict):p.error('Le fichier n’est pas un brouillon v1 reconnu.')
    fields={f['id']:f for _,f in iter_fields(fid)};keep={};dropped=[]
    reset={FORMS[fid]['consent'],FORMS[fid]['attribution']}
    for k,v in d['answers'].items():
        f=fields.get(k)
        if not f or k in reset:dropped.append(k);continue
        opts=[o['code'] for o in f['options']]
        if f['type']=='single' and v is not None and (isinstance(v,bool) or v not in opts):dropped.append(k);continue
        if f['type']=='multi' and (not isinstance(v,list) or any(x not in opts for x in v)):dropped.append(k);continue
        keep[k]=v
    # Intermediate review file, not silently loadable: user explicitly confirms old contents then consents in v2.
    result={'kind':'hiswaca_legacy_review','source_schema_version':d['schema_version'],'target_schema_version':SCHEMA_VERSION,'form_id':fid,'language':d.get('language','fr'),'source_submission_id':d.get('submission_id'),'new_submission_id':str(uuid4()),'compatible_answers_for_manual_review':keep,'fields_excluded_or_reset':dropped,'instruction':'Relire les réponses, les nouveaux champs et la notice. Reporter dans un nouveau questionnaire v2 et donner le consentement et les autorisations à nouveau. Ce fichier ne doit pas être importé comme réponse reçue.'}
    dst.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print('Fichier de rapprochement créé. Aucune réponse envoyée. Les consentements et autorisations ne sont pas reconduits.')
if __name__=='__main__':main()
