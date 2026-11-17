"""Pure diagnostic engine. No network and no Streamlit dependency.
SCHEMA and UI are embedded by scripts/build_standalone.py.
"""
import json,hashlib,re,csv,io,zipfile,html,unicodedata,copy
from datetime import datetime,timezone
from uuid import UUID,uuid4
from collections import Counter
APP_VERSION='2.1.0'
SCHEMA_VERSION=SCHEMA['version']
FORMS=SCHEMA['forms']
LANGUAGES=SCHEMA['languages']
NOTICE=SCHEMA['notice']
MAX_DRAFT_BYTES=1000000
LANG_NAMES={'fr':'Français','en':'English','ar':'العربية','pt':'Português','es':'Español'}
class ValidationError(ValueError):pass

def text(value,lang='fr'):
    if not isinstance(value,dict):return str(value)
    if lang not in LANGUAGES:raise ValidationError('LANGUAGE')
    if lang not in value:raise ValidationError('MISSING_TRANSLATION')
    return value[lang]

def ui(key,lang='fr',**params):
    return UI[key][lang].format(**params)

def iter_fields(form_id):
    for section in FORMS[form_id]['sections']:
        for field in section['fields']:yield section,field

def blank(v):return v is None or v=='' or v==[]

def combine(vals,kind):
    if kind=='all':return False if False in vals else None if None in vals else True
    return True if True in vals else None if None in vals else False

def evaluate(rule,answers,states=None):
    if not rule:return True
    if 'all' in rule:return combine([evaluate(r,answers,states) for r in rule['all']],'all')
    if 'any' in rule:return combine([evaluate(r,answers,states) for r in rule['any']],'any')
    fid=rule['field'];op=rule['op'];v=answers.get(fid)
    if states and states.get(fid)=='NF':return False
    if states and states.get(fid)=='FILTRE_INDETERMINE':return None
    if op=='not_in':return v not in rule['values']
    if blank(v):return None
    if op=='in':return v in rule['values']
    if op=='contains':return isinstance(v,list) and any(x in v for x in rule['values'])
    raise ValidationError('UNKNOWN_FILTER')

def field_states(form_id,answers):
    known={f['id'] for _,f in iter_fields(form_id)}
    effective={k:v for k,v in answers.items() if k in known}
    states={}
    for s,f in iter_fields(form_id):
        fid=f['id'];active=combine([evaluate(s.get('gate'),effective,states),evaluate(f.get('gate'),effective,states)],'all')
        if active is not True:
            effective.pop(fid,None);states[fid]='NF' if active is False else 'FILTRE_INDETERMINE'
        else:
            v=effective.get(fid)
            states[fid]='NR_A_CONTROLER' if blank(v) else 'NSP' if v==98 or v==[98] else 'NA' if v==99 or v==[99] else 'RENSEIGNE'
    return effective,states

def active_sections(form_id,answers):
    a,_=field_states(form_id,answers)
    return [s for s in FORMS[form_id]['sections'] if evaluate(s.get('gate'),a) is not False]

def normalise_digits(value):
    return ''.join(str(unicodedata.decimal(c)) if c.isdecimal() else c for c in value)

def validation(form_id,answers,lang='fr',require_consent=True):
    if form_id not in FORMS or not isinstance(answers,dict):raise ValidationError('INVALID_FORM')
    effective,states=field_states(form_id,answers);errors=[];warnings=[]
    if require_consent and effective.get(FORMS[form_id]['consent'])!=1:errors.append(ui('no_consent',lang))
    if form_id=='producteurs' and effective.get('QP01_Produit')==0:errors.append(ui('ineligible',lang))
    for _,f in iter_fields(form_id):
        fid=f['id'];v=effective.get(fid)
        if blank(v):continue
        opts=[o['code'] for o in f['options']]
        if f['type']=='single':
            if isinstance(v,bool) or not isinstance(v,(str,int)) or v not in opts:errors.append(fid+' : '+ui('bad_code',lang))
        elif f['type']=='multi':
            if not isinstance(v,list) or any(isinstance(n,bool) or not isinstance(n,(str,int)) or n not in opts for n in v):errors.append(fid+' : '+ui('bad_selection',lang));continue
            if len(v)!=len(set(v)):errors.append(fid+' : '+ui('bad_selection',lang))
            if len(v)>f.get('max_choices',len(opts)):errors.append(fid+' : '+ui('too_many',lang))
            if len(v)>1 and any(n in f.get('exclusive',[]) for n in v):errors.append(fid+' : '+ui('exclusive',lang))
        else:
            if not isinstance(v,str) or len(v)>f.get('max_chars',2500) or '\x00' in v:errors.append(fid+' : '+ui('text_required',lang));continue
            if f.get('max_words') and len(v.split())>f['max_words']:warnings.append(fid+' : '+ui('word_warning',lang))
            if f['type']=='email' and not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+',v):errors.append(fid+' : '+ui('email_error',lang))
            if f['type']=='month':
                m=re.fullmatch(r'(0[1-9]|1[0-2])/(\d{4})',normalise_digits(v))
                if not m:errors.append(fid+' : '+ui('month_error',lang))
                elif not 202511<=int(m[2])*100+int(m[1])<=202610:warnings.append(fid+' : '+ui('month_warning',lang))
    prefix=FORMS[form_id].get('priority_prefix')
    if prefix:
        vals=[effective.get(prefix+f'_P{i}') for i in (1,2,3)];valid=[v for v in vals if not blank(v)]
        if all(isinstance(v,int) for v in valid):
            if len(valid)!=len(set(valid)) or (not blank(vals[1]) and blank(vals[0])) or (not blank(vals[2]) and blank(vals[1])):errors.append(ui('priority_error',lang))
    if form_id in ('ins','producteurs'):
        p='QI' if form_id=='ins' else 'QP'
        if effective.get(p+'02a')==0 and effective.get(p+'02b')==1:errors.append(ui('volume_error',lang))
        if effective.get(p+'04a')==5 and effective.get(p+'04b') in (0,1):warnings.append(ui('deadline_warning',lang))
    if form_id=='utilisateurs':
        if effective.get('QU02a')==0 and effective.get('QU02b')==1:errors.append(ui('support_error',lang))
        d=effective.get('QU03_Delai');out=effective.get('QU03_Issue')
        if (d==99 and out in (1,2,3,4)) or (out==99 and d in (1,2,3,4,5)):errors.append(ui('support_error',lang))
    known={f['id'] for _,f in iter_fields(form_id)}
    for _,f in iter_fields(form_id):
        v=effective.get(f['id']);other=f['id'].rsplit('_P',1)[0]+'_Autre' if re.fullmatch(r'(?:QI10|QP10|QU07)_P[123]',f['id']) else f['id']+'_Autre'
        if other in known and (v==97 or isinstance(v,list) and 97 in v) and blank(effective.get(other)):warnings.append(f['id']+' : '+ui('other_warning',lang))
    return list(dict.fromkeys(errors)),list(dict.fromkeys(warnings))

def schema_hash(form_id):return hashlib.sha256(json.dumps(FORMS[form_id],ensure_ascii=False,sort_keys=True).encode()).hexdigest()

def prepare_submission(form_id,answers,lang='fr',submission_id=None,is_test=False,metadata=None):
    if form_id not in FORMS or lang not in LANGUAGES or type(is_test) is not bool:raise ValidationError('INVALID_FORM')
    errors,warnings=validation(form_id,answers,lang)
    if errors:raise ValidationError('\n'.join(errors))
    effective,states=field_states(form_id,answers);fields={f['id']:f for _,f in iter_fields(form_id)}
    contacts={k:v for k,v in effective.items() if fields[k].get('private') and not blank(v)}
    clean={k:v for k,v in effective.items() if not fields[k].get('private') and not blank(v)}
    if not any(k!=FORMS[form_id]['consent'] for k in clean):raise ValidationError(ui('content_required',lang))
    try:uid=str(UUID(submission_id)) if submission_id else str(uuid4())
    except (ValueError,TypeError,AttributeError) as exc:raise ValidationError('INVALID_ID') from exc
    attr=clean.get(FORMS[form_id]['attribution']);meta=metadata or {}
    meta={k:str(v)[:120] for k,v in meta.items() if k in ('relay_id','invitation_id','channel') and v}
    payload={'submission_id':uid,'form_id':form_id,'language':lang,'schema_version':SCHEMA_VERSION,'instrument_version':FORMS[form_id]['version'],'translation_version':'2026-11-16.v2','instrument_hash':schema_hash(form_id),'app_version':APP_VERSION,'is_test':is_test,'answers':clean,'field_states':{k:v for k,v in states.items() if not fields[k].get('private')},'citation_permission':0 if blank(attr) else attr,'citation_explicit':not blank(attr),'warnings':warnings,'short_form':form_id=='abrege','metadata':meta,'response_language':clean.get(FORMS[form_id].get('response_language','')) or 'non_declared','intended_deliverable':'L3'}
    return payload,contacts

def draft_bytes(form_id,answers,lang,submission_id,metadata=None):
    effective,_=field_states(form_id,answers)
    return json.dumps({'kind':'hiswaca_draft','schema_version':SCHEMA_VERSION,'form_id':form_id,'language':lang,'submission_id':str(UUID(submission_id)),'answers':effective,'metadata':metadata or {}},ensure_ascii=False,indent=2).encode('utf-8')

def load_draft(content):
    if len(content)>MAX_DRAFT_BYTES:raise ValidationError('INVALID_FILE')
    try:
        d=json.loads(content.decode('utf-8-sig'))
        if not isinstance(d,dict) or d.get('kind')!='hiswaca_draft' or d.get('schema_version')!=SCHEMA_VERSION or d.get('language') not in LANGUAGES or d.get('form_id') not in FORMS or not isinstance(d.get('answers'),dict):raise ValueError()
        d['submission_id']=str(UUID(d['submission_id']))
        known={f['id'] for _,f in iter_fields(d['form_id'])}
        if set(d['answers'])-known:raise ValueError()
        errors,_=validation(d['form_id'],d['answers'],d['language'],False)
        if errors:raise ValueError()
        d['answers'],_=field_states(d['form_id'],d['answers'])
        d['metadata']={k:str(v)[:120] for k,v in (d.get('metadata') or {}).items() if k in ('relay_id','invitation_id','channel')}
        return d
    except (ValueError,TypeError,KeyError,UnicodeDecodeError,AttributeError) as exc:raise ValidationError('INVALID_FILE') from exc

def load_pending(content,test_mode):
    if len(content)>MAX_DRAFT_BYTES:raise ValidationError('INVALID_FILE')
    try:
        d=json.loads(content.decode('utf-8-sig'));p=d['payload'];c=d['contacts']
        if d.get('kind')!='hiswaca_pending' or p['schema_version']!=SCHEMA_VERSION or p['is_test'] is not test_mode:raise ValueError()
        a={**p['answers'],**c};clean,contacts=prepare_submission(p['form_id'],a,p['language'],p['submission_id'],test_mode,p.get('metadata'))
        if any(clean.get(k)!=p.get(k) for k in ('answers','field_states','citation_permission','citation_explicit','instrument_hash','metadata','intended_deliverable')) or contacts!=c:raise ValueError()
        if p.get('citation_permission') in (1,2) and p.get('attribution_authority_confirmed') is not True:raise ValueError()
        return {'payload':p,'contacts':c}
    except (ValueError,TypeError,KeyError,UnicodeDecodeError,AttributeError) as exc:raise ValidationError('INVALID_FILE') from exc

def answer_label(field,value,lang='fr'):
    opts={o['code']:text(o['label'],lang) for o in field['options']}
    if blank(value):return ''
    if isinstance(value,list):return ' ; '.join(str(n)+' — '+opts.get(n,str(n)) for n in value)
    if field['type']=='single':return str(value)+' — '+opts.get(value,str(value))
    return str(value)

def receipt_text(payload,receipt):
    lang=payload['language'];lines=['HISWACA — '+ui('receipt_title',lang),ui('reference',lang)+' : '+receipt['receipt'],'UTC : '+receipt['received_at'],ui('version',lang)+' : '+payload['schema_version']]
    if payload['is_test']:lines += [ui('test',lang)]
    for _,f in iter_fields(payload['form_id']):
        if not f.get('private') and not blank(payload['answers'].get(f['id'])):lines.append(f['id']+' — '+text(f['label'],lang)+'\n'+answer_label(f,payload['answers'][f['id']],lang))
    return '\n\n'.join(lines+[ui('not_validation',lang)])

def csv_safe(v):
    if v is None:return ''
    s=str(v)
    return "'"+s if s.lstrip().startswith(('=','+','-','@')) or s.startswith(('\t','\r','\n')) else s

def csv_bytes(rows,columns):
    out=io.StringIO(newline='');w=csv.DictWriter(out,fieldnames=columns,delimiter=';',lineterminator='\r\n',extrasaction='ignore');w.writeheader()
    for r in rows:w.writerow({k:csv_safe(r.get(k,'')) for k in columns})
    return out.getvalue().encode('utf-8-sig')

def collection_wave(timestamp):
    try:
        dt=datetime.fromisoformat(timestamp.replace('Z','+00:00'))
        if dt.tzinfo is None:return 'Date à vérifier'
        start=datetime(2026,11,19,tzinfo=timezone.utc);end=datetime(2026,11,27,tzinfo=timezone.utc)
        return 'Avant ouverture' if dt<start else 'Vague principale' if dt<end else 'Tardive / complément'
    except (ValueError,TypeError,AttributeError):return 'Date à vérifier'

def contribution_rows(records):
    out=[]
    for r in records:
        p=r['payload'];f=FORMS.get(p.get('form_id'),{});a=p.get('answers',{});review=r.get('review') or {};m=p.get('metadata') or {}
        institution=a.get(f.get('institution',''),'');country=a.get(f.get('country',''),'');status=a.get(f.get('status',''),'')
        out.append({'ID réponse':r['receipt'],'Institution':institution,'Pays / zone':country,'Profil':p['form_id'],'Profil d’origine abrégé':a.get('RA01_Profil','') if p['form_id']=='abrege' else '', 'Statut contribution':status,'Unité':a.get(f.get('unit',''),''),'Domaine':a.get(f.get('sector',''),''),'Langue interface':p.get('language'),'Langue texte':p.get('response_language','non_declared'),'INS relais':a.get(f.get('relay',''),''),'ID relais':m.get('relay_id',''),'ID invitation':m.get('invitation_id',''),'Origine invitation':a.get(f.get('origin',''),''),'Date réponse':r['received_at'],'Vague':collection_wave(r['received_at']),'Exploitabilité':review.get('status','À examiner'),'Lié à':review.get('linked_to',''),'Droits de citation':p.get('citation_permission',0),'Permission explicite':p.get('citation_explicit',False),'Test':p.get('is_test',True),'Version':p.get('schema_version',''),'Formulaire abrégé':p.get('short_form',False),'Champs actifs non renseignés':sum(v=='NR_A_CONTROLER' for v in p.get('field_states',{}).values()),'Filtres indéterminés':sum(v=='FILTRE_INDETERMINE' for v in p.get('field_states',{}).values()),'Destination':'L3','Ancienne version':p.get('schema_version')!=SCHEMA_VERSION})
    return out

def response_rows(records):
    rows=[]
    for r in records:
        p=r['payload'];f_id=p['form_id'];known={f['id']:f for _,f in iter_fields(f_id)} if f_id in FORMS and p.get('schema_version')==SCHEMA_VERSION else {};a=p.get('answers',{});states=p.get('field_states',{})
        for fid in dict.fromkeys([*known,*a]):
            f=known.get(fid,{});v=a.get(fid)
            if f.get('private'):continue
            vals=v if isinstance(v,list) and v else [v]
            for rank,val in enumerate(vals,1):
                rows.append({'ID réponse':r['receipt'],'Profil':f_id,'Code question':fid,'Libellé FR':text(f['label']) if f else 'Ancien champ : consulter le dictionnaire de sa version','Réponse brute':json.dumps(v,ensure_ascii=False) if isinstance(v,list) else v,'Code option':val if isinstance(v,list) else '', 'Rang option':rank if isinstance(v,list) else '', 'Code analytique':val if f.get('type') in ('single','multi') else '', 'Statut information':states.get(fid,'NON_PRESENT_VERSION'),'Traduction FR':'','Statut traduction':'À contrôler' if p.get('language')!='fr' and f.get('type') in ('text','textarea') and not blank(v) else '', 'Langue interface':p.get('language'),'Version':p.get('schema_version'),'Test':p.get('is_test',True),'Destination':'L3','Référence précise':f.get('source','') if p.get('schema_version')==SCHEMA_VERSION else 'Consulter le dictionnaire de la version originale'})
    return rows

def dictionary_rows():
    rows=[]
    for fid,form in FORMS.items():
        for s,f in iter_fields(fid):
            for opt in f['options'] or [{'code':'','label':dict.fromkeys(LANGUAGES,'')}]:
                row={'Parcours':fid,'Rubrique':s['id'],'Champ':f['id'],'Type':f['type'],'Privé':bool(f.get('private')),'Code':opt['code'],'Filtre':json.dumps({'section':s.get('gate'),'champ':f.get('gate')},ensure_ascii=False),'Max choix':f.get('max_choices',''),'Mots indicatifs':f.get('max_words',''),'Source':f.get('source',s['source']),'Version':SCHEMA_VERSION}
                for l in LANGUAGES:row['Question '+l]=text(f['label'],l);row['Modalité '+l]=text(opt['label'],l)
                rows.append(row)
    return rows

def export_bundle(records,registries=None):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as z:
        for name,rows in [('Contributions',contribution_rows(records)),('Reponses',response_rows(records)),('Dictionnaire',dictionary_rows())]:
            columns=list(rows[0]) if rows else ['Aucun enregistrement'];z.writestr(name+'.csv',csv_bytes(rows,columns))
        for r in records:z.writestr('originaux/'+r['id']+'.json',json.dumps(r,ensure_ascii=False,indent=2))
        if registries:
            for kind,rows in registries.items():
                if not rows:continue
                clean=[{**r.get('data',{}),'_record_id':r['record_id'],'_version':r['version'],'_is_test':r['is_test']} for r in rows]
                cols=list(dict.fromkeys(k for r in clean for k in r));z.writestr('registres/'+kind+'.csv',csv_bytes(clean,cols))
        z.writestr('LISEZ_MOI.txt','EXPORT INTERNE RESTREINT. Les contacts structurés sont séparés, mais les récits et noms d’institutions peuvent identifier les contributeurs. Ne pas publier sans revue des autorisations.\nLes JSON sont les originaux. Les formules potentielles sont neutralisées dans les CSV. NR_A_CONTROLER n’est pas NR confirmé; FILTRE_INDETERMINE n’est pas NF. Les classes ne sont pas des quantités.\nExclure les tests, doublons et hors champ des analyses. Ne pas additionner les demandes de plusieurs producteurs en total national. Aucune extrapolation ni score de maturité. Données de consultation uniquement pour L3. Les anciennes versions restent identifiées et ne sont pas automatiquement converties.\nLes traductions et les décisions de codage sont ajoutées dans le registre dédié sans remplacer l’original.\n')
    return buf.getvalue()

def rule_text(rule,lang):
    if not rule:return ''
    if 'all' in rule:return '('+(' '+ui('logic_and',lang)+' ').join(rule_text(x,lang) for x in rule['all'])+')'
    if 'any' in rule:return '('+(' '+ui('logic_or',lang)+' ').join(rule_text(x,lang) for x in rule['any'])+')'
    symbols={'in':'∈','not_in':'∉','contains':'∩ ≠ ∅'}
    return rule['field']+' '+symbols[rule['op']]+' {'+', '.join(str(x) for x in rule['values'])+'}'

def fallback_text(form_id,lang):
    f=FORMS[form_id];lines=[text(f['title'],lang),SCHEMA_VERSION,ui('dates',lang),text(NOTICE,lang),ui('fallback_filter_guide',lang)]
    for s in f['sections']:
        lines.extend(['\n'+s['id']+' — '+text(s['title'],lang),text(s['note'],lang)])
        for field in s['fields']:
            lines.append(field['id']+' — '+text(field['label'],lang))
            if s.get('gate') or field.get('gate'):
                lines.append(ui('condition',lang)+' : '+rule_text({'all':[x for x in [s.get('gate'),field.get('gate')] if x]},lang))
            if field.get('max_choices'):lines.append(ui('max_choices',lang,n=field['max_choices']))
            if field.get('max_words'):lines.append(ui('words',lang,n=field['max_words']))
            lines.extend('[ ] '+str(o['code'])+' '+text(o['label'],lang) for o in field['options'])
            lines.append('____________________________')
    lines.append(ui('not_validation',lang))
    return '\n'.join(lines).encode('utf-8-sig')


# Interface helpers — values remain the original instrument codes.
def remaining_choices(field, selected):
    selected = list(dict.fromkeys(selected or []))
    codes = [o['code'] for o in field.get('options', [])]
    if len(selected) >= field.get('max_choices', len(codes)):
        return []
    exclusive = field.get('exclusive', [])
    if any(v in exclusive for v in selected):
        return []
    return [c for c in codes if c not in selected and (not selected or c not in exclusive)]


def priority_choices(form_id, field, answers):
    prefix = FORMS[form_id].get('priority_prefix')
    fid = field['id']
    codes = [o['code'] for o in field.get('options', [])]
    if not prefix or fid not in [prefix + '_P' + str(i) for i in (1, 2, 3)]:
        return codes
    rank = int(fid[-1])
    used = [answers.get(prefix + '_P' + str(i)) for i in range(1, rank)]
    if rank > 1 and (any(blank(v) for v in used) or used[0] in (0, 98)):
        return []
    return [c for c in codes if c not in used]


def submission_requirements(form_id, answers, lang, settings, info,
                            confirmed, authority, access_code='', storage_error=''):
    """Return explicit blocking reasons; never bypass campaign or consent rules."""
    errors, _ = validation(form_id, answers, lang)
    effective, _ = field_states(form_id, answers)
    substantive = [f for _, f in iter_fields(form_id)
                   if f['id'] != FORMS[form_id]['consent'] and not f.get('private')]
    if not any(not blank(effective.get(f['id'])) for f in substantive):
        errors.append(ui('content_required', lang))
    if not confirmed:
        errors.append(ui('need_confirmation', lang))
    if effective.get(FORMS[form_id]['attribution']) in (1, 2) and not authority:
        errors.append(ui('need_authority', lang))
    if settings.mode == 'preview':
        errors.append(ui('preview_block', lang))
    elif storage_error or not isinstance(info, dict):
        errors.append(ui('service_error', lang))
    else:
        if info.get('active') is not True:
            errors.append(ui('closed', lang))
        if info.get('requires_access_code') and not str(access_code).strip():
            errors.append(ui('need_access', lang))
        if type(info.get('test_mode')) is bool and info['test_mode'] != settings.test_mode:
            errors.append(ui('mode_mismatch', lang))
    if settings.mode == 'production' and not settings.test_mode and lang not in settings.approved_languages:
        errors.append(ui('unreviewed', lang))
    return list(dict.fromkeys(errors))
