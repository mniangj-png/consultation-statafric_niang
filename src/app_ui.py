"""Public collection UI. The builder embeds all local dependencies in app.py."""
import streamlit as st
import time
from collections.abc import Mapping

def read_config():
    try:return dict(st.secrets)
    except FileNotFoundError:return {}

def get_answers(form_id):return st.session_state.setdefault('answers_'+form_id,{})

def clear_widget_keys():
    for k in list(st.session_state):
        if k.startswith(('w|','authority_','confirm_')):del st.session_state[k]

def on_language_change():clear_widget_keys()

def capture_answer(form_id,fid,key):
    answers=get_answers(form_id);answers[fid]=st.session_state[key]
    if fid==FORMS[form_id]['consent'] and answers[fid]!=1:
        st.session_state['answers_'+form_id]={fid:answers[fid]}
    else:st.session_state['answers_'+form_id]=field_states(form_id,answers)[0]
    valid=get_answers(form_id)
    for k in list(st.session_state):
        if k.startswith('w|'+form_id+'|') and k.split('|',3)[-1] not in valid:del st.session_state[k]
    for k in ('authority_'+form_id,'confirm_'+form_id):st.session_state.pop(k,None)

def render_field(form_id,field,lang):
    fid=field['id'];key=f'w|{form_id}|{lang}|{fid}';answers=get_answers(form_id)
    if key not in st.session_state:
        value=answers.get(fid)
        st.session_state[key]=([] if value is None else value) if field['type']=='multi' else ('' if value is None else value) if field['type'] in ('text','textarea','month','email') else value
    opts={o['code']:text(o['label'],lang) for o in field['options']}
    kw={'key':key,'on_change':capture_answer,'args':(form_id,fid,key)}
    label=text(field['label'],lang)
    if field['type']=='single':st.selectbox(label,[None,*opts],format_func=lambda v:ui('empty',lang) if v is None else opts[v],**kw)
    elif field['type']=='multi':
        st.multiselect(label,list(opts),format_func=lambda v:opts[v],**kw)
        if field.get('max_choices'):st.caption(ui('max_choices',lang,n=field['max_choices']))
    elif field['type']=='textarea':
        st.text_area(label,height=115,max_chars=field.get('max_chars',2500),**kw)
        if field.get('max_words'):st.caption(ui('words',lang,n=field['max_words']))
    else:st.text_input(label,max_chars=field.get('max_chars',500),**kw)
    answers[fid]=st.session_state[key]

def safe_public_error(code,lang):
    if code=='COLLECTION_CLOSED':return ui('closed',lang)
    if code=='INVALID_ACCESS_CODE':return ui('invalid_access',lang)
    if code=='SUBMISSION_CONFLICT':return ui('conflict',lang)
    if code=='NETWORK_UNCERTAIN':return ui('pending',lang)
    return ui('service_error',lang)

def perform_submission(store,pending,access,lang):
    try:receipt=store.submit(pending['payload'],pending['contacts'],access)
    except StorageError as exc:st.session_state['last_submit_error']=str(exc)
    else:
        st.session_state['last_success']={'payload':pending['payload'],'receipt':receipt}
        st.session_state.pop('pending',None);st.session_state.pop('last_submit_error',None)
    st.rerun()

def main():
    st.set_page_config(page_title='HISWACA — Diagnostic',page_icon='📋',layout='centered')
    # Imported state must be applied before widget instantiation.
    if 'draft_to_apply' in st.session_state:
        d=st.session_state.pop('draft_to_apply');clear_widget_keys()
        for k in ('ui_form','ui_language','upload_draft'):st.session_state.pop(k,None)
        st.session_state['ui_form']=d['form_id'];st.session_state['ui_language']=d['language']
        st.session_state['answers_'+d['form_id']]=d['answers'];st.session_state['id_'+d['form_id']]=d['submission_id']
        st.session_state['meta_'+d['form_id']]=d.get('metadata',{});st.session_state['step_'+d['form_id']]=FORMS[d['form_id']]['sections'][0]['id']
    if 'ui_language' not in st.session_state:
        candidate=st.query_params.get('lang','fr');st.session_state['ui_language']=candidate if candidate in LANGUAGES else 'fr'
    lang=st.sidebar.selectbox('Langue / Language / اللغة',LANGUAGES,key='ui_language',format_func=lambda l:LANG_NAMES[l],on_change=on_language_change)
    st.query_params['lang']=lang
    if lang=='ar':
        st.markdown('<style>[data-testid="stMarkdownContainer"], [data-testid="stWidgetLabel"], textarea, input {direction:rtl;text-align:right;unicode-bidi:plaintext;} pre,code {direction:ltr;text-align:left;} [data-baseweb="select"] {direction:rtl;}</style>',unsafe_allow_html=True)
    st.caption('HISWACA – SOP 1 · P178497 · ET-AUC-478525-CS-INDV · v'+APP_VERSION)
    st.title(ui('title',lang));st.caption(ui('dates',lang))
    cfg=read_config()
    try:settings=Settings.from_mapping(cfg)
    except ConfigurationError:
        st.error(ui('service_error',lang));st.stop()
    store=make_store(settings)
    if settings.contact_email:st.caption(ui('contact',lang)+' : '+settings.contact_email)
    if settings.mode=='preview':st.warning(ui('preview',lang))
    elif settings.test_mode:st.warning(ui('test',lang))
    language_ready=settings.test_mode or lang in settings.approved_languages
    if settings.mode=='production' and not language_ready:st.warning(ui('unreviewed',lang))
    if 'pending' in st.session_state:
        pending=st.session_state['pending'];st.warning(safe_public_error(st.session_state.get('last_submit_error','NETWORK_UNCERTAIN'),lang))
        st.code(pending['payload']['submission_id'],language=None)
        access=st.text_input(ui('access',lang),type='password',key='retry_code')
        pending_file={'kind':'hiswaca_pending',**pending}
        st.download_button(ui('pending_download',lang),json.dumps(pending_file,ensure_ascii=False,indent=2).encode(),file_name='HISWACA_pending.json',mime='application/json')
        st.caption(ui('draft_warning',lang))
        if st.button(ui('retry',lang),type='primary',disabled=store is None):perform_submission(store,pending,access,lang)
        st.stop()
    if 'last_success' in st.session_state:
        result=st.session_state['last_success'];p=result['payload'];receipt=result['receipt']
        st.success(ui('success_test' if p['is_test'] else 'success',lang));st.code(receipt['receipt'],language=None);st.caption('UTC : '+receipt['received_at'])
        st.download_button(ui('receipt',lang),receipt_text(p,receipt).encode(),file_name=receipt['receipt']+'.txt',mime='text/plain')
        st.info(ui('supplement',lang))
        if p['form_id']=='ins':st.info(text(FORMS['ins']['relay_note'],lang))
        if st.button(ui('new',lang)):
            keep=lang
            for k in list(st.session_state):del st.session_state[k]
            st.session_state['ui_language']=keep;st.rerun()
        st.stop()
    if 'ui_form' not in st.session_state:
        q=st.query_params.get('form','ins');st.session_state['ui_form']=q if q in FORMS else 'ins'
    form_id=st.sidebar.selectbox(ui('form',lang),list(FORMS),key='ui_form',format_func=lambda f:text(FORMS[f]['title'],lang))
    st.query_params['form']=form_id;form=FORMS[form_id]
    st.header(text(form['title'],lang));st.caption(text(form['duration'],lang)+' · '+form['version'])
    meta=st.session_state.setdefault('meta_'+form_id,{k:str(st.query_params.get(q,''))[:120] for k,q in [('relay_id','relay'),('invitation_id','inv')] if st.query_params.get(q)})
    # URL tags track invitations only. They never authorise access or fill answers.
    if form_id=='ins':st.info(text(form['relay_note'],lang))
    if form_id=='abrege':st.info(ui('short',lang))
    with st.expander(ui('notice',lang),expanded=not get_answers(form_id)):
        st.markdown(text(NOTICE,lang))
        # Deployment-specific notice must be supplied in all approved languages.
        custom=cfg.get('notice',{})
        if isinstance(custom,Mapping) and custom.get(lang):st.text(str(custom[lang]))
    st.sidebar.download_button(ui('fallback',lang),fallback_text(form_id,lang),file_name=f'HISWACA_{form_id}_{lang}.txt',mime='text/plain')
    with st.sidebar.expander(ui('resume',lang)):
        upload=st.file_uploader(ui('upload',lang),type=['json'],key='upload_draft')
        if st.button(ui('import',lang),disabled=upload is None):
            try:
                raw=upload.getvalue()
                kind=json.loads(raw.decode('utf-8-sig')).get('kind') if len(raw)<=MAX_DRAFT_BYTES else None
                if kind=='hiswaca_pending':st.session_state['pending']=load_pending(raw,settings.test_mode)
                else:st.session_state['draft_to_apply']=load_draft(raw)
            except (ValueError,AttributeError,ValidationError):st.error(ui('invalid_file',lang))
            else:st.rerun()
    answers=get_answers(form_id);uid=st.session_state.setdefault('id_'+form_id,str(uuid4()))
    sections=active_sections(form_id,answers);ids=[s['id'] for s in sections];step_key='step_'+form_id
    current=st.session_state.setdefault(step_key,ids[0])
    if current not in ids+['review']:current=ids[0];st.session_state[step_key]=current
    if current!='review':
        s=next(s for s in sections if s['id']==current);i=ids.index(current)
        st.progress(i/(len(sections)+1),text=ui('progress',lang,n=i+1,total=len(sections)))
        st.subheader(s['id']+' — '+text(s['title'],lang))
        if text(s['note'],lang):st.markdown(text(s['note'],lang))
        if evaluate(s.get('gate'),answers) is None:st.info(ui('indeterminate',lang))
        else:
            for f in s['fields']:
                _,states=field_states(form_id,get_answers(form_id))
                if states[f['id']] not in ('NF','FILTRE_INDETERMINE'):render_field(form_id,f,lang)
        answers=get_answers(form_id);consent=answers.get(form['consent']);eligible=not(form_id=='producteurs' and answers.get('QP01_Produit')==0)
        if consent==0:st.info(ui('refused',lang))
        elif not eligible:st.info(ui('ineligible',lang))
        elif consent==1:
            visible=[s['id'] for s in active_sections(form_id,answers)];i=visible.index(current) if current in visible else 0
            c1,c2,c3=st.columns(3)
            if c1.button(ui('previous',lang),disabled=i==0,key='prev'):
                st.session_state[step_key]=visible[max(0,i-1)];st.rerun()
            if c2.button(ui('next',lang),type='primary',key='next'):
                st.session_state[step_key]=visible[i+1] if i+1<len(visible) else 'review';st.rerun()
            if c3.button(ui('review',lang),key='review'):
                st.session_state[step_key]='review';st.rerun()
    else:
        st.progress(1.0,text=ui('review_title',lang));st.info(ui('partial',lang))
        errors,warnings=validation(form_id,answers,lang);effective,states=field_states(form_id,answers)
        for s in form['sections']:
            with st.expander(s['id']+' — '+text(s['title'],lang)):
                for f in s['fields']:
                    if not blank(effective.get(f['id'])):st.text(text(f['label'],lang)+'\n'+answer_label(f,effective[f['id']],lang))
                if st.button(ui('edit',lang),key='edit_'+s['id']):st.session_state[step_key]=s['id'];st.rerun()
        for e in errors:st.error(e)
        for w in warnings:st.warning(w)
        attr=effective.get(form['attribution']);authority=True
        if attr in (1,2):authority=st.checkbox(ui('authority',lang),key='authority_'+form_id)
        else:st.caption(ui('anonymous',lang))
        info=None
        if store:
            cache=st.session_state.get('campaign_cache')
            if not cache or time.monotonic()-cache[0]>20:
                try:info=store.campaign_info();st.session_state['campaign_cache']=(time.monotonic(),info)
                except StorageError:st.error(ui('service_error',lang))
            else:info=cache[1]
        open_now=bool(info and info.get('active'))
        if info and not open_now:st.warning(ui('closed',lang))
        access=st.text_input(ui('access',lang),type='password',key='access') if info and info.get('requires_access_code') else ''
        final=st.checkbox(ui('confirm',lang),key='confirm_'+form_id)
        if st.button(ui('submit',lang),type='primary',key='submit',disabled=bool(errors) or not final or not authority or not open_now or not language_ready or store is None):
            try:p,c=prepare_submission(form_id,answers,lang,uid,settings.test_mode,meta)
            except ValidationError as exc:st.error(str(exc))
            else:
                p['attribution_authority_confirmed']=bool(attr in (1,2) and authority)
                st.session_state['pending']={'payload':p,'contacts':c};perform_submission(store,st.session_state['pending'],access,lang)
    if get_answers(form_id).get(form['consent'])==1:
        with st.sidebar.expander(ui('draft',lang)):
            st.caption(ui('draft_warning',lang))
            st.download_button(ui('download_draft',lang),draft_bytes(form_id,get_answers(form_id),lang,uid,meta),file_name=f'HISWACA_brouillon_{form_id}.json',mime='application/json',key='download_'+form_id)
    st.caption(ui('not_validation',lang))

if __name__=='__main__':main()
