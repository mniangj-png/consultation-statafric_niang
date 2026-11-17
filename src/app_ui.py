# Public collection UI, interface patch 2.1.0. Instrument codes remain unchanged.
import streamlit as st
import time
import logging
from pathlib import Path
from collections.abc import Mapping

MAIN_TITLE = 'Appui statistique aux discours des décideurs'
SAFE_REJECTIONS = {'INVALID_ACCESS_CODE', 'COLLECTION_CLOSED', 'INVALID_WRITER',
                   'INVALID_SUBMISSION', 'QUOTA_REACHED', 'TEST_MODE_MISMATCH'}


def read_config():
    try:
        return dict(st.secrets)
    except FileNotFoundError:
        return {}


def get_answers(form_id):
    return st.session_state.setdefault('answers_' + form_id, {})


def clear_widget_keys(form_id=None):
    prefixes = ('w|', 'add|', 'remove|') if form_id is None else tuple(
        p + form_id + '|' for p in ('w|', 'add|', 'remove|'))
    for key in list(st.session_state):
        if key.startswith(prefixes):
            del st.session_state[key]
        elif (key.startswith(('authority_', 'confirm_')) and
              (form_id is None or key.endswith('_' + form_id))):
            del st.session_state[key]


def on_language_change():
    # Canonical answers are independent of the rendered widget language.
    clear_widget_keys()


def _store_answers(form_id, answers):
    if answers.get(FORMS[form_id]['consent']) != 1:
        answers = {FORMS[form_id]['consent']: answers.get(FORMS[form_id]['consent'])}
    st.session_state['answers_' + form_id] = field_states(form_id, answers)[0]
    clear_widget_keys(form_id)
    st.session_state.pop('submit_messages_' + form_id, None)


def capture_answer(form_id, fid, key):
    answers = dict(get_answers(form_id))
    answers[fid] = st.session_state[key]
    prefix = FORMS[form_id].get('priority_prefix')
    if prefix and fid in [prefix + '_P' + str(i) for i in (1, 2, 3)]:
        # Earlier ranks take precedence. Clear downstream ranks made invalid by editing.
        seen = set()
        gap = False
        for rank in (1, 2, 3):
            target = prefix + '_P' + str(rank)
            value = answers.get(target)
            if gap or (rank > 1 and answers.get(prefix + '_P1') in (0, 98)) or value in seen:
                answers.pop(target, None)
                gap = True
            elif blank(value):
                gap = True
            else:
                seen.add(value)
    _store_answers(form_id, answers)


def add_answer(form_id, fid, key):
    value = st.session_state.get(key)
    field = next(f for _, f in iter_fields(form_id) if f['id'] == fid)
    answers = dict(get_answers(form_id))
    selected = list(answers.get(fid) or [])
    if value in remaining_choices(field, selected):
        answers[fid] = selected + [value]
    _store_answers(form_id, answers)


def remove_answer(form_id, fid, value):
    answers = dict(get_answers(form_id))
    answers[fid] = [v for v in (answers.get(fid) or []) if v != value]
    _store_answers(form_id, answers)


def render_field(form_id, field, lang):
    fid = field['id']
    key = f'w|{form_id}|{lang}|{fid}'
    answers = get_answers(form_id)
    label = text(field['label'], lang)
    options = {o['code']: text(o['label'], lang) for o in field['options']}
    if field['type'] == 'multi':
        selected = list(answers.get(fid) or [])
        st.markdown('**' + label + '**')
        for value in selected:
            col_text, col_remove = st.columns([5, 1])
            col_text.text(options.get(value, str(value)))
            col_remove.button(ui('remove_choice', lang),
                              key=f'remove|{form_id}|{lang}|{fid}|{value}',
                              on_click=remove_answer, args=(form_id, fid, value))
        remaining = remaining_choices(field, selected)
        if remaining:
            add_key = f'add|{form_id}|{lang}|{fid}'
            if add_key not in st.session_state:
                st.session_state[add_key] = None
            st.selectbox(ui('add_choice', lang) + ' — ' + label, [None, *remaining],
                         key=add_key,
                         format_func=lambda v: ui('choose_next', lang) if v is None else options[v],
                         on_change=add_answer, args=(form_id, fid, add_key))
        else:
            st.caption(ui('multi_complete', lang))
        st.caption(ui('multi_help', lang))
        if field.get('max_choices'):
            st.caption(ui('max_choices', lang, n=field['max_choices']))
        return
    if key not in st.session_state:
        value = answers.get(fid)
        st.session_state[key] = ('' if value is None else value) if field['type'] in (
            'text', 'textarea', 'month', 'email') else value
    kwargs = {'key': key, 'on_change': capture_answer, 'args': (form_id, fid, key)}
    if field['type'] == 'single':
        allowed = priority_choices(form_id, field, answers)
        if st.session_state.get(key) not in [None, *allowed]:
            st.session_state[key] = None
            answers.pop(fid, None)
        st.selectbox(label, [None, *allowed],
                     format_func=lambda v: ui('empty', lang) if v is None else options[v],
                     disabled=not allowed, **kwargs)
        prefix = FORMS[form_id].get('priority_prefix')
        if prefix and fid in (prefix + '_P2', prefix + '_P3'):
            st.caption(ui('priority_help', lang))
    elif field['type'] == 'textarea':
        st.text_area(label, height=115, max_chars=field.get('max_chars', 2500), **kwargs)
        if field.get('max_words'):
            st.caption(ui('words', lang, n=field['max_words']))
    else:
        st.text_input(label, max_chars=field.get('max_chars', 500), **kwargs)
    get_answers(form_id)[fid] = st.session_state[key]


def safe_public_error(code, lang):
    messages = {'COLLECTION_CLOSED': 'closed', 'INVALID_ACCESS_CODE': 'invalid_access',
                'SUBMISSION_CONFLICT': 'conflict', 'NETWORK_UNCERTAIN': 'pending',
                'TEST_MODE_MISMATCH': 'mode_mismatch'}
    return ui(messages.get(code, 'service_error'), lang)


def perform_submission(store, pending, access, lang):
    try:
        receipt = store.submit(pending['payload'], pending['contacts'], access)
    except StorageError as exc:
        st.session_state['last_submit_error'] = str(exc)
    else:
        st.session_state['last_success'] = {'payload': pending['payload'], 'receipt': receipt}
        st.session_state.pop('pending', None)
        st.session_state.pop('last_submit_error', None)
    st.rerun()


def campaign_status(store, settings, force=False):
    if store is None:
        return None, ''
    cache_key = (settings.mode, settings.url, settings.campaign, settings.test_mode,
                 hashlib.sha256(settings.writer_token.encode()).hexdigest())
    cached = st.session_state.get('campaign_cache_v21')
    if not force and cached and cached[0] == cache_key and time.monotonic() - cached[1] < 15:
        return cached[2], ''
    try:
        info = store.campaign_info()
    except StorageError as exc:
        st.session_state.pop('campaign_cache_v21', None)
        return None, str(exc)
    st.session_state['campaign_cache_v21'] = (cache_key, time.monotonic(), info)
    return info, ''


def show_header(form_id, lang):
    # First visible element in the main page, including pending/receipt/error screens.
    st.title(MAIN_TITLE)
    if lang != 'fr':
        st.caption(ui('title', lang))
    st.header(text(FORMS[form_id]['title'], lang))
    st.caption('HISWACA – SOP 1 · P178497 · ET-AUC-478525-CS-INDV · v' + APP_VERSION)
    st.caption(ui('dates', lang))
    if lang == 'ar':
        st.markdown('<style>[data-testid="stMarkdownContainer"], [data-testid="stWidgetLabel"], '
                    'textarea, input {direction:rtl;text-align:right;unicode-bidi:plaintext;} '
                    'h1 {direction:ltr!important;text-align:left!important;} '
                    'pre,code {direction:ltr;text-align:left;} [data-baseweb="select"] '
                    '{direction:rtl;}</style>', unsafe_allow_html=True)


def initialise_navigation():
    """Separate URL routing, widget selection and canonical answers."""
    if 'draft_to_apply' in st.session_state:
        data = st.session_state.pop('draft_to_apply')
        clear_widget_keys()
        for key in ('ui_form', 'ui_language', 'upload_draft'):
            st.session_state.pop(key, None)
        st.session_state['ui_form'] = data['form_id']
        st.session_state['ui_language'] = data['language']
        st.session_state['answers_' + data['form_id']] = data['answers']
        st.session_state['id_' + data['form_id']] = data['submission_id']
        st.session_state['meta_' + data['form_id']] = data.get('metadata', {})
        st.session_state['step_' + data['form_id']] = FORMS[data['form_id']]['sections'][0]['id']
        st.query_params['form'] = data['form_id']
        st.query_params['lang'] = data['language']
        st.session_state['_last_url'] = (data['form_id'], data['language'])
    qform = st.query_params.get('form', 'ins')
    qlang = st.query_params.get('lang', 'fr')
    target = (qform if qform in FORMS else 'ins', qlang if qlang in LANGUAGES else 'fr')
    last_url = st.session_state.get('_last_url')
    if last_url is not None and target != last_url:
        if target[1] != st.session_state.get('ui_language'):
            clear_widget_keys()
        st.session_state['ui_form'], st.session_state['ui_language'] = target
    st.session_state.setdefault('ui_form', target[0])
    st.session_state.setdefault('ui_language', target[1])
    if st.session_state['ui_form'] not in FORMS:
        st.session_state['ui_form'] = target[0]
    if st.session_state['ui_language'] not in LANGUAGES:
        st.session_state['ui_language'] = target[1]


def jump_to(step_key, section_id):
    st.session_state[step_key] = section_id
    st.rerun()


def main():
    st.set_page_config(page_title=MAIN_TITLE, page_icon='📋', layout='centered')
    initialise_navigation()
    st.sidebar.selectbox('Langue / Language / اللغة', LANGUAGES, key='ui_language',
                         format_func=lambda lang: LANG_NAMES[lang], on_change=on_language_change)
    lang = st.session_state['ui_language']
    locked = 'pending' in st.session_state or 'last_success' in st.session_state
    st.sidebar.selectbox(ui('form', lang), list(FORMS), key='ui_form', disabled=locked,
                         format_func=lambda f: text(FORMS[f]['title'], lang))
    form_id = st.session_state['ui_form']
    if 'pending' in st.session_state:
        form_id = st.session_state['pending']['payload']['form_id']
    elif 'last_success' in st.session_state:
        form_id = st.session_state['last_success']['payload']['form_id']
    st.query_params['form'] = form_id
    st.query_params['lang'] = lang
    st.session_state['_last_url'] = (form_id, lang)
    show_header(form_id, lang)
    cfg = read_config()
    try:
        settings = Settings.from_mapping(cfg)
    except ConfigurationError:
        # Do not display raw secrets or backend responses to participants.
        logging.getLogger('hiswaca').error('HISWACA_CONFIGURATION_INVALID : vérifier les clés de configuration et la notice.')
        st.error(ui('service_error', lang))
        st.caption(ui('error_code', lang, code='HISWACA_CONFIGURATION_INVALID'))
        st.stop()
    store = make_store(settings)
    form = FORMS[form_id]
    if settings.contact_email:
        st.caption(ui('contact', lang) + ' : ' + settings.contact_email)
    if settings.mode == 'preview':
        st.warning(ui('preview', lang))
    elif settings.test_mode:
        st.warning(ui('test', lang))
    if settings.mode == 'production' and not settings.test_mode and lang not in settings.approved_languages:
        st.warning(ui('unreviewed', lang))
    with st.sidebar.expander(ui('quick_help', lang)):
        st.markdown(ui('quick_help_text', lang))
    guide_path = Path(__file__).resolve().parent / 'docs' / 'Guide_remplissage_HISWACA_FR.docx'
    if guide_path.is_file():
        st.sidebar.download_button(ui('guide_fr', lang), guide_path.read_bytes(),
                                   file_name=guide_path.name,
                                   mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                   key='guide_download')
    if 'pending' in st.session_state:
        pending = st.session_state['pending']
        error = st.session_state.get('last_submit_error', 'NETWORK_UNCERTAIN')
        st.warning(safe_public_error(error, lang))
        st.code(pending['payload']['submission_id'], language=None)
        access = st.text_input(ui('access', lang), type='password', key='retry_code')
        st.download_button(ui('pending_download', lang),
                           json.dumps({'kind': 'hiswaca_pending', **pending}, ensure_ascii=False, indent=2).encode(),
                           file_name='HISWACA_pending.json', mime='application/json')
        st.caption(ui('draft_warning', lang))
        if st.button(ui('retry', lang), type='primary', disabled=store is None):
            # A fresh campaign check must NOT block an idempotent retry after closing.
            perform_submission(store, pending, access, lang)
        if error in SAFE_REJECTIONS and st.button(ui('rejected_back', lang), key='back_rejected'):
            st.session_state.pop('pending', None)
            st.session_state.pop('last_submit_error', None)
            jump_to('step_' + form_id, 'review')
        st.stop()
    if 'last_success' in st.session_state:
        result = st.session_state['last_success']
        payload, receipt = result['payload'], result['receipt']
        st.success(ui('success_test' if payload['is_test'] else 'success', lang))
        st.code(receipt['receipt'], language=None)
        st.caption('UTC : ' + receipt['received_at'])
        st.download_button(ui('receipt', lang), receipt_text(payload, receipt).encode(),
                           file_name=receipt['receipt'] + '.txt', mime='text/plain')
        st.info(ui('supplement', lang))
        if form_id == 'ins':
            st.info(text(FORMS['ins']['relay_note'], lang))
        if st.button(ui('new', lang)):
            for key in list(st.session_state):
                del st.session_state[key]
            st.session_state['ui_language'] = lang
            st.rerun()
        st.stop()
    st.caption(text(form['duration'], lang) + ' · ' + form['version'])
    meta = st.session_state.setdefault('meta_' + form_id, {
        k: str(st.query_params.get(q, ''))[:120] for k, q in [('relay_id', 'relay'), ('invitation_id', 'inv')]
        if st.query_params.get(q)})
    if form_id == 'ins':
        st.info(text(form['relay_note'], lang))
    if form_id == 'abrege':
        st.info(ui('short', lang))
    with st.expander(ui('notice', lang), expanded=not get_answers(form_id)):
        st.markdown(text(NOTICE, lang))
        custom = cfg.get('notice', {})
        if isinstance(custom, Mapping) and custom.get(lang):
            st.text(str(custom[lang]))
    st.sidebar.download_button(ui('fallback', lang), fallback_text(form_id, lang),
                               file_name=f'HISWACA_{form_id}_{lang}.txt', mime='text/plain')
    with st.sidebar.expander(ui('resume', lang)):
        upload = st.file_uploader(ui('upload', lang), type=['json'], key='upload_draft')
        if st.button(ui('import', lang), disabled=upload is None):
            try:
                raw = upload.getvalue()
                kind = json.loads(raw.decode('utf-8-sig')).get('kind') if len(raw) <= MAX_DRAFT_BYTES else None
                if kind == 'hiswaca_pending':
                    st.session_state['pending'] = load_pending(raw, settings.test_mode)
                else:
                    st.session_state['draft_to_apply'] = load_draft(raw)
            except (ValueError, AttributeError, ValidationError):
                st.error(ui('invalid_file', lang))
            else:
                st.rerun()
    answers = get_answers(form_id)
    uid = st.session_state.setdefault('id_' + form_id, str(uuid4()))
    sections = active_sections(form_id, answers)
    ids = [s['id'] for s in sections]
    step_key = 'step_' + form_id
    current = st.session_state.setdefault(step_key, ids[0])
    if current not in ids + ['review']:
        current = ids[0]
        st.session_state[step_key] = current
    if current != 'review':
        section = next(s for s in sections if s['id'] == current)
        index = ids.index(current)
        st.progress(index / (len(sections) + 1), text=ui('progress', lang, n=index + 1, total=len(sections)))
        st.subheader(section['id'] + ' — ' + text(section['title'], lang))
        if text(section['note'], lang):
            st.markdown(text(section['note'], lang))
        if evaluate(section.get('gate'), answers) is None:
            st.info(ui('indeterminate', lang))
        else:
            for field in section['fields']:
                _, states = field_states(form_id, get_answers(form_id))
                if states[field['id']] not in ('NF', 'FILTRE_INDETERMINE'):
                    render_field(form_id, field, lang)
        answers = get_answers(form_id)
        consent = answers.get(form['consent'])
        eligible = not (form_id == 'producteurs' and answers.get('QP01_Produit') == 0)
        if consent == 0:
            st.info(ui('refused', lang))
        elif not eligible:
            st.info(ui('ineligible', lang))
        elif consent == 1:
            visible = [s['id'] for s in active_sections(form_id, answers)]
            index = visible.index(current) if current in visible else 0
            c1, c2, c3 = st.columns(3)
            if c1.button(ui('previous', lang), disabled=index == 0, key='prev'):
                jump_to(step_key, visible[max(0, index - 1)])
            if c2.button(ui('next', lang), type='primary', key='next'):
                jump_to(step_key, visible[index + 1] if index + 1 < len(visible) else 'review')
            if c3.button(ui('review', lang), key='review'):
                jump_to(step_key, 'review')
    else:
        st.progress(1.0, text=ui('review_title', lang))
        if st.button(ui('previous_review', lang), key='prev_review_top'):
            jump_to(step_key, ids[-1])
        st.info(ui('partial', lang))
        errors, warnings = validation(form_id, answers, lang)
        effective, states = field_states(form_id, answers)
        for section in sections:
            with st.expander(section['id'] + ' — ' + text(section['title'], lang)):
                for field in section['fields']:
                    if not blank(effective.get(field['id'])):
                        st.text(text(field['label'], lang) + '\n' + answer_label(field, effective[field['id']], lang))
                if st.button(ui('edit', lang), key='edit_' + section['id']):
                    jump_to(step_key, section['id'])
        for warning in warnings:
            st.warning(warning)
        attr = effective.get(form['attribution'])
        authority = True
        if attr in (1, 2):
            authority = st.checkbox(ui('authority', lang), key='authority_' + form_id)
        else:
            st.caption(ui('anonymous', lang))
        refresh = st.button(ui('refresh', lang), key='refresh_campaign')
        info, storage_error = campaign_status(store, settings, force=refresh)
        if info and info.get('opens_at') and info.get('closes_at'):
            st.caption(ui('campaign_dates', lang, start=info['opens_at'], end=info['closes_at']))
        if storage_error:
            st.caption(ui('error_code', lang, code=storage_error))
        access = st.text_input(ui('access', lang), type='password', key='access') if info and info.get('requires_access_code') else ''
        confirmed = st.checkbox(ui('confirm', lang), key='confirm_' + form_id)
        reasons = submission_requirements(form_id, answers, lang, settings, info,
                                         confirmed, authority, access, storage_error)
        st.subheader(ui('checklist', lang))
        if reasons:
            for reason in reasons:
                st.info(reason)
        else:
            st.success(ui('ready', lang))
        c_previous, c_submit = st.columns([1, 2])
        if c_previous.button(ui('previous_review', lang), key='prev_review'):
            jump_to(step_key, ids[-1])
        # Always clickable: submission is validated on click, never silently disabled.
        if c_submit.button(ui('submit', lang), type='primary', key='submit'):
            fresh_info, fresh_error = campaign_status(store, settings, force=True)
            blocks = submission_requirements(form_id, answers, lang, settings, fresh_info,
                                              confirmed, authority, access, fresh_error)
            if blocks:
                st.error(ui('not_sent', lang))
                for block in blocks:
                    st.error(block)
            else:
                try:
                    payload, contacts = prepare_submission(form_id, answers, lang, uid, settings.test_mode, meta)
                except ValidationError as exc:
                    st.error(str(exc))
                else:
                    payload['attribution_authority_confirmed'] = bool(attr in (1, 2) and authority)
                    st.session_state['pending'] = {'payload': payload, 'contacts': contacts}
                    perform_submission(store, st.session_state['pending'], access, lang)
    if get_answers(form_id).get(form['consent']) == 1:
        with st.sidebar.expander(ui('draft', lang)):
            st.caption(ui('draft_warning', lang))
            st.download_button(ui('download_draft', lang), draft_bytes(form_id, get_answers(form_id), lang, uid, meta),
                               file_name=f'HISWACA_brouillon_{form_id}.json', mime='application/json',
                               key='download_' + form_id)
    st.caption(ui('not_validation', lang))


if __name__ == '__main__':
    main()
