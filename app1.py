"""Application publique : quatre questionnaires, en français et en anglais.

Démarrage : streamlit run app.py
Sans secrets, la prévisualisation est possible mais aucun envoi n'est enregistré.
"""
from __future__ import annotations
import hashlib
import json
import time
from uuid import uuid4
import streamlit as st
from diagnostic.schema import FORMS,NOTICE,SCHEMA_VERSION,text,iter_fields
from diagnostic.engine import (ValidationError,active_sections,answer_label,blank,draft_bytes,evaluate,
                               field_states,load_draft,prepare_submission,receipt_text,validation)
from diagnostic.storage import Settings,ConfigurationError,StorageError,make_store

st.set_page_config(page_title="HISWACA — Diagnostic",page_icon="📋",layout="centered")


def tr(fr,en,lang):
    return fr if lang=="fr" else en


def config():
    try:
        return dict(st.secrets)
    except FileNotFoundError:
        return {}


def get_answers(form_id):
    return st.session_state.setdefault("answers_"+form_id,{})


def capture(form_id,fid,key):
    answers=get_answers(form_id)
    answers[fid]=st.session_state[key]
    if fid==FORMS[form_id]["consent"] and answers[fid]!=1:
        st.session_state["answers_"+form_id]={fid:answers[fid]}
    else:
        st.session_state["answers_"+form_id]=field_states(form_id,answers)[0]
    # Les clés des widgets masqués sont retirées avant leur recréation.
    valid=get_answers(form_id)
    for k in list(st.session_state):
        if k.startswith("w_"+form_id+"_"):
            target=k.split("_",3)[-1]
            if target not in valid and target!=fid:
                del st.session_state[k]


def render_field(form_id,field,lang):
    answers=get_answers(form_id)
    fid=field["id"]
    key=f"w_{form_id}_{lang}_{fid}"
    opts={o["code"]:text(o["label"],lang) for o in field["options"]}
    label=f"{fid} — {text(field['label'],lang)}"
    kwargs={"key":key,"on_change":capture,"args":(form_id,fid,key)}
    if key not in st.session_state:
        value=answers.get(fid)
        st.session_state[key]=([] if value is None else value) if field["type"]=="multi" else (("" if value is None else value) if field["type"] in ["text","textarea","month","email"] else value)
    if field["type"]=="single":
        def label_option(code):
            return tr("— Non renseigné —","— Not answered —",lang) if code is None else f"{code} · {opts[code]}"
        st.selectbox(label,options=[None,*opts],format_func=label_option,**kwargs)
    elif field["type"]=="multi":
        st.multiselect(label,options=list(opts),format_func=lambda c:f"{c} · {opts[c]}",**kwargs)
        if field.get("max_choices"):
            st.caption(tr(f"Au plus {field['max_choices']} choix.",f"Up to {field['max_choices']} choices.",lang))
    elif field["type"]=="textarea":
        st.text_area(label,height=110,max_chars=field.get("max_chars",2500),**kwargs)
        if field.get("max_words"):
            n=len(str(st.session_state[key]).split())
            st.caption(tr(f"{n} / {field['max_words']} mots.",f"{n} / {field['max_words']} words.",lang))
    else:
        st.text_input(label,max_chars=field.get("max_chars",500),**kwargs)
    # Nécessaire à la première création, les callbacks assurent ensuite la persistance.
    get_answers(form_id)[fid]=st.session_state[key]


def public_error(code,lang):
    messages={
      "NETWORK_UNCERTAIN":("Aucun accusé de réception n’a été reçu. L’état de l’envoi est incertain. Réessayez le même envoi : son identifiant empêche un double enregistrement.","No receipt was received. Submission status is uncertain. Retry the same submission: its ID prevents a duplicate record."),
      "COLLECTION_CLOSED":("La collecte est fermée. Conservez votre brouillon et contactez l’équipe.","The collection is closed. Keep your draft and contact the team."),
      "INVALID_ACCESS_CODE":("Le code d’accès est incorrect. Vérifiez le code reçu dans l’invitation.","The access code is incorrect. Check the code in your invitation."),
      "SUBMISSION_CONFLICT":("Cet identifiant correspond déjà à une autre version reçue. Contactez l’équipe avec votre référence ; ne tentez pas d’écraser la réponse.","This ID already corresponds to a different received version. Contact the team with your reference; do not overwrite the response."),
      "QUOTA_REACHED":("La capacité prévue est atteinte. Contactez l’équipe ; ne présumez pas que votre réponse a été reçue.","The configured capacity has been reached. Contact the team; do not assume your response was received."),
      "INVALID_WRITER":("Le service de collecte doit être configuré par l’administrateur.","The collection service needs administrator configuration."),
      "INVALID_SUBMISSION":("L’envoi n’a pas été accepté. Conservez votre brouillon et contactez l’équipe.","The submission was not accepted. Keep your draft and contact the team."),
    }
    fr,en=messages.get(code,("Le service n’a pas confirmé la réception. Conservez votre brouillon et contactez l’équipe.","The service did not confirm receipt. Keep your draft and contact the team."))
    return tr(fr,en,lang)


def perform_submission(store,pending,access_code,lang):
    try:
        receipt=store.submit(pending["payload"],pending["contacts"],access_code)
    except StorageError as exc:
        st.session_state["last_submit_error"]=str(exc)
        st.rerun()
    else:
        st.session_state["last_success"]={"payload":pending["payload"],"receipt":receipt}
        st.session_state.pop("pending",None)
        st.session_state.pop("last_submit_error",None)
        st.rerun()


def main():
    # Appliquer l'import AVANT la création des widgets de cette exécution.
    if "draft_to_apply" in st.session_state:
        data=st.session_state.pop("draft_to_apply")
        for k in list(st.session_state):
            if k.startswith("w_") or k in ["ui_form","ui_language","draft_upload"]:
                del st.session_state[k]
        st.session_state["ui_form"]=data["form_id"]
        st.session_state["ui_language"]=data["language"]
        st.session_state["answers_"+data["form_id"]]=data["answers"]
        st.session_state["id_"+data["form_id"]]=data["submission_id"]
        st.session_state["step_"+data["form_id"]]=FORMS[data["form_id"]]["sections"][0]["id"]
        st.query_params["form"]=data["form_id"]
        st.query_params["lang"]=data["language"]
    cfg=config()
    try:
        settings=Settings.from_mapping(cfg)
    except ConfigurationError as exc:
        st.error(str(exc)); st.stop()
    store=make_store(settings)
    if "ui_language" not in st.session_state:
        q=st.query_params.get("lang","fr")
        st.session_state["ui_language"]=q if q in ["fr","en"] else "fr"
    lang=st.sidebar.selectbox("Langue / Language",["fr","en"],key="ui_language",format_func=lambda l:"Français" if l=="fr" else "English")
    st.query_params["lang"]=lang
    st.caption("HISWACA – SOP 1 · P178497 · ET-AUC-478525-CS-INDV")
    st.title(tr("Appui statistique aux discours des décideurs","Statistical support for decision-makers’ speeches",lang))
    if settings.mode=="preview":
        st.warning(tr("Prévisualisation uniquement — aucun envoi n’est possible. Ne saisissez pas de données réelles.","Preview only — submissions are disabled. Do not enter real data.",lang))
    elif settings.mode=="local" or settings.test_mode:
        st.warning(tr("MODE TEST — utiliser uniquement des réponses fictives. Ces réponses sont séparées du diagnostic réel.","TEST MODE — use fictitious responses only. These responses are separate from the actual diagnostic.",lang))
    st.caption(tr("Référence : 1er novembre 2025 – 31 octobre 2026. Réponse souhaitée : 17 novembre 2026.","Reference period: 1 November 2025 – 31 October 2026. Requested response date: 17 November 2026.",lang))
    if settings.contact_email:
        st.caption(tr("Contact de la mission : ","Assignment contact: ",lang)+settings.contact_email)

    # Une soumission au résultat incertain est figée : seuls ses essais identiques sont permis.
    if "pending" in st.session_state:
        pending=st.session_state["pending"]
        st.warning(public_error(st.session_state.get("last_submit_error","REMOTE_ERROR"),lang))
        st.code(pending["payload"]["submission_id"],language=None)
        access=st.text_input(tr("Code d’accès, si demandé","Access code, if required",lang),type="password",key="retry_code")
        st.download_button(tr("Conserver la preuve de l’envoi en attente","Keep a copy of the pending submission",lang),
            json.dumps(pending,ensure_ascii=False,indent=2).encode(),file_name="HISWACA_envoi_en_attente.json",mime="application/json")
        st.caption(tr("Ce fichier peut contenir vos coordonnées. Ce n’est pas un accusé de réception.","This file may contain your contact details. It is not a receipt.",lang))
        if st.button(tr("Réessayer exactement le même envoi","Retry the exact same submission",lang),type="primary") and store:
            perform_submission(store,pending,access,lang)
        st.stop()

    if "last_success" in st.session_state:
        result=st.session_state["last_success"]
        receipt=result["receipt"]; payload=result["payload"]
        st.success(tr("Votre réponse de test a été enregistrée." if payload["is_test"] else "Votre contribution a été enregistrée. Merci.",
                      "Your test response was recorded." if payload["is_test"] else "Your contribution was recorded. Thank you.",lang))
        st.code(receipt["receipt"],language=None)
        st.caption("UTC : "+receipt["received_at"])
        st.download_button(tr("Télécharger mon accusé et mes réponses","Download my receipt and responses",lang),
                           receipt_text(payload,receipt).encode("utf-8"),file_name=receipt["receipt"]+".txt",mime="text/plain")
        st.info(tr("Pour un complément, faites une nouvelle contribution et rappelez cette référence. Aucune réponse déjà reçue n’est écrasée.","For a supplement, make a new contribution and include this reference. No received response is overwritten.",lang))
        if st.button(tr("Commencer une nouvelle contribution","Start a new contribution",lang)):
            for k in list(st.session_state):
                if k!="ui_language": del st.session_state[k]
            st.rerun()
        st.stop()

    if "ui_form" not in st.session_state:
        q=st.query_params.get("form","ins")
        st.session_state["ui_form"]=q if q in FORMS else "ins"
    form_id=st.sidebar.selectbox(tr("Votre questionnaire","Your questionnaire",lang),list(FORMS),
                 key="ui_form",format_func=lambda f:text(FORMS[f]["title"],lang))
    st.query_params["form"]=form_id
    form=FORMS[form_id]
    st.header(text(form["title"],lang))
    st.caption(text(form["duration"],lang)+" · "+SCHEMA_VERSION)
    if form_id=="abrege":
        st.info(tr("Cette version est enregistrée comme réponse partielle. Les questions omises ne seront pas déduites de vos réponses.","This version is recorded as a partial response. Answers to omitted questions will not be inferred.",lang))
    with st.expander(tr("Notice de participation et protection des réponses","Participation notice and protection of responses",lang),expanded=not get_answers(form_id)):
        st.markdown(text(NOTICE,lang))
        st.text(settings.notice_controller)
        if settings.mode=="production": st.text(settings.retention_notice)
        st.caption(tr("Les conditions techniques d’hébergement sont gérées par l’équipe. Les champs de contact sont facultatifs. Aucun document confidentiel n’est demandé.","Hosting arrangements are managed by the team. Contact fields are optional. No confidential document is requested.",lang))

    with st.sidebar.expander(tr("Reprendre un brouillon","Resume a draft",lang)):
        upload=st.file_uploader(tr("Brouillon JSON téléchargé depuis cette application","JSON draft downloaded from this application",lang),type=["json"],key="draft_upload")
        if st.button(tr("Importer le brouillon","Import draft",lang),disabled=upload is None):
            try:
                data=load_draft(upload.getvalue())
            except ValidationError as exc:
                st.error(str(exc))
            else:
                st.session_state["draft_to_apply"]=data
                st.rerun()

    answers=get_answers(form_id)
    uid=st.session_state.setdefault("id_"+form_id,str(uuid4()))
    sections=active_sections(form_id,answers)
    step_key="step_"+form_id
    current=st.session_state.setdefault(step_key,sections[0]["id"])
    ids=[s["id"] for s in sections]
    if current not in ids+["review"]:
        current=ids[0];st.session_state[step_key]=current
    if current!="review":
        section=next(s for s in sections if s["id"]==current)
        idx=ids.index(current)
        st.progress(idx/(len(sections)+1),text=tr(f"Rubrique {idx+1} sur {len(sections)}","Section "+str(idx+1)+" of "+str(len(sections)),lang))
        st.subheader(section["id"]+" — "+text(section["title"],lang))
        if text(section["note"],lang): st.markdown(text(section["note"],lang))
        section_active=evaluate(section.get("gate"),answers)
        if section_active is None:
            st.info(tr("Cette rubrique dépend d’une réponse amont non renseignée. Revenez à la rubrique précédente pour préciser le parcours, ou poursuivez avec une réponse partielle.","This section depends on an unanswered earlier item. Return to clarify the route, or continue with a partial response.",lang))
        else:
            for field in section["fields"]:
                if section["id"]==form["sections"][0]["id"] and field["id"]!=form["consent"] and get_answers(form_id).get(form["consent"])!=1:
                    continue
                effective,states=field_states(form_id,get_answers(form_id))
                if states[field["id"]] in ["NF","FILTRE_INDETERMINE"]:
                    continue
                render_field(form_id,field,lang)
        answers=get_answers(form_id)
        consent_value=answers.get(form["consent"])
        if consent_value==0:
            st.info(tr("Votre choix est respecté. Aucune réponse n’a été enregistrée dans la base.","Your choice is respected. No response has been stored in the database.",lang))
        elif consent_value==1:
            visible=active_sections(form_id,answers); visible_ids=[s["id"] for s in visible]
            n=visible_ids.index(current) if current in visible_ids else 0
            c1,c2,c3=st.columns(3)
            if c1.button(tr("Précédent","Previous",lang),disabled=n==0):
                st.session_state[step_key]=visible_ids[max(0,n-1)];st.rerun()
            if c2.button(tr("Suivant","Next",lang),type="primary"):
                st.session_state[step_key]=visible_ids[n+1] if n+1<len(visible_ids) else "review";st.rerun()
            if c3.button(tr("Relire et envoyer","Review and submit",lang)):
                st.session_state[step_key]="review";st.rerun()
    else:
        st.progress(1.0,text=tr("Relecture avant envoi","Review before submission",lang))
        errors,warnings=validation(form_id,answers,lang)
        effective,states=field_states(form_id,answers)
        st.info(tr("Une réponse partielle est recevable. Les champs laissés vides restent non renseignés. Vérifiez les priorités, la période et l’autorisation de citation.","Partial responses are accepted. Blank fields remain unanswered. Check priorities, reference period and citation permission.",lang))
        for section in form["sections"]:
            with st.expander(section["id"]+" — "+text(section["title"],lang)):
                for field in section["fields"]:
                    fid=field["id"]; value=effective.get(fid)
                    if not blank(value):
                        st.text(fid+" — "+text(field["label"],lang)+"\n"+answer_label(field,value,lang))
                if st.button(tr("Modifier cette rubrique","Edit this section",lang),key="edit_"+section["id"]):
                    st.session_state[step_key]=section["id"];st.rerun()
        for err in errors: st.error(err)
        for warn in warnings: st.warning(warn)
        attr=effective.get(form["attribution"])
        authority=True
        if attr in [1,2]:
            authority=st.checkbox(tr("Je confirme disposer de l’autorisation d’attribution institutionnelle et, pour le choix 2, de citation.","I confirm that I am authorised to allow institutional attribution and, for option 2, quotation.",lang),key="authority_"+form_id)
        else:
            st.caption(tr("Sans choix explicite, seule une synthèse anonymisée est autorisée.","Without an explicit choice, only an anonymised summary is authorised.",lang))
        info=None
        if store is not None:
            cache=st.session_state.get("campaign_info_cache")
            if not cache or time.monotonic()-cache[0]>30:
                try:
                    info=store.campaign_info()
                    st.session_state["campaign_info_cache"]=(time.monotonic(),info)
                except StorageError as exc:
                    st.error(public_error(str(exc),lang))
            else: info=cache[1]
        is_open=bool(info and info.get("active"))
        code=""
        if info and info.get("requires_access_code"):
            code=st.text_input(tr("Code d’accès reçu dans l’invitation","Access code received in the invitation",lang),type="password",key="access_code")
        if info and not is_open:
            st.warning(tr("La campagne n’est pas ouverte à cet instant. Téléchargez votre brouillon.","The campaign is not currently open. Download your draft.",lang))
        final=st.checkbox(tr("Je souhaite transmettre cette contribution à l’équipe de la mission.","I wish to submit this contribution to the assignment team.",lang),key="confirm_"+form_id)
        disabled=bool(errors) or not authority or not final or not is_open or store is None
        if st.button(tr("Envoyer ma contribution","Submit my contribution",lang),type="primary",disabled=disabled):
            try:
                payload,contacts=prepare_submission(form_id,answers,lang,uid,settings.test_mode)
            except ValidationError as exc:
                st.error(str(exc))
            else:
                payload["attribution_authority_confirmed"]=bool(attr in [1,2] and authority)
                pending={"payload":payload,"contacts":contacts}
                st.session_state["pending"]=pending
                perform_submission(store,pending,code,lang)
    # Placé après les champs : le brouillon contient les dernières valeurs transmises.
    if get_answers(form_id).get(form["consent"])==1:
        with st.sidebar.expander(tr("Conserver mon brouillon","Keep my draft",lang)):
            st.caption(tr("Le fichier peut contenir vos coordonnées facultatives. Conservez-le sur un appareil sûr. Il ne vaut pas envoi.","The file may contain your optional contact details. Keep it on a safe device. Downloading is not submission.",lang))
            st.download_button(tr("Télécharger le brouillon JSON","Download JSON draft",lang),draft_bytes(form_id,get_answers(form_id),lang,uid),
                               file_name=f"HISWACA_brouillon_{form_id}.json",mime="application/json",key="download_draft_"+form_id)
    st.caption(tr("Une réception ne vaut ni validation des réponses ni approbation institutionnelle.","Receipt does not constitute validation of responses or institutional approval.",lang))


main()
