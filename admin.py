"""Administration séparée : authentification OIDC obligatoire en production.

Démarrage : streamlit run admin.py
Ne jamais mettre SUPABASE_ADMIN_KEY dans les secrets de app.py.
"""
from __future__ import annotations
import json
import time
from collections import Counter
from urllib.parse import urlencode,urlsplit,urlunsplit
import streamlit as st
from diagnostic.schema import FORMS,text
from diagnostic.storage import Settings,ConfigurationError,StorageError,make_store
from diagnostic.exports import export_bundle,csv_bytes,contribution_rows

st.set_page_config(page_title="HISWACA — Administration",page_icon="🔒",layout="wide")


def read_config():
    try: return dict(st.secrets)
    except FileNotFoundError: return {}


def authenticate(cfg,settings):
    if settings.mode=="local" and cfg.get("ALLOW_LOCAL_ADMIN") is True:
        st.warning("Administration locale de test, sans données réelles. Ne pas publier cette configuration.")
        return "test-local"
    if settings.mode!="production":
        st.info("Administration désactivée en prévisualisation. Configurer les secrets d’administration.")
        st.stop()
    allowed={str(v).strip().lower() for v in cfg.get("ADMIN_ALLOWED_EMAILS",[]) if str(v).strip()}
    if not allowed or "auth" not in cfg:
        st.error("Configurer l’authentification OIDC et une liste ADMIN_ALLOWED_EMAILS non vide.")
        st.stop()
    if not st.user.is_logged_in:
        st.info("Espace réservé au consultant et aux personnes habilitées par STATAFRIC.")
        if st.button("Connexion sécurisée",type="primary"):
            st.login()
        st.stop()
    email=str(st.user.get("email","")).lower()
    verified=st.user.get("email_verified",False) is True
    expiry=st.user.get("exp")
    try: valid_expiry=float(expiry)>time.time()
    except (ValueError,TypeError): valid_expiry=False
    if email not in allowed or not verified or not valid_expiry:
        st.error("Accès refusé : compte non autorisé, adresse non vérifiée ou session expirée.")
        if st.button("Se déconnecter"):
            st.logout()
        st.stop()
    st.sidebar.caption("Compte : "+email)
    if st.sidebar.button("Se déconnecter"):
        # Retirer toute réponse conservée en mémoire de session.
        for key in list(st.session_state): del st.session_state[key]
        st.logout()
    return email


def main():
    cfg=read_config()
    try: settings=Settings.from_mapping(cfg,admin=True)
    except ConfigurationError as exc:
        st.error(str(exc));st.stop()
    actor=authenticate(cfg,settings)
    store=make_store(settings,admin=True)
    st.title("Diagnostic HISWACA — administration")
    st.caption("Les compteurs portent sur des contributions, pas sur des INS distincts. Aucun score ni taux de représentativité n’est calculé.")
    if st.button("Charger ou actualiser les réponses",type="primary"):
        try:
            st.session_state["admin_records"]=store.records()
            st.session_state["admin_reviews"]=store.reviews()
            st.session_state["loaded_at"]=time.strftime("%Y-%m-%d %H:%M:%S UTC",time.gmtime())
        except StorageError:
            st.error("Lecture impossible. Vérifier la connexion et la configuration ; aucun export partiel n’est présenté comme complet.")
    if "loaded_at" in st.session_state:
        st.caption("Dernière lecture réussie : "+st.session_state["loaded_at"])
    records=st.session_state.get("admin_records",[])
    selected_forms=st.multiselect("Questionnaires",list(FORMS),default=list(FORMS),format_func=lambda x:text(FORMS[x]["title"]))
    include_tests=st.checkbox("Inclure les réponses de test",value=False)
    filtered=[r for r in records if r["payload"]["form_id"] in selected_forms and (include_tests or not r["payload"]["is_test"])]
    cols=st.columns(3)
    cols[0].metric("Contributions affichées",len(filtered))
    cols[1].metric("Réponses abrégées",sum(r["payload"]["form_id"]=="abrege" for r in filtered))
    cols[2].metric("À examiner ou à clarifier",sum((r.get("review") or {}).get("status","À examiner") in ["À examiner","À clarifier"] for r in filtered))
    if records and not filtered: st.info("Aucune contribution ne correspond aux filtres. Les réponses de test sont masquées par défaut.")
    if filtered:
        st.dataframe(contribution_rows(filtered),hide_index=True,use_container_width=True)
        st.warning("Export interne : l’absence de coordonnées ne rend pas les récits ni les noms d’institutions anonymes. Vérifier les droits de citation avant toute diffusion.")
        st.download_button("Télécharger l’export analytique ZIP (CSV + JSON)",export_bundle(filtered),
                           file_name="HISWACA_export_analytique.zip",mime="application/zip")
        selection=st.selectbox("Contribution à examiner",[r["id"] for r in filtered],format_func=lambda uid:next(r["receipt"] for r in filtered if r["id"]==uid))
        record=next(r for r in filtered if r["id"]==selection)
        with st.expander("Original analytique et états des questions"):
            st.json(record["payload"])
        with st.form("review_form"):
            st.subheader("Ajouter une décision de contrôle")
            status=st.selectbox("Statut",["À examiner","Exploitable","À clarifier","Doublon","Hors champ"])
            note=st.text_area("Justification obligatoire",max_chars=5000)
            links=[None]+[r["id"] for r in records if r["id"]!=selection]
            linked=st.selectbox("Lier à une contribution, obligatoire pour un doublon",links,format_func=lambda uid:"Aucun lien" if uid is None else "REP-"+uid)
            sent=st.form_submit_button("Enregistrer la décision")
        if sent:
            if not note.strip() or (status=="Doublon" and linked is None):
                st.error("Renseigner une justification et, pour un doublon, la contribution liée.")
            else:
                try:
                    store.add_review(selection,actor,status,note.strip(),linked or "")
                    st.session_state["admin_records"]=store.records()
                    st.session_state["admin_reviews"]=store.reviews()
                except StorageError:
                    st.error("L’enregistrement de la décision n’a pas été confirmé.")
                else:
                    st.success("Décision ajoutée. L’original de la réponse n’a pas été modifié.")
        with st.expander("Historique des décisions"):
            history=[r for r in st.session_state.get("admin_reviews",[]) if r["response_id"]==selection]
            st.dataframe(history,hide_index=True,use_container_width=True)
        with st.expander("Coordonnées facultatives — accès restreint"):
            st.caption("Les coordonnées sont stockées séparément. Ne pas les joindre à un export analytique ni les publier.")
            consent=st.checkbox("L’accès aux coordonnées est nécessaire au suivi de la mission.")
            if st.button("Préparer l’export des contacts",disabled=not consent):
                try:
                    ids={r["id"] for r in filtered}
                    contacts=store.all_contacts()
                    rows=[{"ID réponse":"REP-"+r["response_id"],"Champ":k,"Valeur":v} for r in contacts if r["response_id"] in ids for k,v in r["contact"].items()]
                    st.session_state["contacts_export"]=csv_bytes(rows,["ID réponse","Champ","Valeur"])
                    st.session_state["contacts_export_scope"]=sorted(ids)
                except StorageError:
                    st.error("Lecture des contacts impossible.")
            if st.session_state.get("contacts_export_scope")!=sorted(r["id"] for r in filtered):
                st.session_state.pop("contacts_export",None)
            if consent and "contacts_export" in st.session_state:
                st.download_button("Télécharger les contacts séparément",st.session_state["contacts_export"],file_name="HISWACA_contacts_RESTREINT.csv",mime="text/csv")
            elif not consent:
                st.session_state.pop("contacts_export",None)
    st.divider()
    with st.expander("Liens à transmettre aux parties prenantes"):
        base=st.text_input("URL de l’application de collecte",value=settings.collection_url,placeholder="https://votre-application.streamlit.app/")
        parsed=urlsplit(base)
        if parsed.scheme=="https" and parsed.netloc and not parsed.username and not parsed.password:
            base=urlunsplit((parsed.scheme,parsed.netloc,parsed.path or "/","",""))
            for f in FORMS:
                st.markdown("**"+text(FORMS[f]["title"])+"**")
                for lang in ["fr","en"]:
                    st.code(base+"?"+urlencode({"form":f,"lang":lang}),language=None)
        elif base:
            st.warning("Renseigner une URL HTTPS valide, sans identifiant ni mot de passe.")
        st.caption("Ces liens sélectionnent le parcours et la langue. Ils ne prouvent ni l’identité ni l’habilitation institutionnelle du répondant.")
    if settings.mode=="production":
        with st.expander("Ouverture ou fermeture de la campagne"):
            st.caption("La date souhaitée du 17 novembre est indicative. La clôture technique est indépendante et paramétrée en base.")
            if st.button("Afficher l’état de la campagne"):
                try: st.json(store.admin_campaign())
                except StorageError: st.error("État indisponible.")
            action=st.selectbox("Action à appliquer",["Fermer la collecte","Ouvrir la collecte"])
            confirm=st.checkbox("Je confirme être habilité à modifier l’ouverture de cette campagne.")
            if st.button("Appliquer",disabled=not confirm):
                try: store.set_active(action=="Ouvrir la collecte")
                except StorageError: st.error("Modification non confirmée.")
                else: st.success("État mis à jour. Les bornes de dates en base restent applicables.")


main()
