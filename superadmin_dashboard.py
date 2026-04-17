from __future__ import annotations

from datetime import datetime
import io
import zipfile

import pandas as pd
import streamlit as st

from dashboard_common import (
    build_analysis_sheets,
    build_report_docx_bytes,
    dataframe_to_xlsx_bytes,
    get_github_config_from_streamlit,
    load_json_records_from_repo,
    records_to_dataframe,
)

APP_VERSION = "superadmin-based-on-dashboard-common-2026-04-17-v1"

st.set_page_config(page_title="Dashboard Superadmin - validation du document", layout="wide")


def require_password() -> None:
    expected = ""
    try:
        expected = st.secrets.get("SUPERADMIN_PASSWORD", "")
    except Exception:
        expected = ""
    if not expected:
        st.warning("Aucun mot de passe Superadmin n’est défini dans les secrets Streamlit. Le tableau de bord est ouvert.")
        return
    if st.session_state.get("superadmin_ok"):
        return
    st.title("Dashboard Superadmin")
    pwd = st.text_input("Mot de passe Superadmin", type="password")
    if st.button("Ouvrir"):
        if pwd == expected:
            st.session_state.superadmin_ok = True
            st.rerun()
        else:
            st.error("Mot de passe incorrect.")
    st.stop()


@st.cache_data(show_spinner=False, ttl=60)
def load_submissions() -> tuple[str, pd.DataFrame]:
    cfg = get_github_config_from_streamlit(st)
    branch, records = load_json_records_from_repo(cfg, "submissions")
    df = records_to_dataframe(records or [])
    return str(branch), df


def make_zip(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    buf.seek(0)
    return buf.getvalue()


def safe_metric_nunique(df: pd.DataFrame, column: str) -> int:
    if column not in df.columns:
        return 0
    return int(df[column].replace("", pd.NA).dropna().nunique())


def main() -> None:
    require_password()
    st.title("Dashboard Superadmin")
    st.caption(
        "Génération d’un classeur d’analyse et d’un rapport à partir des soumissions finales, "
        f"avec nettoyage hérité de dashboard_common.py | version {APP_VERSION}"
    )

    top1, top2 = st.columns([3, 1])
    with top1:
        st.info(
            "Ce dashboard repose sur dashboard_common.py. Le chargement applique donc directement "
            "la suppression des emails de test et la déduplication des soumissions par répondant."
        )
    with top2:
        if st.button("Actualiser les données", use_container_width=True):
            load_submissions.clear()
            st.rerun()

    with st.spinner("Chargement des soumissions finales..."):
        branch, df = load_submissions()

    st.success(f"Soumissions chargées depuis GitHub / branche : {branch}")

    if df.empty:
        st.warning("Aucune soumission finale n’est disponible pour la génération.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Soumissions retenues", len(df))
    col2.metric("Institutions distinctes", safe_metric_nunique(df, "institution_acronym"))
    col3.metric("Pays / CER distincts", safe_metric_nunique(df, "country_or_rec"))
    col4.metric("Langues distinctes", safe_metric_nunique(df, "language"))

    with st.expander("Aperçu des données utilisées", expanded=False):
        st.dataframe(df, use_container_width=True, hide_index=True)

    sheets = build_analysis_sheets(df)
    workbook_bytes = dataframe_to_xlsx_bytes(sheets)
    report_bytes = build_report_docx_bytes(df)
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    st.subheader("Fichiers générés")
    c1, c2, c3 = st.columns(3)
    c1.download_button(
        "Télécharger le classeur d’analyse (.xlsx)",
        data=workbook_bytes,
        file_name=f"classeur_analyse_validation_{now}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    c2.download_button(
        "Télécharger le rapport (.docx)",
        data=report_bytes,
        file_name=f"rapport_validation_{now}.docx",
        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    bundle_bytes = make_zip(
        {
            f"classeur_analyse_validation_{now}.xlsx": workbook_bytes,
            f"rapport_validation_{now}.docx": report_bytes,
        }
    )
    c3.download_button(
        "Télécharger le paquet complet (.zip)",
        data=bundle_bytes,
        file_name=f"package_validation_{now}.zip",
        mime="application/zip",
    )

    tab1, tab2, tab3 = st.tabs(["Classeur - aperçu", "Rapport - contenu synthétique", "Paramètres"])

    with tab1:
        sheet_name = st.selectbox("Feuille du classeur", list(sheets.keys()))
        st.dataframe(sheets[sheet_name], use_container_width=True, hide_index=True)

    with tab2:
        st.markdown(
            "Le rapport DOCX généré comprend une vue d’ensemble, la distribution des validations globales, "
            "la position finale des institutions, les points méthodologiques les plus réservés, les domaines les plus réservés et un extrait des justifications."
        )

    with tab3:
        cfg = get_github_config_from_streamlit(st)
        st.code(
            "\n".join(
                [
                    f"GitHub owner : {cfg.owner}",
                    f"GitHub repo  : {cfg.repo}",
                    f"GitHub branch: {cfg.branch}",
                    f"Version dashboard : {APP_VERSION}",
                ]
            )
        )
        st.info(
            "Le tableau de bord génère les fichiers à la demande. Il ne pousse pas automatiquement le classeur ni le rapport dans GitHub."
        )


if __name__ == "__main__":
    main()
