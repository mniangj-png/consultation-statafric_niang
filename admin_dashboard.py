from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from dashboard_common import (
    apply_filters,
    build_analysis_sheets,
    dataframe_to_csv_bytes,
    dataframe_to_xlsx_bytes,
    get_github_config_from_streamlit,
    load_json_records_from_repo,
    records_to_dataframe,
    records_to_json_bytes,
)

APP_VERSION = "admin-based-on-dashboard-common-2026-04-17-v1"
MAX_FILTER_DATE = date(2026, 5, 31)
CACHE_TTL_SECONDS = 60

st.set_page_config(page_title="Dashboard Admin - validation du document", layout="wide")


def require_password() -> None:
    expected = ""
    try:
        expected = st.secrets.get("ADMIN_PASSWORD", "")
    except Exception:
        expected = ""
    if not expected:
        st.info("Aucun mot de passe Admin n’est défini dans les secrets Streamlit. Le tableau de bord est ouvert.")
        return
    if st.session_state.get("admin_ok"):
        return

    st.title("Dashboard Admin")
    pwd = st.text_input("Mot de passe Admin", type="password")
    if st.button("Ouvrir"):
        if pwd == expected:
            st.session_state.admin_ok = True
            st.rerun()
        else:
            st.error("Mot de passe incorrect.")
    st.stop()


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_data(source_kind: str) -> tuple[str, pd.DataFrame, list[dict]]:
    cfg = get_github_config_from_streamlit(st)
    loaded = load_json_records_from_repo(cfg, source_kind)

    # Compatibilité prudente avec plusieurs signatures éventuelles
    if isinstance(loaded, tuple):
        if len(loaded) >= 2:
            branch, records = loaded[0], loaded[1]
        elif len(loaded) == 1:
            branch, records = getattr(cfg, "branch", "main"), loaded[0]
        else:
            branch, records = getattr(cfg, "branch", "main"), []
    else:
        branch, records = getattr(cfg, "branch", "main"), loaded

    records = records or []
    df = records_to_dataframe(records)
    return str(branch), df, records


def _default_date_from(df: pd.DataFrame) -> date | None:
    for col in ["submitted_at", "saved_at", "expires_at"]:
        if col in df.columns and df[col].notna().any():
            return df[col].dt.date.min()
    return None


def _default_date_to() -> date:
    return MAX_FILTER_DATE


def _safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series(dtype="object")


def main() -> None:
    require_password()

    st.title("Dashboard Admin")
    st.caption(f"Téléchargement et exploration des réponses JSON, CSV et XLSX | version {APP_VERSION}")
    st.info(
        "Ce dashboard s’appuie directement sur dashboard_common.py. "
        "La suppression des données de test et la déduplication doivent donc être héritées de ce module."
    )

    top1, top2 = st.columns([3, 1])
    with top1:
        kind = st.radio(
            "Jeu de données",
            options=["submissions", "drafts"],
            format_func=lambda x: "Soumissions finales" if x == "submissions" else "Brouillons",
            horizontal=True,
        )
    with top2:
        if st.button("Actualiser les données", use_container_width=True):
            load_data.clear()
            st.rerun()

    with st.spinner("Chargement des données depuis GitHub..."):
        branch, df, records = load_data(kind)

    st.success(
        f"Données chargées depuis la branche GitHub : {branch} | "
        f"Actualisation automatique toutes les {CACHE_TTL_SECONDS} secondes | "
        f"Date maximale par défaut : {MAX_FILTER_DATE.strftime('%Y/%m/%d')}"
    )

    if df.empty:
        st.warning("Aucune donnée n’a été trouvée pour cette source.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Enregistrements", len(df))
    col2.metric(
        "Institutions distinctes",
        _safe_series(df, "institution_acronym").replace("", pd.NA).dropna().nunique(),
    )
    col3.metric(
        "Pays / CER distincts",
        _safe_series(df, "country_or_rec").replace("", pd.NA).dropna().nunique(),
    )
    col4.metric(
        "Langues distinctes",
        _safe_series(df, "language").replace("", pd.NA).dropna().nunique(),
    )

    with st.expander("Filtres", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        statuses = c1.multiselect("Statut", sorted(_safe_series(df, "status").dropna().astype(str).unique().tolist()))
        languages = c2.multiselect("Langue", sorted(_safe_series(df, "language").dropna().astype(str).unique().tolist()))
        institution_types = c3.multiselect(
            "Type d’institution",
            sorted(_safe_series(df, "institution_type").dropna().astype(str).unique().tolist()),
        )
        countries = c4.multiselect("Pays / CER", sorted(_safe_series(df, "country_or_rec").dropna().astype(str).unique().tolist()))

        d1, d2 = st.columns(2)
        date_from = d1.date_input("Date minimale de soumission", value=_default_date_from(df))
        date_to = d2.date_input("Date maximale de soumission", value=_default_date_to())

    filtered = apply_filters(
        df,
        statuses=statuses or None,
        languages=languages or None,
        institution_types=institution_types or None,
        countries=countries or None,
        date_from=pd.Timestamp(date_from) if isinstance(date_from, date) else None,
        date_to=pd.Timestamp(date_to) if isinstance(date_to, date) else None,
    )

    tabs = st.tabs(["Tableau", "Téléchargements", "Résumé", "Enregistrement brut", "Paramètres"])
    tab1, tab2, tab3, tab4, tab5 = tabs

    with tab1:
        st.caption(f"Enregistrements après filtrage : {len(filtered)}")
        st.dataframe(filtered, use_container_width=True, hide_index=True)

    with tab2:
        json_bytes = records_to_json_bytes(filtered.to_dict(orient="records"))
        csv_bytes = dataframe_to_csv_bytes(filtered)
        xlsx_bytes = dataframe_to_xlsx_bytes({"donnees_filtrees": filtered})

        c1, c2, c3 = st.columns(3)
        c1.download_button(
            "Télécharger JSON",
            data=json_bytes,
            file_name=f"validation_{kind}.json",
            mime="application/json",
        )
        c2.download_button(
            "Télécharger CSV",
            data=csv_bytes,
            file_name=f"validation_{kind}.csv",
            mime="text/csv",
        )
        c3.download_button(
            "Télécharger XLSX",
            data=xlsx_bytes,
            file_name=f"validation_{kind}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with tab3:
        st.write("Résumé rapide des données filtrées")
        if not filtered.empty:
            sheets = build_analysis_sheets(filtered)
            summary_sheet = sheets.get("synthese")
            if summary_sheet is not None and not summary_sheet.empty:
                st.dataframe(summary_sheet, use_container_width=True, hide_index=True)
            else:
                st.info("Aucune feuille de synthèse disponible.")
            if "overall_validation" in filtered.columns:
                st.bar_chart(filtered["overall_validation"].fillna("NA").astype(str).value_counts())
        else:
            st.info("Aucune donnée après filtrage.")

    with tab4:
        if not filtered.empty:
            selected = st.selectbox(
                "Choisir un enregistrement",
                options=filtered.index.tolist(),
                format_func=lambda idx: str(
                    filtered.loc[idx].get("submission_id")
                    or filtered.loc[idx].get("draft_token")
                    or filtered.loc[idx].get("respondent_key")
                    or idx
                ),
            )
            st.json(filtered.loc[selected].to_dict())
        else:
            st.info("Aucun enregistrement à afficher.")

    with tab5:
        cfg = get_github_config_from_streamlit(st)
        st.code(
            "\n".join(
                [
                    f"GitHub owner : {getattr(cfg, 'owner', '')}",
                    f"GitHub repo  : {getattr(cfg, 'repo', '')}",
                    f"GitHub branch: {getattr(cfg, 'branch', '')}",
                    f"Enregistrements chargés : {len(records)}",
                    f"Enregistrements après transformation : {len(df)}",
                ]
            )
        )
        st.info(
            "Si des doublons ou des données de test apparaissent encore ici alors que le dashboard Superadmin "
            "basé sur dashboard_common.py est correct, cela signifie en pratique que ce fichier n’est probablement "
            "pas celui qui est réellement exécuté."
        )


if __name__ == "__main__":
    main()
