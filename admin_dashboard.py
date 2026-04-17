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

    # Compatibilité avec plusieurs signatures possibles de dashboard_common.py
    if isinstance(loaded, tuple):
        if len(loaded) >= 2:
            branch = str(loaded[0])
            records = loaded[1] or []
        elif len(loaded) == 1:
            branch = str(getattr(cfg, "branch", "main"))
            records = loaded[0] or []
        else:
            branch = str(getattr(cfg, "branch", "main"))
            records = []
    else:
        branch = str(getattr(cfg, "branch", "main"))
        records = loaded or []

    df = records_to_dataframe(records)
    return branch, df, list(records)


def _default_date_from(df: pd.DataFrame) -> date | None:
    for col in ["submitted_at", "saved_at"]:
        if col in df.columns and df[col].notna().any():
            return df[col].dt.date.min()
    return None


def _default_date_to() -> date:
    return MAX_FILTER_DATE


def _cleaning_note(kind: str) -> str:
    label = "soumissions finales" if kind == "submissions" else "brouillons"
    return (
        f"Le module dashboard_common est utilisé pour charger les {label}. "
        "La suppression des données de test et la déduplication des répondants sont donc appliquées en amont, "
        "au moment du chargement."
    )


def main() -> None:
    require_password()
    st.title("Dashboard Admin")
    st.caption(f"Téléchargement et exploration des réponses JSON, CSV et XLSX | version {APP_VERSION}")

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
    st.info(_cleaning_note(kind))

    if df.empty:
        st.warning("Aucune donnée n’a été trouvée pour cette source après nettoyage.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Enregistrements", len(df))
    col2.metric(
        "Institutions distinctes",
        df.get("institution_acronym", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique(),
    )
    col3.metric(
        "Pays / CER distincts",
        df.get("country_or_rec", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique(),
    )
    col4.metric(
        "Langues distinctes",
        df.get("language", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique(),
    )

    with st.expander("Filtres", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        statuses = c1.multiselect("Statut", sorted(df.get("status", pd.Series(dtype=str)).dropna().unique().tolist()))
        languages = c2.multiselect("Langue", sorted(df.get("language", pd.Series(dtype=str)).dropna().unique().tolist()))
        institution_types = c3.multiselect(
            "Type d’institution",
            sorted(df.get("institution_type", pd.Series(dtype=str)).dropna().unique().tolist()),
        )
        countries = c4.multiselect("Pays / CER", sorted(df.get("country_or_rec", pd.Series(dtype=str)).dropna().unique().tolist()))

        d1, d2 = st.columns(2)
        default_from = _default_date_from(df)
        default_to = _default_date_to()
        date_from = d1.date_input("Date minimale de soumission", value=default_from)
        date_to = d2.date_input("Date maximale de soumission", value=default_to)

    filtered = apply_filters(
        df,
        statuses=statuses or None,
        languages=languages or None,
        institution_types=institution_types or None,
        countries=countries or None,
        date_from=pd.Timestamp(date_from) if isinstance(date_from, date) else None,
        date_to=pd.Timestamp(date_to) if isinstance(date_to, date) else None,
    )

    tabs = st.tabs(["Tableau", "Téléchargements", "Résumé", "Enregistrement brut"])

    with tabs[0]:
        st.caption(f"Enregistrements après filtrage : {len(filtered)}")
        st.dataframe(filtered, use_container_width=True, hide_index=True)

    with tabs[1]:
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

    with tabs[2]:
        st.write("Résumé rapide des données filtrées")
        if not filtered.empty:
            sheets = build_analysis_sheets(filtered)
            if "synthese" in sheets:
                st.dataframe(sheets["synthese"], use_container_width=True, hide_index=True)
            else:
                st.info("La feuille de synthèse n’est pas disponible dans cette version de dashboard_common.")
            if "overall_validation" in filtered.columns:
                st.bar_chart(filtered["overall_validation"].fillna("NA").value_counts())
        else:
            st.info("Aucune donnée après filtrage.")

    with tabs[3]:
        if not filtered.empty:
            selected = st.selectbox(
                "Choisir un enregistrement",
                options=filtered.index.tolist(),
                format_func=lambda idx: filtered.loc[idx].get("submission_id") or filtered.loc[idx].get("draft_token") or str(idx),
            )
            st.json(filtered.loc[selected].to_dict())
        else:
            st.info("Aucun enregistrement à afficher.")

    with st.expander("Paramètres et contrôle", expanded=False):
        cfg = get_github_config_from_streamlit(st)
        st.code(
            "\n".join(
                [
                    f"GitHub owner : {cfg.owner}",
                    f"GitHub repo  : {cfg.repo}",
                    f"GitHub branch: {cfg.branch}",
                    f"Jeu chargé   : {kind}",
                    f"Enregistrements chargés (après nettoyage) : {len(df)}",
                    f"Enregistrements filtrés : {len(filtered)}",
                ]
            )
        )
        st.caption(
            "Si dashboard_common.py a bien été remplacé par la version corrigée, les données de test et les doublons ne doivent plus apparaître ici."
        )


if __name__ == "__main__":
    main()
