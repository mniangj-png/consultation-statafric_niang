
from __future__ import annotations

from datetime import date
import re

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

st.set_page_config(page_title="Dashboard Admin - validation du document", layout="wide")

# Date plafond demandée pour inclure automatiquement les nouvelles soumissions
MAX_FILTER_DATE = date(2026, 5, 31)
CACHE_TTL_SECONDS = 60

TEST_EMAILS = {
    "kl@od.sd",
    "in@bc.sd",
    "de@re.bh",
    "gh@fg.jh",
}


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


def _normalize_email(value: object) -> str:
    if value is None:
        return ""
    try:
        text = str(value).strip().lower()
    except Exception:
        return ""
    return text


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    try:
        text = str(value).strip().lower()
    except Exception:
        return ""
    text = re.sub(r"\s+", " ", text)
    return text


def _build_respondent_key(row: pd.Series) -> str:
    email = _normalize_email(row.get("email"))
    if email and email not in TEST_EMAILS:
        return f"email::{email}"

    parts = [
        _clean_text(row.get("institution_type")),
        _clean_text(row.get("country_or_rec")),
        _clean_text(row.get("institution_acronym")),
        _clean_text(row.get("respondent_title")),
    ]
    composite = "|".join(parts)
    return f"resp::{composite}"


def _deduplicate_and_remove_tests(df: pd.DataFrame, source_kind: str) -> tuple[pd.DataFrame, dict]:
    summary = {
        "rows_initial": int(len(df)),
        "rows_removed_tests": 0,
        "rows_removed_duplicates": 0,
        "rows_final": int(len(df)),
    }

    if df.empty:
        return df, summary

    cleaned = df.copy()

    if "email" in cleaned.columns:
        cleaned["_email_norm"] = cleaned["email"].map(_normalize_email)
        test_mask = cleaned["_email_norm"].isin(TEST_EMAILS)
        summary["rows_removed_tests"] = int(test_mask.sum())
        cleaned = cleaned.loc[~test_mask].copy()
    else:
        cleaned["_email_norm"] = ""

    # Déduplication uniquement sur les soumissions finales
    if source_kind == "submissions":
        cleaned["_respondent_key"] = cleaned.apply(_build_respondent_key, axis=1)

        if "submitted_at" in cleaned.columns:
            cleaned["_submitted_sort"] = pd.to_datetime(cleaned["submitted_at"], errors="coerce", utc=True)
        elif "saved_at" in cleaned.columns:
            cleaned["_submitted_sort"] = pd.to_datetime(cleaned["saved_at"], errors="coerce", utc=True)
        else:
            cleaned["_submitted_sort"] = pd.NaT

        if "submission_id" in cleaned.columns:
            cleaned["_submission_id_sort"] = cleaned["submission_id"].astype(str)
        else:
            cleaned["_submission_id_sort"] = ""

        before = len(cleaned)
        cleaned = (
            cleaned.sort_values(
                by=["_respondent_key", "_submitted_sort", "_submission_id_sort"],
                ascending=[True, True, True],
                na_position="last",
            )
            .drop_duplicates(subset=["_respondent_key"], keep="last")
            .copy()
        )
        summary["rows_removed_duplicates"] = int(before - len(cleaned))

    drop_cols = [c for c in ["_email_norm", "_respondent_key", "_submitted_sort", "_submission_id_sort"] if c in cleaned.columns]
    if drop_cols:
        cleaned = cleaned.drop(columns=drop_cols)

    summary["rows_final"] = int(len(cleaned))
    return cleaned, summary


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_data(source_kind: str) -> tuple[str, pd.DataFrame, list[dict], dict]:
    cfg = get_github_config_from_streamlit(st)
    loaded = load_json_records_from_repo(cfg, source_kind)

    # Compatibilité avec plusieurs signatures possibles de load_json_records_from_repo
    branch = cfg.get("branch", "main") if isinstance(cfg, dict) else "main"
    records: list[dict] = []
    if isinstance(loaded, tuple):
        if len(loaded) >= 2 and isinstance(loaded[0], str):
            branch = loaded[0]
            records = loaded[1] or []
        elif len(loaded) >= 1:
            records = loaded[0] or []
    elif isinstance(loaded, list):
        records = loaded

    df = records_to_dataframe(records)
    df, summary = _deduplicate_and_remove_tests(df, source_kind)
    filtered_records = df.to_dict(orient="records")
    return branch, df, filtered_records, summary


def _default_date_from(df: pd.DataFrame) -> date | None:
    if "submitted_at" in df.columns and df["submitted_at"].notna().any():
        return df["submitted_at"].dt.date.min()
    if "saved_at" in df.columns and df["saved_at"].notna().any():
        return df["saved_at"].dt.date.min()
    return None


def _default_date_to() -> date:
    return MAX_FILTER_DATE


def main() -> None:
    require_password()
    st.title("Dashboard Admin")
    st.caption("Téléchargement et exploration des réponses JSON, CSV et XLSX")

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
        branch, df, records, cleanup = load_data(kind)

    st.success(
        f"Données chargées depuis la branche GitHub : {branch} | "
        f"Actualisation automatique toutes les {CACHE_TTL_SECONDS} secondes | "
        f"Date maximale par défaut : {MAX_FILTER_DATE.strftime('%Y/%m/%d')}"
    )

    st.info(
        "Nettoyage appliqué : "
        f"{cleanup['rows_removed_tests']} donnée(s) test supprimée(s), "
        f"{cleanup['rows_removed_duplicates']} doublon(s) supprimé(s), "
        f"{cleanup['rows_final']} enregistrement(s) conservé(s)."
    )

    if df.empty:
        st.warning("Aucune donnée n’a été trouvée pour cette source.")
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
            "Type d’institution", sorted(df.get("institution_type", pd.Series(dtype=str)).dropna().unique().tolist())
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

    tab1, tab2, tab3, tab4 = st.tabs(["Tableau", "Téléchargements", "Résumé", "Enregistrement brut"])

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
            st.dataframe(build_analysis_sheets(filtered)["synthese"], use_container_width=True, hide_index=True)
            if "overall_validation" in filtered.columns:
                st.bar_chart(filtered["overall_validation"].value_counts())
        else:
            st.info("Aucune donnée après filtrage.")

    with tab4:
        if not filtered.empty:
            selected = st.selectbox(
                "Choisir un enregistrement",
                options=filtered.index.tolist(),
                format_func=lambda idx: filtered.loc[idx].get("submission_id") or filtered.loc[idx].get("draft_token") or str(idx),
            )
            st.json(filtered.loc[selected].to_dict())
        else:
            st.info("Aucun enregistrement à afficher.")


if __name__ == "__main__":
    main()
