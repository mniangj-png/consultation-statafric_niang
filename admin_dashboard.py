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

st.set_page_config(page_title="Dashboard Admin - validation du document", layout="wide")

APP_VERSION = "admin-clean-v3-2026-04-16"
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


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_data(source_kind: str, cache_buster: str) -> tuple[str, pd.DataFrame, list[dict]]:
    cfg = get_github_config_from_streamlit(st)
    loaded = load_json_records_from_repo(cfg, source_kind)
    if isinstance(loaded, tuple):
        if len(loaded) >= 2:
            branch, records = loaded[0], loaded[1]
        elif len(loaded) == 1:
            branch, records = "unknown", loaded[0]
        else:
            branch, records = "unknown", []
    else:
        branch, records = "unknown", loaded
    df = records_to_dataframe(records)
    return branch, df, records


def _default_date_from(df: pd.DataFrame) -> date | None:
    for col in ("submitted_at", "saved_at"):
        if col in df.columns:
            ser = pd.to_datetime(df[col], errors="coerce", utc=True)
            if ser.notna().any():
                return ser.dt.date.min()
    return None


def _default_date_to() -> date:
    return MAX_FILTER_DATE


def _normalize_email(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip().lower()


def _norm_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip().lower()


def _respondent_key(row: pd.Series) -> str:
    email = _normalize_email(row.get("email"))
    if email and email not in TEST_EMAILS and "@" in email:
        return f"email::{email}"
    return "meta::" + "|".join(
        [
            _norm_text(row.get("institution_type")),
            _norm_text(row.get("country_or_rec")),
            _norm_text(row.get("institution_acronym")),
            _norm_text(row.get("respondent_title")),
        ]
    )


def clean_dataset(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if df is None or df.empty:
        empty = pd.DataFrame() if df is None else df.copy()
        return empty, {
            "source_rows": 0,
            "tests_removed": 0,
            "duplicates_removed": 0,
            "final_rows": 0,
        }

    out = df.copy()
    out["_email_norm"] = out["email"].map(_normalize_email) if "email" in out.columns else ""
    source_rows = len(out)

    tests_mask = out["_email_norm"].isin(TEST_EMAILS)
    tests_removed = int(tests_mask.sum())
    out = out.loc[~tests_mask].copy()

    out["_respondent_key"] = out.apply(_respondent_key, axis=1)

    if "submitted_at" in out.columns:
        out["_sort_ts"] = pd.to_datetime(out["submitted_at"], errors="coerce", utc=True)
    elif "saved_at" in out.columns:
        out["_sort_ts"] = pd.to_datetime(out["saved_at"], errors="coerce", utc=True)
    else:
        out["_sort_ts"] = pd.NaT

    out["_row_order"] = range(len(out))
    before = len(out)
    out = (
        out.sort_values(
            by=["_respondent_key", "_sort_ts", "_row_order"],
            ascending=[True, True, True],
            kind="mergesort",
        )
        .drop_duplicates(subset=["_respondent_key"], keep="last")
        .copy()
    )
    duplicates_removed = before - len(out)

    out = out.drop(columns=[c for c in ["_email_norm", "_respondent_key", "_sort_ts", "_row_order"] if c in out.columns])
    return out, {
        "source_rows": source_rows,
        "tests_removed": tests_removed,
        "duplicates_removed": duplicates_removed,
        "final_rows": len(out),
    }


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
        branch, df_raw, _records = load_data(kind, APP_VERSION)

    df, cleanup = clean_dataset(df_raw)

    st.success(
        f"Données chargées depuis la branche GitHub : {branch} | "
        f"Version nettoyage : {APP_VERSION} | "
        f"Actualisation automatique toutes les {CACHE_TTL_SECONDS} secondes"
    )

    c0, c1, c2, c3 = st.columns(4)
    c0.metric("Lignes source", cleanup["source_rows"])
    c1.metric("Tests supprimés", cleanup["tests_removed"])
    c2.metric("Doublons supprimés", cleanup["duplicates_removed"])
    c3.metric("Lignes finales conservées", cleanup["final_rows"])

    if df.empty:
        st.warning("Aucune donnée exploitable n’a été trouvée après nettoyage.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Enregistrements", len(df))
    col2.metric("Institutions distinctes", df.get("institution_acronym", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique())
    col3.metric("Pays / CER distincts", df.get("country_or_rec", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique())
    col4.metric("Langues distinctes", df.get("language", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique())

    with st.expander("Filtres", expanded=True):
        c1, c2, c3, c4 = st.columns(4)
        statuses = c1.multiselect("Statut", sorted(df.get("status", pd.Series(dtype=str)).dropna().unique().tolist()))
        languages = c2.multiselect("Langue", sorted(df.get("language", pd.Series(dtype=str)).dropna().unique().tolist()))
        institution_types = c3.multiselect("Type d’institution", sorted(df.get("institution_type", pd.Series(dtype=str)).dropna().unique().tolist()))
        countries = c4.multiselect("Pays / CER", sorted(df.get("country_or_rec", pd.Series(dtype=str)).dropna().unique().tolist()))
        d1, d2 = st.columns(2)
        default_from = _default_date_from(df)
        date_from = d1.date_input("Date minimale de soumission", value=default_from)
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

    tab1, tab2, tab3, tab4 = st.tabs(["Tableau", "Téléchargements", "Résumé", "Enregistrement brut"])

    with tab1:
        st.caption(f"Enregistrements après filtrage : {len(filtered)}")
        st.dataframe(filtered, use_container_width=True, hide_index=True)

    with tab2:
        json_bytes = records_to_json_bytes(filtered.to_dict(orient="records"))
        csv_bytes = dataframe_to_csv_bytes(filtered)
        xlsx_bytes = dataframe_to_xlsx_bytes({"donnees_filtrees": filtered})
        cc1, cc2, cc3 = st.columns(3)
        cc1.download_button("Télécharger JSON", data=json_bytes, file_name=f"validation_{kind}.json", mime="application/json")
        cc2.download_button("Télécharger CSV", data=csv_bytes, file_name=f"validation_{kind}.csv", mime="text/csv")
        cc3.download_button("Télécharger XLSX", data=xlsx_bytes, file_name=f"validation_{kind}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

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
