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


@st.cache_data(show_spinner=False)
def load_data(source_kind: str) -> tuple[str, pd.DataFrame, list[dict], list[str]]:
    cfg = get_github_config_from_streamlit(st)
    branch, records, paths = load_json_records_from_repo(cfg, source_kind)
    df = records_to_dataframe(records)
    return branch, df, records, paths


def main() -> None:
    require_password()
    st.title("Dashboard Admin")
    st.caption("Téléchargement et exploration des réponses JSON, CSV et XLSX")

    kind = st.radio(
        "Jeu de données",
        options=["submissions", "drafts"],
        format_func=lambda x: "Soumissions finales" if x == "submissions" else "Brouillons",
        horizontal=True,
    )

    refresh = st.button("Actualiser les données")
    if refresh:
        load_data.clear()

    with st.spinner("Chargement des données depuis GitHub..."):
        branch, df, records, paths = load_data(kind)

    st.success(f"Données chargées depuis la branche GitHub : {branch}")
    st.caption(f"Fichiers JSON détectés dans {kind} : {len(paths)}")

    if df.empty:
        if paths:
            st.warning("Des fichiers JSON ont été détectés, mais aucun enregistrement exploitable n’a pu être chargé. Vérifiez le format JSON des fichiers.")
            with st.expander("Chemins détectés"):
                st.write(paths)
        else:
            st.warning("Aucune donnée n’a été trouvée pour cette source.")
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
        min_date = None
        max_date = None
        if "submitted_at" in df.columns and df["submitted_at"].notna().any():
            min_date = df["submitted_at"].dt.date.min()
            max_date = df["submitted_at"].dt.date.max()
        date_from = d1.date_input("Date minimale de soumission", value=min_date if min_date else None)
        date_to = d2.date_input("Date maximale de soumission", value=max_date if max_date else None)

    filtered = apply_filters(
        df,
        statuses=statuses or None,
        languages=languages or None,
        institution_types=institution_types or None,
        countries=countries or None,
        date_from=pd.Timestamp(date_from) if isinstance(date_from, date) else None,
        date_to=pd.Timestamp(date_to) if isinstance(date_to, date) else None,
    )

    tab1, tab2, tab3, tab4 = st.tabs(["Tableau", "Téléchargements", "Résumé", "Enregistrement brut"])  # st.tabs

    with tab1:
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
        with st.expander("Chemins GitHub détectés", expanded=False):
            st.write(paths)
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
