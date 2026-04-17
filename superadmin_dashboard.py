
from __future__ import annotations

from datetime import datetime
import io
import json
import re
import zipfile
from typing import Any

import pandas as pd
import streamlit as st

from dashboard_common import (
    build_report_docx_bytes,
    get_github_config_from_streamlit,
    load_json_records_from_repo,
    records_to_dataframe,
)

st.set_page_config(page_title="Dashboard Superadmin - validation du document", layout="wide")

APP_VERSION = "superadmin-clean-2026-04-17-v1"
TEST_EMAILS = {"kl@od.sd", "in@bc.sd", "de@re.bh", "gh@fg.jh"}

STRATEGIC_LABELS = {
    "strategic_prioritization_criteria": "Critères de priorisation retenus",
    "strategic_scoring_logic": "Logique de notation multicritère (scoring)",
    "strategic_core_extensions": "Distinction noyau / extensions",
    "strategic_gender_integration": "Intégration transversale du genre",
    "strategic_min_disaggregations": "Désagrégations minimales proposées",
    "strategic_data_sources": "Sources de données et dispositifs de production",
    "strategic_governance_roles": "Gouvernance et répartition des rôles",
    "strategic_roadmap_update": "Feuille de route de mise en œuvre et mécanisme de mise à jour",
}
DOMAIN_LABELS = {
    "domain_d01": "D01 Croissance économique, transformation structurelle et commerce",
    "domain_d02": "D02 Emploi, travail décent et protection sociale",
    "domain_d03": "D03 Agriculture durable, sécurité alimentaire et nutrition",
    "domain_d04": "D04 Infrastructures, industrialisation et innovation",
    "domain_d05": "D05 Inclusion, pauvreté et inégalités",
    "domain_d06": "D06 Éducation, compétences et capital humain",
    "domain_d07": "D07 Santé, bien-être et accès universel",
    "domain_d08": "D08 Égalité des genres et autonomisation",
    "domain_d09": "D09 Environnement, résilience climatique et villes durables",
    "domain_d10": "D10 Gouvernance, paix et institutions",
    "domain_d11": "D11 Économie bleue et gestion des océans",
    "domain_d12": "D12 Partenariats et financement du développement",
}


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


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return re.sub(r"\s+", " ", str(value).strip().lower())


def _respondent_key(row: pd.Series) -> str:
    email = _norm_text(row.get("email"))
    if email and email not in TEST_EMAILS:
        return f"email:{email}"
    parts = [
        _norm_text(row.get("institution_type")),
        _norm_text(row.get("country_or_rec")),
        _norm_text(row.get("institution_acronym")),
        _norm_text(row.get("respondent_title")),
    ]
    return "fallback:" + "|".join(parts)


def clean_submissions_df(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    stats = {
        "source_rows": int(len(df)),
        "tests_removed": 0,
        "duplicates_removed": 0,
        "final_rows": 0,
    }
    if df.empty:
        return df.copy(), pd.DataFrame(), stats

    cleaned = df.copy()
    if "email" not in cleaned.columns:
        cleaned["email"] = ""

    cleaned["_email_norm"] = cleaned["email"].map(_norm_text)
    test_mask = cleaned["_email_norm"].isin(TEST_EMAILS)
    tests_df = cleaned.loc[test_mask].copy()
    stats["tests_removed"] = int(test_mask.sum())
    cleaned = cleaned.loc[~test_mask].copy()

    cleaned["_respondent_key"] = cleaned.apply(_respondent_key, axis=1)
    if "submitted_at" in cleaned.columns:
        cleaned["_sort_time"] = pd.to_datetime(cleaned["submitted_at"], errors="coerce", utc=True)
    elif "saved_at" in cleaned.columns:
        cleaned["_sort_time"] = pd.to_datetime(cleaned["saved_at"], errors="coerce", utc=True)
    else:
        cleaned["_sort_time"] = pd.NaT

    before = len(cleaned)
    cleaned = (
        cleaned.sort_values(
            by=["_respondent_key", "_sort_time", "_source_path"],
            ascending=[True, False, False],
            na_position="last",
        )
        .drop_duplicates(subset=["_respondent_key"], keep="first")
        .copy()
    )
    stats["duplicates_removed"] = int(before - len(cleaned))
    stats["final_rows"] = int(len(cleaned))

    dedup_keys = set(cleaned["_respondent_key"].tolist())
    duplicates_df = df.copy()
    duplicates_df["_email_norm"] = duplicates_df.get("email", "").map(_norm_text)
    duplicates_df = duplicates_df.loc[~duplicates_df["_email_norm"].isin(TEST_EMAILS)].copy()
    duplicates_df["_respondent_key"] = duplicates_df.apply(_respondent_key, axis=1)
    if "submitted_at" in duplicates_df.columns:
        duplicates_df["_sort_time"] = pd.to_datetime(duplicates_df["submitted_at"], errors="coerce", utc=True)
    elif "saved_at" in duplicates_df.columns:
        duplicates_df["_sort_time"] = pd.to_datetime(duplicates_df["saved_at"], errors="coerce", utc=True)
    else:
        duplicates_df["_sort_time"] = pd.NaT
    duplicates_df = duplicates_df.sort_values(["_respondent_key", "_sort_time", "_source_path"], ascending=[True, False, False], na_position="last")
    duplicates_df = duplicates_df.loc[duplicates_df.duplicated(subset=["_respondent_key"], keep="first")].copy()

    journal_df = pd.concat([
        tests_df.assign(motif_suppression="Email de test"),
        duplicates_df.assign(motif_suppression="Doublon supprimé (dernière réponse conservée)")
    ], ignore_index=True, sort=False)

    cleaned.drop(columns=["_email_norm", "_respondent_key", "_sort_time"], inplace=True, errors="ignore")
    journal_df.drop(columns=["_email_norm", "_respondent_key", "_sort_time"], inplace=True, errors="ignore")
    return cleaned, journal_df, stats


def _safe_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _traceability_df(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        common = {
            "institution": r.get("institution_acronym"),
            "type_institution": r.get("institution_type"),
            "pays_cer": r.get("country_or_rec"),
            "email": r.get("email"),
            "langue": r.get("language"),
            "soumis_le": r.get("submitted_at"),
            "submission_id": r.get("submission_id"),
            "source_path": r.get("_source_path"),
        }
        if pd.notna(r.get("overall_validation")):
            rows.append({**common, "section": "Validation générale", "item_code": "overall_validation", "item_label": "Validation globale du document", "reponse": r.get("overall_validation"), "justification": r.get("overall_validation_why", "")})
        if pd.notna(r.get("operational_usability")):
            rows.append({**common, "section": "Validation générale", "item_code": "operational_usability", "item_label": "Document suffisamment opérationnel", "reponse": r.get("operational_usability"), "justification": r.get("operational_usability_why", "")})
        for code, label in STRATEGIC_LABELS.items():
            if pd.notna(r.get(code)):
                rows.append({**common, "section": "Choix stratégiques", "item_code": code, "item_label": label, "reponse": r.get(code), "justification": r.get(f"{code}_why", "")})
        for code, label in DOMAIN_LABELS.items():
            if pd.notna(r.get(code)):
                rows.append({**common, "section": "Domaines thématiques", "item_code": code, "item_label": label, "reponse": r.get(code), "justification": r.get(f"{code}_why", "")})
        if pd.notna(r.get("final_institutional_position")):
            rows.append({**common, "section": "Position finale", "item_code": "final_institutional_position", "item_label": "Position finale de l’institution", "reponse": r.get("final_institutional_position"), "justification": r.get("final_institutional_position_why", "")})
        for field, label in [
            ("strategic_comments", "Commentaires sur les choix stratégiques"),
            ("domain_comments", "Commentaires sur les domaines"),
            ("top_3_revisions", "Trois révisions prioritaires"),
        ]:
            val = r.get(field)
            if pd.notna(val) and str(val).strip():
                rows.append({**common, "section": "Commentaires libres", "item_code": field, "item_label": label, "reponse": "Commentaire libre", "justification": str(val).strip()})
    trace_df = pd.DataFrame(rows)
    if not trace_df.empty and "soumis_le" in trace_df.columns:
        trace_df["soumis_le"] = _safe_dt(trace_df["soumis_le"])
    return trace_df


def _uniq_join(series: pd.Series) -> str:
    vals = [str(x).strip() for x in series.dropna().astype(str) if str(x).strip()]
    return "; ".join(list(dict.fromkeys(vals)))


def build_analysis_sheets_enhanced(df: pd.DataFrame, journal_df: pd.DataFrame, stats: dict[str, int]) -> dict[str, pd.DataFrame]:
    sheets: dict[str, pd.DataFrame] = {}
    cleaned = df.copy()
    if "submitted_at" in cleaned.columns:
        cleaned["submitted_at"] = _safe_dt(cleaned["submitted_at"])
    if "saved_at" in cleaned.columns:
        cleaned["saved_at"] = _safe_dt(cleaned["saved_at"])
    if not journal_df.empty:
        if "submitted_at" in journal_df.columns:
            journal_df["submitted_at"] = _safe_dt(journal_df["submitted_at"])
        if "saved_at" in journal_df.columns:
            journal_df["saved_at"] = _safe_dt(journal_df["saved_at"])

    summary = pd.DataFrame([
        ["Lignes source", stats["source_rows"]],
        ["Tests supprimés", stats["tests_removed"]],
        ["Doublons supprimés", stats["duplicates_removed"]],
        ["Lignes finales conservées", stats["final_rows"]],
        ["Institutions distinctes", cleaned.get("institution_acronym", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique()],
        ["Pays / CER distincts", cleaned.get("country_or_rec", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique()],
        ["Langues distinctes", cleaned.get("language", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique()],
    ], columns=["indicateur", "valeur"])
    sheets["synthese_kpis"] = summary

    trace_df = _traceability_df(cleaned)
    reserve_codes = {
        "Validé sous réserve",
        "Non-validé",
        "Plutôt non",
        "Non",
        "Oui, sous réserve d’ajustements limités",
        "Non, une révision plus substantielle est nécessaire",
    }
    trace_obs = trace_df.loc[
        (trace_df.get("reponse", pd.Series(dtype=str)).isin(reserve_codes))
        | (trace_df.get("justification", pd.Series(dtype=str)).fillna("").astype(str).str.strip() != "")
    ].copy()

    if not trace_df.empty:
        counts = trace_df.pivot_table(
            index=["section", "item_code", "item_label"],
            columns="reponse",
            values="institution",
            aggfunc="count",
            fill_value=0,
        ).reset_index()
        counts.columns.name = None
    else:
        counts = pd.DataFrame(columns=["section", "item_code", "item_label"])

    needed_cols = [
        "Validé", "Validé sous réserve", "Non-validé", "Sans avis",
        "Oui", "Plutôt oui", "Plutôt non", "Non",
        "Oui, sous réserve d’ajustements limités",
        "Non, une révision plus substantielle est nécessaire",
        "À discuter en atelier", "Commentaire libre",
    ]
    for col in needed_cols:
        if col not in counts.columns:
            counts[col] = 0

    if not trace_obs.empty:
        agg = trace_obs.groupby(["section", "item_code", "item_label"]).agg(
            nb_institutions=("institution", "nunique"),
            institutions=("institution", _uniq_join),
            pays_cer=("pays_cer", _uniq_join),
            extraits=("justification", lambda s: " | ".join([str(x).strip() for x in s.fillna("") if str(x).strip()][:5])),
        ).reset_index()
    else:
        agg = pd.DataFrame(columns=["section", "item_code", "item_label", "nb_institutions", "institutions", "pays_cer", "extraits"])

    arbitrages = counts.merge(agg, on=["section", "item_code", "item_label"], how="left").fillna({
        "nb_institutions": 0,
        "institutions": "",
        "pays_cer": "",
        "extraits": "",
    })

    def recommendation(row: pd.Series) -> str:
        if row.get("Non-validé", 0) > 0 or row.get("Non, une révision plus substantielle est nécessaire", 0) > 0 or row.get("Plutôt non", 0) > 0 or row.get("Non", 0) > 0:
            return "Arbitrage de fond requis"
        if row.get("Validé sous réserve", 0) > 0 or row.get("Oui, sous réserve d’ajustements limités", 0) > 0:
            return "Révision ciblée recommandée"
        if row.get("Commentaire libre", 0) > 0:
            return "Précision éditoriale utile"
        return "Validation sans réserve"

    def priority(row: pd.Series) -> str:
        if row.get("Non-validé", 0) > 0 or row.get("Non, une révision plus substantielle est nécessaire", 0) > 0 or row.get("Plutôt non", 0) > 0 or row.get("Non", 0) > 0:
            return "Haute"
        if row.get("Validé sous réserve", 0) >= 2 or row.get("Oui, sous réserve d’ajustements limités", 0) >= 2 or row.get("nb_institutions", 0) >= 2:
            return "Moyenne"
        if row.get("Validé sous réserve", 0) > 0 or row.get("Oui, sous réserve d’ajustements limités", 0) > 0 or row.get("Commentaire libre", 0) > 0:
            return "Faible"
        return "Nulle"

    if not arbitrages.empty:
        arbitrages["orientation_recommandee"] = arbitrages.apply(recommendation, axis=1)
        arbitrages["priorite"] = arbitrages.apply(priority, axis=1)
    else:
        arbitrages["orientation_recommandee"] = []
        arbitrages["priorite"] = []

    overall_counts = cleaned.get("overall_validation", pd.Series(dtype=str)).fillna("Sans valeur").value_counts().reset_index()
    overall_counts.columns = ["modalite", "nombre"]
    final_counts = cleaned.get("final_institutional_position", pd.Series(dtype=str)).fillna("Sans valeur").value_counts().reset_index()
    final_counts.columns = ["modalite", "nombre"]

    note_rows = [
        ["Périmètre retenu", f"{len(cleaned)} réponses finales uniques retenues après suppression de {stats['tests_removed']} tests et de {stats['duplicates_removed']} doublons."],
        ["Couverture institutionnelle", "; ".join(sorted(cleaned.get("institution_acronym", pd.Series(dtype=str)).dropna().astype(str).tolist()))],
        ["Couverture pays / CER", "; ".join(sorted(cleaned.get("country_or_rec", pd.Series(dtype=str)).dropna().astype(str).tolist()))],
        ["Validation globale - tendance", "; ".join(f"{r.modalite}: {r.nombre}" for r in overall_counts.itertuples(index=False))],
        ["Position finale - tendance", "; ".join(f"{r.modalite}: {r.nombre}" for r in final_counts.itertuples(index=False))],
    ]
    note_df = pd.DataFrame(note_rows, columns=["rubrique", "constat_synthetique"])

    version_consolidee = arbitrages[["section", "item_label", "orientation_recommandee", "priorite", "institutions", "extraits"]].copy()
    version_consolidee.columns = [
        "section_document",
        "element",
        "orientation_consolidee",
        "priorite",
        "institutions_concernees",
        "commentaires_a_integrer",
    ]
    for col in ["decision_finale", "responsable_revision", "statut_redaction"]:
        version_consolidee[col] = ""

    sheets["base_nettoyee"] = cleaned
    sheets["journal_nettoyage"] = journal_df
    sheets["validations_globales"] = overall_counts
    sheets["positions_finales"] = final_counts
    sheets["traceabilite_commentaires"] = trace_df
    sheets["arbitrages_recommandes"] = arbitrages
    sheets["version_consolidee_support"] = version_consolidee
    sheets["matrice_strategique"] = arbitrages.loc[arbitrages["section"] == "Choix stratégiques"].copy()
    sheets["matrice_domaines"] = arbitrages.loc[arbitrages["section"] == "Domaines thématiques"].copy()
    sheets["note_synthese_support"] = note_df
    return sheets


def _excel_safe_scalar(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return ""
        if value.tzinfo is not None:
            value = value.tz_convert(None)
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone().replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return value


def dataframe_to_xlsx_bytes_local(sheets: dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for name, df in sheets.items():
            safe_name = re.sub(r"[:\\/*?\[\]]", "_", name)[:31] or "sheet"
            safe_df = df.copy().astype(object)
            for col in safe_df.columns:
                safe_df[col] = safe_df[col].map(_excel_safe_scalar)
            safe_df.to_excel(writer, index=False, sheet_name=safe_name)
            ws = writer.sheets[safe_name]
            ws.freeze_panes = "A2"
            for cell in ws[1]:
                cell.font = cell.font.copy(bold=True)
            for col in ws.columns:
                max_len = 0
                col_letter = col[0].column_letter
                for cell in col:
                    try:
                        max_len = max(max_len, len(str(cell.value or "")))
                    except Exception:
                        pass
                ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 40)
    output.seek(0)
    return output.getvalue()


def make_zip(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    buf.seek(0)
    return buf.getvalue()


@st.cache_data(show_spinner=False)
def load_submissions_clean() -> tuple[str, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    cfg = get_github_config_from_streamlit(st)
    loaded = load_json_records_from_repo(cfg, "submissions")
    if isinstance(loaded, tuple):
        if len(loaded) >= 2:
            branch = loaded[0]
            records = loaded[1]
        elif len(loaded) == 1:
            branch = getattr(cfg, "branch", "main")
            records = loaded[0]
        else:
            branch = getattr(cfg, "branch", "main")
            records = []
    else:
        branch = getattr(cfg, "branch", "main")
        records = loaded
    raw_df = records_to_dataframe(records or [])
    cleaned_df, journal_df, stats = clean_submissions_df(raw_df)
    return str(branch), cleaned_df, journal_df, stats


def main() -> None:
    require_password()
    st.title("Dashboard Superadmin")
    st.caption(f"Génération d’un classeur d’analyse consolidé | version {APP_VERSION}")

    with st.spinner("Chargement et nettoyage des soumissions finales..."):
        branch, df, journal_df, stats = load_submissions_clean()
    st.success(f"Soumissions chargées depuis GitHub / branche : {branch}")

    c0, c1, c2, c3 = st.columns(4)
    c0.metric("Lignes source", stats["source_rows"])
    c1.metric("Tests supprimés", stats["tests_removed"])
    c2.metric("Doublons supprimés", stats["duplicates_removed"])
    c3.metric("Lignes finales conservées", stats["final_rows"])

    if df.empty:
        st.warning("Aucune soumission finale n’est disponible après nettoyage.")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Institutions distinctes", df.get("institution_acronym", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique())
    col2.metric("Pays / CER distincts", df.get("country_or_rec", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique())
    col3.metric("Langues distinctes", df.get("language", pd.Series(dtype=str)).replace("", pd.NA).dropna().nunique())

    with st.expander("Aperçu de la base nettoyée", expanded=False):
        st.dataframe(df, use_container_width=True, hide_index=True)
    with st.expander("Journal de nettoyage", expanded=False):
        st.dataframe(journal_df, use_container_width=True, hide_index=True)

    sheets = build_analysis_sheets_enhanced(df, journal_df, stats)
    workbook_bytes = dataframe_to_xlsx_bytes_local(sheets)
    try:
        report_bytes = build_report_docx_bytes(df)
    except Exception:
        report_bytes = None
    now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    st.subheader("Fichiers générés")
    c1, c2, c3 = st.columns(3)
    c1.download_button(
        "Télécharger le classeur d’analyse (.xlsx)",
        data=workbook_bytes,
        file_name=f"classeur_analyse_validation_{now}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    if report_bytes:
        c2.download_button(
            "Télécharger le rapport (.docx)",
            data=report_bytes,
            file_name=f"rapport_validation_{now}.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
    bundle_files = {f"classeur_analyse_validation_{now}.xlsx": workbook_bytes}
    if report_bytes:
        bundle_files[f"rapport_validation_{now}.docx"] = report_bytes
    bundle_bytes = make_zip(bundle_files)
    c3.download_button(
        "Télécharger le paquet complet (.zip)",
        data=bundle_bytes,
        file_name=f"package_validation_{now}.zip",
        mime="application/zip",
    )

    tab1, tab2, tab3 = st.tabs(["Classeur - aperçu", "Arbitrages", "Paramètres"])
    with tab1:
        sheet_name = st.selectbox("Feuille du classeur", list(sheets.keys()))
        st.dataframe(sheets[sheet_name], use_container_width=True, hide_index=True)

    with tab2:
        st.dataframe(sheets["arbitrages_recommandes"], use_container_width=True, hide_index=True)

    with tab3:
        cfg = get_github_config_from_streamlit(st)
        owner = getattr(cfg, "owner", None) or (cfg.get("owner") if isinstance(cfg, dict) else "")
        repo = getattr(cfg, "repo", None) or (cfg.get("repo") if isinstance(cfg, dict) else "")
        branch_cfg = getattr(cfg, "branch", None) or (cfg.get("branch") if isinstance(cfg, dict) else "")
        st.code("\n".join([
            f"GitHub owner : {owner}",
            f"GitHub repo  : {repo}",
            f"GitHub branch: {branch_cfg}",
        ]))
        st.info("Le classeur intègre la base nettoyée, le journal de nettoyage, la traçabilité des commentaires, les arbitrages recommandés et un support de version consolidée.")

if __name__ == "__main__":
    main()
