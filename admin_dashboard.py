
from __future__ import annotations

import base64
import io
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
import streamlit as st

APP_VERSION = "admin-clean-2026-04-16-v1"
RESPONSE_PATH_ROOT = "data/validation_doc"
MAX_FILTER_DATE = date(2026, 5, 31)
CACHE_TTL_SECONDS = 60
TEST_EMAILS = {"kl@od.sd", "in@bc.sd", "de@re.bh", "gh@fg.jh"}


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


def _sanitize_secret(value: Any) -> str:
    if value is None:
        return ""
    value = str(value).strip().strip('"').strip("'")
    value = value.replace("\r", "").replace("\n", "").strip()
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    return value


def get_github_config_from_streamlit() -> dict[str, str]:
    owner = os.getenv("GITHUB_OWNER", "mniangj-png")
    repo = os.getenv("GITHUB_REPO", "consultation-statafric_niang")
    branch = os.getenv("GITHUB_BRANCH", "main")
    token = os.getenv("GITHUB_TOKEN", "") or os.getenv("GH_TOKEN", "")
    try:
        gh = st.secrets.get("github", {})
        owner = _sanitize_secret(gh.get("owner", owner)) or owner
        repo = _sanitize_secret(gh.get("repo", repo)) or repo
        branch = _sanitize_secret(gh.get("branch", branch)) or branch
        token = _sanitize_secret(gh.get("token", token)) or token
    except Exception:
        pass
    try:
        token = _sanitize_secret(st.secrets.get("GITHUB_TOKEN", token)) or token
    except Exception:
        pass
    return {"owner": owner, "repo": repo, "branch": branch, "token": token}


def github_headers(cfg: dict[str, str]) -> dict[str, str]:
    headers = {"Accept": "application/vnd.github+json"}
    token = _sanitize_secret(cfg.get("token", ""))
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def github_api_get(cfg: dict[str, str], url: str, **kwargs) -> requests.Response:
    headers = github_headers(cfg)
    r = requests.get(url, headers=headers, timeout=30, **kwargs)
    r.raise_for_status()
    return r


def list_json_paths(cfg: dict[str, str], source_kind: str) -> list[str]:
    """Recursively list .json files under submissions or drafts."""
    root = f"{RESPONSE_PATH_ROOT}/{source_kind}"
    base = f"https://api.github.com/repos/{cfg['owner']}/{cfg['repo']}/contents/{root}"
    paths: list[str] = []
    stack = [(base, root)]
    while stack:
        url, path = stack.pop()
        r = github_api_get(cfg, url, params={"ref": cfg["branch"]})
        payload = r.json()
        if isinstance(payload, dict) and payload.get("type") == "file":
            if str(payload.get("path", "")).lower().endswith(".json"):
                paths.append(payload["path"])
            continue
        if not isinstance(payload, list):
            continue
        for item in payload:
            item_type = item.get("type")
            item_path = item.get("path", "")
            item_url = item.get("url")
            if item_type == "dir" and item_url:
                stack.append((item_url, item_path))
            elif item_type == "file" and str(item_path).lower().endswith(".json"):
                paths.append(item_path)
    return sorted(paths)


def load_json_record(cfg: dict[str, str], path: str) -> dict[str, Any] | None:
    url = f"https://api.github.com/repos/{cfg['owner']}/{cfg['repo']}/contents/{path}"
    try:
        r = github_api_get(cfg, url, params={"ref": cfg["branch"]})
        item = r.json()
        content = base64.b64decode(item["content"]).decode("utf-8")
        data = json.loads(content)
        if isinstance(data, dict):
            data["_source_path"] = path
            return data
    except Exception:
        return None
    return None


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_raw_records(source_kind: str) -> tuple[str, list[dict[str, Any]], list[str]]:
    cfg = get_github_config_from_streamlit()
    paths = list_json_paths(cfg, source_kind)
    records = []
    for path in paths:
        rec = load_json_record(cfg, path)
        if rec is not None:
            records.append(rec)
    return cfg["branch"], records, paths


def records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    for col in ["submitted_at", "saved_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
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


def clean_records_df(df: pd.DataFrame, source_kind: str) -> tuple[pd.DataFrame, dict[str, int]]:
    stats = {
        "source_rows": int(len(df)),
        "tests_removed": 0,
        "duplicates_removed": 0,
        "final_rows": 0,
    }
    if df.empty:
        return df.copy(), stats

    cleaned = df.copy()

    if "email" not in cleaned.columns:
        cleaned["email"] = ""
    cleaned["_email_norm"] = cleaned["email"].map(_norm_text)

    tests_mask = cleaned["_email_norm"].isin(TEST_EMAILS)
    stats["tests_removed"] = int(tests_mask.sum())
    cleaned = cleaned.loc[~tests_mask].copy()

    cleaned["_respondent_key"] = cleaned.apply(_respondent_key, axis=1)

    if "submitted_at" in cleaned.columns:
        cleaned["_sort_time"] = cleaned["submitted_at"]
    elif "saved_at" in cleaned.columns:
        cleaned["_sort_time"] = cleaned["saved_at"]
    else:
        cleaned["_sort_time"] = pd.NaT
    cleaned["_sort_time"] = pd.to_datetime(cleaned["_sort_time"], errors="coerce", utc=True)

    # Keep the latest record for each respondent, for both submissions and drafts.
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

    # Remove helper columns not meant for display/exports.
    cleaned.drop(columns=["_email_norm", "_respondent_key", "_sort_time"], inplace=True, errors="ignore")
    return cleaned, stats


def apply_filters(
    df: pd.DataFrame,
    statuses: list[str] | None = None,
    languages: list[str] | None = None,
    institution_types: list[str] | None = None,
    countries: list[str] | None = None,
    date_from: pd.Timestamp | None = None,
    date_to: pd.Timestamp | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    filtered = df.copy()
    if statuses and "status" in filtered.columns:
        filtered = filtered[filtered["status"].isin(statuses)]
    if languages and "language" in filtered.columns:
        filtered = filtered[filtered["language"].isin(languages)]
    if institution_types and "institution_type" in filtered.columns:
        filtered = filtered[filtered["institution_type"].isin(institution_types)]
    if countries and "country_or_rec" in filtered.columns:
        filtered = filtered[filtered["country_or_rec"].isin(countries)]

    date_col = "submitted_at" if "submitted_at" in filtered.columns else ("saved_at" if "saved_at" in filtered.columns else None)
    if date_col:
        if date_from is not None:
            start = pd.Timestamp(date_from).tz_localize("UTC") if pd.Timestamp(date_from).tzinfo is None else pd.Timestamp(date_from).tz_convert("UTC")
            filtered = filtered[filtered[date_col] >= start]
        if date_to is not None:
            end = pd.Timestamp(date_to)
            if end.tzinfo is None:
                end = end.tz_localize("UTC")
            else:
                end = end.tz_convert("UTC")
            end = end + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
            filtered = filtered[filtered[date_col] <= end]
    return filtered


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, pd.Timestamp)):
        if pd.isna(value):
            return None
        if getattr(value, "tzinfo", None) is not None:
            value = value.tz_convert(None) if hasattr(value, "tz_convert") else value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat(sep=" ")
    if isinstance(value, (list, dict)):
        return value
    if pd.isna(value):
        return None
    try:
        import numpy as np
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
        if isinstance(value, (np.bool_,)):
            return bool(value)
    except Exception:
        pass
    return value


def records_to_json_bytes(records: list[dict[str, Any]]) -> bytes:
    safe = []
    for rec in records:
        safe.append({k: _json_safe(v) for k, v in rec.items()})
    return json.dumps(safe, ensure_ascii=False, indent=2).encode("utf-8")


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    safe = df.copy()
    for col in safe.columns:
        safe[col] = safe[col].map(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else _json_safe(x))
    return safe.to_csv(index=False).encode("utf-8-sig")


def _excel_safe_scalar(value: Any) -> Any:
    if isinstance(value, (datetime, pd.Timestamp)):
        if pd.isna(value):
            return ""
        if getattr(value, "tzinfo", None) is not None:
            value = value.tz_convert(None) if hasattr(value, "tz_convert") else value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    if pd.isna(value):
        return ""
    return value


def dataframe_to_xlsx_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_name = re.sub(r"[:\\/*?\[\]]", "_", sheet_name)[:31] or "sheet"
            safe_df = df.copy().astype(object)
            for col in safe_df.columns:
                safe_df[col] = safe_df[col].map(_excel_safe_scalar)
            safe_df.to_excel(writer, index=False, sheet_name=safe_name)
    output.seek(0)
    return output.getvalue()


def build_summary_sheet(df: pd.DataFrame) -> pd.DataFrame:
    rows = [{"indicateur": "Enregistrements", "valeur": len(df)}]
    if "institution_acronym" in df.columns:
        rows.append({"indicateur": "Institutions distinctes", "valeur": df["institution_acronym"].replace("", pd.NA).dropna().nunique()})
    if "country_or_rec" in df.columns:
        rows.append({"indicateur": "Pays / CER distincts", "valeur": df["country_or_rec"].replace("", pd.NA).dropna().nunique()})
    if "language" in df.columns:
        rows.append({"indicateur": "Langues distinctes", "valeur": df["language"].replace("", pd.NA).dropna().nunique()})
    return pd.DataFrame(rows)


def _default_date_from(df: pd.DataFrame) -> date | None:
    for col in ["submitted_at", "saved_at"]:
        if col in df.columns and df[col].notna().any():
            return df[col].dt.date.min()
    return None


def _default_date_to() -> date:
    return MAX_FILTER_DATE


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
            load_raw_records.clear()
            st.rerun()

    with st.spinner("Chargement des données depuis GitHub..."):
        branch, records, paths = load_raw_records(kind)

    raw_df = records_to_dataframe(records)
    df, clean_stats = clean_records_df(raw_df, kind)

    st.success(
        f"Données chargées depuis la branche GitHub : {branch} | "
        f"Actualisation automatique toutes les {CACHE_TTL_SECONDS} secondes | "
        f"Date maximale par défaut : {MAX_FILTER_DATE.strftime('%Y/%m/%d')}"
    )

    c0, c1, c2, c3 = st.columns(4)
    c0.metric("Lignes source", clean_stats["source_rows"])
    c1.metric("Tests supprimés", clean_stats["tests_removed"])
    c2.metric("Doublons supprimés", clean_stats["duplicates_removed"])
    c3.metric("Lignes finales conservées", clean_stats["final_rows"])

    if df.empty:
        st.warning("Aucune donnée n’a été trouvée après nettoyage.")
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
        institution_types = c3.multiselect("Type d’institution", sorted(df.get("institution_type", pd.Series(dtype=str)).dropna().unique().tolist()))
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
        c1.download_button("Télécharger JSON", data=json_bytes, file_name=f"validation_{kind}.json", mime="application/json")
        c2.download_button("Télécharger CSV", data=csv_bytes, file_name=f"validation_{kind}.csv", mime="text/csv")
        c3.download_button(
            "Télécharger XLSX",
            data=xlsx_bytes,
            file_name=f"validation_{kind}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with tab3:
        st.write("Résumé rapide des données filtrées")
        if not filtered.empty:
            st.dataframe(build_summary_sheet(filtered), use_container_width=True, hide_index=True)
            if "overall_validation" in filtered.columns:
                st.bar_chart(filtered["overall_validation"].fillna("NA").value_counts())
        else:
            st.info("Aucune donnée après filtrage.")

    with tab4:
        if not filtered.empty:
            selected = st.selectbox(
                "Choisir un enregistrement",
                options=filtered.index.tolist(),
                format_func=lambda idx: filtered.loc[idx].get("submission_id") or filtered.loc[idx].get("draft_token") or str(idx),
            )
            st.json({k: _json_safe(v) for k, v in filtered.loc[selected].to_dict().items()})
        else:
            st.info("Aucun enregistrement à afficher.")

    st.markdown("### Fichiers JSON détectés")
    st.caption(f"{len(paths)} fichier(s) JSON lu(s) dans le dépôt.")
    with st.expander("Afficher les chemins GitHub détectés"):
        st.write(paths)


if __name__ == "__main__":
    main()
