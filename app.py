
import io
import json
import os
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import streamlit as st

# Optional deps (Google Sheets / Dropbox)
try:
    import gspread  # type: ignore
    from google.oauth2.service_account import Credentials  # type: ignore
except Exception:
    gspread = None
    Credentials = None

try:
    import dropbox  # type: ignore
except Exception:
    dropbox = None


# =========================
# Configuration
# =========================

APP_TITLE_FR = "Questionnaire de consultation"
APP_TITLE_EN = "Consultation questionnaire"

DB_PATH = "responses.db"
LONG_LIST_CSV = os.path.join("data", "indicator_longlist.csv")
LONG_LIST_XLSX = os.path.join("data", "longlist.xlsx")

UK_FR = "UK (Inconnu)"
UK_EN = "UK (Unknown)"

ROLE_OPTIONS_FR = [
    "DG/DGA/SG",
    "Directeur",
    "Conseiller",
    "Chef de division",
    "Chef de bureau",
    "Autre",
]
ROLE_OPTIONS_EN = [
    "DG/DGA/SG",
    "Director",
    "Advisor",
    "Head of division",
    "Head of office",
    "Other",
]


# =========================
# Helpers : i18n and state
# =========================

def t(lang: str, fr: str, en: str) -> str:
    return fr if lang == "fr" else en


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_query_params() -> Dict[str, List[str]]:
    """Compatibility across Streamlit versions."""
    try:
        # Streamlit >= 1.30
        qp = st.query_params  # type: ignore
        return {k: list(v) if isinstance(v, (list, tuple)) else [str(v)] for k, v in qp.items()}
    except Exception:
        try:
            return st.experimental_get_query_params()
        except Exception:
            return {}


def set_query_params(params: Dict[str, Any]) -> None:
    try:
        st.query_params.update(params)  # type: ignore
    except Exception:
        try:
            st.experimental_set_query_params(**params)
        except Exception:
            pass


def init_session() -> None:
    if "lang" not in st.session_state:
        st.session_state.lang = "fr"
    if "nav_idx" not in st.session_state:
        st.session_state.nav_idx = 0
    if "responses" not in st.session_state:
        st.session_state.responses = {}
    if "submission_id" not in st.session_state:
        st.session_state.submission_id = None
    if "admin_authed" not in st.session_state:
        st.session_state.admin_authed = False


def resp_get(key: str, default=None):
    return st.session_state.responses.get(key, default)


def resp_set(key: str, value) -> None:
    st.session_state.responses[key] = value


# =========================
# Data : longlist loader
# =========================

@st.cache_data(show_spinner=False)
def load_longlist() -> pd.DataFrame:
    """
    Load indicator longlist from CSV (preferred) or XLSX.
    The app still runs if the file is missing, but selection lists will be empty.
    """
    if os.path.exists(LONG_LIST_CSV):
        df = pd.read_csv(LONG_LIST_CSV, dtype=str).fillna("")
        return df
    if os.path.exists(LONG_LIST_XLSX):
        df = pd.read_excel(LONG_LIST_XLSX, dtype=str).fillna("")
        # Expected columns in user file:
        # Domain_code, Domain_label_fr, Stat_label_fr
        if set(["Domain_code", "Domain_label_fr", "Stat_label_fr"]).issubset(df.columns):
            df["domain_code"] = df["Domain_code"].astype(str).str.strip()
            df["domain_label_fr"] = df["Domain_label_fr"].astype(str).str.split("|", n=1).str[-1].str.strip()
            df["domain_label_en"] = df["domain_label_fr"]
            df["stat_code"] = df["Stat_label_fr"].astype(str).str.split("|", n=1).str[0].str.strip()
            df["stat_label_fr"] = df["Stat_label_fr"].astype(str).str.split("|", n=1).str[-1].str.strip()
            df["stat_label_en"] = df["stat_label_fr"]
            return df[["domain_code", "domain_label_fr", "domain_label_en", "stat_code", "stat_label_fr", "stat_label_en"]]
    return pd.DataFrame(columns=["domain_code", "domain_label_fr", "domain_label_en", "stat_code", "stat_label_fr", "stat_label_en"])


def domains_from_longlist(df_long: pd.DataFrame, lang: str) -> List[Tuple[str, str]]:
    if df_long.empty:
        return []
    col = "domain_label_fr" if lang == "fr" else "domain_label_en"
    tmp = df_long[["domain_code", col]].drop_duplicates().sort_values(["domain_code", col])
    return [(r["domain_code"], r[col]) for _, r in tmp.iterrows()]


def stats_for_domain(df_long: pd.DataFrame, domain_code: str, lang: str) -> List[Tuple[str, str]]:
    if df_long.empty or not domain_code:
        return []
    col = "stat_label_fr" if lang == "fr" else "stat_label_en"
    tmp = df_long[df_long["domain_code"] == domain_code][["stat_code", col]].drop_duplicates()
    tmp = tmp.sort_values(["stat_code", col])
    return [(r["stat_code"], r[col]) for _, r in tmp.iterrows()]


# =========================
# Storage : SQLite + optional Google Sheets + Dropbox
# =========================

def db_init() -> None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS submissions(
            submission_id TEXT PRIMARY KEY,
            submitted_at_utc TEXT,
            lang TEXT,
            payload_json TEXT
        )
    """)
    con.commit()
    con.close()


def db_save_submission(submission_id: str, lang: str, payload: Dict[str, Any]) -> None:
    db_init()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO submissions(submission_id, submitted_at_utc, lang, payload_json)
        VALUES(?, ?, ?, ?)
    """, (submission_id, now_utc_iso(), lang, json.dumps(payload, ensure_ascii=False)))
    con.commit()
    con.close()


def db_read_submissions(limit: int = 2000) -> pd.DataFrame:
    if not os.path.exists(DB_PATH):
        return pd.DataFrame(columns=["submission_id", "submitted_at_utc", "lang", "payload_json"])
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT submission_id, submitted_at_utc, lang, payload_json FROM submissions ORDER BY submitted_at_utc DESC LIMIT ?",
        con,
        params=(limit,),
    )
    con.close()
    return df


def flatten_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Create a 'flat' row for exports / Google Sheets."""
    out: Dict[str, Any] = {}
    # Identification
    out["organisation"] = payload.get("organisation", "")
    out["pays"] = payload.get("pays", "")
    out["type_acteur"] = payload.get("type_acteur", "")
    out["fonction"] = payload.get("fonction", "")
    out["email"] = payload.get("email", "")
    out["lang"] = payload.get("lang", "")
    # Domains
    top5 = payload.get("top5_domains", [])
    for i in range(5):
        out[f"top_domain_{i+1}"] = top5[i] if i < len(top5) else ""
    # Stats count
    selected_stats = payload.get("selected_stats", [])
    out["nb_stats"] = len(selected_stats) if isinstance(selected_stats, list) else 0
    out["stats_list"] = "; ".join(selected_stats) if isinstance(selected_stats, list) else ""
    # Optional open questions
    out["comment_1"] = payload.get("open_q1", "")
    out["comment_2"] = payload.get("open_q2", "")
    return out


def google_sheets_append(payload: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Append a row into a Google Sheet if configured.
    Requires secrets:
      GOOGLE_SHEET_ID
      GOOGLE_SERVICE_ACCOUNT (dict)
    """
    if gspread is None or Credentials is None:
        return False, "Bibliothèques Google Sheets non disponibles (gspread/google-auth)."
    try:
        sheet_id = st.secrets.get("GOOGLE_SHEET_ID", None)
        sa_info = st.secrets.get("GOOGLE_SERVICE_ACCOUNT", None)
        if not sheet_id or not sa_info:
            return False, "Google Sheets non configuré (secrets manquants)."
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet("responses")
        except Exception:
            ws = sh.add_worksheet(title="responses", rows=2000, cols=80)

        row = flatten_payload(payload)
        # Ensure header
        existing = ws.get_all_values()
        if not existing:
            ws.append_row(list(row.keys()), value_input_option="RAW")
        else:
            header = existing[0]
            # Add any missing columns at end
            for k in row.keys():
                if k not in header:
                    header.append(k)
            # If header grew, update first row
            ws.update("A1", [header])
        # Align order with header
        header = ws.row_values(1)
        values = [row.get(h, "") for h in header]
        ws.append_row(values, value_input_option="RAW")
        return True, "OK"
    except Exception as e:
        return False, f"Erreur Google Sheets : {e}"


def dropbox_upload_json(submission_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Upload the JSON submission to Dropbox if configured.
    Requires secret: DROPBOX_ACCESS_TOKEN
    Optional: DROPBOX_FOLDER (default /consultation_stat_niang)
    """
    if dropbox is None:
        return False, "Bibliothèque Dropbox non disponible."
    try:
        token = st.secrets.get("DROPBOX_ACCESS_TOKEN", None)
        if not token:
            return False, "Dropbox non configuré (DROPBOX_ACCESS_TOKEN manquant)."
        folder = st.secrets.get("DROPBOX_FOLDER", "/consultation_stat_niang")
        folder = folder if folder.startswith("/") else "/" + folder
        path = f"{folder}/submissions/{submission_id}.json"
        dbx = dropbox.Dropbox(token)
        content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        dbx.files_upload(content, path, mode=dropbox.files.WriteMode.overwrite)
        return True, "OK"
    except Exception as e:
        return False, f"Erreur Dropbox : {e}"


# =========================
# Validation logic (quality controls)
# =========================

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def validate_r2(lang: str) -> List[str]:
    errs: List[str] = []
    organisation = resp_get("organisation", "").strip()
    pays = resp_get("pays", "").strip()
    type_acteur = resp_get("type_acteur", "").strip()
    fonction = resp_get("fonction", "").strip()
    fonction_autre = resp_get("fonction_autre", "").strip()
    email = resp_get("email", "").strip()

    if not organisation:
        errs.append(t(lang, "Organisation : champ obligatoire.", "Organization: required field."))
    if not pays:
        errs.append(t(lang, "Pays : champ obligatoire.", "Country: required field."))
    if not type_acteur:
        errs.append(t(lang, "Type d’acteur : champ obligatoire.", "Stakeholder type: required field."))
    if not fonction:
        errs.append(t(lang, "Fonction : champ obligatoire.", "Role/Function: required field."))
    if fonction == "Autre" or fonction == "Other":
        if not fonction_autre:
            errs.append(t(lang, "Fonction (Autre) : précisez.", "Role (Other): please specify."))
    if not email:
        errs.append(t(lang, "Email : champ obligatoire.", "Email: required field."))
    elif not EMAIL_RE.match(email):
        errs.append(t(lang, "Email : format invalide.", "Email: invalid format."))
    return errs


def validate_r4(lang: str) -> List[str]:
    errs: List[str] = []
    pre = resp_get("preselected_domains", [])
    top5 = resp_get("top5_domains", [])
    if not isinstance(pre, list):
        pre = []
    if not isinstance(top5, list):
        top5 = []
    if len(pre) < 5 or len(pre) > 10:
        errs.append(t(lang, "Rubrique 4 : pré-sélectionnez entre 5 et 10 domaines.", "Section 4: pre-select 5 to 10 domains."))
    if len(set(pre)) != len(pre):
        errs.append(t(lang, "Rubrique 4 : la pré-sélection contient des doublons.", "Section 4: duplicates found in pre-selection."))
    if len(top5) != 5:
        errs.append(t(lang, "Rubrique 4 : le TOP 5 doit contenir exactement 5 domaines.", "Section 4: TOP 5 must contain exactly 5 domains."))
    else:
        if len(set(top5)) != 5:
            errs.append(t(lang, "Rubrique 4 : le TOP 5 contient des doublons.", "Section 4: TOP 5 contains duplicates."))
        missing = [d for d in top5 if d not in pre]
        if missing:
            errs.append(t(lang, "Rubrique 4 : chaque domaine du TOP 5 doit provenir de la pré-sélection.", "Section 4: TOP 5 must be selected from pre-selection."))
    return errs


def validate_r5(lang: str) -> List[str]:
    errs: List[str] = []
    top5 = resp_get("top5_domains", [])
    selected_by_domain: Dict[str, List[str]] = resp_get("selected_by_domain", {})
    if not isinstance(selected_by_domain, dict):
        selected_by_domain = {}

    all_stats: List[str] = []
    for d in top5:
        stats = selected_by_domain.get(d, [])
        if not isinstance(stats, list):
            stats = []
        if len(stats) < 1:
            errs.append(t(lang, f"Rubrique 5 : choisissez au moins 1 statistique pour {d}.",
                          f"Section 5: select at least 1 indicator for {d}."))
        if len(stats) > 3:
            errs.append(t(lang, f"Rubrique 5 : maximum 3 statistiques pour {d}.",
                          f"Section 5: maximum 3 indicators for {d}."))
        all_stats.extend(stats)

    if len(all_stats) < 5 or len(all_stats) > 15:
        errs.append(t(lang, "Rubrique 5 : le total des statistiques doit être entre 5 et 15.",
                      "Section 5: total number of indicators must be between 5 and 15."))

    if len(set(all_stats)) != len(all_stats):
        errs.append(t(lang, "Rubrique 5 : une même statistique ne doit pas être sélectionnée plusieurs fois.",
                      "Section 5: the same indicator must not be selected more than once."))

    # scoring
    scoring: Dict[str, Dict[str, Any]] = resp_get("scoring", {})
    if not isinstance(scoring, dict):
        scoring = {}

    for s in all_stats:
        if s not in scoring:
            errs.append(t(lang, f"Rubrique 5 : vous devez noter la statistique {s}.",
                          f"Section 5: you must score indicator {s}."))
            continue
        for k in ["gap", "demand", "feasibility"]:
            if k not in scoring[s]:
                errs.append(t(lang, f"Rubrique 5 : la note '{k}' manque pour {s}.",
                              f"Section 5: missing score '{k}' for {s}."))
            else:
                try:
                    v = int(scoring[s][k])
                    if v < 0 or v > 3:
                        errs.append(t(lang, f"Rubrique 5 : note invalide pour {s} ({k}).",
                                      f"Section 5: invalid score for {s} ({k})."))
                except Exception:
                    errs.append(t(lang, f"Rubrique 5 : note invalide pour {s} ({k}).",
                                  f"Section 5: invalid score for {s} ({k})."))
    return errs


def validate_r6(lang: str) -> List[str]:
    errs: List[str] = []
    tbl = resp_get("gender_table", {})
    if not isinstance(tbl, dict) or not tbl:
        errs.append(t(lang, "Rubrique 6 : veuillez renseigner le tableau.", "Section 6: please complete the table."))
        return errs
    for k, v in tbl.items():
        if not v:
            errs.append(t(lang, f"Rubrique 6 : ligne non renseignée : {k}.", f"Section 6: missing answer for: {k}."))
    return errs


def validate_r8(lang: str) -> List[str]:
    errs: List[str] = []
    tbl = resp_get("capacity_table", {})
    if not isinstance(tbl, dict) or not tbl:
        errs.append(t(lang, "Rubrique 8 : veuillez renseigner le tableau.", "Section 8: please complete the table."))
        return errs
    for k, v in tbl.items():
        if not v:
            errs.append(t(lang, f"Rubrique 8 : ligne non renseignée : {k}.", f"Section 8: missing answer for: {k}."))
    return errs


def validate_all(lang: str) -> List[str]:
    errs = []
    errs.extend(validate_r2(lang))
    errs.extend(validate_r4(lang))
    errs.extend(validate_r5(lang))
    errs.extend(validate_r6(lang))
    errs.extend(validate_r8(lang))
    # Open questions optional: handled in send step as warnings
    return errs


# =========================
# Navigation
# =========================

def get_steps(lang: str) -> List[Tuple[str, str]]:
    # Rubrics 7 and 11 removed, plus final SEND tab
    return [
        ("R1", t(lang, "Rubrique 1 : Instructions", "Section 1: Instructions")),
        ("R2", t(lang, "Rubrique 2 : Identification du répondant", "Section 2: Respondent identification")),
        ("R3", t(lang, "Rubrique 3 : Portée de la réponse", "Section 3: Scope of response")),
        ("R4", t(lang, "Rubrique 4 : Domaines prioritaires", "Section 4: Priority domains")),
        ("R5", t(lang, "Rubrique 5 : Statistiques prioritaires et notation", "Section 5: Priority indicators and scoring")),
        ("R6", t(lang, "Rubrique 6 : Perspective de genre", "Section 6: Gender perspective")),
        ("R8", t(lang, "Rubrique 8 : Capacité et faisabilité (12–24 mois)", "Section 8: Capacity and feasibility (12–24 months)")),
        ("R9", t(lang, "Rubrique 9 : Harmonisation et qualité", "Section 9: Harmonization and quality")),
        ("R10", t(lang, "Rubrique 10 : Diffusion", "Section 10: Dissemination")),
        ("R12", t(lang, "Rubrique 12 : Questions ouvertes", "Section 12: Open questions")),
        ("SEND", t(lang, "ENVOYER", "SUBMIT")),
    ]


def render_sidebar(lang: str, steps: List[Tuple[str, str]]) -> None:
    st.sidebar.header(t(lang, "Navigation", "Navigation"))
    labels = [s[1] for s in steps]

    # Keep radio in sync with nav_idx (fixes "Next/Prev not working")
    if "nav_radio" not in st.session_state:
        st.session_state.nav_radio = st.session_state.nav_idx

    chosen = st.sidebar.radio(
        t(lang, "Aller à", "Go to"),
        options=list(range(len(labels))),
        index=int(st.session_state.nav_idx),
        format_func=lambda i: labels[i],
        key="nav_radio"
    )

    # User clicked in sidebar
    if int(chosen) != int(st.session_state.nav_idx):
        st.session_state.nav_idx = int(chosen)

    st.sidebar.divider()
    st.sidebar.caption(
        t(
            lang,
            "Note : les contrôles qualité peuvent bloquer la progression si une contrainte n’est pas respectée.",
            "Note: quality checks may prevent moving forward when constraints are not met."
        )
    )

    st.sidebar.markdown("---")
    st.sidebar.caption(
        t(
            lang,
            "UK : Inconnu (score 0). Utilisez UK uniquement si l’information est indisponible.",
            "UK: Unknown (score 0). Use UK only when information is unavailable."
        )
    )


def nav_buttons(lang: str, steps: List[Tuple[str, str]], df_long: pd.DataFrame) -> None:
    """Bottom nav buttons, with blocking based on current step validations."""
    step_key = steps[st.session_state.nav_idx][0]
    errors: List[str] = []

    # Blocking rules per step
    if step_key == "R2":
        errors = validate_r2(lang)
    elif step_key == "R4":
        errors = validate_r4(lang)
    elif step_key == "R5":
        errors = validate_r5(lang)
    elif step_key == "R6":
        errors = validate_r6(lang)
    elif step_key == "R8":
        errors = validate_r8(lang)

    col1, col2, col3 = st.columns([1, 1, 3])
    with col1:
        prev_disabled = st.session_state.nav_idx <= 0
        if st.button(t(lang, "⬅ Précédent", "⬅ Previous"), disabled=prev_disabled):
            st.session_state.nav_idx = max(0, st.session_state.nav_idx - 1)
            st.session_state.nav_radio = st.session_state.nav_idx
            st.rerun()
    with col2:
        next_disabled = (st.session_state.nav_idx >= len(steps) - 1) or bool(errors)
        if st.button(t(lang, "Suivant ➡", "Next ➡"), disabled=next_disabled):
            st.session_state.nav_idx = min(len(steps) - 1, st.session_state.nav_idx + 1)
            st.session_state.nav_radio = st.session_state.nav_idx
            st.rerun()
    with col3:
        if errors:
            st.error("\n".join(errors))


# =========================
# UI : Rubrics
# =========================

def rubric_1(lang: str) -> None:
    st.subheader(t(lang, "Rubrique 1 : Instructions", "Section 1: Instructions"))
    st.markdown(
        t(
            lang,
            """
### Objectif
Ce questionnaire vise à recueillir votre avis sur **les statistiques socio-économiques prioritaires** à produire et diffuser au niveau continental.

### Comment répondre
1. **Identifiez** votre organisation (Rubrique 2).
2. **Pré-sélectionnez 5 à 10 domaines** et classez un **TOP 5** (Rubrique 4).
3. Pour chaque domaine du TOP 5 : choisissez **1 à 3 statistiques** et attribuez des **notes** (Rubrique 5).
4. Complétez les rubriques transversales : **genre** et **capacité/faisabilité**.

### Barème de notation (Rubrique 5)
- **3** : élevé / très important  
- **2** : moyen  
- **1** : faible  
- **0** : UK (Inconnu)

> Conseil : privilégiez les statistiques réellement **utilisables, demandées et faisables** à horizon 12–24 mois.
            """,
            """
### Purpose
This questionnaire collects your views on **priority socio-economic statistics** to be produced and disseminated at continental level.

### How to answer
1. **Identify** your organization (Section 2).
2. **Pre-select 5–10 domains** and rank a **TOP 5** (Section 4).
3. For each TOP 5 domain: select **1–3 indicators** and provide **scores** (Section 5).
4. Complete cross-cutting sections: **gender** and **capacity/feasibility**.

### Scoring scale (Section 5)
- **3**: high  
- **2**: medium  
- **1**: low  
- **0**: UK (Unknown)

> Tip: prioritize indicators that are **useful, demanded and feasible** within 12–24 months.
            """
        )
    )


def rubric_2(lang: str) -> None:
    st.subheader(t(lang, "Rubrique 2 : Identification du répondant", "Section 2: Respondent identification"))
    st.info(
        t(
            lang,
            "Merci de renseigner ces informations. Elles servent uniquement à l’analyse et ne seront pas publiées nominativement.",
            "Please provide these details. They are used for analysis and will not be published in a personally identifiable way."
        )
    )

    resp_set("lang", lang)

    st.text_input(t(lang, "Organisation", "Organization"), key="org_input", value=resp_get("organisation", ""))
    resp_set("organisation", st.session_state.get("org_input", "").strip())

    col1, col2 = st.columns(2)
    with col1:
        st.text_input(t(lang, "Pays", "Country"), key="country_input", value=resp_get("pays", ""))
        resp_set("pays", st.session_state.get("country_input", "").strip())
    with col2:
        st.text_input(t(lang, "Email", "Email"), key="email_input", value=resp_get("email", ""))
        resp_set("email", st.session_state.get("email_input", "").strip())

    type_options = [
        ("NSO", {"fr": "Institut national de statistique", "en": "National Statistical Office"}),
        ("Ministry", {"fr": "Ministère / Service statistique sectoriel", "en": "Ministry / Sector statistical unit"}),
        ("REC", {"fr": "Communauté économique régionale", "en": "Regional Economic Community"}),
        ("CivilSoc", {"fr": "Société civile", "en": "Civil society"}),
        ("DevPartner", {"fr": "Partenaire technique et financier", "en": "Development partner"}),
        ("Academia", {"fr": "Université / Recherche", "en": "Academia / Research"}),
        ("Other", {"fr": "Autre", "en": "Other"}),
    ]
    type_labels = [t(lang, x[1]["fr"], x[1]["en"]) for x in type_options]
    type_keys = [x[0] for x in type_options]

    selected_idx = 0
    if resp_get("type_acteur"):
        try:
            selected_idx = type_keys.index(resp_get("type_acteur"))
        except Exception:
            selected_idx = 0

    chosen = st.selectbox(t(lang, "Type d’acteur", "Stakeholder type"), options=list(range(len(type_keys))),
                          index=selected_idx, format_func=lambda i: type_labels[i])
    resp_set("type_acteur", type_keys[int(chosen)])

    # Fonction dropdown
    role_opts = ROLE_OPTIONS_FR if lang == "fr" else ROLE_OPTIONS_EN
    role_default = resp_get("fonction", role_opts[0] if role_opts else "")
    try:
        role_idx = role_opts.index(role_default)
    except Exception:
        role_idx = 0

    chosen_role = st.selectbox(t(lang, "Fonction", "Role/Function"), options=role_opts, index=role_idx)
    resp_set("fonction", chosen_role)

    if chosen_role in ["Autre", "Other"]:
        st.text_input(t(lang, "Préciser (fonction)", "Specify (role)"),
                      key="fonction_autre_input", value=resp_get("fonction_autre", ""))
        resp_set("fonction_autre", st.session_state.get("fonction_autre_input", "").strip())
    else:
        resp_set("fonction_autre", "")

    # Live errors
    errs = validate_r2(lang)
    if errs:
        st.warning(t(lang, "Veuillez corriger les éléments ci-dessous :", "Please fix the following:"))
        st.write("\n".join([f"- {e}" for e in errs]))


def rubric_3(lang: str) -> None:
    st.subheader(t(lang, "Rubrique 3 : Portée de la réponse", "Section 3: Scope of response"))
    st.markdown(
        t(
            lang,
            "Indiquez le périmètre principal de votre réponse. Cela aide à interpréter vos priorités.",
            "Indicate the main scope of your response. This helps interpret your priorities."
        )
    )

    scope_opts = [
        ("National", {"fr": "National", "en": "National"}),
        ("Regional", {"fr": "Régional (CER)", "en": "Regional (REC)"}),
        ("Continental", {"fr": "Continental (UA)", "en": "Continental (AU)"}),
        ("Global", {"fr": "International", "en": "International"}),
        ("Other", {"fr": "Autre", "en": "Other"}),
    ]
    labels = [t(lang, x[1]["fr"], x[1]["en"]) for x in scope_opts]
    keys = [x[0] for x in scope_opts]
    default = resp_get("scope", "National")
    idx = keys.index(default) if default in keys else 0
    chosen = st.radio(t(lang, "Portée", "Scope"), options=list(range(len(keys))), index=idx, format_func=lambda i: labels[i])
    resp_set("scope", keys[int(chosen)])

    if resp_get("scope") == "Other":
        st.text_input(t(lang, "Préciser", "Specify"), key="scope_other_input", value=resp_get("scope_other", ""))
        resp_set("scope_other", st.session_state.get("scope_other_input", "").strip())
    else:
        resp_set("scope_other", "")



def rubric_4(lang: str, df_long: pd.DataFrame) -> None:
    st.subheader(t(lang, "Rubrique 4 : Domaines prioritaires", "Section 4: Priority domains"))

    domains = domains_from_longlist(df_long, lang)
    if not domains:
        st.error(t(lang, "La liste des domaines n’est pas disponible (fichier longlist manquant).",
                   "Domain list is not available (missing longlist file)."))
        return

    code_to_label = {c: lbl for c, lbl in domains}

    # Build display labels without showing codes (codes are stored internally)
    labels = [code_to_label[c] for c, _ in domains]
    # Disambiguate duplicates if any (rare)
    seen = {}
    for i, (c, _) in enumerate(domains):
        lbl = code_to_label[c]
        seen[lbl] = seen.get(lbl, 0) + 1
    display_labels = []
    label_to_code = {}
    for c, _ in domains:
        lbl = code_to_label[c]
        disp = lbl if seen[lbl] == 1 else f"{lbl} ({c})"
        display_labels.append(disp)
        label_to_code[disp] = c

    st.markdown(
        t(
            lang,
            """
### Étape 1 : Pré-sélection
Sélectionnez **entre 5 et 10 domaines** (sans doublons).
            """,
            """
### Step 1: Pre-selection
Select **5 to 10 domains** (no duplicates).
            """
        )
    )

    pre_default_codes = resp_get("preselected_domains", [])
    pre_default_disp = []
    for c in pre_default_codes:
        lbl = code_to_label.get(c, "")
        if not lbl:
            continue
        disp = lbl if seen.get(lbl, 1) == 1 else f"{lbl} ({c})"
        if disp in label_to_code:
            pre_default_disp.append(disp)

    pre_disp = st.multiselect(
        t(lang, "Pré-sélection (5–10 domaines)", "Pre-selection (5–10 domains)"),
        options=display_labels,
        default=pre_default_disp
    )
    pre_codes = [label_to_code[x] for x in pre_disp]
    resp_set("preselected_domains", pre_codes)

    st.divider()
    st.markdown(
        t(
            lang,
            """
### Étape 2 : Classement TOP 5
Classez exactement **5 domaines** parmi votre pré-sélection.
            """,
            """
### Step 2: Rank TOP 5
Rank exactly **5 domains** from your pre-selection.
            """
        )
    )

    if len(pre_codes) < 5:
        st.warning(t(lang, "Sélectionnez d’abord au moins 5 domaines dans la pré-sélection.",
                     "Please pre-select at least 5 domains first."))
        resp_set("top5_domains", [])
        return

    top5: List[str] = []
    pre_option_codes = pre_codes.copy()

    # Ranking with 5 selectboxes (codes hidden via format_func)
    for i in range(5):
        key = f"top5_rank_{i+1}"
        prev = resp_get(key, pre_option_codes[0] if pre_option_codes else "")
        if prev not in pre_option_codes and pre_option_codes:
            prev = pre_option_codes[0]
        choice = st.selectbox(
            t(lang, f"Rang {i+1}", f"Rank {i+1}"),
            options=pre_option_codes,
            index=pre_option_codes.index(prev) if prev in pre_option_codes else 0,
            format_func=lambda c: code_to_label.get(c, c),
            key=key
        )
        top5.append(choice)

    resp_set("top5_domains", top5)

    errs = validate_r4(lang)
    if errs:
        st.warning(t(lang, "Contrôles qualité :", "Quality checks:"))
        st.write("\n".join([f"- {e}" for e in errs]))



def rubric_5(lang: str, df_long: pd.DataFrame) -> None:
    st.subheader(t(lang, "Rubrique 5 : Statistiques prioritaires et notation", "Section 5: Priority indicators and scoring"))

    top5 = resp_get("top5_domains", [])
    if not top5 or len(top5) != 5:
        st.warning(t(lang, "Veuillez d’abord finaliser le TOP 5 des domaines (Rubrique 4).",
                     "Please complete TOP 5 domains first (Section 4)."))
        return

    # mapping for domain display
    dom_map = {c: lbl for c, lbl in domains_from_longlist(df_long, lang)}

    st.markdown(
        t(
            lang,
            """
### Étape A : Sélection des statistiques
Pour chaque domaine du TOP 5 : choisissez **1 à 3 statistiques**.
- Total attendu : **entre 5 et 15** statistiques.
- Une statistique ne doit pas apparaître dans deux domaines.

### Étape B : Notation multicritères
Pour chaque statistique sélectionnée, attribuez une note (0–3) sur :
- **Écart de données** : manque actuel / insuffisance
- **Demande politique** : intérêt politique / stratégique
- **Faisabilité** : capacité de production à 12–24 mois
            """,
            """
### Step A: Select indicators
For each TOP 5 domain: select **1 to 3 indicators**.
- Expected total: **5 to 15** indicators.
- The same indicator must not be selected under two domains.

### Step B: Multi-criteria scoring
For each selected indicator, provide a score (0–3) for:
- **Data gap**
- **Political demand**
- **Feasibility (12–24 months)**
            """
        )
    )

    selected_by_domain: Dict[str, List[str]] = resp_get("selected_by_domain", {})
    if not isinstance(selected_by_domain, dict):
        selected_by_domain = {}

    scoring: Dict[str, Dict[str, Any]] = resp_get("scoring", {})
    if not isinstance(scoring, dict):
        scoring = {}

    # Ensure dict keys exist
    for d in top5:
        if d not in selected_by_domain:
            selected_by_domain[d] = []

    # UI selection per domain (codes hidden)
    for d in top5:
        st.markdown(f"#### {dom_map.get(d, d)}")

        stats_opts = stats_for_domain(df_long, d, lang)
        stat_code_to_label = {c: lbl for c, lbl in stats_opts}

        # build display labels without showing stat codes
        labels = [stat_code_to_label[c] for c, _ in stats_opts]
        seen = {}
        for c, _ in stats_opts:
            lbl = stat_code_to_label[c]
            seen[lbl] = seen.get(lbl, 0) + 1
        display_labels = []
        label_to_code = {}
        for c, _ in stats_opts:
            lbl = stat_code_to_label[c]
            disp = lbl if seen[lbl] == 1 else f"{lbl} ({c})"
            display_labels.append(disp)
            label_to_code[disp] = c

        default_codes = selected_by_domain.get(d, [])
        default_disp = []
        for c in default_codes:
            lbl = stat_code_to_label.get(c, "")
            if not lbl:
                continue
            disp = lbl if seen.get(lbl, 1) == 1 else f"{lbl} ({c})"
            if disp in label_to_code:
                default_disp.append(disp)

        picked_disp = st.multiselect(
            t(lang, "Choisir 1 à 3 statistiques", "Select 1 to 3 indicators"),
            options=display_labels,
            default=default_disp,
            key=f"stats_ms_{d}"
        )
        picked_codes = [label_to_code[x] for x in picked_disp]

        if len(picked_codes) > 3:
            st.warning(t(lang, "Maximum 3 statistiques : seules les 3 premières sont retenues.",
                         "Maximum 3 indicators: only the first 3 are kept."))
            picked_codes = picked_codes[:3]

        selected_by_domain[d] = picked_codes

    # Uniqueness check
    flattened = []
    for d in top5:
        flattened.extend(selected_by_domain.get(d, []))
    duplicates = [x for x in set(flattened) if flattened.count(x) > 1]
    if duplicates:
        st.error(
            t(
                lang,
                "Une ou plusieurs statistiques sont sélectionnées dans plusieurs domaines. Veuillez corriger.",
                "One or more indicators are selected under multiple domains. Please correct."
            )
        )

    resp_set("selected_by_domain", selected_by_domain)
    resp_set("selected_stats", flattened)

    # Map codes to labels for display in scoring
    global_map = {}
    for d in top5:
        for c, lbl in stats_for_domain(df_long, d, lang):
            global_map[c] = lbl

    st.divider()
    st.markdown("### " + t(lang, "Notation multicritères (0–3)", "Multi-criteria scoring (0–3)"))

    for s in flattened:
        if s not in scoring:
            scoring[s] = {"gap": 0, "demand": 0, "feasibility": 0}

        st.markdown(f"**{global_map.get(s, s)}**")

        c1, c2, c3 = st.columns(3)
        with c1:
            scoring[s]["gap"] = st.selectbox(
                t(lang, "Écart de données", "Data gap"),
                options=[0, 1, 2, 3],
                index=int(scoring[s].get("gap", 0)),
                help=t(lang, f"0 = {UK_FR}, 3 = Élevé", f"0 = {UK_EN}, 3 = High"),
                key=f"sc_gap_{s}"
            )
        with c2:
            scoring[s]["demand"] = st.selectbox(
                t(lang, "Demande politique", "Political demand"),
                options=[0, 1, 2, 3],
                index=int(scoring[s].get("demand", 0)),
                help=t(lang, f"0 = {UK_FR}, 3 = Élevé", f"0 = {UK_EN}, 3 = High"),
                key=f"sc_dem_{s}"
            )
        with c3:
            scoring[s]["feasibility"] = st.selectbox(
                t(lang, "Faisabilité 12–24 mois", "Feasibility 12–24 months"),
                options=[0, 1, 2, 3],
                index=int(scoring[s].get("feasibility", 0)),
                help=t(lang, f"0 = {UK_FR}, 3 = Élevé", f"0 = {UK_EN}, 3 = High"),
                key=f"sc_fea_{s}"
            )

    resp_set("scoring", scoring)

    errs = validate_r5(lang)
    if errs:
        st.warning(t(lang, "Contrôles qualité :", "Quality checks:"))
        st.write("\n".join([f"- {e}" for e in errs]))


def rubric_6(lang: str) -> None:
    st.subheader(t(lang, "Rubrique 6 : Perspective de genre", "Section 6: Gender perspective"))
    st.markdown(
        t(
            lang,
            "Indiquez si les statistiques prioritaires doivent intégrer ces dimensions (Oui/Non/Selon indicateur/UK).",
            "Indicate whether priority indicators should integrate these dimensions (Yes/No/Indicator-specific/UK)."
        )
    )

    options = [
        (t(lang, "Oui", "Yes"), "YES"),
        (t(lang, "Non", "No"), "NO"),
        (t(lang, "Selon indicateur", "Indicator-specific"), "SPEC"),
        (UK_FR if lang == "fr" else UK_EN, "UK"),
    ]
    labels = [x[0] for x in options]
    code_map = {x[0]: x[1] for x in options}

    items_fr = [
        "Désagrégation par sexe",
        "Désagrégation par âge",
        "Milieu urbain / rural",
        "Handicap",
        "Quintile de richesse",
    ]
    items_en = [
        "Disaggregation by sex",
        "Disaggregation by age",
        "Urban / rural",
        "Disability",
        "Wealth quintile",
    ]
    items = items_fr if lang == "fr" else items_en

    tbl = resp_get("gender_table", {})
    if not isinstance(tbl, dict):
        tbl = {}

    for it in items:
        prev = tbl.get(it, "UK")
        prev_label = next((lab for lab, code in code_map.items() if code == prev), labels[-1])
        chosen = st.radio(it, options=labels, index=labels.index(prev_label), horizontal=True, key=f"gender_{it}")
        tbl[it] = code_map[chosen]

    resp_set("gender_table", tbl)

    errs = validate_r6(lang)
    if errs:
        st.warning(t(lang, "Contrôles qualité :", "Quality checks:"))
        st.write("\n".join([f"- {e}" for e in errs]))


def rubric_8(lang: str) -> None:
    st.subheader(t(lang, "Rubrique 8 : Capacité et faisabilité (12–24 mois)", "Section 8: Capacity and feasibility (12–24 months)"))
    st.markdown(
        t(
            lang,
            "Évaluez le niveau de capacité pour produire les statistiques prioritaires dans les 12–24 mois à venir.",
            "Assess your capacity to produce priority statistics in the coming 12–24 months."
        )
    )

    scale = [
        (t(lang, "Élevé", "High"), "HIGH"),
        (t(lang, "Moyen", "Medium"), "MED"),
        (t(lang, "Faible", "Low"), "LOW"),
        (UK_FR if lang == "fr" else UK_EN, "UK"),
    ]
    labels = [x[0] for x in scale]
    code_map = {x[0]: x[1] for x in scale}

    items_fr = [
        "Compétences statistiques disponibles",
        "Accès aux données administratives",
        "Financement disponible",
        "Outils numériques (collecte, traitement, diffusion)",
        "Cadre juridique pour le partage de données",
        "Coordination interinstitutionnelle",
    ]
    items_en = [
        "Available statistical skills",
        "Access to administrative data",
        "Available funding",
        "Digital tools (collection, processing, dissemination)",
        "Legal framework for data sharing",
        "Inter-institutional coordination",
    ]
    items = items_fr if lang == "fr" else items_en

    tbl = resp_get("capacity_table", {})
    if not isinstance(tbl, dict):
        tbl = {}

    for it in items:
        prev = tbl.get(it, "UK")
        prev_label = next((lab for lab, code in code_map.items() if code == prev), labels[-1])
        chosen = st.radio(it, options=labels, index=labels.index(prev_label), horizontal=True, key=f"cap_{it}")
        tbl[it] = code_map[chosen]

    resp_set("capacity_table", tbl)

    errs = validate_r8(lang)
    if errs:
        st.warning(t(lang, "Contrôles qualité :", "Quality checks:"))
        st.write("\n".join([f"- {e}" for e in errs]))


def rubric_9(lang: str) -> None:
    st.subheader(t(lang, "Rubrique 9 : Harmonisation et qualité", "Section 9: Harmonization and quality"))
    st.markdown(
        t(
            lang,
            "Indiquez les exigences clés attendues en matière d’harmonisation et d’assurance qualité.",
            "Indicate key expectations regarding harmonization and quality assurance."
        )
    )

    opts_fr = [
        "Normes internationales (ONU, FMI, UA, etc.)",
        "Méthodologies harmonisées au niveau continental",
        "Calendrier de diffusion et révisions documentées",
        "Accès aux métadonnées",
        "Autre",
    ]
    opts_en = [
        "International standards (UN, IMF, AU, etc.)",
        "Continental harmonized methodologies",
        "Release calendar and documented revisions",
        "Access to metadata",
        "Other",
    ]
    opts = opts_fr if lang == "fr" else opts_en
    default = resp_get("quality_expectations", [])
    sel = st.multiselect(t(lang, "Sélectionnez", "Select"), options=opts, default=default)
    resp_set("quality_expectations", sel)
    if ("Autre" in sel) or ("Other" in sel):
        st.text_input(t(lang, "Préciser (Autre)", "Specify (Other)"),
                      key="q9_other_input", value=resp_get("quality_other", ""))
        resp_set("quality_other", st.session_state.get("q9_other_input", "").strip())
    else:
        resp_set("quality_other", "")


def rubric_10(lang: str) -> None:
    st.subheader(t(lang, "Rubrique 10 : Diffusion", "Section 10: Dissemination"))
    st.markdown(
        t(
            lang,
            "Indiquez les canaux de diffusion jugés les plus utiles pour les statistiques prioritaires.",
            "Indicate the dissemination channels you find most useful for priority statistics."
        )
    )
    opts_fr = [
        "Portail web / tableaux de bord",
        "Communiqués / notes de conjoncture",
        "Microdonnées anonymisées (accès sécurisé)",
        "API / Open data",
        "Ateliers et webinaires",
        "Autre",
    ]
    opts_en = [
        "Web portal / dashboards",
        "Press releases / bulletins",
        "Anonymized microdata (secure access)",
        "API / Open data",
        "Workshops and webinars",
        "Other",
    ]
    opts = opts_fr if lang == "fr" else opts_en
    default = resp_get("dissemination_channels", [])
    sel = st.multiselect(t(lang, "Sélectionnez", "Select"), options=opts, default=default)
    resp_set("dissemination_channels", sel)
    if ("Autre" in sel) or ("Other" in sel):
        st.text_input(t(lang, "Préciser (Autre)", "Specify (Other)"),
                      key="q10_other_input", value=resp_get("dissemination_other", ""))
        resp_set("dissemination_other", st.session_state.get("q10_other_input", "").strip())
    else:
        resp_set("dissemination_other", "")


def rubric_12(lang: str) -> None:
    st.subheader(t(lang, "Rubrique 12 : Questions ouvertes", "Section 12: Open questions"))
    st.markdown(
        t(
            lang,
            "Ces questions sont **optionnelles**, mais une alerte apparaîtra si vous laissez le champ vide.",
            "These questions are **optional**, but you will see a warning if left empty."
        )
    )

    q1 = st.text_area(
        t(lang, "1) Commentaires / recommandations clés", "1) Key comments / recommendations"),
        value=resp_get("open_q1", ""),
        height=120,
        key="open_q1_input"
    )
    resp_set("open_q1", q1.strip())

    q2 = st.text_area(
        t(lang, "2) Besoins de soutien (technique, financier, etc.)", "2) Support needs (technical, financial, etc.)"),
        value=resp_get("open_q2", ""),
        height=120,
        key="open_q2_input"
    )
    resp_set("open_q2", q2.strip())

    if not resp_get("open_q1", ""):
        st.warning(t(lang, "Alerte : la question 1 est vide (vous pouvez tout de même continuer).",
                     "Warning: question 1 is empty (you can still proceed)."))
    if not resp_get("open_q2", ""):
        st.warning(t(lang, "Alerte : la question 2 est vide (vous pouvez tout de même continuer).",
                     "Warning: question 2 is empty (you can still proceed)."))


def rubric_send(lang: str, df_long: pd.DataFrame) -> None:
    st.subheader(t(lang, "ENVOYER le questionnaire", "SUBMIT questionnaire"))

    errors = validate_all(lang)
    if errors:
        st.error(t(lang, "Le questionnaire contient des erreurs bloquantes :", "There are blocking errors:"))
        st.write("\n".join([f"- {e}" for e in errors]))
        st.info(t(lang, "Retournez aux rubriques concernées via la navigation.", "Go back to the relevant sections using navigation."))
        return

    # Optional warnings
    if not resp_get("open_q1", "") or not resp_get("open_q2", ""):
        st.warning(t(lang, "Certaines questions ouvertes sont vides (optionnel).", "Some open questions are empty (optional)."))

    st.markdown(t(lang, "### Résumé", "### Summary"))
    st.write({
        t(lang, "Organisation", "Organization"): resp_get("organisation", ""),
        t(lang, "Pays", "Country"): resp_get("pays", ""),
        t(lang, "Type d’acteur", "Stakeholder type"): resp_get("type_acteur", ""),
        t(lang, "Fonction", "Role"): resp_get("fonction", ""),
        t(lang, "Email", "Email"): resp_get("email", ""),
        t(lang, "TOP 5 domaines", "TOP 5 domains"): [domains_from_longlist(df_long, lang) and dict(domains_from_longlist(df_long, lang)).get(c, c) for c in resp_get("top5_domains", [])],
        t(lang, "Nb statistiques", "No. of indicators"): len(resp_get("selected_stats", [])),
    })

    if st.button(t(lang, "✅ ENVOYER et enregistrer", "✅ SUBMIT and save")):
        submission_id = str(uuid.uuid4())
        payload = st.session_state.responses.copy()
        payload["submission_id"] = submission_id
        payload["submitted_at_utc"] = now_utc_iso()

        # Save locally (SQLite)
        db_save_submission(submission_id, lang, payload)

        # Optional storage integrations
        gs_ok, gs_msg = google_sheets_append(payload)
        dbx_ok, dbx_msg = dropbox_upload_json(submission_id, payload)

        st.success(t(lang, "Merci ! Votre questionnaire a été enregistré.", "Thank you! Your submission has been saved."))
        st.caption(f"ID : {submission_id}")

        # Provide downloads
        st.download_button(
            t(lang, "Télécharger une copie (JSON)", "Download a copy (JSON)"),
            data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
            file_name=f"submission_{submission_id}.json",
            mime="application/json",
        )

        # Inform integrations
        with st.expander(t(lang, "Détails du stockage", "Storage details"), expanded=False):
            st.write(t(lang, "SQLite : OK (responses.db)", "SQLite: OK (responses.db)"))
            st.write(f"Google Sheets : {'OK' if gs_ok else 'NON'} — {gs_msg}")
            st.write(f"Dropbox : {'OK' if dbx_ok else 'NON'} — {dbx_msg}")

        # Reset session for new response (optional)
        st.session_state.submission_id = submission_id


# =========================
# Admin dashboard
# =========================

def admin_login(lang: str) -> None:
    st.subheader(t(lang, "Administration", "Administration"))
    pw = st.text_input(t(lang, "Mot de passe admin", "Admin password"), type="password")
    if st.button(t(lang, "Se connecter", "Login")):
        expected = st.secrets.get("ADMIN_PASSWORD", None)
        if expected and pw == expected:
            st.session_state.admin_authed = True
            st.success(t(lang, "Connexion réussie.", "Logged in."))
            st.rerun()
        else:
            st.error(t(lang, "Mot de passe incorrect ou secret ADMIN_PASSWORD manquant.", "Incorrect password or missing ADMIN_PASSWORD secret."))


def admin_dashboard(lang: str) -> None:
    st.subheader(t(lang, "Tableau de bord admin", "Admin dashboard"))

    df = db_read_submissions(limit=5000)
    st.metric(t(lang, "Nombre de réponses", "Number of responses"), len(df))

    if df.empty:
        st.info(t(lang, "Aucune réponse pour le moment.", "No responses yet."))
        return

    # Parse payload
    payloads = []
    for _, r in df.iterrows():
        try:
            payloads.append(json.loads(r["payload_json"]))
        except Exception:
            payloads.append({})
    flat = pd.DataFrame([flatten_payload(p) for p in payloads])
    flat.insert(0, "submission_id", df["submission_id"].values)
    flat.insert(1, "submitted_at_utc", df["submitted_at_utc"].values)

    st.dataframe(flat, use_container_width=True)

    # Export Excel
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        flat.to_excel(writer, sheet_name="submissions", index=False)
        df.to_excel(writer, sheet_name="raw_json", index=False)
    out.seek(0)

    st.download_button(
        t(lang, "Exporter en Excel", "Export to Excel"),
        data=out.getvalue(),
        file_name="consultation_stat_niang_export.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Download DB file
    if os.path.exists(DB_PATH):
        with open(DB_PATH, "rb") as f:
            st.download_button(
                t(lang, "Télécharger responses.db", "Download responses.db"),
                data=f.read(),
                file_name="responses.db",
                mime="application/octet-stream",
            )

    # Push last export to Dropbox (optional)
    if dropbox is not None and st.secrets.get("DROPBOX_ACCESS_TOKEN", None):
        if st.button(t(lang, "Uploader l’export Excel sur Dropbox", "Upload Excel export to Dropbox")):
            try:
                token = st.secrets["DROPBOX_ACCESS_TOKEN"]
                folder = st.secrets.get("DROPBOX_FOLDER", "/consultation_stat_niang")
                folder = folder if folder.startswith("/") else "/" + folder
                path = f"{folder}/exports/export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
                dbx = dropbox.Dropbox(token)
                dbx.files_upload(out.getvalue(), path, mode=dropbox.files.WriteMode.overwrite)
                st.success(t(lang, "Export envoyé sur Dropbox.", "Export uploaded to Dropbox."))
            except Exception as e:
                st.error(f"Dropbox : {e}")


    # Push DB to Dropbox (optional)
    if dropbox is not None and st.secrets.get("DROPBOX_ACCESS_TOKEN", None) and os.path.exists(DB_PATH):
        if st.button(t(lang, "Uploader responses.db sur Dropbox", "Upload responses.db to Dropbox")):
            try:
                token = st.secrets["DROPBOX_ACCESS_TOKEN"]
                folder = st.secrets.get("DROPBOX_FOLDER", "/consultation_stat_niang")
                folder = folder if folder.startswith("/") else "/" + folder
                path = f"{folder}/exports/responses_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.db"
                dbx = dropbox.Dropbox(token)
                with open(DB_PATH, "rb") as fdb:
                    dbx.files_upload(fdb.read(), path, mode=dropbox.files.WriteMode.overwrite)
                st.success(t(lang, "Base envoyée sur Dropbox.", "Database uploaded to Dropbox."))
            except Exception as e:
                st.error(f"Dropbox : {e}")

    st.info(
        t(
            lang,
            "Astuce : utilisez ?admin=1 dans l’URL pour afficher l’espace admin.",
            "Tip: use ?admin=1 in the URL to access the admin space."
        )
    )


# =========================
# Main
# =========================

def main() -> None:
    st.set_page_config(page_title=APP_TITLE_FR, layout="wide")
    init_session()

    # Language toggle
    st.sidebar.title("🌐")
    lang = st.sidebar.selectbox(
        "Langue / Language",
        options=["fr", "en"],
        index=0 if st.session_state.lang == "fr" else 1
    )
    st.session_state.lang = lang

    # Admin access
    qp = get_query_params()
    is_admin = "admin" in qp and qp["admin"] and qp["admin"][0] in ["1", "true", "yes"]

    df_long = load_longlist()

    # Header
    st.title(t(lang, APP_TITLE_FR, APP_TITLE_EN))
    st.caption(t(lang, "Application unifiée (FR/EN) – codes masqués – contrôles qualité intégrés.",
                 "Unified app (FR/EN) – hidden codes – built-in quality controls."))

    # Sidebar navigation
    steps = get_steps(lang)
    render_sidebar(lang, steps)

    # Admin view
    if is_admin:
        if not st.session_state.admin_authed:
            admin_login(lang)
            return
        admin_dashboard(lang)
        return

    # Normal flow
    step_key = steps[st.session_state.nav_idx][0]

    if step_key == "R1":
        rubric_1(lang)
    elif step_key == "R2":
        rubric_2(lang)
    elif step_key == "R3":
        rubric_3(lang)
    elif step_key == "R4":
        rubric_4(lang, df_long)
    elif step_key == "R5":
        rubric_5(lang, df_long)
    elif step_key == "R6":
        rubric_6(lang)
    elif step_key == "R8":
        rubric_8(lang)
    elif step_key == "R9":
        rubric_9(lang)
    elif step_key == "R10":
        rubric_10(lang)
    elif step_key == "R12":
        rubric_12(lang)
    elif step_key == "SEND":
        rubric_send(lang, df_long)

    st.divider()
    nav_buttons(lang, steps, df_long)


if __name__ == "__main__":
    main()
