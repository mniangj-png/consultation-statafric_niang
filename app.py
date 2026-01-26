
import json
import os
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import streamlit as st
import io
import matplotlib.pyplot as plt
from docx import Document
from docx.shared import Inches

# =========================================================
# Configuration générale
# =========================================================
APP_TITLE_FR = "Questionnaire STATAFRIC – Statistiques socio-économiques prioritaires"
APP_TITLE_EN = "STATAFRIC questionnaire – Socio-economic priority statistics"

# --- Admin (hidden route)
ADMIN_QUERY_PARAM = "admin"


DATA_DIR = Path(__file__).parent / "data"
DB_PATH = Path(__file__).parent / "responses.db"

# Barème de notation : High=3, Med=2, Low=1, UK=0 (unknown) 
SCORE_MAP_STD = {"High": 3, "Med": 2, "Medium": 2, "Low": 1, "UK": 0}
# Coût/charge : Low/Med/High/UK (inverse : Low = meilleur) 
SCORE_MAP_COST = {"Low": 3, "Med": 2, "Medium": 2, "High": 1, "UK": 0}

# Réponses standardisées (codes internes stables)
YN_UK = ["Yes", "No", "UK"]
HML_UK = ["High", "Med", "Low", "UK"]
INCLUDE_OPT_UK = ["Include", "Optional", "UK"]

# Liste des pays (déroulante) 
COUNTRIES_FR = [
    "Algérie","Angola","Bénin","Botswana","Burkina Faso","Burundi","Cameroun","Cabo Verde","République centrafricaine","Tchad",
    "Comores","Congo","Côte d'Ivoire","République démocratique du Congo","Djibouti","Egypte","Guinée équatoriale","Érythrée","Ethiopie",
    "Gabon","Gambie","Ghana","Guinée","Guinée-Bissau","Kenya","Lesotho","Libéria","Libye","Madagascar","Malawi","Mali","Mauritanie",
    "Île Maurice","Maroc","Mozambique","Namibie","Niger","Nigeria","Rwanda","Sao Tomé-et-Principe","Sénégal","Seychelles","Sierra Leone",
    "Somalie","Soudan du Sud","Afrique du Sud","Soudan","Eswatini","République-Unie de Tanzanie","Togo","Tunisie","Ouganda","Zambie","Zimbabwe"
]

# Domaines (codes + labels) 
DOMAIN_CODE_TO_LABEL_FR = {
    "D01": "Croissance Économique, Transformation Structurelle et Commerce",
    "D02": "Emploi, Travail Décent et Protection Sociale",
    "D03": "Agriculture Durable, Sécurité Alimentaire et Nutrition",
    "D04": "Infrastructures, Industrialisation et Innovation",
    "D05": "Inclusion, Pauvreté et Inégalités",
    "D06": "Éducation, Compétences et Capital Humain",
    "D07": "Santé, Bien-être et Accès Universel",
    "D08": "Égalité des Genres et Autonomisation",
    "D09": "Environnement, Résilience Climatique et Villes Durables",
    "D10": "Gouvernance, Paix et Institutions",
    "D11": "Économie Bleue et Gestion des Océans",
    "D12": "Partenariats et Financement du Développement",
}

DOMAIN_CODE_TO_LABEL_EN = {
    "D01": "Economic growth, structural transformation and trade",
    "D02": "Employment, decent work and social protection",
    "D03": "Sustainable agriculture, food security and nutrition",
    "D04": "Infrastructure, industrialization and innovation",
    "D05": "Inclusion, poverty and inequalities",
    "D06": "Education, skills and human capital",
    "D07": "Health, well-being and universal access",
    "D08": "Gender equality and empowerment",
    "D09": "Environment, climate resilience and sustainable cities",
    "D10": "Governance, peace and institutions",
    "D11": "Blue economy and ocean management",
    "D12": "Partnerships and development financing",
}

REGIONS_AU = [
    ("North", {"fr": "Afrique du Nord", "en": "North"}),
    ("West", {"fr": "Ouest", "en": "West"}),
    ("Central", {"fr": "Centre", "en": "Central"}),
    ("East", {"fr": "Est", "en": "East"}),
    ("Southern", {"fr": "Sud", "en": "Southern"}),
    ("Diaspora", {"fr": "Diaspora", "en": "Diaspora"}),
    ("AUC", {"fr": "CUA", "en": "AUC"}),
    ("Other", {"fr": "Autre", "en": "Other"}),
]

STAKEHOLDER_TYPES = [
    ("AUC_STATAFRIC", {"fr": "CUA / STATAFRIC", "en": "AUC / STATAFRIC"}),
    ("REC", {"fr": "CER", "en": "REC"}),
    ("NSO", {"fr": "INS", "en": "NSO"}),
    ("Ministry", {"fr": "Ministère", "en": "Ministry"}),
    ("DP", {"fr": "PTF", "en": "DP"}),
    ("CSO", {"fr": "OSC", "en": "CSO"}),
    ("Academia", {"fr": "Université / Recherche", "en": "Academia"}),
    ("Other", {"fr": "Autre", "en": "Other"}),
]

RESPONSE_LANG_OPTIONS = ["Français", "English", "العربية", "Português", "Kiswahili", "Español"]  # 

SCOPE_OPTIONS = [
    ("institutional", {"fr": "Position institutionnelle", "en": "Institutional position"}),
    ("expert", {"fr": "Opinion d’expert", "en": "Expert opinion"}),
    ("synthesis", {"fr": "Synthèse de plusieurs services", "en": "Synthesis across services"}),
]

SNDS_STATUS = [
    ("yes", {"fr": "Oui", "en": "Yes"}),
    ("no", {"fr": "Non", "en": "No"}),
    ("ongoing", {"fr": "En cours", "en": "Ongoing"}),
    ("unknown", {"fr": "Ne sait pas", "en": "Unknown"}),
]

DATA_SOURCES = [
    ("hh_survey", {"fr": "Enquêtes ménages", "en": "Household surveys"}),
    ("biz_survey", {"fr": "Enquêtes entreprises", "en": "Business surveys"}),
    ("census", {"fr": "Recensements", "en": "Censuses"}),
    ("admin", {"fr": "Données administratives", "en": "Administrative data"}),
    ("crvs", {"fr": "Registres CRVS (si pertinent)", "en": "CRVS registers (if relevant)"}),
    ("geospatial", {"fr": "Données géospatiales", "en": "Geospatial data"}),
    ("private", {"fr": "Données privées / big data (à encadrer)", "en": "Private data & big data (with safeguards)"}),
]

PROD_LEVEL = [
    ("national", {"fr": "National", "en": "National"}),
    ("rec", {"fr": "CER", "en": "REC"}),
    ("continental", {"fr": "Continental", "en": "Continental"}),
    ("mixed", {"fr": "Combinaison", "en": "Mixed"}),
]

QUALITY_APPROACH = [
    ("standards", {"fr": "Normes et classifications communes", "en": "Common standards and classifications"}),
    ("metadata", {"fr": "Métadonnées obligatoires et gabarit unique", "en": "Mandatory metadata and common template"}),
    ("qaf", {"fr": "Cadre d’assurance qualité continental", "en": "Continental quality assurance framework"}),
    ("audits", {"fr": "Audits qualité ciblés", "en": "Targeted quality audits"}),
    ("calendar", {"fr": "Calendrier de diffusion et politique de révision", "en": "Release calendar and revision policy"}),
    ("cop", {"fr": "Formation et communautés de pratique", "en": "Training and communities of practice"}),
]

DISSEMINATION_PRODUCTS = [
    ("dashboard", {"fr": "Tableau de bord continental", "en": "Continental dashboard"}),
    ("portal_api", {"fr": "Portail de données et API", "en": "Data portal and API"}),
    ("briefs", {"fr": "Notes analytiques thématiques", "en": "Thematic briefs"}),
    ("release_cal", {"fr": "Calendrier de diffusion", "en": "Release calendar"}),
    ("microdata", {"fr": "Microdonnées (accès contrôlé)", "en": "Microdata (controlled access)"}),
]

VALIDATION_MECHANISMS = [
    ("written_consult", {"fr": "Consultation écrite de tous les pays (questionnaire)", "en": "Written consultation of all countries (questionnaire)"}),
    ("two_rounds", {"fr": "Itération 2 tours : liste provisoire puis approbation", "en": "Two-round process: provisional list then clearance"}),
    ("publish_scores", {"fr": "Publication des scores et du consensus (transparence)", "en": "Publish scores and consensus (transparency)"}),
    ("coverage_quota", {"fr": "Quota de couverture : au moins X indicateurs par domaine", "en": "Coverage rule: at least X indicators per domain"}),
    ("focal_endorse", {"fr": "Approbation par point focal national (écrit)", "en": "Written endorsement by national focal point"}),
]

AGREEMENT_LEVEL = [
    ("majority", {"fr": "Simple majorité", "en": "Simple majority"}),
    ("broad_consensus", {"fr": "Consensus large", "en": "Broad consensus"}),
    ("other", {"fr": "Autre", "en": "Other"}),
]

# Rubrique 5 : critères (ordre et libellés) 
R5_CRITERIA = [
    ("policy_demand", {"fr": "Demande politique", "en": "Policy demand"}, "std"),
    ("harmonization", {"fr": "Harmonisation", "en": "Harmonization"}, "std"),
    ("availability", {"fr": "Disponibilité actuelle", "en": "Current availability"}, "std"),
    ("feasibility", {"fr": "Faisabilité (12–24 mois)", "en": "Feasibility (12–24 months)"}, "std"),
    ("cost_burden", {"fr": "Coût / charge", "en": "Cost / burden"}, "cost"),
    ("quick_results", {"fr": "Résultats rapides (12–24 mois)", "en": "Quick results (12–24 months)"}, "std"),
    ("gender_impact", {"fr": "Impact genre", "en": "Gender impact"}, "std"),
]

# Rubrique 6 (tableau) : exigences genre  (complété par le contenu du questionnaire)
GENDER_REQ_YESNO = [
    ("sex_disagg", {"fr": "Désagrégation par sexe", "en": "Sex disaggregation"}),
    ("age_groups", {"fr": "Désagrégation par âge", "en": "Age groups"}),
    ("urban_rural", {"fr": "Milieu urbain / rural", "en": "Urban / rural"}),
    ("disability", {"fr": "Handicap", "en": "Disability"}),
    ("wealth_quintile", {"fr": "Quintile de richesse", "en": "Wealth quintile"}),
]
GENDER_REQ_INCLUDE = [
    ("gbv", {"fr": "Indicateurs sur VBG", "en": "GBV indicators"}),
    ("unpaid_care", {"fr": "Temps domestique non rémunéré", "en": "Unpaid care work"}),
]
GENDER_MAIN_PRIORITY = [
    ("eco_emp", {"fr": "Autonomisation économique", "en": "Economic empowerment"}),
    ("services", {"fr": "Accès aux services", "en": "Access to services"}),
    ("gbv", {"fr": "VBG", "en": "GBV"}),
    ("participation", {"fr": "Participation", "en": "Participation"}),
    ("unpaid_care", {"fr": "Temps domestique", "en": "Unpaid care"}),
    ("other", {"fr": "Autre", "en": "Other"}),
]

# Rubrique 8 (tableau) : contraintes capacité/faisabilité (12–24 mois) 
R8_CONSTRAINTS = [
    ("skills", {"fr": "Compétences statistiques", "en": "Statistical skills"}),
    ("admin_access", {"fr": "Accès aux données administratives", "en": "Access to admin data"}),
    ("funding", {"fr": "Financement", "en": "Funding"}),
    ("it_tools", {"fr": "Outils numériques", "en": "IT tools"}),
    ("legal", {"fr": "Cadre juridique (partage de données)", "en": "Legal framework (data sharing)"}),
    ("coord", {"fr": "Coordination interinstitutionnelle", "en": "Inter-institutional coordination"}),
]

# =========================================================
# Helpers
# =========================================================
@st.cache_data
def load_longlist() -> pd.DataFrame:
    path = DATA_DIR / "indicator_longlist.csv"
    df = pd.read_csv(path, dtype=str).fillna("")
    # fallback si stat_label_en vide : utiliser le français
    df["stat_label_display_en"] = df["stat_label_en"].where(df["stat_label_en"].str.len() > 0, df["stat_label_fr"])
    return df

def t(lang: str, fr: str, en: str) -> str:
    return fr if lang == "fr" else en

def label_for_option(lang: str, opt: Dict[str, str]) -> str:
    return opt["fr"] if lang == "fr" else opt["en"]

def option_list(lang: str, items: List[Tuple[str, Dict[str, str]]]) -> List[Tuple[str, str]]:
    """Return list of (code, label)"""
    return [(code, label_for_option(lang, meta)) for code, meta in items]


# =========================================================
# Admin helpers
# =========================================================
def _get_query_params() -> Dict[str, List[str]]:
    """Compatible query params getter across Streamlit versions."""
    try:
        qp = st.query_params  # Streamlit >= 1.27
        out: Dict[str, List[str]] = {}
        for k, v in qp.items():
            if isinstance(v, list):
                out[k] = [str(x) for x in v]
            else:
                out[k] = [str(v)]
        return out
    except Exception:
        try:
            return st.experimental_get_query_params()
        except Exception:
            return {}

def _is_admin_route() -> bool:
    qp = _get_query_params()
    v = qp.get(ADMIN_QUERY_PARAM, ["0"])[0].lower()
    return v in ("1", "true", "yes", "y")

def _admin_password() -> str:
    """Admin password from Streamlit secrets or env. Empty => not configured."""
    try:
        if "ADMIN_PASSWORD" in st.secrets:
            return str(st.secrets["ADMIN_PASSWORD"]).strip()
    except Exception:
        pass
    return os.environ.get("ADMIN_PASSWORD", "").strip()

def _load_all_submissions(db_path: str) -> pd.DataFrame:
    if not Path(db_path).exists():
        return pd.DataFrame(columns=["response_id", "submitted_at", "lang_ui", "payload_json"])
    con = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT response_id, submitted_at, lang_ui, payload_json FROM responses", con)
    con.close()
    return df

def _parse_payloads(df: pd.DataFrame) -> List[Dict]:
    payloads = []
    for _, r in df.iterrows():
        try:
            payloads.append(json.loads(r["payload_json"]))
        except Exception:
            continue
    return payloads

def _flatten_for_export(payloads: List[Dict]) -> pd.DataFrame:
    rows = []
    for p in payloads:
        r2 = p.get("r2_respondent", {})
        r3 = p.get("r3_scope", {})
        r4 = p.get("r4_priorities", {})
        r6 = p.get("r6_gender", {})
        r7 = p.get("r7_sources", {})
        r8 = p.get("r8_constraints", {})
        r9 = p.get("r9_quality", {})
        r10 = p.get("r10_governance", {})
        r11 = p.get("r11_final", {})
        top5 = r4.get("top5") or ["", "", "", "", ""]
        rows.append({
            "response_id": p.get("meta", {}).get("response_id"),
            "submitted_at": p.get("meta", {}).get("submitted_at"),
            "lang_ui": p.get("meta", {}).get("lang_ui"),
            "organisation": r2.get("organisation"),
            "country": r2.get("country"),
            "au_region": r2.get("au_region"),
            "stakeholder_type": r2.get("stakeholder_type"),
            "stakeholder_other": r2.get("stakeholder_other"),
            "position": r2.get("position"),
            "email": r2.get("email"),
            "scope": r3.get("scope"),
            "consulted_within_org": r3.get("consulted_within_org"),
            "snds_status": r3.get("snds_status"),
            "top5_domain_1": top5[0] if len(top5) > 0 else "",
            "top5_domain_2": top5[1] if len(top5) > 1 else "",
            "top5_domain_3": top5[2] if len(top5) > 2 else "",
            "top5_domain_4": top5[3] if len(top5) > 3 else "",
            "top5_domain_5": top5[4] if len(top5) > 4 else "",
            "gender_main_priority": r6.get("main_priority"),
            "gender_main_priority_other": r6.get("main_priority_other"),
            "data_sources": ", ".join(r7.get("sources", []) or []) if isinstance(r7.get("sources"), list) else r7.get("sources"),
            "production_level": r7.get("production_level"),
            "quality_calendar": r9.get("calendar"),
            "quality_revision_policy": r9.get("revision_policy"),
            "quality_microdata": r9.get("microdata_access"),
            "governance_method": r10.get("agreement_method"),
            "governance_other": r10.get("agreement_other"),
            "final_comment": r11.get("final_comment"),
            **{f"constraint_{k}": v for k, v in (r8 or {}).items()},
        })
    return pd.DataFrame(rows)

def _r5_rows_df(payloads: List[Dict]) -> pd.DataFrame:
    rows = []
    for p in payloads:
        rid = p.get("meta", {}).get("response_id")
        for row in p.get("r5_indicators_scoring", []) or []:
            r = dict(row)
            r["response_id"] = rid
            rows.append(r)
    return pd.DataFrame(rows)

def _value_counts_any(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Counts values across multiple columns (used for top5 domain codes)."""
    if not cols:
        return pd.DataFrame(columns=["value", "n"])
    s = pd.concat([df[c] for c in cols if c in df.columns], ignore_index=True)
    s = s.dropna()
    s = s[s != ""]
    vc = s.value_counts().reset_index()
    vc.columns = ["value", "n"]
    return vc

def _plot_barh(df_counts: pd.DataFrame, label_col: str, value_col: str, title: str):
    if df_counts.empty:
        st.info("Aucune donnée.")
        return
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.barh(df_counts[label_col].astype(str).iloc[::-1], df_counts[value_col].iloc[::-1])
    ax.set_title(title)
    ax.set_xlabel(value_col)
    st.pyplot(fig, clear_figure=True)

def _make_excel_export(df_flat: pd.DataFrame, df_r5: pd.DataFrame, payloads: List[Dict]) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as xl:
        df_flat.to_excel(xl, index=False, sheet_name="submissions")
        if not df_r5.empty:
            df_r5.to_excel(xl, index=False, sheet_name="r5_scoring")
        # JSONL
        jsonl = "\n".join([json.dumps(p, ensure_ascii=False) for p in payloads])
        pd.DataFrame({"payload_jsonl": [jsonl]}).to_excel(xl, index=False, sheet_name="payloads_jsonl")
    return bio.getvalue()

def _build_word_report_bytes(lang: str, payloads: List[Dict], df_flat: pd.DataFrame, df_r5: pd.DataFrame) -> bytes:
    doc = Document()
    title = "Rapport de synthèse – Consultation statistiques prioritaires (STATAFRIC)" if lang == "fr" else             "Summary report – Priority statistics consultation (STATAFRIC)"
    doc.add_heading(title, 0)
    doc.add_paragraph(("Généré le : " if lang == "fr" else "Generated on: ") + datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))

    n = len(payloads)
    doc.add_heading("1. Vue d’ensemble" if lang == "fr" else "1. Overview", level=1)
    doc.add_paragraph((f"Nombre total de réponses : {n}" if lang == "fr" else f"Total submissions: {n}"))
    if n == 0:
        bio = io.BytesIO()
        doc.save(bio)
        return bio.getvalue()

    # Profil des répondants
    doc.add_heading("2. Profil des répondants" if lang == "fr" else "2. Respondent profile", level=1)
    countries = df_flat.get("country", pd.Series([], dtype=str)).dropna()
    countries = countries[countries != ""]
    if len(countries) > 0:
        doc.add_paragraph((f"Pays couverts : {countries.nunique()}" if lang == "fr" else f"Countries covered: {countries.nunique()}"))

    stypes = df_flat.get("stakeholder_type", pd.Series([], dtype=str)).dropna()
    stypes = stypes[stypes != ""]
    if len(stypes) > 0:
        vc = stypes.value_counts().head(15)
        table = doc.add_table(rows=1, cols=2)
        hdr = table.rows[0].cells
        hdr[0].text = "Type d’acteur" if lang == "fr" else "Stakeholder type"
        hdr[1].text = "N"
        for k, v in vc.items():
            row = table.add_row().cells
            row[0].text = str(k)
            row[1].text = str(int(v))

    # Domaines prioritaires
    doc.add_heading("3. Domaines prioritaires (Top 5)" if lang == "fr" else "3. Priority domains (Top 5)", level=1)
    top_cols = [c for c in df_flat.columns if c.startswith("top5_domain_")]
    if top_cols:
        vc = _value_counts_any(df_flat, top_cols).head(20)
        table = doc.add_table(rows=1, cols=2)
        hdr = table.rows[0].cells
        hdr[0].text = "Code domaine" if lang == "fr" else "Domain code"
        hdr[1].text = "N"
        for _, r in vc.iterrows():
            row = table.add_row().cells
            row[0].text = str(r["value"])
            row[1].text = str(int(r["n"]))

    # Statistiques proposées
    doc.add_heading("4. Statistiques proposées et notation" if lang == "fr" else "4. Proposed statistics and scoring", level=1)
    if not df_r5.empty and "stat_label" in df_r5.columns:
        vc = df_r5["stat_label"].fillna("").replace("", np.nan).dropna().value_counts().head(20)
        doc.add_paragraph("Top statistiques (fréquence)" if lang == "fr" else "Top statistics (frequency)")
        table = doc.add_table(rows=1, cols=2)
        hdr = table.rows[0].cells
        hdr[0].text = "Statistique" if lang == "fr" else "Statistic"
        hdr[1].text = "N"
        for k, v in vc.items():
            row = table.add_row().cells
            row[0].text = str(k)[:160]
            row[1].text = str(int(v))

    # Perspective de genre
    doc.add_heading("5. Perspective de genre" if lang == "fr" else "5. Gender perspective", level=1)
    # requirements yes/no distribution
    g_yesno = {}
    for p in payloads:
        rr = p.get("r6_gender", {}).get("requirements_yesno", {}) or {}
        for k, v in rr.items():
            g_yesno.setdefault(k, []).append(v or "UK")
    if g_yesno:
        table = doc.add_table(rows=1, cols=4)
        hdr = table.rows[0].cells
        hdr[0].text = "Élément" if lang == "fr" else "Item"
        hdr[1].text = "Oui" if lang == "fr" else "Yes"
        hdr[2].text = "Non" if lang == "fr" else "No"
        hdr[3].text = "UK"
        for k, vals in g_yesno.items():
            ser = pd.Series(vals).fillna("UK")
            yes = int((ser == "Yes").sum())
            no = int((ser == "No").sum())
            uk = int((ser == "UK").sum())
            row = table.add_row().cells
            row[0].text = str(k)
            row[1].text = str(yes)
            row[2].text = str(no)
            row[3].text = str(uk)

    # Capacité & faisabilité
    doc.add_heading("6. Capacité et faisabilité (12–24 mois)" if lang == "fr" else "6. Capacity & feasibility (12–24 months)", level=1)
    constraint_cols = [c for c in df_flat.columns if c.startswith("constraint_")]
    if constraint_cols:
        for c in constraint_cols[:12]:
            ser = df_flat[c].fillna("UK")
            vc = ser.value_counts()
            doc.add_paragraph(f"{c} : " + ", ".join([f"{k}={int(v)}" for k, v in vc.items()]))

    bio = io.BytesIO()
    doc.save(bio)
    return bio.getvalue()


def init_state():
    if "step" not in st.session_state:
        st.session_state.step = 1

    # ---- Identité répondant
    st.session_state.setdefault("org", "")
    st.session_state.setdefault("country", "")
    st.session_state.setdefault("region", "West")
    st.session_state.setdefault("stakeholder_type", "NSO")
    st.session_state.setdefault("stakeholder_other", "")
    st.session_state.setdefault("position", "")
    st.session_state.setdefault("email", "")
    st.session_state.setdefault("phone", "")
    st.session_state.setdefault("response_language", "Français")

    # ---- Portée
    st.session_state.setdefault("scope", "institutional")
    st.session_state.setdefault("consulted", "Yes")
    st.session_state.setdefault("snds", "yes")

    # ---- Rubrique 4
    st.session_state.setdefault("r4_preselect", [])
    st.session_state.setdefault("r4_top5", ["", "", "", "", ""])  # codes domaines

    # ---- Rubrique 5 : 5 domaines x 3 stats = 15 slots
    for di in range(5):
        for si in range(3):
            st.session_state.setdefault(f"r5_stat_{di}_{si}", "")
            st.session_state.setdefault(f"r5_stat_custom_{di}_{si}", "")
            for crit_key, _, _ in R5_CRITERIA:
                st.session_state.setdefault(f"r5_{crit_key}_{di}_{si}", "UK")
            st.session_state.setdefault(f"r5_comment_{di}_{si}", "")

    # ---- Rubrique 6
    for key, _ in GENDER_REQ_YESNO:
        st.session_state.setdefault(f"r6_{key}", "UK")
    for key, _ in GENDER_REQ_INCLUDE:
        st.session_state.setdefault(f"r6_{key}", "UK")
    st.session_state.setdefault("r6_main_priority", "eco_emp")
    st.session_state.setdefault("r6_main_priority_other", "")

    # ---- Rubrique 7
    st.session_state.setdefault("r7_sources", [])
    st.session_state.setdefault("r7_prod_level", "national")

    # ---- Rubrique 8
    for key, _ in R8_CONSTRAINTS:
        st.session_state.setdefault(f"r8_{key}", "UK")

    # ---- Rubrique 9,10,11
    st.session_state.setdefault("r9_quality", [])
    st.session_state.setdefault("r10_products", [])
    for key, _ in VALIDATION_MECHANISMS:
        st.session_state.setdefault(f"r11_{key}", "UK")
    st.session_state.setdefault("r11_agreement", "majority")
    st.session_state.setdefault("r11_agreement_other", "")

    # ---- Rubrique 12
    st.session_state.setdefault("r12_rec", "")
    st.session_state.setdefault("r12_missing", "")

def get_top5_domains() -> List[str]:
    top5 = [d for d in st.session_state.r4_top5 if d]
    # si l’utilisateur n’a pas encore rempli, renvoyer []
    return top5 if len(top5) == 5 and len(set(top5)) == 5 else []

def get_stats_options_for_domain(df_long: pd.DataFrame, domain_code: str, lang: str) -> List[str]:
    if lang == "fr":
        stats = df_long[df_long["domain_code"] == domain_code]["stat_label_fr"].tolist()
    else:
        stats = df_long[df_long["domain_code"] == domain_code]["stat_label_display_en"].tolist()
    stats = sorted(list(dict.fromkeys(stats)))
    return stats

def compute_r5_selected(lang: str, df_long: pd.DataFrame) -> List[Dict]:
    """Compile toutes les stats réellement sélectionnées, avec critères."""
    top5 = get_top5_domains()
    if not top5:
        return []

    compiled = []
    for di, dcode in enumerate(top5):
        for si in range(3):
            stat = st.session_state.get(f"r5_stat_{di}_{si}", "").strip()
            custom = st.session_state.get(f"r5_stat_custom_{di}_{si}", "").strip()
            if stat == "__CUSTOM__":
                stat_label = custom
            else:
                stat_label = stat

            if not stat_label:
                continue

            # critères
            row = {
                "domain_code": dcode,
                "domain_label_fr": DOMAIN_CODE_TO_LABEL_FR.get(dcode, dcode),
                "domain_label_en": DOMAIN_CODE_TO_LABEL_EN.get(dcode, dcode),
                "stat_label": stat_label,
                "slot": f"{di+1}.{si+1}"
            }
            for crit_key, _, crit_kind in R5_CRITERIA:
                row[crit_key] = st.session_state.get(f"r5_{crit_key}_{di}_{si}", "UK")
                if crit_kind == "cost":
                    row[f"{crit_key}_score"] = SCORE_MAP_COST.get(row[crit_key], 0)
                else:
                    row[f"{crit_key}_score"] = SCORE_MAP_STD.get(row[crit_key], 0)
            row["comment"] = st.session_state.get(f"r5_comment_{di}_{si}", "").strip()
            row["total_score"] = sum(row[f"{k}_score"] for k,_,_ in R5_CRITERIA)
            compiled.append(row)
    return compiled

# =========================================================
# Validations (contrôles qualité)
# =========================================================
def validate_r4() -> List[str]:
    errors = []
    pre = st.session_state.r4_preselect
    top5 = st.session_state.r4_top5

    # R4_Q1 : 5 à 10 domaines préselectionnés sans doublon 
    if len(pre) < 5:
        errors.append("R4_Q1 : minimum 5 domaines à présélectionner.")
    if len(pre) > 10:
        errors.append("R4_Q1 : maximum 10 domaines à présélectionner.")
    if len(set(pre)) != len(pre):
        errors.append("R4_Q1 : doublons détectés dans la présélection.")

    # Top5 : 5 domaines uniques, inclus dans la présélection et sans doublon 
    top5_filled = [d for d in top5 if d]
    if len(top5_filled) != 5:
        errors.append("R4_Q2 : vous devez renseigner exactement 5 domaines (Top 5).")
    if len(set(top5_filled)) != len(top5_filled):
        errors.append("R4_Q2 : doublons détectés dans le Top 5.")
    if any(d not in pre for d in top5_filled):
        errors.append("R4_Q2 : tous les domaines du Top 5 doivent appartenir à la présélection (R4_Q1).")
    return errors

def validate_r5(lang: str, df_long: pd.DataFrame) -> List[str]:
    errors = []
    top5 = get_top5_domains()
    if not top5:
        errors.append("R5 : le Top 5 de la Rubrique 4 doit être finalisé avant la notation.")
        return errors

    selected = compute_r5_selected(lang, df_long)

    # Total stats : min 5, max 15  + 
    if len(selected) < 5:
        errors.append("R5 : vous devez proposer au moins 5 statistiques au total.")
    if len(selected) > 15:
        errors.append("R5 : vous ne pouvez pas dépasser 15 statistiques au total.")

    # Max 3 stats par domaine et min 1 par domaine Top5 
    by_domain = {}
    for row in selected:
        by_domain.setdefault(row["domain_code"], []).append(row)

    for dcode in top5:
        n = len(by_domain.get(dcode, []))
        if n == 0:
            errors.append(f"R5 : au moins 1 statistique est requise pour le domaine {dcode}.")
        if n > 3:
            errors.append(f"R5 : maximum 3 statistiques pour le domaine {dcode}.")

    # Pas de duplication de statistique 
    labels = [r["stat_label"].strip().casefold() for r in selected]
    if len(set(labels)) != len(labels):
        errors.append("R5 : doublons détectés dans les statistiques proposées (Stat_label).")

    # Notation doit couvrir toutes les stats préselectionnées 
    for row in selected:
        for crit_key, _, _ in R5_CRITERIA:
            if row.get(crit_key, "UK") not in ("High","Med","Low","UK","Medium"):
                errors.append("R5 : une ou plusieurs notations sont invalides (valeurs attendues : High/Med/Low/UK).")
                break

    return errors

def validate_r6() -> List[str]:
    errors = []
    for key, meta in GENDER_REQ_YESNO:
        if st.session_state.get(f"r6_{key}", "UK") not in YN_UK:
            errors.append("R6 : valeur invalide dans le tableau (Oui/Non/UK).")
            break
    for key, meta in GENDER_REQ_INCLUDE:
        if st.session_state.get(f"r6_{key}", "UK") not in INCLUDE_OPT_UK:
            errors.append("R6 : valeur invalide dans le tableau (Inclure/Optionnel/UK).")
            break
    if st.session_state.get("r6_main_priority") == "other" and not st.session_state.get("r6_main_priority_other", "").strip():
        errors.append("R6 : précisez votre priorité genre principale (Autre).")
    return errors

def validate_r8() -> List[str]:
    errors = []
    for key, meta in R8_CONSTRAINTS:
        if st.session_state.get(f"r8_{key}", "UK") not in HML_UK:
            errors.append("R8 : toutes les contraintes doivent être notées (High/Med/Low/UK).")
            break
    return errors

def validate_r9() -> List[str]:
    errors = []
    sel = st.session_state.get("r9_quality", [])
    if len(sel) == 0:
        errors.append("R9 : cochez au moins 1 option.")
    if len(sel) > 3:
        errors.append("R9 : maximum 3 options.")
    return errors

def validate_r10() -> List[str]:
    errors = []
    if len(st.session_state.get("r10_products", [])) == 0:
        errors.append("R10 : cochez au moins 1 produit de diffusion.")
    return errors

def validate_r11() -> List[str]:
    errors = []
    for key, meta in VALIDATION_MECHANISMS:
        if st.session_state.get(f"r11_{key}", "UK") not in YN_UK:
            errors.append("R11 : toutes les modalités doivent être renseignées (Oui/Non/UK).")
            break
    if st.session_state.get("r11_agreement") == "other" and not st.session_state.get("r11_agreement_other", "").strip():
        errors.append("R11 : précisez le niveau d’accord (Autre).")
    return errors

def current_step_errors(lang: str, df_long: pd.DataFrame) -> List[str]:
    step = st.session_state.step
    if step == 4:
        return validate_r4()
    if step == 5:
        return validate_r5(lang, df_long)
    if step == 6:
        return validate_r6()
    if step == 8:
        return validate_r8()
    if step == 9:
        return validate_r9()
    if step == 10:
        return validate_r10()
    if step == 11:
        return validate_r11()
    return []

# =========================================================
# Stockage
# =========================================================
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS responses (
            response_id TEXT PRIMARY KEY,
            submitted_at TEXT,
            lang_ui TEXT,
            payload_json TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS r5_scoring (
            response_id TEXT,
            domain_code TEXT,
            stat_label TEXT,
            policy_demand TEXT,
            harmonization TEXT,
            availability TEXT,
            feasibility TEXT,
            cost_burden TEXT,
            quick_results TEXT,
            gender_impact TEXT,
            total_score INTEGER,
            comment TEXT
        )
    """)
    con.commit()
    con.close()

def save_response(payload: Dict, r5_rows: List[Dict]):
    init_db()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    rid = payload["meta"]["response_id"]
    cur.execute(
        "INSERT OR REPLACE INTO responses(response_id, submitted_at, lang_ui, payload_json) VALUES (?, ?, ?, ?)",
        (rid, payload["meta"]["submitted_at"], payload["meta"]["lang_ui"], json.dumps(payload, ensure_ascii=False))
    )
    # vider et réinsérer R5 (simple)
    cur.execute("DELETE FROM r5_scoring WHERE response_id = ?", (rid,))
    for row in r5_rows:
        cur.execute("""
            INSERT INTO r5_scoring(
              response_id, domain_code, stat_label, policy_demand, harmonization, availability, feasibility,
              cost_burden, quick_results, gender_impact, total_score, comment
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rid,
            row["domain_code"],
            row["stat_label"],
            row["policy_demand"],
            row["harmonization"],
            row["availability"],
            row["feasibility"],
            row["cost_burden"],
            row["quick_results"],
            row["gender_impact"],
            int(row["total_score"]),
            row.get("comment","")
        ))
    con.commit()
    con.close()

# =========================================================
# UI – Rubriques
# =========================================================
def ui_header(lang: str):
    st.title(APP_TITLE_FR if lang == "fr" else APP_TITLE_EN)
    st.caption(
        t(
            lang,
            "Temps estimé : 20–25 minutes. UK = Inconnu (0).",
            "Estimated time: 20–25 minutes. UK = Unknown (0)."
        )
    )
    st.divider()

def ui_sidebar(lang: str):
    st.sidebar.header(t(lang, "Navigation", "Navigation"))
    steps = [
        (1, t(lang, "Rubrique 1 : Instructions", "Section 1: Instructions")),
        (2, t(lang, "Rubrique 2 : Identification du répondant", "Section 2: Respondent identification")),
        (3, t(lang, "Rubrique 3 : Portée de la réponse", "Section 3: Scope of response")),
        (4, t(lang, "Rubrique 4 : Domaines prioritaires", "Section 4: Priority domains")),
        (5, t(lang, "Rubrique 5 : Statistiques et notation", "Section 5: Indicators and scoring")),
        (6, t(lang, "Rubrique 6 : Perspective de genre", "Section 6: Gender perspective")),
        (7, t(lang, "Rubrique 7 : Sources et niveau de production", "Section 7: Data sources and production level")),
        (8, t(lang, "Rubrique 8 : Capacité et faisabilité", "Section 8: Capacity and feasibility")),
        (9, t(lang, "Rubrique 9 : Harmonisation et qualité", "Section 9: Harmonization and quality")),
        (10, t(lang, "Rubrique 10 : Diffusion", "Section 10: Dissemination")),
        (11, t(lang, "Rubrique 11 : Validation", "Section 11: Validation")),
        (12, t(lang, "Rubrique 12 : Questions ouvertes", "Section 12: Open questions")),
        (13, t(lang, "Relecture et soumission", "Review and submit")),
    ]
    labels = [s[1] for s in steps]
    current_idx = [s[0] for s in steps].index(st.session_state.step)
    new_idx = st.sidebar.radio(
        t(lang, "Aller à :", "Go to:"),
        options=list(range(len(labels))),
        index=current_idx,
        format_func=lambda i: labels[i],
        key="nav_radio"
    )
    st.session_state.step = steps[new_idx][0]

    st.sidebar.divider()
    st.sidebar.caption(
        t(lang,
          "Note : les contrôles qualité bloquent la progression si une contrainte n’est pas respectée.",
          "Note: quality checks prevent moving forward when constraints are not met.")
    )

def nav_buttons(lang: str, df_long: pd.DataFrame):
    errors = current_step_errors(lang, df_long)
    col1, col2, col3 = st.columns([1,1,3])
    with col1:
        if st.session_state.step > 1 and st.button(t(lang, "⬅ Précédent", "⬅ Previous")):
            st.session_state.step -= 1
            st.rerun()
    with col2:
        # autoriser Next si pas d’erreurs
        next_label = t(lang, "Suivant ➡", "Next ➡")
        disabled = bool(errors) or st.session_state.step >= 13
        if st.button(next_label, disabled=disabled):
            st.session_state.step += 1
            st.rerun()
    with col3:
        if errors:
            st.error("\n".join(errors))

def rubric_1(lang: str):
    st.subheader(t(lang, "Rubrique 1 : Instructions", "Section 1: Instructions"))
    st.markdown(
        t(
            lang,
            "- Objectif : construire une liste de statistiques prioritaires validable au niveau continental.\n"
            "- Étapes : (i) domaines prioritaires, (ii) statistiques par domaine, (iii) notation multicritères, (iv) exigences genre et modalités de production/diffusion.\n"
            "- Barème : High = 3, Med = 2, Low = 1, UK = 0 (inconnu).\n",
            "- Goal: build a continentally validable list of priority statistics.\n"
            "- Steps: (i) priority domains, (ii) indicators per domain, (iii) multi-criteria scoring, (iv) gender requirements and production/dissemination modalities.\n"
            "- Scoring: High = 3, Med = 2, Low = 1, UK = 0 (unknown).\n"
        )
    )
    st.info(t(lang, "NB : UK = Inconnu (0).", "Note: UK = Unknown (0)."))

def rubric_2(lang: str):
    st.subheader(t(lang, "Rubrique 2 : Identification du répondant", "Section 2: Respondent identification"))
    c1, c2 = st.columns(2)

    with c1:
        st.text_input(t(lang, "Organisation", "Organization"), key="org")
        st.selectbox(t(lang, "Pays", "Country"), options=[""] + COUNTRIES_FR, key="country")
        region_opts = option_list(lang, REGIONS_AU)
        st.selectbox(
            t(lang, "Région UA", "AU region"),
            options=[c for c,_ in region_opts],
            format_func=lambda x: dict(region_opts).get(x, x),
            key="region"
        )
    with c2:
        st.selectbox(
            t(lang, "Type d’acteur", "Stakeholder type"),
            options=[c for c,_ in option_list(lang, STAKEHOLDER_TYPES)],
            format_func=lambda x: dict(option_list(lang, STAKEHOLDER_TYPES)).get(x, x),
            key="stakeholder_type"
        )
        if st.session_state.stakeholder_type == "Other":
            st.text_input(t(lang, "Précisez (autre)", "Specify (other)"), key="stakeholder_other")
        st.text_input(t(lang, "Fonction", "Position"), key="position")
        st.text_input("Email", key="email")
        st.text_input(t(lang, "Téléphone", "Phone"), key="phone")
        st.selectbox(t(lang, "Langue de réponse", "Response language"), options=RESPONSE_LANG_OPTIONS, key="response_language")

def rubric_3(lang: str):
    st.subheader(t(lang, "Rubrique 3 : Portée de votre réponse", "Section 3: Scope of your response"))
    scope_opts = option_list(lang, SCOPE_OPTIONS)
    st.radio(
        t(lang, "Votre réponse reflète principalement :", "Your response mainly reflects:"),
        options=[c for c,_ in scope_opts],
        format_func=lambda x: dict(scope_opts)[x],
        key="scope",
        horizontal=False
    )
    st.radio(
        t(lang, "Avez-vous consulté d’autres collègues avant de répondre ?", "Did you consult colleagues before answering?"),
        options=YN_UK[:2],
        format_func=lambda x: t(lang, "Oui" if x=="Yes" else "Non", "Yes" if x=="Yes" else "No"),
        key="consulted",
        horizontal=True
    )
    snds_opts = option_list(lang, SNDS_STATUS)
    st.radio(
        t(lang, "Votre pays dispose-t-il d’une SNDS/plan statistique actif ?", "Does your country have an active NSDS/statistical plan?"),
        options=[c for c,_ in snds_opts],
        format_func=lambda x: dict(snds_opts)[x],
        key="snds",
        horizontal=False
    )

def rubric_4(lang: str):
    st.subheader(t(lang, "Rubrique 4 : Sélection des domaines prioritaires", "Section 4: Priority domains selection"))

    st.markdown(
        t(
            lang,
            "**R4_Q1** – Présélectionnez jusqu’à 10 domaines (minimum 5) *sans doublon*.",
            "**Q4_Q1** – Preselect up to 10 domains (minimum 5) *without duplicates*."
        )
    )

    domains = list(DOMAIN_CODE_TO_LABEL_FR.keys())
    domain_labels = {d: f"{d} | {DOMAIN_CODE_TO_LABEL_FR[d]}" if lang=="fr" else f"{d} | {DOMAIN_CODE_TO_LABEL_EN[d]}" for d in domains}

    st.multiselect(
        t(lang, "Domaines (max 10)", "Domains (max 10)"),
        options=domains,
        format_func=lambda x: domain_labels.get(x, x),
        key="r4_preselect",
        max_selections=10
    )

    st.divider()

    st.markdown(
        t(
            lang,
            "**R4_Q2** – Parmi les domaines présélectionnés, choisissez **5** domaines par ordre de priorité (Top 5).",
            "**Q4_Q2** – From preselected domains, choose **5** domains by order of priority (Top 5)."
        )
    )

    pre = st.session_state.r4_preselect
    if len(pre) == 0:
        st.warning(t(lang, "Veuillez d’abord présélectionner des domaines.", "Please preselect domains first."))
        return

    # Top 5 (sélecteurs en cascade pour empêcher les doublons)
    chosen = []
    for i in range(5):
        available = [d for d in pre if d not in chosen]
        current = st.session_state.r4_top5[i] if st.session_state.r4_top5[i] in available else ""
        opts = [""] + available
        val = st.selectbox(
            t(lang, f"Rang {i+1}", f"Rank {i+1}"),
            options=opts,
            index=opts.index(current) if current in opts else 0,
            format_func=lambda x: domain_labels.get(x, x) if x else t(lang, "(sélectionner)", "(select)"),
            key=f"r4_top5_{i}"
        )
        st.session_state.r4_top5[i] = val
        if val:
            chosen.append(val)

def rubric_5(lang: str, df_long: pd.DataFrame):
    st.subheader(t(lang, "Rubrique 5 : Notation multicritères des statistiques proposées", "Section 5: Multi-criteria scoring of proposed indicators"))

    st.info(
        t(
            lang,
            "Rappel : 3 statistiques maximum par domaine du Top 5 ; au moins 1 par domaine ; total entre 5 et 15 ; pas de doublons ; toutes les stats doivent être notées.",
            "Reminder: max 3 indicators per Top 5 domain; at least 1 per domain; total 5–15; no duplicates; all indicators must be scored."
        )
    )  # 

    top5 = get_top5_domains()
    if not top5:
        st.warning(t(lang, "Finalisez le Top 5 (Rubrique 4) avant de poursuivre.", "Please finalize Top 5 (Section 4) before proceeding."))
        return

    domain_title = lambda d: f"{d} | {DOMAIN_CODE_TO_LABEL_FR[d]}" if lang=="fr" else f"{d} | {DOMAIN_CODE_TO_LABEL_EN[d]}"

    for di, dcode in enumerate(top5):
        st.markdown(f"### {domain_title(dcode)}")

        # stats possibles filtrées par domaine
        stats = get_stats_options_for_domain(df_long, dcode, lang)
        stats_opts = [""] + stats + ["__CUSTOM__"]

        for si in range(3):
            cols = st.columns([2, 1, 1, 1, 1, 1, 1, 1])
            with cols[0]:
                stat_key = f"r5_stat_{di}_{si}"
                current = st.session_state.get(stat_key, "")
                if current not in stats_opts:
                    current = ""
                st.selectbox(
                    t(lang, f"Statistique #{si+1}", f"Indicator #{si+1}"),
                    options=stats_opts,
                    index=stats_opts.index(current) if current in stats_opts else 0,
                    format_func=lambda x: t(lang, "(vide)", "(empty)") if x=="" else (t(lang, "Autre (préciser)", "Other (specify)") if x=="__CUSTOM__" else x),
                    key=stat_key
                )
                if st.session_state[stat_key] == "__CUSTOM__":
                    st.text_input(
                        t(lang, "Libellé de la statistique", "Indicator label"),
                        key=f"r5_stat_custom_{di}_{si}",
                        placeholder=t(lang, "Ex : Taux de pauvreté monétaire (nouvelle mesure)", "e.g., Monetary poverty rate (new measure)")
                    )

            # Critères (1 colonne chacun)
            for ci, (crit_key, crit_meta, crit_kind) in enumerate(R5_CRITERIA, start=1):
                with cols[ci]:
                    st.selectbox(
                        crit_meta["fr"] if lang=="fr" else crit_meta["en"],
                        options=HML_UK if crit_kind!="cost" else ["Low","Med","High","UK"],
                        key=f"r5_{crit_key}_{di}_{si}"
                    )

            # commentaire sur une ligne séparée pour lisibilité
            if st.session_state.get(stat_key, ""):
                st.text_input(
                    t(lang, "Commentaire bref (optionnel)", "Brief comment (optional)"),
                    key=f"r5_comment_{di}_{si}",
                    placeholder=t(lang, "Justification courte (1–2 phrases).", "Short justification (1–2 sentences).")
                )
        st.divider()

    # Résumé
    selected = compute_r5_selected(lang, df_long)
    if selected:
        st.markdown("#### " + t(lang, "Résumé des statistiques sélectionnées", "Summary of selected indicators"))
        df = pd.DataFrame(selected)[["domain_code","stat_label","total_score"]]
        st.dataframe(df, use_container_width=True)

def rubric_6(lang: str):
    st.subheader(t(lang, "Rubrique 6 : Perspective de genre (exigences minimales)", "Section 6: Gender perspective (minimum requirements)"))
    st.caption(t(lang, "UK = Inconnu.", "UK = Unknown."))

    st.markdown(t(lang, "**Tableau 1 : Désagrégations minimales**", "**Table 1: Minimum disaggregations**"))
    c1, c2 = st.columns([2,2])
    with c1:
        for key, meta in GENDER_REQ_YESNO:
            st.selectbox(
                meta["fr"] if lang=="fr" else meta["en"],
                options=YN_UK,
                format_func=lambda x: ({"Yes": t(lang,"Oui","Yes"), "No": t(lang,"Non","No"), "UK": "UK"})[x],
                key=f"r6_{key}"
            )
    with c2:
        st.markdown(t(lang, "**Tableau 2 : Thématiques à inclure**", "**Table 2: Topics to include**"))
        for key, meta in GENDER_REQ_INCLUDE:
            st.selectbox(
                meta["fr"] if lang=="fr" else meta["en"],
                options=INCLUDE_OPT_UK,
                format_func=lambda x: ({"Include": t(lang,"À inclure","Include"), "Optional": t(lang,"Optionnel","Optional"), "UK": "UK"})[x],
                key=f"r6_{key}"
            )

    st.divider()
    st.markdown(t(lang, "**Votre priorité genre principale**", "**Your main gender priority**"))
    mp_opts = option_list(lang, GENDER_MAIN_PRIORITY)
    st.radio(
        t(lang, "Choix", "Choice"),
        options=[c for c,_ in mp_opts],
        format_func=lambda x: dict(mp_opts)[x],
        key="r6_main_priority",
        horizontal=False
    )
    if st.session_state.r6_main_priority == "other":
        st.text_input(t(lang, "Précisez", "Please specify"), key="r6_main_priority_other")

def rubric_7(lang: str):
    st.subheader(t(lang, "Rubrique 7 : Sources de données et niveau de production", "Section 7: Data sources and production level"))
    src_opts = option_list(lang, DATA_SOURCES)
    st.multiselect(
        t(lang, "Sources de données pertinentes", "Relevant data sources"),
        options=[c for c,_ in src_opts],
        format_func=lambda x: dict(src_opts)[x],
        key="r7_sources",
    )
    prod_opts = option_list(lang, PROD_LEVEL)
    st.radio(
        t(lang, "Niveau de production souhaité", "Preferred production level"),
        options=[c for c,_ in prod_opts],
        format_func=lambda x: dict(prod_opts)[x],
        key="r7_prod_level",
        horizontal=False
    )

def rubric_8(lang: str):
    st.subheader(t(lang, "Rubrique 8 : Capacité et faisabilité (12–24 mois)", "Section 8: Capacity and feasibility (12–24 months)"))
    st.caption(t(lang, "UK = Inconnu.", "UK = Unknown."))

    df = pd.DataFrame([{
        t(lang,"Contrainte","Constraint"): (meta["fr"] if lang=="fr" else meta["en"]),
        t(lang,"Niveau","Level"): st.session_state.get(f"r8_{key}", "UK")
    } for key, meta in R8_CONSTRAINTS])

    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            t(lang,"Niveau","Level"): st.column_config.SelectboxColumn(options=HML_UK)
        },
        key="r8_editor"
    )

    # réinjecter dans le state
    for i, (key, meta) in enumerate(R8_CONSTRAINTS):
        st.session_state[f"r8_{key}"] = edited.iloc[i][t(lang,"Niveau","Level")]

def rubric_9(lang: str):
    st.subheader(t(lang, "Rubrique 9 : Harmonisation, qualité et métadonnées", "Section 9: Harmonization, quality and metadata"))
    qa_opts = option_list(lang, QUALITY_APPROACH)
    st.multiselect(
        t(lang, "Cochez jusqu’à 3 options", "Tick up to 3 options"),
        options=[c for c,_ in qa_opts],
        format_func=lambda x: dict(qa_opts)[x],
        key="r9_quality",
        max_selections=3
    )

def rubric_10(lang: str):
    st.subheader(t(lang, "Rubrique 10 : Diffusion et usage", "Section 10: Dissemination and use"))
    prod_opts = option_list(lang, DISSEMINATION_PRODUCTS)
    st.multiselect(
        t(lang, "Produits de diffusion à accompagner", "Dissemination products to accompany"),
        options=[c for c,_ in prod_opts],
        format_func=lambda x: dict(prod_opts)[x],
        key="r10_products",
    )

def rubric_11(lang: str):
    st.subheader(t(lang, "Rubrique 11 : Validation et appropriation", "Section 11: Validation and ownership"))
    st.caption("UK = Unknown")

    for key, meta in VALIDATION_MECHANISMS:
        st.selectbox(
            meta["fr"] if lang=="fr" else meta["en"],
            options=YN_UK,
            format_func=lambda x: ({"Yes": t(lang,"Oui","Yes"), "No": t(lang,"Non","No"), "UK": "UK"})[x],
            key=f"r11_{key}"
        )

    st.divider()
    agree_opts = option_list(lang, AGREEMENT_LEVEL)
    st.radio(
        t(lang, "Niveau d’accord souhaité", "Preferred agreement level"),
        options=[c for c,_ in agree_opts],
        format_func=lambda x: dict(agree_opts)[x],
        key="r11_agreement",
        horizontal=False
    )
    if st.session_state.r11_agreement == "other":
        st.text_input(t(lang, "Précisez", "Please specify"), key="r11_agreement_other")

def rubric_12(lang: str):
    st.subheader(t(lang, "Rubrique 12 : Questions ouvertes", "Section 12: Open questions"))
    st.text_area(
        t(lang, "(1) Votre recommandation prioritaire (une seule) pour réussir en 12–24 mois :", "(1) Your single top recommendation to succeed within 12–24 months:"),
        key="r12_rec",
        height=110
    )
    st.text_area(
        t(lang, "(2) Un indicateur essentiel manquant (si applicable) et justification :", "(2) One essential missing indicator (if any) and justification:"),
        key="r12_missing",
        height=110
    )

def review_submit(lang: str, df_long: pd.DataFrame):
    st.subheader(t(lang, "Relecture et soumission", "Review and submit"))

    # validations globales
    errors = []
    errors += validate_r4()
    errors += validate_r5(lang, df_long)
    errors += validate_r6()
    errors += validate_r8()
    errors += validate_r9()
    errors += validate_r10()
    errors += validate_r11()

    if errors:
        st.error(t(lang, "Des erreurs doivent être corrigées avant soumission :", "Please fix the following errors before submitting:"))
        st.write("\n".join(errors))
        return

    r5_rows = compute_r5_selected(lang, df_long)

    payload = {
        "meta": {
            "response_id": str(uuid.uuid4()),
            "submitted_at": datetime.utcnow().isoformat() + "Z",
            "lang_ui": lang,
            "app_version": "v1.0-streamlit",
        },
        "r2_respondent": {
            "organisation": st.session_state.org,
            "country": st.session_state.country,
            "au_region": st.session_state.region,
            "stakeholder_type": st.session_state.stakeholder_type,
            "stakeholder_other": st.session_state.stakeholder_other,
            "position": st.session_state.position,
            "email": st.session_state.email,
            "phone": st.session_state.phone,
            "response_language": st.session_state.response_language,
        },
        "r3_scope": {
            "scope": st.session_state.scope,
            "consulted_colleagues": st.session_state.consulted,
            "snds_status": st.session_state.snds,
        },
        "r4_domains": {
            "preselected": st.session_state.r4_preselect,
            "top5": st.session_state.r4_top5,
        },
        "r5_indicators_scoring": r5_rows,
        "r6_gender": {
            "requirements_yesno": {k: st.session_state.get(f"r6_{k}") for k,_ in GENDER_REQ_YESNO},
            "requirements_include": {k: st.session_state.get(f"r6_{k}") for k,_ in GENDER_REQ_INCLUDE},
            "main_priority": st.session_state.r6_main_priority,
            "main_priority_other": st.session_state.r6_main_priority_other,
        },
        "r7_sources": {
            "sources": st.session_state.r7_sources,
            "production_level": st.session_state.r7_prod_level,
        },
        "r8_constraints": {k: st.session_state.get(f"r8_{k}") for k,_ in R8_CONSTRAINTS},
        "r9_quality": st.session_state.r9_quality,
        "r10_dissemination": st.session_state.r10_products,
        "r11_validation": {
            "mechanisms": {k: st.session_state.get(f"r11_{k}") for k,_ in VALIDATION_MECHANISMS},
            "agreement_level": st.session_state.r11_agreement,
            "agreement_level_other": st.session_state.r11_agreement_other,
        },
        "r12_open": {
            "recommendation": st.session_state.r12_rec,
            "missing_indicator": st.session_state.r12_missing,
        },
    }

    col1, col2 = st.columns([1,1])
    with col1:
        if st.button(t(lang, "✅ Soumettre et enregistrer", "✅ Submit and save")):
            save_response(payload, r5_rows)
            st.success(t(lang, "Soumission enregistrée. Merci !", "Submission saved. Thank you!"))

    # Téléchargements
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    with col2:
        st.download_button(
            t(lang, "Télécharger la réponse (JSON)", "Download response (JSON)"),
            data=payload_json.encode("utf-8"),
            file_name=f"statafric_response_{payload['meta']['response_id']}.json",
            mime="application/json"
        )

    # Export CSV (R5)
    if r5_rows:
        df_r5 = pd.DataFrame(r5_rows)
        st.download_button(
            t(lang, "Télécharger la table R5 (CSV)", "Download R5 table (CSV)"),
            data=df_r5.to_csv(index=False).encode("utf-8"),
            file_name=f"statafric_r5_{payload['meta']['response_id']}.csv",
            mime="text/csv"
        )

# =========================================================
# Main
# =========================================================

def admin_ui(lang: str, df_long: pd.DataFrame):
    st.title("Admin – " + ("Questionnaire STATAFRIC" if lang=="fr" else "STATAFRIC questionnaire"))
    st.caption(t(lang,
                 "Accès réservé. Utilisez ?admin=1 et un mot de passe admin.",
                 "Restricted access. Use ?admin=1 and an admin password."))

    pwd_required = _admin_password()
    st.session_state.setdefault("admin_ok", False)

    if not pwd_required:
        st.error(t(lang,
                   "ADMIN_PASSWORD n’est pas configuré. Définissez-le via Streamlit secrets ou variable d’environnement.",
                   "ADMIN_PASSWORD is not configured. Set it via Streamlit secrets or environment variable."))
        return

    if not st.session_state.admin_ok:
        with st.sidebar:
            st.subheader("Admin login")
            pwd = st.text_input("Password", type="password")
            if st.button("Login"):
                if pwd == pwd_required:
                    st.session_state.admin_ok = True
                    st.rerun()
                else:
                    st.error("Mot de passe incorrect." if lang=="fr" else "Incorrect password.")
        st.stop()

    # Load data
    df_raw = _load_all_submissions(str(DB_PATH))
    payloads = _parse_payloads(df_raw)
    df_flat = _flatten_for_export(payloads)
    df_r5 = _r5_rows_df(payloads)

    tab1, tab2, tab3 = st.tabs([
        t(lang, "Tableau de bord", "Dashboard"),
        t(lang, "Exports", "Exports"),
        t(lang, "Rapport Word", "Word report"),
    ])

    with tab1:
        col1, col2, col3 = st.columns(3)
        col1.metric(t(lang, "Nombre de réponses", "Submissions"), len(payloads))
        if "country" in df_flat.columns:
            col2.metric(t(lang, "Pays couverts", "Countries"), int(df_flat["country"].replace("", np.nan).dropna().nunique()))
        if "stakeholder_type" in df_flat.columns:
            col3.metric(t(lang, "Types d’acteurs", "Stakeholder types"), int(df_flat["stakeholder_type"].replace("", np.nan).dropna().nunique()))

        st.subheader(t(lang, "Domaines prioritaires (Top 5)", "Priority domains (Top 5)"))
        top_cols=[c for c in df_flat.columns if c.startswith("top5_domain_")]
        vc=_value_counts_any(df_flat, top_cols).head(25)
        if not vc.empty:
            st.dataframe(vc, use_container_width=True)
            _plot_barh(vc.head(15), "value", "n", t(lang, "Top domaines (fréquence)", "Top domains (frequency)"))

        st.subheader(t(lang, "Statistiques proposées (fréquence)", "Proposed statistics (frequency)"))
        if not df_r5.empty and "stat_label" in df_r5.columns:
            vc2=df_r5["stat_label"].fillna("").replace("", np.nan).dropna().value_counts().head(25).reset_index()
            vc2.columns=["stat_label","n"]
            st.dataframe(vc2, use_container_width=True)
            _plot_barh(vc2.head(15), "stat_label", "n", t(lang, "Top statistiques", "Top statistics"))

    with tab2:
        st.subheader(t(lang, "Téléchargements", "Downloads"))
        # CSV exports
        csv1 = df_flat.to_csv(index=False).encode("utf-8")
        st.download_button(t(lang, "Télécharger les réponses (CSV)", "Download submissions (CSV)"),
                           data=csv1, file_name="submissions.csv", mime="text/csv")

        if not df_r5.empty:
            csv2 = df_r5.to_csv(index=False).encode("utf-8")
            st.download_button(t(lang, "Télécharger la notation R5 (CSV)", "Download R5 scoring (CSV)"),
                               data=csv2, file_name="r5_scoring.csv", mime="text/csv")

        # Excel export
        xlsx_bytes = _make_excel_export(df_flat, df_r5, payloads)
        st.download_button(t(lang, "Télécharger l’export complet (Excel)", "Download full export (Excel)"),
                           data=xlsx_bytes, file_name="export_full.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # JSONL export
        jsonl="\n".join([json.dumps(p, ensure_ascii=False) for p in payloads]).encode("utf-8")
        st.download_button(t(lang, "Télécharger les payloads (JSONL)", "Download payloads (JSONL)"),
                           data=jsonl, file_name="payloads.jsonl", mime="application/json")

        # SQLite
        if Path(DB_PATH).exists():
            with open(DB_PATH, "rb") as f:
                st.download_button(t(lang, "Télécharger la base SQLite", "Download SQLite database"),
                                   data=f.read(), file_name="responses.db", mime="application/octet-stream")

    with tab3:
        st.subheader(t(lang, "Génération du rapport", "Report generation"))
        st.write(t(lang,
                   "Le rapport .docx contient une synthèse, des tableaux de fréquences et des éléments d’analyse.",
                   "The .docx report includes a synthesis, frequency tables and analysis elements."))
        if st.button(t(lang, "Générer le rapport Word", "Generate Word report")):
            report_bytes = _build_word_report_bytes(lang, payloads, df_flat, df_r5)
            st.download_button(t(lang, "Télécharger le rapport (.docx)", "Download report (.docx)"),
                               data=report_bytes, file_name="report_summary.docx",
                               mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document")

def main():
    # Must be the first Streamlit command
    st.set_page_config(page_title="STATAFRIC questionnaire", layout="wide")

    # Langue UI (FR/EN) = bascule globale
    lang_ui = st.sidebar.selectbox(
        "Language / Langue",
        options=["fr", "en"],
        format_func=lambda x: "Français" if x == "fr" else "English",
    )

    df_long = load_longlist()

    # Hidden admin route (use ?admin=1)
    if _is_admin_route():
        admin_ui(lang_ui, df_long)
        return

    init_state()

    ui_header(lang_ui)
    ui_sidebar(lang_ui)

    # Contenu par rubrique
    step = st.session_state.step
    if step == 1:
        rubric_1(lang_ui)
    elif step == 2:
        rubric_2(lang_ui)
    elif step == 3:
        rubric_3(lang_ui)
    elif step == 4:
        rubric_4(lang_ui)
    elif step == 5:
        rubric_5(lang_ui, df_long)
    elif step == 6:
        rubric_6(lang_ui)
    elif step == 7:
        rubric_7(lang_ui)
    elif step == 8:
        rubric_8(lang_ui)
    elif step == 9:
        rubric_9(lang_ui)
    elif step == 10:
        rubric_10(lang_ui)
    elif step == 11:
        rubric_11(lang_ui)
    elif step == 12:
        rubric_12(lang_ui)
    else:
        review_submit(lang_ui, df_long)

    st.divider()
    nav_buttons(lang_ui, df_long)

if __name__ == "__main__":
    main()

            st.error("Veuillez remplir intégralement le TOP 5 avant de soumettre.")
