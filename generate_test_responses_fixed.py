#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Générer des réponses de test (N soumissions) pour l'app Streamlit "Questionnaire de consultation" (v22).

Ce script :
- lit la longlist (longlist.xlsx) pour respecter les domaines/statistiques existants ;
- lit la liste des pays (COUNTRY_ISO3_with_EN.xlsx) ;
- construit des payloads compatibles avec la structure enregistrée par l'app ;
- insère les soumissions dans la base SQLite (responses.db par défaut), table `submissions` ;
- (optionnel) exporte aussi les payloads en JSONL.

Usage (exemples) :
  python generate_test_responses.py --n 200 --db responses.db \
      --longlist longlist.xlsx --countries COUNTRY_ISO3_with_EN.xlsx \
      --app app22.py --lang mixed

Remarques :
- Le script ne "clique" pas l'UI Streamlit ; il peuple la base locale utilisée par l'app.
- Les emails de test sont uniques (test0001@example.org, etc.).
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import random
import sqlite3
import uuid
import hashlib
import re
from pathlib import Path
from io import BytesIO
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# -------------------------
# Utilitaires temps / DB
# -------------------------

def now_utc_iso() -> str:
    # Même format que l'app : "YYYY-MM-DDTHH:MM:SSZ"
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def db_init(db_path: str) -> None:
    """
    Initialise le schéma minimal de l'app (submissions + drafts + app_config).
    (Identique à l'esprit du db_init() de l'app).
    """
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS submissions(
            submission_id TEXT PRIMARY KEY,
            submitted_at_utc TEXT,
            lang TEXT,
            email TEXT,
            payload_json TEXT
        )
        """
    )

    # Index utile
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_submissions_email ON submissions(email)")
    except Exception:
        pass

    # Drafts (pour compatibilité avec l'app)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS drafts(
            draft_id TEXT PRIMARY KEY,
            updated_at_utc TEXT,
            email TEXT,
            payload_json TEXT
        )
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_drafts_email ON drafts(email)")
    except Exception:
        pass

    # App config
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS app_config(
            k TEXT PRIMARY KEY,
            v TEXT,
            updated_at_utc TEXT
        )
        """
    )

    con.commit()
    con.close()


def db_save_submission(db_path: str, submission_id: str, lang: str, email: str, payload: Dict[str, Any]) -> None:
    db_init(db_path)
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO submissions(submission_id, submitted_at_utc, lang, email, payload_json)
        VALUES(?, ?, ?, ?, ?)
        """,
        (
            submission_id,
            now_utc_iso(),
            lang,
            (email or "").strip().lower(),
            json.dumps(payload, ensure_ascii=False),
        ),
    )
    con.commit()
    con.close()


# -------------------------
# Lecture longlist / pays
# -------------------------

@dataclass(frozen=True)
class Country:
    iso3: str
    name_fr: str
    name_en: str


def load_countries(countries_path: str) -> List[Country]:
    """
    Charge la table des pays (ISO3 + noms FR/EN) depuis .xlsx/.xls OU .csv.
    Retour : liste de Country(iso3, name_fr, name_en).
    """
    p = Path(countries_path)
    if not p.exists():
        raise FileNotFoundError(f"Fichier pays introuvable : {countries_path}")

    ext = p.suffix.lower()
    if ext in {".xlsx", ".xls"}:
        df = pd.read_excel(p, dtype=str).fillna("")
    elif ext == ".csv":
        try:
            df = pd.read_csv(p, dtype=str).fillna("")
        except UnicodeDecodeError:
            df = pd.read_csv(p, dtype=str, encoding="latin-1").fillna("")
    else:
        try:
            df = pd.read_excel(p, dtype=str).fillna("")
        except Exception:
            df = pd.read_csv(p, dtype=str).fillna("")

    cols_l = {c.strip().lower(): c for c in df.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            nl = n.lower()
            if nl in cols_l:
                return cols_l[nl]
        return None

    iso3_col = pick("ISO3", "iso3", "country_iso3", "countrycode", "code")
    fr_col   = pick("Country_fr", "country_fr", "COUNTRY_NAME_FR", "country_name_fr", "countrylabel_fr", "country_label_fr", "countryname_fr", "name_fr", "fr", "country")
    en_col   = pick("Country_en", "country_en", "COUNTRY_NAME_EN", "country_name_en", "countrylabel_en", "country_label_en", "countryname_en", "name_en", "en")

    if iso3_col is None or fr_col is None:
        raise ValueError(
            "Colonnes manquantes dans le fichier pays. Attendu au minimum : ISO3 et Country_fr (ou variantes). "
            f"Colonnes trouvées : {list(df.columns)}"
        )

    countries: List[Country] = []
    for _, row in df.iterrows():
        iso3 = str(row[iso3_col]).strip()
        fr = str(row[fr_col]).strip()
        en = str(row[en_col]).strip() if en_col else ""
        if not iso3 or not fr:
            continue
        countries.append(Country(iso3=iso3, name_fr=fr, name_en=en))

    # Dédupliquer iso3
    out = []
    seen = set()
    for c in countries:
        if c.iso3 in seen:
            continue
        seen.add(c.iso3)
        out.append(c)
    return out
def load_longlist(longlist_path: str) -> Tuple[List[str], Dict[str, List[str]]]:
    """
    Charge la longlist depuis un fichier .xlsx/.xls OU .csv.

    Sorties :
      - domains : liste de libellés de domaine (FR) dans l'ordre d'apparition ;
      - stats_by_domain : dict {domain_label_fr: [code_statistique, ... ] }
    """
    p = Path(longlist_path)
    if not p.exists():
        raise FileNotFoundError(f"Longlist introuvable : {longlist_path}")

    ext = p.suffix.lower()
    if ext in {".xlsx", ".xls"}:
        df = pd.read_excel(p, dtype=str).fillna("")
    elif ext == ".csv":
        # try utf-8 then latin-1
        try:
            df = pd.read_csv(p, dtype=str).fillna("")
        except UnicodeDecodeError:
            df = pd.read_csv(p, dtype=str, encoding="latin-1").fillna("")
    else:
        # fallback : try excel then csv
        try:
            df = pd.read_excel(p, dtype=str).fillna("")
        except Exception:
            df = pd.read_csv(p, dtype=str).fillna("")

    # Normaliser colonnes (tolérance aux variantes)
    cols = {c.strip(): c for c in df.columns}
    cols_l = {c.strip().lower(): c for c in df.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n in cols:
                return cols[n]
            nl = n.lower()
            if nl in cols_l:
                return cols_l[nl]
        return None

    domain_code_col = pick("Domain_code", "domain_code", "domain", "domain_id", "domainid")
    domain_fr_col   = pick("Domain_label_fr", "domain_label_fr", "domain_fr", "domain_name_fr", "domainname_fr", "domain_label")
    stat_code_col   = pick("Stat_code", "stat_code", "indicator_code", "indicatorcode", "code")
    stat_fr_col     = pick("Stat_label_fr", "stat_label_fr", "indicator_label_fr", "label_fr", "name_fr", "indicator_fr")
    stat_en_col     = pick("Stat_label_en", "stat_label_en", "indicator_label_en", "label_en", "name_en", "indicator_en")

    # Vérifier les colonnes indispensables (le code statistique peut être absent : il sera alors généré)
    missing_required = [k for k, v in {
        "Domain_code/domain_code": domain_code_col,
        "Domain_label_fr/domain_label_fr": domain_fr_col,
        "Stat_label_fr/stat_label_fr": stat_fr_col,
    }.items() if v is None]

    if missing_required:
        raise ValueError(
            "Colonnes manquantes dans la longlist. Attendu au minimum : "
            "Domain_code, Domain_label_fr, Stat_label_fr (ou variantes). "
            f"Manquantes : {', '.join(missing_required)}. Colonnes trouvées : {list(df.columns)}"
        )

    def _slug(s: str, maxlen: int = 24) -> str:
        s = re.sub(r"[^A-Za-z0-9]+", "_", (s or "").upper()).strip("_")
        return s[:maxlen] if s else ""

    def _gen_stat_code(dom_code: str, label_fr: str, idx: int) -> str:
        dom = _slug(dom_code, 12) or "DOM"
        base = _slug(label_fr, 24) or f"STAT{idx:03d}"
        h = hashlib.md5((label_fr or "").encode("utf-8")).hexdigest()[:6]
        return f"{dom}_{base}_{h}"
    # Construire la structure attendue
    domains: List[str] = []
    stats_by_domain: Dict[str, List[Dict[str, str]]] = {}

    for _, row in df.iterrows():
        dom_label_fr = str(row[domain_fr_col]).strip()
        if not dom_label_fr:
            continue
        if dom_label_fr not in stats_by_domain:
            stats_by_domain[dom_label_fr] = []
            domains.append(dom_label_fr)

        code = str(row[stat_code_col]).strip() if stat_code_col else ""
        lbl_fr = str(row[stat_fr_col]).strip()
        if not code:
            code = _gen_stat_code(str(row[domain_code_col]).strip(), lbl_fr, len(stats_by_domain[dom_label_fr]) + 1)
        lbl_en = str(row[stat_en_col]).strip() if stat_en_col else ""

        if not code and not lbl_fr:
            continue

        stats_by_domain[dom_label_fr].append(code)

        # Dédupliquer (au cas où) en gardant l'ordre
    for dom, lst in stats_by_domain.items():
        seen = set()
        out = []
        for code in lst:
            if code in seen:
                continue
            seen.add(code)
            out.append(code)
        stats_by_domain[dom] = out

    return domains, stats_by_domain

def choice_weighted(items: List[str], weights: Optional[List[float]] = None) -> str:
    if not items:
        raise ValueError("Liste vide.")
    if not weights:
        return random.choice(items)
    return random.choices(items, weights=weights, k=1)[0]


def sample_k(items: List[str], k_min: int, k_max: int) -> List[str]:
    k_max = min(k_max, len(items))
    k_min = min(k_min, k_max)
    k = random.randint(k_min, k_max)
    return random.sample(items, k=k)


def make_open_text(lang: str) -> Tuple[str, str, str]:
    # Petits textes plausibles, variés
    fr_templates = [
        "Ajouter un indicateur sur la pauvreté multidimensionnelle et sa ventilation régionale.",
        "Proposer des statistiques trimestrielles plus régulières sur l’emploi et les prix.",
        "Renforcer la diffusion via un portail unique (API + métadonnées complètes).",
        "Améliorer l’accès aux données administratives (éducation, santé, état civil).",
    ]
    en_templates = [
        "Add an indicator on multidimensional poverty with regional breakdowns.",
        "Improve the regular production of quarterly employment and price statistics.",
        "Strengthen dissemination via a single portal (API + complete metadata).",
        "Improve access to administrative data (education, health, civil registration).",
    ]
    tpls = fr_templates if lang == "fr" else en_templates
    # Parfois laisser vide pour tester les champs optionnels
    def maybe_text() -> str:
        return "" if random.random() < 0.20 else random.choice(tpls)

    return (maybe_text(), maybe_text(), maybe_text())


def generate_payload(
    i: int,
    lang: str,
    domains: List[str],
    stats_by_domain: Dict[str, List[str]],
    countries: List[Country],
    role_options_fr: List[str],
    role_options_en: List[str],
    actor_codes: List[str],
    scope_codes: List[str],
    snds_codes: List[str],
    gender_items_fr: List[str],
    gender_items_en: List[str],
    capacity_items_fr: List[str],
    capacity_items_en: List[str],
    quality_opts_fr: List[str],
    quality_opts_en: List[str],
    dissemination_opts_fr: List[str],
    dissemination_opts_en: List[str],
    datasrc_opts_fr: List[str],
    datasrc_opts_en: List[str],
    gender_priority_codes: List[str],
) -> Dict[str, Any]:

    # Identifiants
    email = f"test{i:04d}@example.org"
    draft_id = str(uuid.uuid4())

    # Pays
    c = random.choice(countries)

    # Type acteur / fonction
    type_acteur = random.choice(actor_codes)
    type_acteur_autre = ""
    if type_acteur in ("OTHER", "AUTRE"):
        type_acteur_autre = "Organisation de test" if lang == "fr" else "Test organisation"

    role_options = role_options_fr if lang == "fr" else role_options_en
    fonction = random.choice(role_options)
    fonction_autre = ""
    if fonction in ("Autre", "Other"):
        fonction_autre = "Responsable programme" if lang == "fr" else "Programme officer"

    # Portée / SNDS
    scope = random.choice(scope_codes)
    scope_other = ""
    if scope == "OTHER":
        scope_other = "Sous-national" if lang == "fr" else "Sub-national"

    snds_status = random.choice(snds_codes)

    # Préselection domaines (6–10), puis TOP5 ordonné
    preselected_domains = sample_k(domains, 6, min(10, len(domains)))
    top5_domains = random.sample(preselected_domains, k=5)
    random.shuffle(top5_domains)  # l'ordre fait office de rang 1..5

    # Sélection statistiques : 1–3 par domaine du top5
    selected_by_domain: Dict[str, List[str]] = {}
    selected_stats: List[str] = []
    for d in top5_domains:
        stats = stats_by_domain.get(d, [])
        if not stats:
            continue
        k = random.randint(1, min(3, len(stats)))
        picked = random.sample(stats, k=k)
        selected_by_domain[d] = picked
        selected_stats.extend(picked)

    # Unicité (au cas où)
    selected_stats = list(dict.fromkeys(selected_stats))

    # Scoring
    def score_draw() -> int:
        # 0 = NSP/DK, sinon 1–3 ; on met un peu de NSP
        return choice_weighted([0, 1, 2, 3], weights=[0.10, 0.25, 0.35, 0.30])  # type: ignore

    scoring: Dict[str, Dict[str, int]] = {}
    for s in selected_stats:
        scoring[s] = {
            "demand": score_draw(),
            "availability": score_draw(),
            "feasibility": score_draw(),
        }

    # Table genre (codes : YES/NO/SPEC/UK)
    gender_items = gender_items_fr if lang == "fr" else gender_items_en
    gender_table: Dict[str, str] = {}
    for it in gender_items:
        gender_table[it] = choice_weighted(
            ["YES", "NO", "SPEC", "UK"],
            weights=[0.55, 0.20, 0.10, 0.15],
        )

    # Priorités genre : 1–3 codes distincts
    gp = gender_priority_codes[:] if gender_priority_codes else ["ECO", "EDU", "HLT", "GBV", "DEC", "OTHER"]
    n_gp = random.randint(1, 3)
    chosen_gp = random.sample(gp, k=min(n_gp, len(gp)))
    # Compléter à 3 valeurs (optionnel dans l'app, mais on remplit proprement)
    while len(chosen_gp) < 3:
        chosen_gp.append("")
    p1, p2, p3 = chosen_gp[0], chosen_gp[1], chosen_gp[2]
    gender_priority_other = ""
    if "OTHER" in chosen_gp or "AUTRE" in chosen_gp:
        gender_priority_other = "Accès aux services" if lang == "fr" else "Access to services"

    # Table capacité (codes : HIGH/MED/LOW/UK)
    cap_items = capacity_items_fr if lang == "fr" else capacity_items_en
    capacity_table: Dict[str, str] = {}
    for it in cap_items:
        capacity_table[it] = choice_weighted(
            ["HIGH", "MED", "LOW", "UK"],
            weights=[0.35, 0.35, 0.20, 0.10],
        )

    # Rubrique 9 : attentes qualité (1–3)
    q_opts = quality_opts_fr if lang == "fr" else quality_opts_en
    quality_expectations = sample_k(q_opts, 1, 3)
    quality_other = ""
    if any(x.lower().startswith("autre") or x.lower().startswith("other") for x in quality_expectations):
        quality_other = "Normes nationales" if lang == "fr" else "National standards"

    # Rubrique 10 : diffusion (1–3)
    d_opts = dissemination_opts_fr if lang == "fr" else dissemination_opts_en
    dissemination_channels = sample_k(d_opts, 1, 3)
    dissemination_other = ""
    if any(x.lower().startswith("autre") or x.lower().startswith("other") for x in dissemination_channels):
        dissemination_other = "Tableaux de bord interactifs" if lang == "fr" else "Interactive dashboards"

    # Rubrique 11 : sources (2–4)
    ds_opts = datasrc_opts_fr if lang == "fr" else datasrc_opts_en
    data_sources = sample_k(ds_opts, 2, 4)
    data_sources_other = ""
    if any(x.lower().startswith("autre") or x.lower().startswith("other") for x in data_sources):
        data_sources_other = "Données de téléphonie mobile" if lang == "fr" else "Mobile phone data"

    # Rubrique 12 : questions ouvertes + confirmation consultation
    open_q1, open_q2, open_q3 = make_open_text(lang)
    consulted_colleagues = "YES" if random.random() < 0.65 else "NO"

    # Organisation (contrôle de qualité dans l'app : longueur minimale)
    org_prefix = "Institut national" if lang == "fr" else "National institute"
    organisation = f"{org_prefix} – réponse test {i:04d}"

    payload: Dict[str, Any] = {
        "draft_id": draft_id,

        # Rubrique 2
        "organisation": organisation,
        "pays": c.iso3,
        "pays_name_fr": c.name_fr,
        "pays_name_en": c.name_en,
        "email": email,
        "type_acteur": type_acteur,
        "type_acteur_autre": type_acteur_autre,
        "fonction": fonction,
        "fonction_autre": fonction_autre,

        # Rubrique 3
        "scope": scope,
        "scope_other": scope_other,
        "snds_status": snds_status,

        # Rubrique 4/5
        "preselected_domains": preselected_domains,
        "top5_domains": top5_domains,
        "selected_by_domain": selected_by_domain,
        "selected_stats": selected_stats,
        "scoring": scoring,
        "scoring_version": 3,

        # Rubrique 6–8
        "gender_table": gender_table,
        "gender_priority_main": p1,  # compat
        "gender_priority_1": p1,
        "gender_priority_2": p2,
        "gender_priority_3": p3,
        "gender_priority_other": gender_priority_other,
        "capacity_table": capacity_table,

        # Rubrique 9–12
        "quality_expectations": quality_expectations,
        "quality_other": quality_other,
        "dissemination_channels": dissemination_channels,
        "dissemination_other": dissemination_other,
        "data_sources": data_sources,
        "data_sources_other": data_sources_other,
        "open_q1": open_q1,
        "open_q2": open_q2,
        "open_q3": open_q3,
        "consulted_colleagues": consulted_colleagues,
    }

    return payload


# -------------------------
# Main
# -------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=200, help="Nombre de réponses à générer.")
    parser.add_argument("--seed", type=int, default=123, help="Graine aléatoire.")
    parser.add_argument("--db", type=str, default="responses.db", help="Chemin de la base SQLite (responses.db).")
    parser.add_argument("--longlist", type=str, default="data/indicator_longlist.csv", help="Chemin longlist.xlsx.")
    parser.add_argument("--countries", type=str, default="data/COUNTRY_ISO3_with_EN.xlsx", help="Chemin COUNTRY_ISO3_with_EN.xlsx.")
    parser.add_argument("--app", type=str, default="app22.py", help="Chemin app22.py (pour extraire options si possible).")
    parser.add_argument("--lang", type=str, default="mixed", choices=["fr", "en", "mixed"], help="Langue des réponses.")
    parser.add_argument("--jsonl", type=str, default="test_payloads.jsonl", help="Fichier JSONL de sortie (optionnel).")
    parser.add_argument("--no-jsonl", action="store_true", help="Ne pas écrire de JSONL.")

    args = parser.parse_args()
    random.seed(args.seed)

    
    # Résolution robuste des chemins (Streamlit Cloud : les fichiers sont souvent dans ./data)
    longlist_candidates = [
        args.longlist,
        "data/indicator_longlist.csv",
        "data/longlist.xlsx",
        "longlist.xlsx",
        "longlist.csv",
    ]
    longlist_path = next((p for p in longlist_candidates if p and Path(p).exists()), None)
    if longlist_path is None:
        raise FileNotFoundError(
            "Longlist introuvable. Essayé : "
            + ", ".join([p for p in longlist_candidates if p])
            + ". Placez le fichier dans le dépôt (ex : ./data/indicator_longlist.csv) "
            + "ou lancez le script avec --longlist <chemin>."
        )

    countries_candidates = [
        args.countries,
        "data/COUNTRY_ISO3_with_EN.xlsx",
        "COUNTRY_ISO3_with_EN.xlsx",
        "data/countries.csv",
        "countries.csv",
    ]
    countries_path = next((p for p in countries_candidates if p and Path(p).exists()), None)
    if countries_path is None:
        raise FileNotFoundError(
            "Fichier pays introuvable. Essayé : "
            + ", ".join([p for p in countries_candidates if p])
            + ". Placez le fichier dans le dépôt (ex : ./data/COUNTRY_ISO3_with_EN.xlsx) "
            + "ou lancez le script avec --countries <chemin>."
        )

    domains, stats_by_domain = load_longlist(longlist_path)
    countries = load_countries(countries_path)

    # Valeurs par défaut (fallback) si l'extraction AST échoue
    role_fr = ["DG/DGA/SG", "Directeur", "Conseiller", "Chef de division", "Chef de bureau", "Autre"]
    role_en = ["DG/DGA/SG", "Director", "Advisor", "Head of division", "Head of office", "Other"]

    actor_codes = ["NSO", "MIN", "REC", "AU", "DEV", "CSO", "ACAD", "OTHER"]
    scope_codes = ["NAT", "REG", "CONT", "GLOB", "OTHER"]
    snds_codes = ["YES", "NO", "INPR", "DNK"]

    # Listes attendues par l'app (fallback) — on tente de les lire depuis app22.py pour rester synchronisé
    gender_items_fr = [
        "Désagrégation par sexe",
        "Groupes d’âge",
        "Milieu de résidence (urbain/rural)",
        "Handicap",
        "Quintile de richesse (ou niveau de vie)",
        "Violences basées sur le genre (VBG)",
        "Temps domestique non rémunéré",
        "Participation aux instances de décision",
        "Accès aux ressources productives",
        "Autre",
    ]
    gender_items_en = [
        "Sex-disaggregation",
        "Age groups",
        "Place of residence (urban/rural)",
        "Disability",
        "Wealth quintile (or living standard)",
        "Gender-based violence (GBV)",
        "Unpaid domestic work",
        "Participation in decision-making bodies",
        "Access to productive resources",
        "Other",
    ]

    capacity_items_fr = [
        "Compétences statistiques disponibles",
        "Données disponibles",
        "Ressources financières",
        "Infrastructures/IT",
        "Coordination institutionnelle",
        "Partenariats",
    ]
    capacity_items_en = [
        "Statistical skills available",
        "Data availability",
        "Financial resources",
        "Infrastructure/IT",
        "Institutional coordination",
        "Partnerships",
    ]

    quality_opts_fr = [
        "Normes internationales",
        "Cadre qualité national",
        "Normes régionales",
        "Autre",
    ]
    quality_opts_en = [
        "International standards",
        "National quality framework",
        "Regional standards",
        "Other",
    ]

    dissemination_opts_fr = [
        "Site web / portail",
        "Rapports",
        "Tableaux de bord",
        "API / Open data",
        "Autre",
    ]
    dissemination_opts_en = [
        "Website / portal",
        "Reports",
        "Dashboards",
        "API / Open data",
        "Other",
    ]

    datasrc_opts_fr = [
        "Enquêtes",
        "Recensements",
        "Données administratives",
        "État civil (CRVS)",
        "Données géospatiales",
        "Données privées",
        "Autre",
    ]
    datasrc_opts_en = [
        "Surveys",
        "Censuses",
        "Administrative data",
        "Civil registration (CRVS)",
        "Geospatial data",
        "Private data",
        "Other",
    ]

    gender_priority_codes = ["ECO", "EDU", "HLT", "GBV", "DEC", "RES", "OTHER"]

    # Extraction depuis l'app (si disponible)
    if args.app and os.path.exists(args.app):
        try:
            ext = AppAstExtractor(args.app)
            rf = ext.get_global_str_list("ROLE_OPTIONS_FR")
            re_ = ext.get_global_str_list("ROLE_OPTIONS_EN")
            if rf: role_fr = rf
            if re_: role_en = re_

            # Rubrique 6 / 8 : items
            gi_fr = ext.get_func_str_list("rubric_6", "items_fr")
            gi_en = ext.get_func_str_list("rubric_6", "items_en")
            if gi_fr: gender_items_fr = gi_fr
            if gi_en: gender_items_en = gi_en

            ci_fr = ext.get_func_str_list("rubric_8", "items_fr")
            ci_en = ext.get_func_str_list("rubric_8", "items_en")
            if ci_fr: capacity_items_fr = ci_fr
            if ci_en: capacity_items_en = ci_en

            # Rubrique 9/10/11 : options
            q_fr = ext.get_func_str_list("rubric_9", "opts_fr")
            q_en = ext.get_func_str_list("rubric_9", "opts_en")
            if q_fr: quality_opts_fr = q_fr
            if q_en: quality_opts_en = q_en

            d_fr = ext.get_func_str_list("rubric_10", "opts_fr")
            d_en = ext.get_func_str_list("rubric_10", "opts_en")
            if d_fr: dissemination_opts_fr = d_fr
            if d_en: dissemination_opts_en = d_en

            s_fr = ext.get_func_str_list("rubric_11", "opts_fr")
            s_en = ext.get_func_str_list("rubric_11", "opts_en")
            if s_fr: datasrc_opts_fr = s_fr
            if s_en: datasrc_opts_en = s_en

            # Codes : type, scope, snds, gender priorities
            ac = ext.get_func_tuple_codes("rubric_2", "type_options")
            sc = ext.get_func_tuple_codes("rubric_3", "scope_options")
            sn = ext.get_func_tuple_codes("rubric_3", "snds_options")
            gp = ext.get_func_tuple_codes("rubric_7", "gp_opts")
            if ac: actor_codes = ac
            if sc: scope_codes = sc
            if sn: snds_codes = sn
            if gp: gender_priority_codes = gp

        except Exception:
            # Si extraction échoue : on garde les fallback
            pass

    # Génération
    db_init(args.db)
    jsonl_fh = None
    if not args.no_jsonl:
        jsonl_fh = open(args.jsonl, "w", encoding="utf-8")

    for i in range(1, args.n + 1):
        if args.lang == "mixed":
            lang = "fr" if random.random() < 0.70 else "en"
        else:
            lang = args.lang

        payload = generate_payload(
            i=i,
            lang=lang,
            domains=domains,
            stats_by_domain=stats_by_domain,
            countries=countries,
            role_options_fr=role_fr,
            role_options_en=role_en,
            actor_codes=actor_codes,
            scope_codes=scope_codes,
            snds_codes=snds_codes,
            gender_items_fr=gender_items_fr,
            gender_items_en=gender_items_en,
            capacity_items_fr=capacity_items_fr,
            capacity_items_en=capacity_items_en,
            quality_opts_fr=quality_opts_fr,
            quality_opts_en=quality_opts_en,
            dissemination_opts_fr=dissemination_opts_fr,
            dissemination_opts_en=dissemination_opts_en,
            datasrc_opts_fr=datasrc_opts_fr,
            datasrc_opts_en=datasrc_opts_en,
            gender_priority_codes=gender_priority_codes,
        )

        submission_id = str(uuid.uuid4())
        db_save_submission(args.db, submission_id=submission_id, lang=lang, email=payload["email"], payload=payload)

        if jsonl_fh is not None:
            jsonl_fh.write(json.dumps({"submission_id": submission_id, "lang": lang, "payload": payload}, ensure_ascii=False) + "\n")

    if jsonl_fh is not None:
        jsonl_fh.close()

    print(f"OK : {args.n} réponses insérées dans {args.db}")
    if not args.no_jsonl:
        print(f"JSONL : {args.jsonl}")


if __name__ == "__main__":
    main()
