#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Générateur de soumissions test (app22)
- Génère N soumissions (par défaut : 200) au format "payload" attendu par app22.py
- Peut (optionnel) écrire directement dans la base SQLite responses.db (même schéma que l'app)
- Produit aussi un export CSV (submission_id, submitted_at_utc, lang, email, payload_json) et un JSONL (payloads)

Usage (CLI) :
  python generate_test_submissions_app22.py --n 200 --seed 22 --write-db 1 --db-path responses.db

Dans un dépôt GitHub (Streamlit) :
- placez longlist.xlsx et COUNTRY_ISO3_with_EN.xlsx dans ./data/
- exécutez ensuite le script (local) ou utilisez l'app Streamlit fournie séparément.

Points à vérifier :
- Les chemins (data/...) si votre dépôt diffère
- Si vous utilisez Google Sheets / Dropbox, ce script ne pousse PAS vers ces services (il se limite à SQLite + fichiers)
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd


# -------------------------
# Chargement des référentiels (mêmes règles que app22.py)
# -------------------------

def load_longlist(longlist_xlsx: Path, longlist_csv: Optional[Path] = None) -> pd.DataFrame:
    """
    Retourne un DF avec colonnes :
      domain_code, domain_label_fr, domain_label_en, stat_code, stat_label_fr, stat_label_en
    """
    # 1) CSV prioritaire si fourni et présent
    if longlist_csv is not None and longlist_csv.exists():
        df = pd.read_csv(longlist_csv, dtype=str).fillna("")
        need = {"domain_code", "domain_label_fr", "domain_label_en", "stat_code", "stat_label_fr", "stat_label_en"}
        if need.issubset(set(df.columns)):
            return df

    # 2) XLSX (format utilisateur)
    if not longlist_xlsx.exists():
        raise FileNotFoundError(f"Longlist introuvable : {longlist_xlsx}")

    df = pd.read_excel(longlist_xlsx, dtype=str).fillna("")
    if not {"Domain_code", "Domain_label_fr", "Stat_label_fr"}.issubset(set(df.columns)):
        raise ValueError("La longlist doit contenir au minimum : Domain_code, Domain_label_fr, Stat_label_fr")

    out = pd.DataFrame()
    out["domain_code"] = df["Domain_code"].astype(str).str.strip()
    out["domain_label_fr"] = df["Domain_label_fr"].astype(str).str.split("|", n=1).str[-1].str.strip()
    out["stat_code"] = df["Stat_label_fr"].astype(str).str.split("|", n=1).str[0].str.strip()
    out["stat_label_fr"] = df["Stat_label_fr"].astype(str).str.split("|", n=1).str[-1].str.strip()

    if "Domain_label_en" in df.columns:
        out["domain_label_en"] = df["Domain_label_en"].astype(str).str.split("|", n=1).str[-1].str.strip()
    else:
        out["domain_label_en"] = out["domain_label_fr"]

    if "Stat_label_en" in df.columns:
        out["stat_label_en"] = df["Stat_label_en"].astype(str).str.split("|", n=1).str[-1].str.strip()
    else:
        out["stat_label_en"] = out["stat_label_fr"]

    out = out[[
        "domain_code",
        "domain_label_fr",
        "domain_label_en",
        "stat_code",
        "stat_label_fr",
        "stat_label_en",
    ]].dropna().astype(str)

    out = out[out["domain_code"].str.strip() != ""]
    out = out[out["stat_code"].str.strip() != ""]
    return out


def load_countries(country_xlsx: Path) -> pd.DataFrame:
    """
    Colonnes attendues :
      COUNTRY_ISO3, COUNTRY_NAME_FR, COUNTRY_NAME_EN
    """
    if not country_xlsx.exists():
        raise FileNotFoundError(f"Fichier pays introuvable : {country_xlsx}")
    df = pd.read_excel(country_xlsx, dtype=str).fillna("")
    df.columns = [str(c).strip() for c in df.columns]
    need = {"COUNTRY_ISO3", "COUNTRY_NAME_FR", "COUNTRY_NAME_EN"}
    if not need.issubset(set(df.columns)):
        raise ValueError("Le fichier pays doit contenir : COUNTRY_ISO3, COUNTRY_NAME_FR, COUNTRY_NAME_EN")
    df["COUNTRY_ISO3"] = df["COUNTRY_ISO3"].astype(str).str.strip().str.upper()
    df["COUNTRY_NAME_FR"] = df["COUNTRY_NAME_FR"].astype(str).str.strip()
    df["COUNTRY_NAME_EN"] = df["COUNTRY_NAME_EN"].astype(str).str.strip()
    df = df[df["COUNTRY_ISO3"].str.len() == 3]
    return df


# -------------------------
# Constantes (alignées sur app22.py)
# -------------------------

SCORING_VERSION = 3

TYPE_ACTEUR = [
    "NSO", "Ministry", "REC", "AU", "CivilSoc", "DevPartner", "Academia", "Other"
]
TYPE_WEIGHTS = [0.25, 0.22, 0.10, 0.10, 0.06, 0.17, 0.07, 0.03]

SCOPE_KEYS = ["National", "Regional", "Continental", "Global", "Other"]
SCOPE_BY_TYPE = {
    "NSO": ["National"],
    "Ministry": ["National"],
    "REC": ["Regional"],
    "AU": ["Continental"],
    "DevPartner": ["Global", "Continental", "Regional"],
    "CivilSoc": ["National", "Regional"],
    "Academia": ["National", "Global"],
    "Other": ["National", "Regional", "Global"],
}

SNDS_OPTS = ["YES", "NO", "PREP", "IMPL_PREP", "NSP"]
SNDS_WEIGHTS_NSO = [0.45, 0.05, 0.20, 0.25, 0.05]
SNDS_WEIGHTS_MIN = [0.35, 0.10, 0.25, 0.20, 0.10]
SNDS_WEIGHTS_OTH = [0.20, 0.15, 0.20, 0.10, 0.35]

ROLE_OPTIONS_FR = ["DG/DGA/SG", "Directeur", "Conseiller", "Chef de division", "Chef de bureau", "Autre"]
ROLE_OPTIONS_EN = ["DG/DGA/SG", "Director", "Advisor", "Head of division", "Head of office", "Other"]

# Rubrique 6 (codes) : YES / NO / SPEC / UK
GENDER_ITEMS_FR = [
    "Désagrégation par sexe",
    "Désagrégation par âge",
    "Milieu urbain / rural",
    "Handicap",
    "Quintile de richesse",
    "Violences basées sur le genre (VBG)",
    "Temps domestique non rémunéré",
]
GENDER_ITEMS_EN = [
    "Disaggregation by sex",
    "Disaggregation by age",
    "Urban / rural",
    "Disability",
    "Wealth quintile",
    "Gender-based violence (GBV)",
    "Unpaid domestic work",
]
GENDER_CODES = ["YES", "NO", "SPEC", "UK"]
GENDER_W = [0.45, 0.25, 0.22, 0.08]

# Rubrique 7 : codes ECO, SERV, GBV, PART_DEC, CARE, OTHER
GENDER_PRIORITIES = ["ECO", "SERV", "GBV", "PART_DEC", "CARE", "OTHER"]
GENDER_PRIORITIES_W = [0.22, 0.22, 0.20, 0.18, 0.12, 0.06]

# Rubrique 8 (codes) : HIGH / MED / LOW / UK
CAPACITY_ITEMS_FR = [
    "Compétences statistiques disponibles",
    "Accès aux données administratives",
    "Financement disponible",
    "Outils numériques (collecte, traitement, diffusion)",
    "Cadre juridique pour le partage de données",
    "Coordination interinstitutionnelle",
]
CAPACITY_ITEMS_EN = [
    "Available statistical skills",
    "Access to administrative data",
    "Available funding",
    "Digital tools (collection, processing, dissemination)",
    "Legal framework for data sharing",
    "Inter-institutional coordination",
]
CAPACITY_CODES = ["HIGH", "MED", "LOW", "UK"]

# Rubrique 9 : chaînes (langue-dépendant)
QUALITY_OPTS_FR = [
    "Manuels de normes et méthodes communes (par domaine) disponibles",
    "Cadre d’assurance qualité fonctionnel",
    "Procédures de validation et certification des données",
    "Mécanismes de cohérence des données nationales entre secteurs",
    "Renforcement des capacités techniques du SSN",
    "Renforcement du leadership de l’INS au sein du SSN",
    "Groupes techniques spécialisés (GTS/UA) opérationnels",
    "Autre (préciser) ",
]
QUALITY_OPTS_EN = [
    "Manuals on common standards and methods (by domain) available",
    "Functional quality assurance framework",
    "Data validation and certification procedures",
    "Toolkit / mechanisms for cross-sector consistency of national data",
    "Strengthening NSS technical capacity",
    "Strengthening NSO leadership within the NSS",
    "Specialized Technical Groups (STGs/AU) operational",
    "Other (specify) ",
]

# Rubrique 10 : chaînes
DISSEM_OPTS_FR = [
    "Portail web / tableaux de bord",
    "Communiqués / bulletins économiques",
    "Microdonnées anonymisées (accès sécurisé)",
    "API / Open data",
    "Ateliers et webinaires",
    "Autre",
]
DISSEM_OPTS_EN = [
    "Web portal / dashboards",
    "Press releases / short economic bulletins",
    "Anonymized microdata (secure access)",
    "API / Open data",
    "Workshops and webinars",
    "Other",
]

# Rubrique 11 : chaînes
SOURCES_OPTS_FR = [
    "Enquêtes ménages",
    "Enquêtes entreprises",
    "Recensements",
    "Données administratives",
    "Registres état-civil",
    "Données géospatiales",
    "Données privées",
    "Autres",
]
SOURCES_OPTS_EN = [
    "Household surveys",
    "Enterprise surveys",
    "Censuses",
    "Administrative data",
    "Civil registration and vital statistics (CRVS)",
    "Geospatial data",
    "Private data",
    "Other",
]


# -------------------------
# Outils
# -------------------------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def weighted_choice(rng: random.Random, items: List[str], weights: List[float]) -> str:
    return rng.choices(items, weights=weights, k=1)[0]


def pick_k_unique(rng: random.Random, items: List[str], k: int, weights: Optional[List[float]] = None) -> List[str]:
    if k <= 0:
        return []
    if k >= len(items):
        out = items[:]
        rng.shuffle(out)
        return out
    if weights is None:
        return rng.sample(items, k)
    # tirage pondéré sans remise (simple et robuste pour petites listes)
    chosen = []
    pool = list(zip(items, weights))
    for _ in range(k):
        total = sum(w for _, w in pool)
        r = rng.random() * total
        acc = 0.0
        idx = 0
        for i, (_, w) in enumerate(pool):
            acc += w
            if acc >= r:
                idx = i
                break
        chosen.append(pool[idx][0])
        pool.pop(idx)
    return chosen


@dataclass
class RefData:
    longlist: pd.DataFrame
    countries: pd.DataFrame
    domains: List[str]
    stats_by_domain: Dict[str, List[str]]
    country_fr: Dict[str, str]
    country_en: Dict[str, str]


def build_refdata(longlist_df: pd.DataFrame, countries_df: pd.DataFrame) -> RefData:
    domains = sorted(longlist_df["domain_code"].dropna().astype(str).str.strip().unique().tolist())
    stats_by_domain: Dict[str, List[str]] = {}
    for d in domains:
        stats = (
            longlist_df[longlist_df["domain_code"] == d]["stat_code"]
            .dropna().astype(str).str.strip().unique().tolist()
        )
        stats_by_domain[d] = sorted(stats)

    country_fr = dict(zip(countries_df["COUNTRY_ISO3"], countries_df["COUNTRY_NAME_FR"]))
    country_en = dict(zip(countries_df["COUNTRY_ISO3"], countries_df["COUNTRY_NAME_EN"]))
    return RefData(
        longlist=longlist_df,
        countries=countries_df,
        domains=domains,
        stats_by_domain=stats_by_domain,
        country_fr=country_fr,
        country_en=country_en,
    )


def make_org_name(lang: str, type_acteur: str, iso3: str, c_fr: str, c_en: str, rng: random.Random) -> str:
    if type_acteur == "NSO":
        return f"Institut national de la statistique ({c_fr})" if lang == "fr" else f"National Statistical Office ({c_en})"
    if type_acteur == "Ministry":
        return f"Ministère sectoriel - Service statistique ({c_fr})" if lang == "fr" else f"Line ministry - Statistics unit ({c_en})"
    if type_acteur == "DevPartner":
        return "Partenaire technique et financier (PTF)" if lang == "fr" else "Development partner (DP)"
    if type_acteur == "AU":
        return "Union Africaine - Commission (statistiques)" if lang == "fr" else "African Union Commission (statistics)"
    if type_acteur == "REC":
        return "Communauté économique régionale (CER)" if lang == "fr" else "Regional Economic Community (REC)"
    if type_acteur == "CivilSoc":
        return "Organisation de la société civile (OSC)" if lang == "fr" else "Civil society organization (CSO)"
    if type_acteur == "Academia":
        return "Université / Centre de recherche" if lang == "fr" else "University / Research center"
    return f"Organisation {iso3} - Autre" if lang == "fr" else f"Organization {iso3} - Other"


def generate_payload(ref: RefData, i: int, rng: random.Random, lang_fr_ratio: float) -> Dict[str, Any]:
    lang = "fr" if rng.random() < lang_fr_ratio else "en"
    type_acteur = weighted_choice(rng, TYPE_ACTEUR, TYPE_WEIGHTS)

    # Pays
    iso3 = rng.choice(ref.countries["COUNTRY_ISO3"].tolist())
    pays_name_fr = ref.country_fr.get(iso3, "")
    pays_name_en = ref.country_en.get(iso3, "") or pays_name_fr

    # Portée
    scope = rng.choice(SCOPE_BY_TYPE.get(type_acteur, ["National"]))
    scope_other = ""
    if scope == "Other":
        scope_other = "Sous-national / thématique" if lang == "fr" else "Sub-national / thematic"

    # Statut SNDS
    if type_acteur == "NSO":
        snds_status = weighted_choice(rng, SNDS_OPTS, SNDS_WEIGHTS_NSO)
    elif type_acteur == "Ministry":
        snds_status = weighted_choice(rng, SNDS_OPTS, SNDS_WEIGHTS_MIN)
    else:
        snds_status = weighted_choice(rng, SNDS_OPTS, SNDS_WEIGHTS_OTH)

    # Organisation / email / fonction
    organisation = make_org_name(lang, type_acteur, iso3, pays_name_fr, pays_name_en, rng)
    email = f"test_app22_{i:04d}@example.org"
    role_options = ROLE_OPTIONS_FR if lang == "fr" else ROLE_OPTIONS_EN
    fonction = rng.choice(role_options)
    fonction_autre = ""
    if fonction in ["Autre", "Other"]:
        fonction_autre = "Point focal statistique" if lang == "fr" else "Statistics focal point"

    # Domaines
    k_pre = rng.randint(5, 10)
    preselected = pick_k_unique(rng, ref.domains, k_pre)

    # TOP 5 (ordre = rang)
    top5 = pick_k_unique(rng, preselected, 5)

    # Sélection des stats (1–3 par domaine)
    selected_by_domain: Dict[str, List[str]] = {}
    selected_stats: List[str] = []
    for d in top5:
        pool = ref.stats_by_domain.get(d, [])
        if not pool:
            continue
        k_stats = rng.randint(1, 3)
        picked = pick_k_unique(rng, pool, k_stats)
        selected_by_domain[d] = picked
        selected_stats.extend(picked)

    # Notation (0–3)
    def draw_score_dist() -> int:
        # distribution raisonnable : 3 plus fréquent, 0 rare
        return rng.choices([3, 2, 1, 0], weights=[0.38, 0.34, 0.22, 0.06], k=1)[0]

    scoring: Dict[str, Dict[str, int]] = {}
    for s in selected_stats:
        scoring[s] = {
            "demand": draw_score_dist(),
            "availability": draw_score_dist(),
            "feasibility": draw_score_dist(),
        }

    # Rubrique 6 : genre (codes)
    gender_items = GENDER_ITEMS_FR if lang == "fr" else GENDER_ITEMS_EN
    gender_table: Dict[str, str] = {}
    for it in gender_items:
        gender_table[it] = rng.choices(GENDER_CODES, weights=GENDER_W, k=1)[0]

    # Rubrique 7 : priorités genre (codes)
    p1 = rng.choices(GENDER_PRIORITIES, weights=GENDER_PRIORITIES_W, k=1)[0]
    p2 = rng.choice([""] + [x for x in GENDER_PRIORITIES if x != p1])
    p3 = ""
    if p2 != "" and rng.random() < 0.55:
        p3 = rng.choice([""] + [x for x in GENDER_PRIORITIES if x not in [p1, p2]])
    gender_priority_other = ""
    if "OTHER" in [p1, p2, p3]:
        gender_priority_other = "Autre priorité genre (test)" if lang == "fr" else "Other gender priority (test)"

    # Rubrique 8 : capacité (codes)
    cap_items = CAPACITY_ITEMS_FR if lang == "fr" else CAPACITY_ITEMS_EN
    capacity_table: Dict[str, str] = {}
    for it in cap_items:
        if type_acteur in ["NSO", "AU", "REC"]:
            w = [0.38, 0.40, 0.17, 0.05]  # un peu plus de HIGH/MED
        else:
            w = [0.28, 0.40, 0.25, 0.07]
        capacity_table[it] = rng.choices(CAPACITY_CODES, weights=w, k=1)[0]

    # Rubriques 9–11 : selections multi (chaînes)
    qual_opts = QUALITY_OPTS_FR if lang == "fr" else QUALITY_OPTS_EN
    dis_opts = DISSEM_OPTS_FR if lang == "fr" else DISSEM_OPTS_EN
    src_opts = SOURCES_OPTS_FR if lang == "fr" else SOURCES_OPTS_EN

    quality_expectations = pick_k_unique(rng, qual_opts, rng.randint(1, 3))
    quality_other = ""
    if ("Autre" in " ".join(quality_expectations)) or ("Other" in " ".join(quality_expectations)):
        quality_other = "Autre exigence (test)" if lang == "fr" else "Other expectation (test)"

    dissemination_channels = pick_k_unique(rng, dis_opts, rng.randint(1, 3))
    dissemination_other = ""
    if ("Autre" in " ".join(dissemination_channels)) or ("Other" in " ".join(dissemination_channels)):
        dissemination_other = "Autre canal (test)" if lang == "fr" else "Other channel (test)"

    data_sources = pick_k_unique(rng, src_opts, rng.randint(2, 4))
    data_sources_other = ""
    if ("Autres" in " ".join(data_sources)) or ("Other" in " ".join(data_sources)):
        data_sources_other = "Autre source (test)" if lang == "fr" else "Other source (test)"

    # Rubrique 12 : textes (optionnels) + confirmation
    open_q1 = "" if rng.random() < 0.65 else ("Commentaire libre (test)" if lang == "fr" else "Free comment (test)")
    open_q2 = "" if rng.random() < 0.70 else ("Indicateur manquant (test)" if lang == "fr" else "Missing indicator (test)")
    open_q3 = "" if rng.random() < 0.70 else ("Besoin d'appui (test)" if lang == "fr" else "Support need (test)")
    consulted_colleagues = "YES" if rng.random() < 0.62 else "NO"

    payload: Dict[str, Any] = {
        # R2
        "lang": lang,
        "organisation": organisation,
        "pays": iso3,
        "pays_name_fr": pays_name_fr,
        "pays_name_en": pays_name_en,
        "email": email,
        "type_acteur": type_acteur,
        "fonction": fonction,
        "fonction_autre": fonction_autre,

        # R3
        "scope": scope,
        "scope_other": scope_other,
        "snds_status": snds_status,

        # R4
        "preselected_domains": preselected,
        # alias de compatibilité (export admin actuel utilise parfois preselection_domains)
        "preselection_domains": preselected,
        "top5_domains": top5,

        # R5
        "selected_by_domain": selected_by_domain,
        "selected_stats": selected_stats,
        "scoring": scoring,
        "scoring_version": SCORING_VERSION,

        # R6–R8
        "gender_table": gender_table,
        "gender_priority_1": p1,
        "gender_priority_main": p1,
        "gender_priority_2": p2,
        "gender_priority_3": p3,
        "gender_priority_other": gender_priority_other,
        "capacity_table": capacity_table,

        # R9–R11
        "quality_expectations": quality_expectations,
        "quality_other": quality_other,
        "dissemination_channels": dissemination_channels,
        "dissemination_other": dissemination_other,
        "data_sources": data_sources,
        "data_sources_other": data_sources_other,

        # R12
        "open_q1": open_q1,
        "open_q2": open_q2,
        "open_q3": open_q3,
        "consulted_colleagues": consulted_colleagues,
    }
    return payload


# -------------------------
# Écriture DB (même schéma que l'app)
# -------------------------

def db_init(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS submissions(
            submission_id TEXT PRIMARY KEY,
            submitted_at_utc TEXT,
            lang TEXT,
            email TEXT,
            payload_json TEXT
        )
    """)
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_submissions_email ON submissions(email)")
    except Exception:
        pass
    con.commit()
    con.close()


def db_save_submission(db_path: Path, submission_id: str, lang: str, email: str, payload: Dict[str, Any]) -> None:
    db_init(db_path)
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO submissions(submission_id, submitted_at_utc, lang, email, payload_json)
        VALUES(?, ?, ?, ?, ?)
    """, (submission_id, payload.get("submitted_at_utc", ""), lang, (email or "").strip().lower(), json.dumps(payload, ensure_ascii=False)))
    con.commit()
    con.close()


# -------------------------
# Main
# -------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200, help="Nombre de soumissions à générer")
    ap.add_argument("--seed", type=int, default=22, help="Graine aléatoire (reproductibilité)")
    ap.add_argument("--lang-fr-ratio", type=float, default=0.75, help="Part des soumissions en FR (0–1)")
    ap.add_argument("--data-dir", type=str, default="data", help="Dossier contenant longlist.xlsx et COUNTRY_ISO3_with_EN.xlsx")
    ap.add_argument("--longlist-xlsx", type=str, default="", help="Chemin longlist.xlsx (si différent)")
    ap.add_argument("--countries-xlsx", type=str, default="", help="Chemin COUNTRY_ISO3_with_EN.xlsx (si différent)")
    ap.add_argument("--write-db", type=int, default=1, help="1 = écrire dans SQLite ; 0 = ne pas écrire")
    ap.add_argument("--db-path", type=str, default="responses.db", help="Chemin responses.db")
    ap.add_argument("--out-dir", type=str, default="exports_test", help="Dossier de sortie")
    args = ap.parse_args()

    rng = random.Random(args.seed)

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    longlist_xlsx = Path(args.longlist_xlsx) if args.longlist_xlsx else (data_dir / "longlist.xlsx")
    countries_xlsx = Path(args.countries_xlsx) if args.countries_xlsx else (data_dir / "COUNTRY_ISO3_with_EN.xlsx")

    longlist_df = load_longlist(longlist_xlsx=longlist_xlsx, longlist_csv=(data_dir / "indicator_longlist.csv"))
    countries_df = load_countries(countries_xlsx)
    ref = build_refdata(longlist_df, countries_df)

    rows = []
    payloads = []

    db_path = Path(args.db_path)

    for i in range(1, args.n + 1):
        submission_id = str(uuid.uuid4())
        payload = generate_payload(ref, i, rng, args.lang_fr_ratio)
        payload["submission_id"] = submission_id
        payload["submitted_at_utc"] = now_utc_iso()

        if args.write_db == 1:
            db_save_submission(db_path, submission_id, payload.get("lang", "fr"), payload.get("email", ""), payload)

        rows.append({
            "submission_id": submission_id,
            "submitted_at_utc": payload["submitted_at_utc"],
            "lang": payload.get("lang", ""),
            "email": payload.get("email", ""),
            "payload_json": json.dumps(payload, ensure_ascii=False),
        })
        payloads.append(payload)

    # Export CSV type "table submissions" (comme db_dump_csv_bytes)
    df = pd.DataFrame(rows)
    csv_path = out_dir / "submissions_export.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    # JSONL (payloads bruts)
    jsonl_path = out_dir / "payloads.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for p in payloads:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    print(f"OK : {args.n} soumissions générées")
    print(f"- CSV : {csv_path}")
    print(f"- JSONL : {jsonl_path}")
    if args.write_db == 1:
        print(f"- SQLite : {db_path} (table submissions)")


if __name__ == "__main__":
    main()
