#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Génère des réponses fictives (tests) pour l'application Streamlit v18 (P176371),
en insérant directement des soumissions dans la base SQLite (table submissions).

Sortie :
- Ajoute N lignes dans responses.db (par défaut) avec payload_json cohérent (scores, domaines, etc.)

Utilisation (Windows 11, PowerShell) :
  python generate_fake_submissions_v18.py --n 300 --db responses.db --longlist longlist.xlsx

Recommandation :
- Travaillez sur une COPIE de la base (ex : responses_TEST.db) pour éviter toute confusion avec des données réelles.
"""

import argparse
import json
import random
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


# ---------------------------------------------------------------------
# Paramètres "questionnaire"
# ---------------------------------------------------------------------

ACTOR_TYPES = ["NSO", "Ministry", "REC", "AU", "CivilSoc", "DevPartner", "Academia", "Other"]
SCOPES = ["National", "Regional", "Continental", "Global", "Other"]

# Rubrique 9 (qualité/harmonisation) - libellés comme dans l'export actuel
QUALITY_CHOICES_FR = [
    "Normes internationales (ONU, FMI, UA, etc.)",
    "Méthodologies harmonisées au niveau continental",
    "Calendrier de diffusion et révisions documentées",
    "Accès aux métadonnées",
    "Autre",
]
QUALITY_CHOICES_EN = [
    "International standards (UN, IMF, AU, etc.)",
    "Harmonized methodologies at continental level",
    "Release calendar and documented revisions",
    "Access to metadata",
    "Other",
]

# Rubrique 10 (diffusion)
DISS_CHOICES_FR = [
    "Portail web / tableaux de bord",
    "Communiqués / notes de conjoncture",
    "Microdonnées anonymisées (accès sécurisé)",
    "API / Open Data",
    "Ateliers / webinaires",
    "Autre",
]
DISS_CHOICES_EN = [
    "Web portal / dashboards",
    "Press releases / bulletins",
    "Anonymized microdata (secure access)",
    "API / Open Data",
    "Workshops / webinars",
    "Other",
]

# Tables (IMPORTANT : clés alignées avec les mappings du flatten_payload actuel)
GENDER_KEYS_FR = ["Sexe", "Âge", "Milieu urbain/rural", "Handicap", "Quintile de richesse"]
GENDER_KEYS_EN = ["Sex", "Age", "Urban/rural residence", "Disability", "Wealth quintile"]
GENDER_CODES = ["YES", "NO", "SPEC", "UK"]

CAPACITY_KEYS_FR = [
    "Compétences (RH)",
    "Accès aux données administratives",
    "Financement",
    "Outils numériques",
    "Cadre juridique",
    "Coordination institutionnelle",
]
CAPACITY_KEYS_EN = [
    "Human resources skills",
    "Access to administrative data",
    "Funding",
    "Digital tools",
    "Legal framework",
    "Institutional coordination",
]
CAPACITY_CODES = ["HIGH", "MED", "LOW", "UK"]

SCORING_VERSION = 3  # bonne disponibilité = 3


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def ensure_schema(db_path: Path) -> None:
    con = sqlite3.connect(str(db_path))
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
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_submissions_email ON submissions(email)")
    except Exception:
        pass
    con.commit()
    con.close()


def load_longlist(longlist_path: Path) -> Tuple[List[str], Dict[str, List[str]]]:
    """
    Retourne :
      - liste des codes domaines (ex : D01..)
      - mapping domaine -> liste des codes stats (ex : D01S01..)
    """
    xls = pd.ExcelFile(longlist_path)
    df = xls.parse("longlist")

    # Domain codes
    domains = sorted(df["Domain_code"].dropna().astype(str).unique().tolist())

    # Stats codes : extraits des colonnes Stat_label_fr/en (format 'D01S01|Label')
    def extract_stat_code(s) -> str:
        if pd.isna(s):
            return ""
        s = str(s)
        if "|" in s:
            return s.split("|", 1)[0].strip()
        # fallback : si déjà un code
        m = None
        if len(s) >= 6:
            m = s[:6]
        return m or ""

    df["stat_code"] = df["Stat_label_fr"].apply(extract_stat_code)
    df.loc[df["stat_code"].eq(""), "stat_code"] = df["Stat_label_en"].apply(extract_stat_code)

    dom_to_stats: Dict[str, List[str]] = {d: [] for d in domains}
    for _, r in df.iterrows():
        d = str(r.get("Domain_code", "")).strip()
        sc = str(r.get("stat_code", "")).strip()
        if d and sc and d in dom_to_stats:
            dom_to_stats[d].append(sc)

    # Uniques + tri
    for d in dom_to_stats:
        dom_to_stats[d] = sorted(list({x for x in dom_to_stats[d] if x}))

    # Retirer domaines sans stats
    domains = [d for d in domains if len(dom_to_stats.get(d, [])) > 0]
    return domains, dom_to_stats


def weighted_choice(values: List[int], weights: List[float]) -> int:
    return random.choices(values, weights=weights, k=1)[0]


def gen_scoring_for_stats(stats: List[str]) -> Dict[str, Dict[str, int]]:
    """
    Génère les 3 scores (0-3) pour chaque statistique sélectionnée.
    0 = NSP ; 3 = élevé / bonne disponibilité / faisable.
    """
    out: Dict[str, Dict[str, int]] = {}
    for s in stats:
        demand = weighted_choice([0, 1, 2, 3], [0.10, 0.15, 0.35, 0.40])
        availability = weighted_choice([0, 1, 2, 3], [0.15, 0.25, 0.30, 0.30])
        feasibility = weighted_choice([0, 1, 2, 3], [0.15, 0.25, 0.35, 0.25])
        out[s] = {"demand": demand, "availability": availability, "feasibility": feasibility}
    return out


def gen_table(keys: List[str], codes: List[str], p_spec: float = 0.15) -> Dict[str, Dict[str, str]]:
    """
    Format attendu par flatten_payload : {label: {"code": ..., "spec": ...}}
    """
    tbl: Dict[str, Dict[str, str]] = {}
    for k in keys:
        code = random.choices(codes, weights=[0.40, 0.25, 0.20, 0.15], k=1)[0] if len(codes) == 4 else random.choices(codes, weights=[0.30, 0.35, 0.25, 0.10], k=1)[0]
        spec = ""
        if code == "SPEC" and random.random() < 0.9:
            spec = random.choice([
                "Selon le sous-secteur / According to sub-sector",
                "Selon source de données / Depends on data source",
                "Uniquement certaines enquêtes / Only some surveys",
            ])
        if code in ("LOW", "UK") and random.random() < 0.25:
            spec = random.choice([
                "Besoin de renforcement / Capacity building needed",
                "Contraintes institutionnelles / Institutional constraints",
                "Financement insuffisant / Insufficient funding",
            ])
        tbl[k] = {"code": code, "spec": spec}
    return tbl


def gen_quality(lang: str) -> Tuple[List[str], str]:
    choices = QUALITY_CHOICES_FR if lang == "fr" else QUALITY_CHOICES_EN
    # Au moins 1 option ; souvent 2-3
    k = random.choices([1, 2, 3, 4], weights=[0.20, 0.40, 0.30, 0.10], k=1)[0]
    picked = random.sample(choices[:-1], k=min(k, len(choices)-1))  # sans "Autre"
    other = ""
    if random.random() < 0.10:
        picked.append(choices[-1])  # Autre/Other
        other = random.choice([
            "Renforcer l’audit qualité et la traçabilité",
            "Improve revision policies and documentation",
            "Harmoniser les nomenclatures et classifications",
        ])
    return picked, other


def gen_dissemination(lang: str) -> Tuple[List[str], str]:
    choices = DISS_CHOICES_FR if lang == "fr" else DISS_CHOICES_EN
    k = random.choices([1, 2, 3, 4], weights=[0.15, 0.35, 0.35, 0.15], k=1)[0]
    picked = random.sample(choices[:-1], k=min(k, len(choices)-1))
    other = ""
    if random.random() < 0.10:
        picked.append(choices[-1])
        other = random.choice([
            "Diffusion via rapports sectoriels",
            "Dissemination through sector reports",
            "Plateforme open data nationale",
        ])
    return picked, other


def gen_open_text(lang: str) -> Tuple[str, str, str]:
    if lang == "fr":
        q1 = random.choice([
            "Prioriser des indicateurs alignés à l’Agenda 2063 et aux ODD.",
            "Assurer une désagrégation systématique par sexe quand pertinent.",
            "Renforcer la coordination SSN et la diffusion simultanée.",
        ])
        q2 = random.choice([
            "Indicateurs de qualité de l’emploi (informel, sous-emploi).",
            "Statistiques sur la protection sociale et les filets sociaux.",
            "Données sur la couverture sanitaire universelle (CSU).",
        ])
        q3 = random.choice([
            "Appui à la normalisation, métadonnées et calendrier de diffusion.",
            "Renforcement des capacités (RH, outils, gouvernance).",
            "Accès aux données administratives et interopérabilité.",
        ])
    else:
        q1 = random.choice([
            "Prioritize indicators aligned with Agenda 2063 and the SDGs.",
            "Ensure systematic sex-disaggregation where relevant.",
            "Strengthen NSS coordination and simultaneous release.",
        ])
        q2 = random.choice([
            "Employment quality indicators (informality, underemployment).",
            "Social protection and safety nets statistics.",
            "Universal health coverage indicators.",
        ])
        q3 = random.choice([
            "Support on standards, metadata and release calendars.",
            "Capacity building (HR, tools, governance).",
            "Access to administrative data and interoperability.",
        ])
    return q1, q2, q3


def build_payload(i: int, domains: List[str], dom_to_stats: Dict[str, List[str]]) -> Dict:
    lang = random.choices(["fr", "en"], weights=[0.60, 0.40], k=1)[0]

    submission_id = str(uuid.uuid4())
    submitted_at = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 60*24*10))  # sur 10 jours
    submitted_at_utc = submitted_at.strftime("%Y-%m-%dT%H:%M:%SZ")

    actor = random.choices(
        ACTOR_TYPES,
        weights=[0.18, 0.22, 0.10, 0.08, 0.12, 0.18, 0.10, 0.02],
        k=1
    )[0]
    scope = random.choices(SCOPES, weights=[0.45, 0.20, 0.20, 0.10, 0.05], k=1)[0]

    scope_other = ""
    if scope == "Other":
        scope_other = "Sous-national / programme spécifique" if lang == "fr" else "Sub-national / specific programme"

    # Identité
    orgs_fr = ["ANSD", "Ministère de la Santé", "Ministère de l’Éducation", "Commission UA", "Université", "Partenaire technique"]
    orgs_en = ["NSO", "Ministry of Health", "Ministry of Education", "AU Commission", "University", "Development partner"]
    organisation = random.choice(orgs_fr if lang == "fr" else orgs_en)

    countries = ["Sénégal", "Côte d’Ivoire", "Gabon", "Bénin", "Cameroun", "Kenya", "Rwanda", "Nigeria", "Ghana", "Togo"]
    pays = random.choice(countries)

    fonctions_fr = ["Directeur", "Chef de service statistique", "Analyste", "Chargé de programme", "Consultant", "Enseignant-chercheur"]
    fonctions_en = ["Director", "Head of statistics unit", "Analyst", "Programme officer", "Consultant", "Lecturer/Researcher"]
    fonction = random.choice(fonctions_fr if lang == "fr" else fonctions_en)

    email = f"test_{i:04d}@example.org"

    # Domaines : présélection 5-10, puis TOP 5 (rang)
    nb_pre = random.randint(5, 10)
    preselection = random.sample(domains, k=nb_pre)
    top5 = random.sample(preselection, k=5)

    # Stats : 1-3 par domaine du top5
    selected_by_domain: Dict[str, List[str]] = {}
    selected_stats: List[str] = []
    for d in top5:
        pool = dom_to_stats[d]
        k = random.choices([1, 2, 3], weights=[0.45, 0.40, 0.15], k=1)[0]
        k = min(k, len(pool))
        picks = random.sample(pool, k=k)
        selected_by_domain[d] = picks
        selected_stats.extend(picks)
    # Dé-doublonnage au cas où (normalement inutile)
    selected_stats = sorted(list(dict.fromkeys(selected_stats)))

    scoring = gen_scoring_for_stats(selected_stats)

    # Tables
    gender_keys = GENDER_KEYS_FR if lang == "fr" else GENDER_KEYS_EN
    capacity_keys = CAPACITY_KEYS_FR if lang == "fr" else CAPACITY_KEYS_EN
    gender_table = gen_table(gender_keys, GENDER_CODES)
    capacity_table = gen_table(capacity_keys, CAPACITY_CODES)

    quality_expectations, quality_other = gen_quality(lang)
    dissemination_channels, dissemination_other = gen_dissemination(lang)
    open_q1, open_q2, open_q3 = gen_open_text(lang)

    payload = {
        "lang": lang,
        "organisation": organisation,
        "pays": pays,
        "email": email,
        "type_acteur": actor,
        "fonction": fonction,
        "fonction_autre": "",
        "scope": scope,
        "scope_other": scope_other,

        # IMPORTANT : clés attendues par flatten_payload actuel
        "preselection_domains": preselection,
        "top5_domains": top5,

        "selected_by_domain": selected_by_domain,
        "selected_stats": selected_stats,
        "scoring": scoring,
        "scoring_version": SCORING_VERSION,

        "gender_table": gender_table,
        "capacity_table": capacity_table,

        "quality_expectations": quality_expectations,
        "quality_other": quality_other,
        "dissemination_channels": dissemination_channels,
        "dissemination_other": dissemination_other,

        "open_q1": open_q1,
        "open_q2": open_q2,
        "open_q3": open_q3,

        # redondants
        "submission_id": submission_id,
        "submitted_at_utc": submitted_at_utc,
    }
    return payload


def insert_payload(db_path: Path, payload: Dict) -> None:
    submission_id = payload["submission_id"]
    submitted_at_utc = payload["submitted_at_utc"]
    lang = payload.get("lang", "")
    email = payload.get("email", "")
    payload_json = json.dumps(payload, ensure_ascii=False)

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO submissions(submission_id, submitted_at_utc, lang, email, payload_json) VALUES (?, ?, ?, ?, ?)",
        (submission_id, submitted_at_utc, lang, email, payload_json),
    )
    con.commit()
    con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=300, help="Nombre de réponses à générer (défaut : 300).")
    ap.add_argument("--db", type=str, default="responses.db", help="Chemin vers la base SQLite (défaut : responses.db).")
    ap.add_argument("--longlist", type=str, default="longlist.xlsx", help="Chemin du fichier longlist.xlsx (défaut : longlist.xlsx).")
    ap.add_argument("--seed", type=int, default=12345, help="Graine aléatoire pour reproductibilité.")
    args = ap.parse_args()

    random.seed(args.seed)

    db_path = Path(args.db).resolve()
    longlist_path = Path(args.longlist).resolve()

    if not longlist_path.exists():
        raise FileNotFoundError(f"Fichier longlist introuvable : {longlist_path}")

    ensure_schema(db_path)

    domains, dom_to_stats = load_longlist(longlist_path)

    for i in range(1, args.n + 1):
        payload = build_payload(i, domains, dom_to_stats)
        insert_payload(db_path, payload)

    print(f"OK : {args.n} réponses fictives insérées dans : {db_path}")
    print("Conseil : dans l’app, utilisez l’export Excel pour vérifier les champs (submissions / raw_json).")


if __name__ == "__main__":
    main()
