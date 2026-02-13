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


def load_countries(countries_xlsx: str) -> List[Country]:
    df = pd.read_excel(countries_xlsx, dtype=str).fillna("")
    # Colonnes attendues : COUNTRY_VALUE (ISO3 | FR), COUNTRY_VALUE_EN (ISO3 | EN)
    if "COUNTRY_VALUE" not in df.columns or "COUNTRY_VALUE_EN" not in df.columns:
        raise ValueError("Fichier pays inattendu : colonnes COUNTRY_VALUE / COUNTRY_VALUE_EN manquantes.")

    out: List[Country] = []
    for _, r in df.iterrows():
        fr = str(r["COUNTRY_VALUE"]).strip()
        en = str(r["COUNTRY_VALUE_EN"]).strip()

        if "|" in fr:
            iso3 = fr.split("|", 1)[0].strip()
            name_fr = fr.split("|", 1)[1].strip()
        else:
            iso3 = fr.strip()
            name_fr = ""

        if "|" in en:
            name_en = en.split("|", 1)[1].strip()
        else:
            name_en = ""

        if iso3:
            out.append(Country(iso3=iso3, name_fr=name_fr, name_en=name_en))
    if not out:
        raise ValueError("Aucun pays chargé depuis le fichier.")
    return out


def load_longlist(longlist_xlsx: str) -> Tuple[List[str], Dict[str, List[str]]]:
    """
    Retourne :
    - liste des domain_code
    - dict domain_code -> liste de stat_code
    En se basant sur les colonnes attendues de l'app :
      Domain_code, Domain_label_fr, Stat_label_fr (+ éventuellement EN)
    """
    df = pd.read_excel(longlist_xlsx, dtype=str).fillna("")
    required = {"Domain_code", "Domain_label_fr", "Stat_label_fr"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"Longlist inattendue : colonnes requises manquantes : {sorted(required)}")

    df["domain_code"] = df["Domain_code"].astype(str).str.strip()
    df["stat_code"] = df["Stat_label_fr"].astype(str).str.split("|", n=1).str[0].str.strip()

    domains = sorted([d for d in df["domain_code"].unique().tolist() if str(d).strip() != ""])
    stats_by_domain: Dict[str, List[str]] = {}
    for d in domains:
        tmp = df[df["domain_code"] == d]["stat_code"].astype(str).str.strip()
        stats = sorted([s for s in tmp.unique().tolist() if s])
        stats_by_domain[d] = stats

    if not domains:
        raise ValueError("Aucun domaine dans la longlist.")
    return domains, stats_by_domain


# -------------------------
# Extraction des options depuis l'app (AST)
# -------------------------

class AppAstExtractor:
    """
    Lit app22.py sans l'exécuter (pas d'import Streamlit).
    Extrait des listes / options utiles (si disponibles) pour rester synchronisé avec l'app.
    """

    def __init__(self, app_path: str):
        self.app_path = app_path
        self._tree = None
        self._src = None

    def _load(self):
        if self._tree is None:
            with open(self.app_path, "r", encoding="utf-8") as f:
                self._src = f.read()
            self._tree = ast.parse(self._src)

    def get_global_str_list(self, name: str) -> Optional[List[str]]:
        self._load()
        assert self._tree is not None
        for node in self._tree.body:
            if isinstance(node, ast.Assign):
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name) and tgt.id == name:
                        if isinstance(node.value, ast.List) and all(isinstance(e, ast.Constant) and isinstance(e.value, str) for e in node.value.elts):
                            return [e.value for e in node.value.elts]  # type: ignore
        return None

    def get_func_str_list(self, func_name: str, var_name: str) -> Optional[List[str]]:
        self._load()
        assert self._tree is not None
        for node in self._tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == func_name:
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Assign):
                        for tgt in sub.targets:
                            if isinstance(tgt, ast.Name) and tgt.id == var_name:
                                if isinstance(sub.value, ast.List) and all(isinstance(e, ast.Constant) and isinstance(e.value, str) for e in sub.value.elts):
                                    return [e.value for e in sub.value.elts]  # type: ignore
        return None

    def get_func_tuple_codes(self, func_name: str, var_name: str) -> Optional[List[str]]:
        """
        Extrait la 2e composante (code) d'une liste de tuples (label, code)
        où 'code' est un literal string.
        """
        self._load()
        assert self._tree is not None
        for node in self._tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == func_name:
                for sub in ast.walk(node):
                    if isinstance(sub, ast.Assign):
                        for tgt in sub.targets:
                            if isinstance(tgt, ast.Name) and tgt.id == var_name:
                                if isinstance(sub.value, ast.List):
                                    codes: List[str] = []
                                    ok = True
                                    for e in sub.value.elts:
                                        if isinstance(e, ast.Tuple) and len(e.elts) >= 2 and isinstance(e.elts[1], ast.Constant) and isinstance(e.elts[1].value, str):
                                            codes.append(e.elts[1].value)
                                        else:
                                            ok = False
                                            break
                                    return codes if ok else None
        return None


# -------------------------
# Génération de réponses
# -------------------------

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
    parser.add_argument("--longlist", type=str, default="longlist.xlsx", help="Chemin longlist.xlsx.")
    parser.add_argument("--countries", type=str, default="COUNTRY_ISO3_with_EN.xlsx", help="Chemin COUNTRY_ISO3_with_EN.xlsx.")
    parser.add_argument("--app", type=str, default="app22.py", help="Chemin app22.py (pour extraire options si possible).")
    parser.add_argument("--lang", type=str, default="mixed", choices=["fr", "en", "mixed"], help="Langue des réponses.")
    parser.add_argument("--jsonl", type=str, default="test_payloads.jsonl", help="Fichier JSONL de sortie (optionnel).")
    parser.add_argument("--no-jsonl", action="store_true", help="Ne pas écrire de JSONL.")

    args = parser.parse_args()
    random.seed(args.seed)

    domains, stats_by_domain = load_longlist(args.longlist)
    countries = load_countries(args.countries)

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
