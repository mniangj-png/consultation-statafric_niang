"""Generate synthetic test responses for the STATAFRIC consultation Streamlit app.

This script can be used in two ways:
1) CLI (recommended for batch runs):
   python generate_test_responses_fixed_v3.py --app app22.py --n 200

2) Streamlit UI (for Streamlit Cloud / interactive runs):
   streamlit run generate_test_responses_fixed_v3.py

In Streamlit mode, the script displays a small UI, generates the requested number
of responses, writes them to JSONL and/or SQLite, and offers downloads.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import random
import sqlite3
import string
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_choice(seq: Sequence[Any]) -> Any:
    if not seq:
        raise ValueError("Empty sequence")
    return random.choice(list(seq))


def _safe_sample(seq: Sequence[Any], k: int) -> List[Any]:
    seq = list(seq)
    if k <= 0:
        return []
    if k >= len(seq):
        # return a shuffled copy (no ValueError)
        random.shuffle(seq)
        return seq
    return random.sample(seq, k=k)


def _resolve_path(user_value: str, fallbacks: Sequence[str]) -> Path:
    """Resolve a path that might exist in repo root or in ./data.

    If user_value exists as-is, use it.
    Otherwise try fallbacks (relative to cwd).
    """
    p = Path(user_value)
    if p.exists():
        return p
    for fb in fallbacks:
        q = Path(fb)
        if q.exists():
            return q
    raise FileNotFoundError(
        f"Fichier introuvable : '{user_value}'. Essais : {', '.join(fallbacks)}"
    )


def _is_running_in_streamlit() -> bool:
    """Detect whether the script is executed by Streamlit."""
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx  # type: ignore

        return get_script_run_ctx() is not None
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Read inputs (longlist + countries)
# -----------------------------------------------------------------------------


def load_longlist(longlist_xlsx: Path) -> Tuple[List[str], Dict[str, List[str]]]:
    """Return (domains, stats_by_domain).

    Expects columns like : domain_code / stat_code (names can vary; we try to infer).
    """
    df = pd.read_excel(longlist_xlsx, dtype=str).fillna("")
    cols = {c.strip().lower(): c for c in df.columns}

    # Very tolerant column detection
    domain_col = None
    stat_col = None
    for k, orig in cols.items():
        if domain_col is None and ("domain" in k or "domaine" in k):
            domain_col = orig
        if stat_col is None and ("stat" in k or "indicator" in k or "indicateur" in k):
            stat_col = orig

    if domain_col is None or stat_col is None:
        raise ValueError(
            "Colonnes longlist non reconnues. Attendu : une colonne domaine et une colonne statistique/indicateur."
        )

    stats_by_domain: Dict[str, List[str]] = {}
    for _, r in df.iterrows():
        d = str(r[domain_col]).strip()
        s = str(r[stat_col]).strip()
        if not d or not s:
            continue
        stats_by_domain.setdefault(d, []).append(s)

    domains = sorted(stats_by_domain.keys())
    # De-duplicate stats per domain while preserving order
    for d in domains:
        seen = set()
        unique = []
        for s in stats_by_domain[d]:
            if s in seen:
                continue
            seen.add(s)
            unique.append(s)
        stats_by_domain[d] = unique

    return domains, stats_by_domain


def load_countries(country_xlsx: Path) -> List[Tuple[str, str]]:
    """Return list of (iso3, country_en)."""
    df = pd.read_excel(country_xlsx, dtype=str).fillna("")
    cols = {c.strip().lower(): c for c in df.columns}
    iso_col = None
    en_col = None
    for k, orig in cols.items():
        if iso_col is None and "iso" in k and "3" in k:
            iso_col = orig
        if en_col is None and (k.endswith("_en") or "english" in k or "country_en" in k):
            en_col = orig
    if iso_col is None:
        # try a direct name
        for c in df.columns:
            if c.strip().upper() == "ISO3":
                iso_col = c
                break
    if en_col is None:
        # fallback : first non-ISO column
        en_col = next((c for c in df.columns if c != iso_col), None)

    if iso_col is None or en_col is None:
        raise ValueError("Fichier pays non reconnu (ISO3 + nom EN attendus).")

    out: List[Tuple[str, str]] = []
    for _, r in df.iterrows():
        iso3 = str(r[iso_col]).strip().upper()
        name = str(r[en_col]).strip()
        if iso3 and name:
            out.append((iso3, name))
    if not out:
        raise ValueError("Aucun pays valide détecté dans le fichier pays.")
    return out


# -----------------------------------------------------------------------------
# Extract option codes from app (best-effort)
# -----------------------------------------------------------------------------


@dataclass
class AppOptions:
    actor_type_codes: List[str]
    scope_codes: List[str]
    snds_codes: List[str]
    data_source_codes: List[str]
    r9_codes: List[str]
    r10_codes: List[str]
    gender_priority_codes: List[str]


class AppAstExtractor(ast.NodeVisitor):
    """Best-effort extractor for option codes defined in the app.

    We intentionally keep this tolerant; if extraction fails, we will fallback.
    """

    def __init__(self) -> None:
        self.found: Dict[str, List[str]] = {}
        self._current_assign: Optional[str] = None

    def visit_Assign(self, node: ast.Assign) -> Any:  # noqa: N802
        # Only handle simple "name = [...]" or "name = {..}" assignments.
        if len(node.targets) != 1:
            return
        t = node.targets[0]
        if isinstance(t, ast.Name):
            name = t.id
        else:
            return

        # Extract list literal of strings
        val = node.value
        if isinstance(val, ast.List):
            items = []
            ok = True
            for e in val.elts:
                if isinstance(e, ast.Constant) and isinstance(e.value, str):
                    items.append(e.value)
                else:
                    ok = False
                    break
            if ok and items:
                self.found[name] = items
        return


def extract_options_from_app(app_py: Path) -> Optional[AppOptions]:
    try:
        code = app_py.read_text(encoding="utf-8")
        tree = ast.parse(code)
        v = AppAstExtractor()
        v.visit(tree)

        def pick(*names: str) -> List[str]:
            for n in names:
                if n in v.found and v.found[n]:
                    return list(v.found[n])
            return []

        actor = pick("ACTOR_TYPE_CODES", "actor_type_codes", "actor_types")
        scope = pick("SCOPE_CODES", "scope_codes", "scopes")
        snds = pick("SNDS_CODES", "snds_codes", "snds_options")
        data_sources = pick("DATA_SOURCE_CODES", "data_source_codes", "source_codes")
        r9 = pick("R9_CODES", "r9_codes")
        r10 = pick("R10_CODES", "r10_codes")
        gender = pick("GENDER_PRIORITY_CODES", "gender_priority_codes")

        # If nothing useful, return None
        if not any([actor, scope, snds, data_sources, r9, r10, gender]):
            return None

        return AppOptions(
            actor_type_codes=actor,
            scope_codes=scope,
            snds_codes=snds,
            data_source_codes=data_sources,
            r9_codes=r9,
            r10_codes=r10,
            gender_priority_codes=gender,
        )
    except Exception:
        return None


def fallback_options() -> AppOptions:
    return AppOptions(
        actor_type_codes=[
            "NSO",
            "Ministry",
            "CentralBank",
            "REC",
            "AU",
            "DevelopmentPartner",
            "Academia",
            "CivilSociety",
            "PrivateSector",
            "Other",
        ],
        scope_codes=["National", "Regional", "Continental", "Global"],
        # Includes the new combined option mentioned (Option B)
        snds_codes=[
            "Yes_implemented",
            "No",
            "InPreparation",
            "InImplementation_NewInPreparation",
            "DNK",
        ],
        data_source_codes=[
            "Surveys",
            "Censuses",
            "Administrative",
            "CivilRegistration",
            "Geospatial",
            "Private",
            "Other",
        ],
        r9_codes=["Standard", "Metadata", "RevisionPolicy", "QAFramework", "Other"],
        r10_codes=["Calendar", "OpenData", "Microdata", "SDMX", "Other"],
        gender_priority_codes=[
            "Education",
            "Health",
            "EconomicEmpowerment",
            "Violence",
            "DecisionMaking",
            "Other",
        ],
    )


# -----------------------------------------------------------------------------
# Payload generation
# -----------------------------------------------------------------------------


def random_email(i: int) -> str:
    return f"test{i:03d}@example.org"


def random_org_name(i: int) -> str:
    return f"Test Organisation {i:03d}"


def random_string(n: int = 10) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def generate_payload(
    i: int,
    rid: str,
    email: str,
    org_name: str,
    countries: List[Tuple[str, str]],
    domains: List[str],
    stats_by_domain: Dict[str, List[str]],
    opts: AppOptions,
) -> Dict[str, Any]:
    iso3, country_en = _safe_choice(countries)

    actor_type = _safe_choice(opts.actor_type_codes)
    scope = _safe_choice(opts.scope_codes)

    # TOP 5 domains: if too few domains, take all
    top5_domains = _safe_sample(domains, k=min(5, len(domains)))

    # For each domain: pick up to 3 stats (if fewer available, take all)
    selected_stats: Dict[str, List[str]] = {}
    for d in top5_domains:
        stats = stats_by_domain.get(d, [])
        selected_stats[d] = _safe_sample(stats, k=min(3, len(stats)))

    # Scoring (Demand/Availability/Feasibility)
    # - Demand: 1..3
    # - Availability: 0..3 (Good availability = 3)
    # - Feasibility: 1..3
    scores: Dict[str, Dict[str, int]] = {}
    for d, stats in selected_stats.items():
        for s in stats:
            key = f"{d}::{s}"
            scores[key] = {
                "demand": random.randint(1, 3),
                "availability": random.randint(0, 3),
                "feasibility": random.randint(1, 3),
            }

    # SNDS status
    snds_status = _safe_choice(opts.snds_codes)

    # Gender priorities (rank 1-3)
    gender_priorities = _safe_sample(opts.gender_priority_codes, k=random.randint(1, 3))

    # Data sources (2-4)
    data_sources = _safe_sample(opts.data_source_codes, k=random.randint(2, 4))

    # R9 / R10 (1-3 each) with max_selections=3 in app
    r9 = _safe_sample(opts.r9_codes, k=random.randint(1, min(3, len(opts.r9_codes))))
    r10 = _safe_sample(opts.r10_codes, k=random.randint(1, min(3, len(opts.r10_codes))))

    payload: Dict[str, Any] = {
        "rid": rid,
        "meta": {
            "generated_at": _utc_now_iso(),
            "generator": "generate_test_responses_fixed_v3.py",
            "i": i,
        },
        "respondent": {
            "email": email,
            "organisation_name": org_name,
            "actor_type": actor_type,
            "scope": scope,
            "country_iso3": iso3,
            "country_en": country_en,
        },
        "snds_status": snds_status,
        "top5_domains": top5_domains,
        "selected_stats": selected_stats,
        "scores": scores,
        "gender_priorities": gender_priorities,
        "data_sources": data_sources,
        "r9": r9,
        "r10": r10,
        "open_questions": {
            "missing_indicators": f"Missing indicator example {i}",
            "comments": f"Comment {random_string(18)}",
            "consulted_colleagues": random.choice([True, False]),
        },
    }
    return payload


# -----------------------------------------------------------------------------
# Outputs (JSONL + SQLite)
# -----------------------------------------------------------------------------


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS responses (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rid TEXT NOT NULL,
  created_at TEXT NOT NULL,
  email TEXT,
  payload_json TEXT NOT NULL
);
"""


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(CREATE_TABLE_SQL)
        con.commit()
    finally:
        con.close()


def clear_db(db_path: Path) -> None:
    if not db_path.exists():
        return
    con = sqlite3.connect(str(db_path))
    try:
        con.execute("DELETE FROM responses")
        con.commit()
    finally:
        con.close()


def insert_row(db_path: Path, rid: str, created_at: str, email: str, payload: Dict[str, Any]) -> None:
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(
            "INSERT INTO responses (rid, created_at, email, payload_json) VALUES (?, ?, ?, ?)",
            (rid, created_at, email, json.dumps(payload, ensure_ascii=False)),
        )
        con.commit()
    finally:
        con.close()


def write_jsonl(jsonl_path: Path, payloads: List[Dict[str, Any]]) -> None:
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("w", encoding="utf-8") as f:
        for p in payloads:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate synthetic test responses")
    p.add_argument("--app", default="app22.py", help="Path to app python file")
    p.add_argument("--longlist", default="data/longlist.xlsx", help="Path to longlist.xlsx")
    p.add_argument(
        "--countries",
        default="data/COUNTRY_ISO3_with_EN.xlsx",
        help="Path to COUNTRY_ISO3_with_EN.xlsx",
    )
    p.add_argument("--db", default="responses.db", help="SQLite DB output")
    p.add_argument("--jsonl", default="test_responses.jsonl", help="JSONL output")
    p.add_argument("--n", type=int, default=200, help="Number of responses")
    p.add_argument("--seed", type=int, default=42, help="Random seed")
    p.add_argument("--clear", action="store_true", help="Clear DB table before insert")
    p.add_argument("--no-db", action="store_true", help="Do not write SQLite")
    p.add_argument("--no-jsonl", action="store_true", help="Do not write JSONL")
    return p


def main_cli(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    random.seed(args.seed)

    # Resolve paths (root or ./data)
    app_py = _resolve_path(args.app, [args.app])
    longlist_xlsx = _resolve_path(args.longlist, ["longlist.xlsx", "data/longlist.xlsx", args.longlist])
    countries_xlsx = _resolve_path(
        args.countries,
        ["COUNTRY_ISO3_with_EN.xlsx", "data/COUNTRY_ISO3_with_EN.xlsx", args.countries],
    )
    db_path = Path(args.db)
    jsonl_path = Path(args.jsonl)

    domains, stats_by_domain = load_longlist(longlist_xlsx)
    countries = load_countries(countries_xlsx)

    opts = extract_options_from_app(app_py) or fallback_options()

    payloads: List[Dict[str, Any]] = []
    for i in range(1, args.n + 1):
        rid = f"RID_{i:04d}_{random_string(8)}"
        email = random_email(i)
        org = random_org_name(i)
        payloads.append(
            generate_payload(
                i=i,
                rid=rid,
                email=email,
                org_name=org,
                countries=countries,
                domains=domains,
                stats_by_domain=stats_by_domain,
                opts=opts,
            )
        )

    if not args.no_jsonl:
        write_jsonl(jsonl_path, payloads)
        print(f"JSONL écrit : {jsonl_path.resolve()}")

    if not args.no_db:
        init_db(db_path)
        if args.clear:
            clear_db(db_path)
        for p in payloads:
            insert_row(
                db_path=db_path,
                rid=p["rid"],
                created_at=p["meta"]["generated_at"],
                email=p["respondent"]["email"],
                payload=p,
            )
        print(f"SQLite écrit : {db_path.resolve()}")

    print(f"OK : {len(payloads)} réponses générées")
    return 0


# -----------------------------------------------------------------------------
# Streamlit UI
# -----------------------------------------------------------------------------


def main_streamlit() -> None:
    import streamlit as st  # type: ignore

    st.set_page_config(page_title="Générateur de tests", layout="wide")
    st.title("Générateur de réponses test")
    st.caption(
        "Ce module génère des réponses synthétiques (tests) à partir de la longlist et du fichier pays. "
        "Il peut écrire un JSONL et/ou une base SQLite (responses.db)."
    )

    with st.expander("Paramètres", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            n = st.number_input("Nombre de réponses", min_value=1, max_value=5000, value=200, step=10)
            seed = st.number_input("Seed aléatoire", min_value=0, max_value=10_000_000, value=42, step=1)
        with col2:
            app_path = st.text_input("Fichier app (pour extraire des options)", value="app22.py")
            longlist_path = st.text_input("Longlist .xlsx", value="data/longlist.xlsx")
            countries_path = st.text_input("Pays .xlsx", value="data/COUNTRY_ISO3_with_EN.xlsx")
        with col3:
            db_path_str = st.text_input("SQLite (responses.db)", value="responses.db")
            jsonl_path_str = st.text_input("JSONL", value="test_responses.jsonl")
            clear_first = st.checkbox("Vider la table responses avant insertion", value=False)
            write_db = st.checkbox("Écrire SQLite", value=True)
            write_jsonl_flag = st.checkbox("Écrire JSONL", value=True)

    # Resolve files
    st.subheader("Fichiers détectés")
    try:
        app_py = _resolve_path(app_path, [app_path])
        longlist_xlsx = _resolve_path(longlist_path, ["longlist.xlsx", "data/longlist.xlsx", longlist_path])
        countries_xlsx = _resolve_path(
            countries_path,
            ["COUNTRY_ISO3_with_EN.xlsx", "data/COUNTRY_ISO3_with_EN.xlsx", countries_path],
        )
        st.write(
            {
                "app": str(app_py),
                "longlist": str(longlist_xlsx),
                "countries": str(countries_xlsx),
            }
        )
    except Exception as e:
        st.error(str(e))
        st.stop()

    # Quick validation preview
    try:
        domains, stats_by_domain = load_longlist(longlist_xlsx)
        countries = load_countries(countries_xlsx)
        st.write(
            {
                "Nb domaines": len(domains),
                "Nb pays": len(countries),
                "Exemple domaine": domains[0] if domains else None,
            }
        )
    except Exception as e:
        st.error(f"Erreur lecture fichiers : {e}")
        st.stop()

    opts = extract_options_from_app(app_py) or fallback_options()

    st.divider()
    run = st.button("Générer maintenant")
    if not run:
        st.info(
            "Cliquez sur ‘Générer maintenant’. Le traitement peut prendre quelques secondes. "
            "Les fichiers seront écrits dans le répertoire de l’application (Streamlit Cloud : système de fichiers local de l’instance)."
        )
        return

    random.seed(int(seed))
    db_path = Path(db_path_str)
    jsonl_path = Path(jsonl_path_str)

    t0 = time.time()
    progress = st.progress(0)
    payloads: List[Dict[str, Any]] = []
    for i in range(1, int(n) + 1):
        rid = f"RID_{i:04d}_{random_string(8)}"
        email = random_email(i)
        org = random_org_name(i)
        payloads.append(
            generate_payload(
                i=i,
                rid=rid,
                email=email,
                org_name=org,
                countries=countries,
                domains=domains,
                stats_by_domain=stats_by_domain,
                opts=opts,
            )
        )
        if i % max(1, int(n) // 50) == 0:
            progress.progress(min(100, int(i * 100 / int(n))))
    progress.progress(100)

    # Write outputs
    if write_jsonl_flag:
        write_jsonl(jsonl_path, payloads)
    if write_db:
        init_db(db_path)
        if clear_first:
            clear_db(db_path)
        for p in payloads:
            insert_row(
                db_path=db_path,
                rid=p["rid"],
                created_at=p["meta"]["generated_at"],
                email=p["respondent"]["email"],
                payload=p,
            )

    dt = time.time() - t0
    st.success(f"Terminé : {len(payloads)} réponses générées en {dt:.1f} s")

    # Show sample
    st.subheader("Exemple (1 enregistrement)")
    st.json(payloads[0])

    # Download buttons
    st.subheader("Téléchargements")
    if write_jsonl_flag and jsonl_path.exists():
        st.download_button(
            label="Télécharger le JSONL",
            data=jsonl_path.read_bytes(),
            file_name=jsonl_path.name,
            mime="application/json",
        )
    if write_db and db_path.exists():
        st.download_button(
            label="Télécharger la base SQLite",
            data=db_path.read_bytes(),
            file_name=db_path.name,
            mime="application/octet-stream",
        )


def main() -> None:
    if _is_running_in_streamlit():
        main_streamlit()
    else:
        raise SystemExit(main_cli())


if __name__ == "__main__":
    main()
