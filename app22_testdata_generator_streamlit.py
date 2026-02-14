# -*- coding: utf-8 -*-
"""
App Streamlit : génération de 200 soumissions test + export + tables 10–14

Exécution (dans votre dépôt GitHub) :
  streamlit run app22_testdata_generator_streamlit.py

Pré-requis :
- ./data/longlist.xlsx
- ./data/COUNTRY_ISO3_with_EN.xlsx
- (optionnel) Classeur_analyse_app22.xlsx pour produire une version remplie

Sorties :
- exports_test/submissions_export.csv
- exports_test/payloads.jsonl
- exports_test/tables/10_Repondants.csv ... 14_Selections.csv
- (optionnel) Classeur_analyse_app22_rempli.xlsx
"""

from __future__ import annotations

import io
import os
import shutil
import zipfile
from pathlib import Path

import streamlit as st

# Importer les scripts fournis (même dossier)
from generate_test_submissions_app22 import load_longlist, load_countries, build_refdata, generate_payload, now_utc_iso, db_save_submission
from build_tables_10_14_from_export_app22 import build_tables, write_tables_csv, write_to_workbook, read_payloads


st.set_page_config(page_title="Générateur de données test app22", layout="wide")

st.title("Générateur de données test (app22)")
st.caption("Génère des soumissions réalistes, exporte CSV/JSONL, et produit les tables d’entrées 10–14 pour le classeur d’analyse.")

c1, c2, c3 = st.columns(3)
with c1:
    n = st.number_input("Nombre de soumissions", min_value=10, max_value=5000, value=200, step=10)
with c2:
    seed = st.number_input("Graine (seed)", min_value=0, max_value=10_000_000, value=22, step=1)
with c3:
    fr_ratio = st.slider("Part des soumissions en français", min_value=0.0, max_value=1.0, value=0.75, step=0.05)

st.divider()

data_dir = Path("data")
exports_dir = Path("exports_test")
tables_dir = exports_dir / "tables"
db_path = Path("responses.db")

longlist_xlsx = data_dir / "longlist.xlsx"
countries_xlsx = data_dir / "COUNTRY_ISO3_with_EN.xlsx"

if not longlist_xlsx.exists() or not countries_xlsx.exists():
    st.error("Fichiers manquants : vérifiez ./data/longlist.xlsx et ./data/COUNTRY_ISO3_with_EN.xlsx")
    st.stop()

write_db = st.checkbox("Écrire aussi dans SQLite (responses.db)", value=True)
build_tables_flag = st.checkbox("Produire les tables 10–14 (CSVs)", value=True)
fill_workbook_flag = st.checkbox("Produire un classeur d’analyse rempli", value=False)

workbook_template = st.text_input("Chemin du classeur modèle (optionnel)", value="Classeur_analyse_app22.xlsx")

if st.button("Générer"):
    import random, uuid, json
    import pandas as pd

    rng = random.Random(int(seed))

    ll = load_longlist(longlist_xlsx=longlist_xlsx, longlist_csv=(data_dir / "indicator_longlist.csv"))
    cc = load_countries(countries_xlsx)
    ref = build_refdata(ll, cc)

    exports_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    payloads = []

    with st.spinner("Génération en cours..."):
        for i in range(1, int(n) + 1):
            submission_id = str(uuid.uuid4())
            payload = generate_payload(ref, i, rng, float(fr_ratio))
            payload["submission_id"] = submission_id
            payload["submitted_at_utc"] = now_utc_iso()

            if write_db:
                db_save_submission(db_path, submission_id, payload.get("lang", "fr"), payload.get("email", ""), payload)

            rows.append({
                "submission_id": submission_id,
                "submitted_at_utc": payload["submitted_at_utc"],
                "lang": payload.get("lang", ""),
                "email": payload.get("email", ""),
                "payload_json": json.dumps(payload, ensure_ascii=False),
            })
            payloads.append(payload)

        df = pd.DataFrame(rows)
        csv_path = exports_dir / "submissions_export.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        jsonl_path = exports_dir / "payloads.jsonl"
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for p in payloads:
                f.write(json.dumps(p, ensure_ascii=False) + "\n")

        if build_tables_flag:
            tables = build_tables(payloads)
            write_tables_csv(tables_dir, tables)

            if fill_workbook_flag and workbook_template and Path(workbook_template).exists():
                out_xlsx = exports_dir / "Classeur_analyse_app22_rempli.xlsx"
                write_to_workbook(Path(workbook_template), out_xlsx, tables)

    st.success("Génération terminée.")

    st.write("Fichiers produits :")
    st.write(f"- {exports_dir / 'submissions_export.csv'}")
    st.write(f"- {exports_dir / 'payloads.jsonl'}")
    if build_tables_flag:
        st.write(f"- {tables_dir} (CSVs tables 10–14)")
    if write_db:
        st.write(f"- {db_path} (SQLite)")

    # ZIP pour téléchargement
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(csv_path, arcname="submissions_export.csv")
        z.write(jsonl_path, arcname="payloads.jsonl")
        if build_tables_flag and tables_dir.exists():
            for p in tables_dir.glob("*.csv"):
                z.write(p, arcname=f"tables/{p.name}")
        if fill_workbook_flag and (exports_dir / "Classeur_analyse_app22_rempli.xlsx").exists():
            z.write(exports_dir / "Classeur_analyse_app22_rempli.xlsx", arcname="Classeur_analyse_app22_rempli.xlsx")
        if write_db and db_path.exists():
            z.write(db_path, arcname="responses.db")

    zip_buf.seek(0)
    st.download_button(
        "Télécharger tout (ZIP)",
        data=zip_buf.getvalue(),
        file_name="app22_testdata_exports.zip",
        mime="application/zip",
    )
