#!/usr/bin/env python3
"""Produit le dictionnaire et le schéma versionné, sans réponse de participant."""
from pathlib import Path
import json
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from diagnostic.schema import FORMS,SCHEMA_VERSION
from diagnostic.exports import dictionary_rows,csv_bytes
from diagnostic.engine import schema_hash

def main():
    destination=Path(__file__).resolve().parents[1]/'docs'
    rows=dictionary_rows()
    (destination/'Dictionnaire_questionnaires_FR_EN.csv').write_bytes(csv_bytes(rows,list(rows[0])))
    data={'schema_version':SCHEMA_VERSION,'hashes':{f:schema_hash(f) for f in FORMS},'forms':FORMS}
    (destination/'questionnaires.schema.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    print('Dictionnaire et schéma créés ; aucune réponse recueillie.')

if __name__=='__main__':main()
