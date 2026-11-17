"""Regenerate fallback forms, dictionaries and blank registers. No real responses."""
import sys,csv,io,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from src.runtime import *

def main():
    out=ROOT/'outils';out.mkdir(exist_ok=True);forms=out/'formulaires_repli';forms.mkdir(exist_ok=True)
    for f in FORMS:
        for l in LANGUAGES:(forms/f'{f}_{l}.txt').write_bytes(fallback_text(f,l))
    rows=dictionary_rows();(out/'Dictionnaire_5_langues.csv').write_bytes(csv_bytes(rows,list(rows[0])))
    for k,meta in REGISTERS.items():(out/f'Modele_{k}.csv').write_bytes(csv_bytes([],list(meta['columns'])))
    matrix=[{'Parcours':f,'Langue':l,'Version':SCHEMA_VERSION,'Statut final':'À vérifier'} for f in FORMS for l in LANGUAGES]
    (out/'Recette_25_combinaisons.csv').write_bytes(csv_bytes(matrix,list(REGISTERS['recette']['columns'])))
    journal=[{'Parcours':f,'Langue':l,'Version source':FORMS[f]['version'],'Version traduction':'2026-11-16.v2','État rédaction':'Source française transposée' if l=='fr' else 'Traduction rédigée','Relecture indépendante':'Non documentée','Test distant':'Non effectué','Autorisation diffusion':'À renseigner'} for f in FORMS for l in LANGUAGES]
    (out/'Journal_traductions.csv').write_bytes(csv_bytes(journal,list(journal[0])))
    rows=[]
    for fid,form in FORMS.items():
        for sec in form['sections']:rows.append({'Parcours':fid,'Rubrique':sec['id'],'Titre FR':text(sec['title']),'Source':sec['source'],'Champs':len(sec['fields']),'Codes conservés':'Oui','Version':SCHEMA_VERSION})
    (out/'Concordance_methodologie.csv').write_bytes(csv_bytes(rows,list(rows[0])))
    print('25 formulaires de repli, dictionnaire, huit modèles, matrice de recette, journal et concordance générés.')
if __name__=='__main__':main()
