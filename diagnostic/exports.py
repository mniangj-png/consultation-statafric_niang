"""Exports CSV UTF-8 BOM et JSON, sans dépendance tableur.

Les en-têtes de Contributions et Réponses reprennent le classeur source.
Les CSV sont destinés à une analyse interne : retirer les coordonnées ne suffit
pas à anonymiser des récits ou des noms d'institutions.
"""
from __future__ import annotations
import csv
import io
import json
import zipfile
from diagnostic.engine import answer_label,blank
from diagnostic.schema import FORMS,iter_fields,text

CONTRIBUTIONS=["ID réponse","Institution","Pays / zone","Profil","Sélection","Date invitation","Échéance","Relance 1","Relance 2","Date réponse","Type de contribution","Exploitabilité","Droits de citation","Fichier réponse"]
REPONSES=["ID réponse","Code question","Réponse brute","Code analytique","Statut information","Thème","Référence précise"]


def csv_safe(value):
    """Évite qu'un texte reçu soit interprété comme une formule par Excel."""
    if value is None: return ""
    s=str(value)
    if s.lstrip().startswith(("=","+","-","@","\t","\r","\n")) or s.startswith(("\t","\r","\n")):
        return "'"+s
    return s


def csv_bytes(rows: list[dict], columns: list[str]) -> bytes:
    out=io.StringIO(newline="")
    writer=csv.DictWriter(out,fieldnames=columns,delimiter=";",lineterminator="\r\n",extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({c:csv_safe(row.get(c,"")) for c in columns})
    return out.getvalue().encode("utf-8-sig")


def contribution_rows(records):
    out=[]
    for r in records:
        p=r["payload"]; form=FORMS[p["form_id"]]; a=p["answers"]
        fields={f["id"]:f for _,f in iter_fields(p["form_id"])}
        status=answer_label(fields[form["status"]],a.get(form["status"]))
        review=r.get("review") or {}
        out.append(dict(zip(CONTRIBUTIONS,[r["receipt"],a.get(form["institution"],""),a.get(form["country"],""),
              text(form["title"]),"Lien web — origine à qualifier","","2026-11-17","","",r["received_at"],
              ("Réponse abrégée — partielle ; " if p["form_id"]=="abrege" else "")+status,
              review.get("status","À examiner"),str(p["citation_permission"])+( " — choix explicite" if p["citation_explicit"] else " — anonymisation par défaut"),
              f"{r['receipt']}.json"])))
    return out


def response_rows(records):
    rows=[]
    for r in records:
        p=r["payload"]; answers=p["answers"]
        for s,f in iter_fields(p["form_id"]):
            if f.get("private"):
                continue
            fid=f["id"]; v=answers.get(fid); state=p["field_states"].get(fid,"NR_A_CONTROLER")
            values=v if isinstance(v,list) and v else [v]
            for item in values:
                if blank(item):
                    raw=""
                    # NR_A_CONTROLER est volontairement distinct de NR définitif.
                    code={"NF":"NF","FILTRE_INDETERMINE":"À clarifier","NR_A_CONTROLER":""}.get(state,"")
                    status={"NF":"NF","FILTRE_INDETERMINE":"Filtre amont non renseigné","NR_A_CONTROLER":"Blanc à contrôler"}.get(state,state)
                else:
                    raw=answer_label({**f,"type":"single"} if f["type"]=="multi" else f,item,p["language"])
                    code=item if f["type"] in ["single","multi"] else ""
                    status="NSP" if item==98 else ("NA" if item==99 else "Déclaré")
                rows.append(dict(zip(REPONSES,[r["receipt"],fid,raw,code,status,s["theme"],f"{r['receipt']}.json#{fid}"])))
    return rows


def dictionary_rows():
    rows=[]
    for form_id in FORMS:
        for s,f in iter_fields(form_id):
            for option in f["options"] or [{"code":"","label":{"fr":"Texte libre","en":"Free text"}}]:
                rows.append({"Questionnaire":form_id,"Rubrique":s["id"],"Code question":f["id"],
                    "Libellé français":text(f["label"],"fr"),"Libellé anglais":text(f["label"],"en"),
                    "Type":f["type"],"Code option":option["code"],"Option français":text(option["label"],"fr"),
                    "Option anglais":text(option["label"],"en"),"Contact séparé":bool(f.get("private")),
                    "Filtre rubrique":json.dumps(s.get("gate"),ensure_ascii=False),"Filtre champ":json.dumps(f.get("gate"),ensure_ascii=False),
                    "Source":s["source"]})
    return rows


def export_bundle(records):
    """Aucun contact ni clé serveur dans cette archive analytique."""
    bio=io.BytesIO()
    dictionary=dictionary_rows()
    with zipfile.ZipFile(bio,"w",compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Contributions.csv",csv_bytes(contribution_rows(records),CONTRIBUTIONS))
        z.writestr("Reponses.csv",csv_bytes(response_rows(records),REPONSES))
        z.writestr("Dictionnaire.csv",csv_bytes(dictionary,list(dictionary[0])))
        metadata=[]
        for r in records:
            p=r["payload"]
            safe={"receipt":r["receipt"],"received_at":r["received_at"],"payload":p,"review":r.get("review")}
            z.writestr(r["receipt"]+".json",json.dumps(safe,ensure_ascii=False,indent=2))
            metadata.append({"ID réponse":r["receipt"],"Questionnaire":p["form_id"],"Langue":p["language"],
                             "Version":p["schema_version"],"Empreinte instrument":p["instrument_hash"],"Test":p["is_test"],
                             "Permission explicite":p["citation_explicit"],"Complément":p["answers"].get(FORMS[p["form_id"]].get("previous",""),""),
                             "Alertes":" | ".join(p.get("warnings",[]))})
        meta_cols=list(metadata[0]) if metadata else ["ID réponse","Questionnaire","Langue","Version","Empreinte instrument","Test","Permission explicite","Complément","Alertes"]
        z.writestr("Metadonnees.csv",csv_bytes(metadata,meta_cols))
        z.writestr("LISEZ_MOI.txt", """HISWACA — export de travail à accès restreint\n\nLes contacts structurés sont exclus. Les textes libres et noms d'institutions peuvent rester identifiants : cet export n'est PAS automatiquement anonymisé.\nLes permissions de citation s'appliquent séparément à chaque réponse.\nLes en-têtes Contributions.csv et Reponses.csv correspondent au classeur du diagnostic. Importer via Données > À partir d'un fichier texte/CSV : UTF-8, séparateur point-virgule ; garder les identifiants et codes comme texte. Les codes de classes ne sont pas des quantités.\nBlanc à contrôler n'est pas NR confirmé. NF = non posé par filtre. Filtre amont non renseigné n'est pas NF.\nLes originaux JSON font foi pour le texte et les codes. Dans le CSV, une apostrophe de protection est ajoutée aux textes susceptibles d'être interprétés comme une formule.\nLa courte réponse demeure partielle. Plusieurs réponses d'une institution ne constituent pas plusieurs institutions. Aucune extrapolation, aucun score de maturité, aucun taux de réponse automatique.\nUn doublon ne doit pas être supprimé sans trace ; relier les identifiants et examiner les contradictions.\n""")
    return bio.getvalue()
