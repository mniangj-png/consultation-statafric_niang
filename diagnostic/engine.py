"""Logique pure : filtres, validation, normalisation et brouillons.

Aucune dépendance à Streamlit ou à une base de données. Les tests unitaires
peuvent s'exécuter hors connexion. Les réponses brutes ne sont jamais remplacées
par une hypothèse ; un filtre amont vide donne un état distinct de NF.
"""
from __future__ import annotations
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4
from diagnostic import __version__
from diagnostic.schema import FORMS, SCHEMA_VERSION, iter_fields, text

MAX_DRAFT_BYTES = 150_000


class ValidationError(ValueError):
    """Erreur présentable sans exposer de secret ou d'information technique."""


def blank(value: Any) -> bool:
    return value is None or value == "" or value == []


def combine(values: list[bool | None], mode: str) -> bool | None:
    if mode == "all":
        return False if False in values else (None if None in values else True)
    return True if True in values else (None if None in values else False)


def evaluate(rule: dict | None, answers: dict) -> bool | None:
    if not rule:
        return True
    if "all" in rule:
        return combine([evaluate(r, answers) for r in rule["all"]], "all")
    if "any" in rule:
        return combine([evaluate(r, answers) for r in rule["any"]], "any")
    value = answers.get(rule["field"])
    op = rule["op"]
    if op == "not_in":
        return value not in rule["values"]
    if blank(value):
        return None
    if op == "in":
        return value in rule["values"]
    if op == "contains":
        return isinstance(value, list) and any(v in value for v in rule["values"])
    raise ValidationError("Filtre inconnu.")


def field_states(form_id: str, answers: dict) -> tuple[dict, dict]:
    """Renvoie un instantané éligible et l'état de tous les champs.

    Ordre topologique : tous les filtres portent sur une question antérieure.
    Une réponse rendue inapplicable est retirée ; elle ne réapparaît pas ensuite.
    """
    allowed = {f["id"] for _, f in iter_fields(form_id)}
    effective = {k:v for k,v in answers.items() if k in allowed}
    states = {}
    for section, field in iter_fields(form_id):
        status = combine([evaluate(section.get("gate"),effective), evaluate(field.get("gate"),effective)],"all")
        # Les précisions « Autre » suivent aussi l'applicabilité du champ source.
        dep=field.get("gate") or {}
        dep_id=dep.get("field")
        if dep_id and states.get(dep_id) in ("NF", "FILTRE_INDETERMINE"):
            status=False if states[dep_id]=="NF" else None
        fid=field["id"]
        if status is not True:
            effective.pop(fid,None)
            states[fid]="NF" if status is False else "FILTRE_INDETERMINE"
        else:
            value=effective.get(fid)
            if blank(value):
                states[fid]="NR_A_CONTROLER"
            elif value==98:
                states[fid]="NSP"
            elif value==99:
                states[fid]="NA"
            else:
                states[fid]="RENSEIGNE"
    return effective,states


def active_sections(form_id: str, answers: dict) -> list[dict]:
    effective,_=field_states(form_id,answers)
    # Un filtre indéterminé est expliqué à l'écran, pas déclaré comme un saut certain.
    return [s for s in FORMS[form_id]["sections"] if evaluate(s.get("gate"),effective) is not False]


def validation(form_id: str, answers: dict, lang: str="fr", require_consent=True) -> tuple[list[str],list[str]]:
    errors=[]
    warnings=[]
    effective,states=field_states(form_id,answers)
    def msg(fr,en): return fr if lang=="fr" else en
    if require_consent and effective.get(FORMS[form_id]["consent"]) != 1:
        errors.append(msg("L’accord de participation est nécessaire pour envoyer une réponse.","Participation consent is required to submit a response."))
    for _,f in iter_fields(form_id):
        fid=f["id"]
        v=effective.get(fid)
        if blank(v):
            continue
        opts={o["code"] for o in f["options"]}
        if f["type"]=="single":
            if type(v) is not int or v not in opts:
                errors.append(f"{fid} : "+msg("code non autorisé.","invalid code."))
        elif f["type"]=="multi":
            if not isinstance(v,list) or any(type(n) is not int or n not in opts for n in v):
                errors.append(f"{fid} : "+msg("choix non autorisé.","invalid selection."))
                continue
            if len(v)!=len(set(v)):
                errors.append(f"{fid} : "+msg("choix dupliqué.","duplicate choice."))
            if len(v)>f.get("max_choices",len(opts)):
                errors.append(f"{fid} : "+msg("trop de choix.","too many choices."))
            if len(v)>1 and any(n in f.get("exclusive",[]) for n in v):
                errors.append(f"{fid} : "+msg("« aucun », « ne sait pas » ou « non applicable » doit rester seul.","‘None’, ‘Do not know’ or ‘Not applicable’ must be selected alone."))
        else:
            if not isinstance(v,str):
                errors.append(f"{fid} : "+msg("texte attendu.","text expected."))
                continue
            if len(v)>f.get("max_chars",2500):
                errors.append(f"{fid} : "+msg("texte trop long.","text too long."))
            if "\x00" in v:
                errors.append(f"{fid} : "+msg("caractère non autorisé.","invalid character."))
            if f.get("max_words") and len(v.split())>f["max_words"]:
                errors.append(f"{fid} : "+msg(f"limite de {f['max_words']} mots dépassée.",f"limit of {f['max_words']} words exceeded."))
            if f["type"]=="email" and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+",v):
                errors.append(f"{fid} : "+msg("courriel invalide ; corriger ou laisser vide.","invalid email; correct it or leave it blank."))
            if f["type"]=="month":
                m=re.fullmatch(r"(0[1-9]|1[0-2])/(\d{4})",v)
                if not m:
                    errors.append(f"{fid} : "+msg("utiliser MM/AAAA, par exemple 10/2026.","use MM/YYYY, for example 10/2026."))
                elif not (202511<=int(m[2])*100+int(m[1])<=202610):
                    warnings.append(f"{fid} : "+msg("la date est hors de la période de référence ; préciser ou corriger.","the date is outside the reference period; clarify or correct it."))
    prefix="QI10" if form_id=="ins" else ("QU07" if form_id=="utilisateurs" else None)
    if prefix:
        vals=[effective.get(prefix+f"_P{i}") for i in [1,2,3]]
        nonempty=[v for v in vals if not blank(v)]
        if all(type(v) is int for v in nonempty) and len(nonempty)!=len(set(nonempty)):
            errors.append(msg("Les trois priorités doivent être différentes.","The three priorities must be different."))
        if (not blank(vals[1]) and blank(vals[0])) or (not blank(vals[2]) and blank(vals[1])):
            errors.append(msg("Renseigner les priorités dans l’ordre, sans rang vide.","Enter priorities in order, without leaving a rank blank."))
    # Contradictions signalées, jamais corrigées automatiquement.
    if form_id=="ins":
        if effective.get("QI02a")==0 and effective.get("QI02b")==1:
            errors.append(msg("QI02 : aucune demande et présence d’une demande urgente sont incompatibles.","QI02: no requests and an urgent request are incompatible."))
        if effective.get("QI04a")==5 and effective.get("QI04b") in [0,1]:
            warnings.append(msg("QI04 : aucune échéance n’est indiquée, mais son respect est évalué. Vérifier le code 99.","QI04: no deadline is specified, but compliance is assessed. Check code 99."))
        if effective.get("QI02a")==0 and any(effective.get(f"QI09_D{i}")==1 for i in range(1,8)):
            warnings.append(msg("QI02/QI09 : aucune nouvelle demande, mais un dispositif a été utilisé. Cela peut concerner un appui plus ancien ; préciser si nécessaire.","QI02/QI09: no new requests, but an arrangement was used. This may concern older support; clarify if needed."))
    if form_id=="utilisateurs":
        if effective.get("QU02a")==0 and effective.get("QU02b")==1:
            errors.append(msg("QU02 : aucun discours et appui demandé pour un discours sont incompatibles.","QU02: no speech and support requested for a speech are incompatible."))
        d=effective.get("QU03_Delai"); out=effective.get("QU03_Issue")
        if (d==99 and out in [1,2,3,4]) or (out==99 and d in [1,2,3,4,5]):
            errors.append(msg("QU03 : harmoniser l’existence ou l’absence d’un appui demandé à l’INS.","QU03: make the presence or absence of an NSO support request consistent."))
    # Le manque d'une précision libre ne bloque pas une réponse partielle.
    for _,f in iter_fields(form_id):
        v=effective.get(f["id"])
        other_id=(f["id"].rsplit("_P",1)[0]+"_Autre") if re.fullmatch(r"(?:QI10|QU07)_P[123]",f["id"]) else f["id"]+"_Autre"
        if (v==97 or (isinstance(v,list) and 97 in v)) and blank(effective.get(other_id)):
            warnings.append(f["id"]+" : "+msg("la précision « Autre » est utile mais reste facultative.","specifying ‘Other’ would be useful but is optional."))
    return errors,list(dict.fromkeys(warnings))


def schema_hash(form_id: str) -> str:
    return hashlib.sha256(json.dumps(FORMS[form_id],ensure_ascii=False,sort_keys=True).encode()).hexdigest()


def prepare_submission(form_id: str, answers: dict, lang: str="fr", submission_id: str|None=None,
                       is_test: bool=False) -> tuple[dict,dict]:
    if form_id not in FORMS or lang not in ["fr","en"] or type(is_test) is not bool:
        raise ValidationError("Questionnaire, langue ou mode invalide / Invalid form, language or mode.")
    errors,warnings=validation(form_id,answers,lang)
    if errors:
        raise ValidationError("\n".join(errors))
    effective,states=field_states(form_id,answers)
    fields={f["id"]:f for _,f in iter_fields(form_id)}
    contact={k:v for k,v in effective.items() if fields[k].get("private") and not blank(v)}
    clean={k:v for k,v in effective.items() if not fields[k].get("private")}
    # Consentement seul : pas d'enregistrement vide. Un profil seul est recevable,
    # mais restera non évalué et ne sera pas automatiquement déclaré exploitable.
    if not any(not blank(v) for k,v in clean.items() if k!=FORMS[form_id]["consent"]):
        raise ValidationError("Renseigner au moins une information en plus du consentement." if lang=="fr" else "Provide at least one item in addition to consent.")
    uid=str(UUID(submission_id)) if submission_id else str(uuid4())
    attr=clean.get(FORMS[form_id]["attribution"])
    payload={"submission_id":uid,"form_id":form_id,"language":lang,"schema_version":SCHEMA_VERSION,
             "instrument_hash":schema_hash(form_id),"app_version":__version__,"is_test":is_test,
             "answers":clean,"field_states":{k:v for k,v in states.items() if not fields[k].get("private")},
             "citation_permission":0 if blank(attr) else attr,"citation_explicit":not blank(attr),
             "warnings":warnings,"short_form":form_id=="abrege"}
    return payload,contact


def draft_bytes(form_id: str, answers: dict, lang: str, submission_id: str) -> bytes:
    effective,_=field_states(form_id,answers)
    data={"kind":"hiswaca_draft","schema_version":SCHEMA_VERSION,"form_id":form_id,
          "language":lang,"submission_id":str(UUID(submission_id)),"answers":effective}
    return json.dumps(data,ensure_ascii=False,indent=2).encode("utf-8")


def load_draft(content: bytes) -> dict:
    if len(content)>MAX_DRAFT_BYTES:
        raise ValidationError("Brouillon trop volumineux / Draft too large.")
    try:
        data=json.loads(content.decode("utf-8-sig"))
    except (ValueError,UnicodeDecodeError) as exc:
        raise ValidationError("Brouillon JSON invalide / Invalid JSON draft.") from exc
    if not isinstance(data,dict) or data.get("kind")!="hiswaca_draft" or data.get("schema_version")!=SCHEMA_VERSION:
        raise ValidationError("Format ou version de brouillon incompatible / Incompatible draft format or version.")
    form_id=data.get("form_id")
    if not isinstance(form_id,str) or form_id not in FORMS or data.get("language") not in ["fr","en"] or not isinstance(data.get("answers"),dict):
        raise ValidationError("Brouillon non reconnu / Unrecognised draft.")
    try:
        data["submission_id"]=str(UUID(data["submission_id"]))
    except (ValueError,KeyError,TypeError,AttributeError) as exc:
        raise ValidationError("Identifiant de brouillon invalide / Invalid draft ID.") from exc
    known={f["id"] for _,f in iter_fields(form_id)}
    if set(data["answers"])-known:
        raise ValidationError("Le brouillon contient des champs inconnus / The draft contains unknown fields.")
    errors,_=validation(form_id,data["answers"],require_consent=False)
    if errors:
        raise ValidationError("Brouillon invalide : "+"; ".join(errors))
    data["answers"],_=field_states(form_id,data["answers"])
    return data


def answer_label(field: dict, value: Any, lang="fr") -> str:
    options={x["code"]:text(x["label"],lang) for x in field["options"]}
    if blank(value): return ""
    if isinstance(value,list):
        return " ; ".join(f"{n} — {options.get(n,str(n))}" for n in value)
    if field["type"]=="single":
        return f"{value} — {options.get(value,str(value))}"
    return str(value)


def receipt_text(payload: dict,receipt: dict) -> str:
    lang=payload["language"]
    lines=["HISWACA — "+("Accusé de réception" if lang=="fr" else "Receipt"),
           f"Référence / Reference : {receipt['receipt']}",f"Date UTC : {receipt['received_at']}",
           f"Questionnaire : {text(FORMS[payload['form_id']]['title'],lang)}",f"Version : {payload['schema_version']}",
           "", "TEST — NE PAS UTILISER COMME CONTRIBUTION" if payload["is_test"] else "", ""]
    for s,f in iter_fields(payload["form_id"]):
        if not f.get("private") and not blank(payload["answers"].get(f["id"])):
            lines.append(f"{f['id']} — {text(f['label'],lang)}\n{answer_label(f,payload['answers'][f['id']],lang)}\n")
    lines.append("La réception ne vaut ni validation des réponses ni approbation institutionnelle." if lang=="fr" else "Receipt does not constitute validation of responses or institutional approval.")
    return "\n".join(lines)
