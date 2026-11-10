"""Questionnaires FR/EN, transposition des instruments français du 8 novembre 2026.

Les codes source sont conservés. Les suffixes identifient les sous-questions.
Les champs de contact sont marqués private et exclus des réponses analytiques.
Les filtres sont évalués par engine.py, jamais par eval().
"""
from __future__ import annotations
from typing import Any

SCHEMA_VERSION = "2026-11-08.web-1"
REFERENCE_START = "2025-11-01"
REFERENCE_END = "2026-10-31"


def L(fr: str, en: str) -> dict[str, str]:
    return {"fr": fr, "en": en}


def O(*items: tuple[int, str, str]) -> list[dict[str, Any]]:
    return [{"code": n, "label": L(fr, en)} for n, fr, en in items]


def F(fid: str, fr: str, en: str, options=None, kind="single", **kw) -> dict:
    return {"id": fid, "label": L(fr, en), "type": kind, "options": options or [], **kw}


def T(fid, fr, en, **kw):
    return F(fid, fr, en, kind="text", max_chars=kw.pop("max_chars", 500), **kw)


def A(fid, fr, en, **kw):
    return F(fid, fr, en, kind="textarea", max_chars=kw.pop("max_chars", 2500), **kw)


def eq(fid, *values):
    return {"field": fid, "op": "in", "values": list(values)}


def notin(fid, *values):
    # Une non-réponse n'est pas un motif de saut : on laisse le cas accessible.
    return {"field": fid, "op": "not_in", "values": list(values)}


def ALL(*rules):
    return {"all": list(rules)}


def ANY(*rules):
    return {"any": list(rules)}


def S(code, fr, en, fields, note=None, gate=None, theme="Suivi et moyens", source=""):
    return {"id": code, "title": L(fr, en), "fields": fields, "note": note or L("", ""),
            "gate": gate, "theme": theme, "source": source}


YES_NO = O((1,"Oui","Yes"),(0,"Non","No"),(98,"Ne sait pas","Do not know"))
VOL = O((0,"Aucune / aucun","None"),(1,"De 1 à 5","1 to 5"),(2,"De 6 à 20","6 to 20"),
        (3,"De 21 à 50","21 to 50"),(4,"Plus de 50","More than 50"),(98,"Ne sait pas","Do not know"))
BASIS = O((1,"Registre ou inventaire consulté","Register or inventory consulted"),
          (2,"Estimation fondée sur la pratique connue","Estimate based on known practice"),(98,"Ne sait pas","Do not know"))
STATUS = O((1,"Réponse institutionnelle consolidée","Consolidated institutional response"),
           (2,"Contribution d’un service, non consolidée au niveau de l’institution","Service-level contribution, not consolidated at institution level"),
           (3,"Contribution individuelle fondée sur ma pratique professionnelle","Individual contribution based on my professional practice"))
DELAYS = O((1,"Moins de 6 heures","Less than 6 hours"),(2,"De 6 à 24 heures incluses","6 to 24 hours inclusive"),
           (3,"Plus de 24 heures à 3 jours inclus","More than 24 hours up to 3 days inclusive"),
           (4,"Plus de 3 jours","More than 3 days"),(5,"Aucune échéance indiquée","No deadline specified"),(98,"Ne sait pas","Do not know"))
ATTR = O((0,"Synthèse anonymisée seulement, sans attribution identifiable","Anonymised summary only, with no identifiable attribution"),
         (1,"Attribution à l’institution, sans citation textuelle identifiable","Attribution to the institution, without an identifiable quotation"),
         (2,"Attribution à l’institution et citation textuelle","Attribution to the institution and direct quotation"),
         (3,"Me soumettre le passage avant toute attribution ou citation identifiable","Submit the passage to me before any identifiable attribution or quotation"))
FOLLOW = O((1,"Complément","Follow-up clarification"),(2,"Test","Tool test"),(3,"Les deux","Both"),
           (0,"Non","No"),(98,"À déterminer","To be determined"))
ATTR_NOTE = L("Les choix 1 et 2 supposent l’autorisation correspondante. Une contribution individuelle sans cette autorisation reste anonymisée. Sans choix explicite, la synthèse anonymisée (code 0) s’applique. L’absence de retour ne vaut jamais accord de citation.",
              "Options 1 and 2 require the corresponding authorisation. An individual contribution without that authorisation remains anonymised. Without an explicit choice, an anonymised summary (code 0) applies. Silence never constitutes permission to quote.")


def consent(prefix):
    return F(prefix+"_Participation", "Acceptez-vous l’utilisation de votre contribution selon la notice ?",
             "Do you agree to the use of your contribution as described in the notice?",
             O((1,"J’accepte","I agree"),(0,"Je ne participe pas — fin du questionnaire","I do not wish to participate — end of questionnaire")), required=True)


def contacts(prefix, focal=True):
    fields = [T(prefix+"_Contact_fonction", "Contact facultatif : fonction ou service à recontacter",
                "Optional contact: function or service to contact", private=True),
              F(prefix+"_Contact_courriel", "Courriel institutionnel ou générique, facultatif",
                "Institutional or generic email address, optional", kind="email", max_chars=254, private=True)]
    if focal:
        fields.append(F(prefix+"_Contact_relais", "Préférer un relais par le point focal STATAFRIC, facultatif",
                        "Prefer contact through the STATAFRIC focal point, optional",
                        O((1,"Passer par le point focal STATAFRIC","Contact through the STATAFRIC focal point")), private=True))
    return fields


def other(fid, trigger=None, multi=False):
    return T(fid+"_Autre", "Autre : précisez", "Other: please specify", gate={"field":trigger or fid,"op":"contains" if multi else "in","values":[97]})


META_I = O((1,"Présente dans la réponse","Included in the response"),
           (2,"Accessible par un lien ou une référence précise transmis avec la réponse","Available through a specific link or reference sent with the response"),
           (0,"Absente","Absent"),(98,"Ne sait pas","Do not know"),(99,"Non applicable","Not applicable"))
META_U = O((1,"Dans le document ou message utilisé","In the document or message used"),
           (2,"Accessible par un lien ou une référence précise disponible","Accessible through an available specific link or reference"),
           (0,"Absente","Absent"),(98,"Ne sait pas","Do not know"),(99,"Non applicable","Not applicable"))
INS_PRI = O((1,"Clarifier le point d’entrée et les responsabilités","Clarify the contact point and responsibilities"),
            (2,"Mieux préciser et prioriser les demandes urgentes","Clarify and prioritise urgent requests"),
            (3,"Disposer d’un répertoire de chiffres documentés","Have a repository of documented figures"),
            (4,"Simplifier le contrôle et la validation","Simplify checking and validation"),
            (5,"Préparer des messages et exemples de passages de discours","Prepare messages and examples of speech passages"),
            (6,"Organiser les corrections et versions","Organise corrections and versions"),(7,"Former les agents","Train staff"),
            (8,"Suivre les délais et les retours d’utilisation","Monitor deadlines and user feedback"),(97,"Autre","Other"),
            (0,"Aucun besoin exprimé","No need expressed"),(98,"Ne sait pas","Do not know"))
USER_PRI = O((1,"Contact clairement identifié à l’INS","Clearly identified contact at the NSO"),
             (2,"Réponse dans un délai convenu, y compris en urgence","Response within an agreed deadline, including urgent requests"),
             (3,"Fiche courte avec chiffre, source et période","Short fact sheet with figure, source and period"),
             (4,"Explication des définitions et limites","Explanation of definitions and limitations"),
             (5,"Proposition de phrases compréhensibles","Suggested clear wording"),(6,"Relecture rapide du passage statistique","Quick review of the statistical passage"),
             (7,"Alerte en cas de révision ou d’erreur","Alert in the event of a revision or error"),(8,"Exemples et courte formation","Examples and brief training"),
             (97,"Autre","Other"),(0,"Aucun besoin exprimé","No need expressed"),(98,"Ne sait pas","Do not know"))


def priorities(prefix, options):
    gate=eq(prefix+"_P1", *[x["code"] for x in options if x["code"] not in [0,98]])
    return [F(prefix+"_P1","Priorité 1","Priority 1",options),
            F(prefix+"_P2","Priorité 2","Priority 2",[x for x in options if x["code"] not in [0,98]],gate=gate),
            F(prefix+"_P3","Priorité 3","Priority 3",[x for x in options if x["code"] not in [0,98]],gate=gate),
            T(prefix+"_Autre","Autre priorité : précisez","Other priority: please specify",gate=ANY(eq(prefix+"_P1",97),eq(prefix+"_P2",97),eq(prefix+"_P3",97)))]


IGATE = notin("QI02a",0)
ICASE = ALL(IGATE,eq("QI03_Situation",1,2))
IFIG = ALL(IGATE,eq("QI03_Situation",1))
ins=[]
ins.append(S("QI01","Accord de participation et périmètre de la réponse","Participation agreement and scope of the response",[
    consent("QI01"), T("QI01_Pays","Pays","Country"),T("QI01_INS","Nom ou sigle de l’INS","Name or acronym of the NSO"),
    F("QI01_Statut","Statut de la réponse","Response status",STATUS),
    F("QI01_Fonction","Fonction du répondant, sans nom personnel","Respondent’s function, without a personal name",
      O((1,"Production statistique","Statistical production"),(2,"Communication ou diffusion","Communication or dissemination"),
        (3,"Coordination ou direction","Coordination or management"),(4,"Méthodes ou qualité","Methods or quality"),(97,"Autre","Other"))),other("QI01_Fonction"),
    F("QI01_Envoi","Nature de l’envoi","Type of submission",O((1,"Première réponse","First response"),(2,"Complément","Supplement"))),
    T("QI01_Reference_precedente","Référence ou date de l’envoi précédent, si connue","Reference or date of the previous submission, if known",gate=eq("QI01_Envoi",2))],
    note=L("Une réponse consolidée par INS est privilégiée. Pour une contribution de service ou individuelle, répondre sur le périmètre réellement connu et utiliser « ne sait pas » pour le reste de l’INS.",
           "A consolidated response per NSO is preferred. For a service-level or individual contribution, describe only the scope you actually know and use ‘Do not know’ for the rest of the NSO."),source="Questionnaire INS, p. 1"))
ins.append(S("QI02","Demandes reçues pendant les douze mois de référence","Requests received during the twelve-month reference period",[
    F("QI02a","Combien de demandes distinctes avez-vous recensées dans le périmètre de votre réponse ?","How many distinct requests did you identify within the scope of your response?",VOL),
    F("QI02b","Certaines de ces demandes exigeaient-elles une réponse dans les 24 heures ou moins suivant leur réception ?","Did any of these requests require a response within 24 hours of receipt?",YES_NO+O((99,"Non applicable, aucune demande","Not applicable, no requests"))),
    F("QI02c","Sur quoi repose cette réponse ?","What is this response based on?",BASIS)],
    note=L("Une demande désigne un appui sollicité pour préparer un discours, une allocution ou une intervention orale d’un décideur. Plusieurs échanges ou versions concernant le même appui comptent comme une seule demande. Les demandes générales de données sans lien connu avec un discours sont exclues. Si aucune demande n’a été reçue, passer à QI08. Si le nombre est inconnu, QI03 peut être renseignée lorsqu’un cas réel est connu.",
           "A request means support requested to prepare a decision-maker’s speech, address or oral intervention. Multiple exchanges or versions for the same support count as one request. General data requests with no known link to a speech are excluded. If there were no requests, proceed to QI08. If the number is unknown, QI03 may still be completed when a real case is known."),theme="Demande et urgence",source="Questionnaire INS, p. 1–2"))
ins.append(S("QI03","Dernière demande clôturée connue","Most recent known closed request",[
    F("QI03_Situation","Situation","Situation",O((1,"Des chiffres ont été transmis","Figures were provided"),(2,"La demande a été clôturée sans fourniture de chiffres","The request was closed without providing figures"),(3,"Aucune demande clôturée connue pendant la période","No known request closed during the period"),(98,"Je ne dispose pas des informations nécessaires","I do not have the necessary information"))),
    F("QI03_Mois","Mois et année de clôture, MM/AAAA","Month and year of closure, MM/YYYY",kind="month",max_chars=7,gate=eq("QI03_Situation",1,2)),
    T("QI03_Theme","Thème général, sans nom de personne ni titre confidentiel du discours","General topic, without a person’s name or a confidential speech title",gate=eq("QI03_Situation",1,2)),
    F("QI03_Demandeur","Service demandeur","Requesting service",O((1,"Cabinet ou équipe d’un décideur national","Office or team of a national decision-maker"),(2,"Ministère ou administration sectorielle","Ministry or sectoral administration"),(3,"Autorité territoriale ou locale","Subnational or local authority"),(4,"Direction de l’INS","NSO management"),(5,"Institution régionale ou internationale","Regional or international institution"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know")),gate=eq("QI03_Situation",1,2)),other("QI03_Demandeur"),
    F("QI03_Canal","Canal principal de réception","Main channel through which the request was received",O((1,"Courriel","Email"),(2,"Courrier ou note officielle","Letter or official memorandum"),(3,"Téléphone ou échange oral","Telephone or oral exchange"),(4,"Messagerie instantanée","Instant messaging"),(5,"Formulaire ou plateforme","Form or platform"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know")),gate=eq("QI03_Situation",1,2)),other("QI03_Canal")],
    note=L("Choisir la demande la plus récemment clôturée pendant la période de référence, qu’elle ait ou non abouti à la fourniture de chiffres. Ne pas sélectionner uniquement un exemple particulièrement réussi. Les situations 3 et 98 conduisent à QI08. La situation 2 conduit à QI04, puis QI08.",
           "Select the request most recently closed during the reference period, whether or not figures were provided. Do not select only a particularly successful example. Situations 3 and 98 lead to QI08. Situation 2 leads to QI04 and then QI08."),gate=IGATE,theme="Demande et urgence",source="Questionnaire INS, p. 2"))
ins.append(S("QI04","Délai et traitement de cette dernière demande","Deadline and handling of this most recent request",[
    F("QI04a","Quel délai était laissé à l’INS pour répondre, à partir de la réception de la demande ?","How much time was the NSO given to respond from receipt of the request?",DELAYS),
    F("QI04b","La réponse de l’INS a-t-elle été transmise avant l’échéance demandée ?","Was the NSO’s response sent before the requested deadline?",YES_NO+O((99,"Non applicable, aucune échéance indiquée","Not applicable, no deadline specified"))),
    F("QI04c","Si aucun chiffre n’a été fourni ou si le délai n’a pas été respecté, quel a été le principal obstacle ?","If no figures were provided or the deadline was missed, what was the main obstacle?",O((1,"Donnée appropriée indisponible","Suitable data unavailable"),(2,"Besoin insuffisamment précisé","Need insufficiently specified"),(3,"Délai trop court","Deadline too short"),(4,"Contrôle ou validation non terminé","Checking or validation not completed"),(5,"Agent ou service compétent indisponible","Responsible staff member or service unavailable"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know"),(99,"Non applicable","Not applicable"))),other("QI04c")],
    note=L("Les durées sont calendaires, y compris les nuits et week-ends. Une réponse expliquant qu’aucun chiffre ne pouvait être fourni reste distinguée de la fourniture de chiffres par QI03. Le respect d’un délai ne suffit pas à conclure que le besoin statistique a été satisfait.",
           "Times are calendar durations, including nights and weekends. QI03 distinguishes a response explaining that no figures could be provided from an actual provision of figures. Meeting a deadline does not by itself establish that the statistical need was met."),gate=ICASE,theme="Demande et urgence",source="Questionnaire INS, p. 2"))
meta_i=[("S1","Source précise permettant de retrouver le résultat","Specific source enabling the result to be located"),("S2","Date de publication ou version de la source","Publication date or source version"),("S3","Période à laquelle se rapporte la valeur","Period to which the value relates"),("S4","Définition de l’indicateur","Definition of the indicator"),("S5","Unité et base de calcul, notamment dénominateur d’un taux","Unit and calculation basis, particularly the denominator of a rate"),("S6","Population et territoire couverts","Population and geographical coverage"),("S7","Limites utiles à l’interprétation ou à la comparaison","Limitations relevant to interpretation or comparison"),("S8","Statut provisoire ou révisé, lorsqu’il s’applique","Provisional or revised status, where applicable")]
ins.append(S("QI05","Source et informations accompagnant le premier chiffre transmis","Source and information accompanying the first figure provided",[
    F("QI05_Origine","Origine principale de ce chiffre","Main origin of this figure",O((1,"Publication, base ou résultat produit par l’INS","Publication, database or result produced by the NSO"),(2,"Source officielle d’une autre administration nationale","Official source from another national administration"),(3,"Organisation régionale ou internationale","Regional or international organisation"),(4,"Recherche ou source privée identifiée","Identified research or private source"),(5,"Discours ou document antérieur sans retour à la source statistique","Earlier speech or document, without consulting the statistical source"),(6,"Source non identifiée","Unidentified source"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know"))),other("QI05_Origine")]+[F("QI05_"+c,fr,en,META_I) for c,fr,en in meta_i],
    note=L("Considérer le premier chiffre présenté dans la dernière réponse transmise, et non le mieux documenté. Indiquer les informations qui l’accompagnaient effectivement. Une simple mention du nom d’un organisme ne suffit pas pour le code 2. Ne pas recopier une valeur non publique.",
           "Consider the first figure presented in the most recent response, not the best-documented figure. Indicate the information that actually accompanied it. Merely naming an organisation does not qualify for code 2. Do not copy a non-public value."),gate=IFIG,theme="Sources et métadonnées",source="Questionnaire INS, p. 2–3"))
ins.append(S("QI06","Vérification et validation effectivement réalisées","Checking and validation actually performed",[
    F("QI06a","Qui a vérifié le contenu statistique avant son envoi ? Plusieurs réponses possibles.","Who checked the statistical content before it was sent? Multiple answers allowed.",O((1,"L’agent qui l’a préparé","The staff member who prepared it"),(2,"Un autre agent producteur des données","Another staff member producing the data"),(3,"Un responsable technique ou de qualité","A technical or quality manager"),(4,"Le service de communication","The communication service"),(5,"La direction ou son délégataire","Management or its delegate"),(0,"Aucun contrôle connu","No known check"),(97,"Autre fonction","Other function"),(98,"Ne sait pas","Do not know")),kind="multi",exclusive=[0,98]),other("QI06a",multi=True),
    F("QI06b","Quelle trace de la validation statistique finale existe pour cet envoi ?","What record exists of the final statistical validation for this submission?",O((1,"Trace écrite, courriel, visa ou journal de validation","Written record, email, sign-off or validation log"),(2,"Accord oral seulement","Oral agreement only"),(0,"Aucune validation distincte connue","No separate validation known"),(98,"Ne sait pas","Do not know")))],
    note=L("Il s’agit du contrôle du contenu statistique, pas de l’approbation politique du discours.","This concerns checking statistical content, not political approval of the speech."),gate=IFIG,theme="Vérification",source="Questionnaire INS, p. 3"))
ins.append(S("QI07","Format transmis et vérification du passage de discours","Format provided and checking of the speech passage",[
    F("QI07a","Sous quelle forme l’appui a-t-il été transmis ? Jusqu’à trois réponses.","In what form was the support provided? Up to three answers.",O((1,"Tableau ou liste de chiffres","Table or list of figures"),(2,"Fiche accompagnée d’explications","Fact sheet with explanations"),(3,"Proposition de phrases ou de paragraphes","Suggested sentences or paragraphs"),(4,"Graphique ou illustration","Chart or illustration"),(5,"Explication orale uniquement","Oral explanation only"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know")),kind="multi",max_choices=3,exclusive=[98]),other("QI07a",multi=True),
    F("QI07b","L’INS a-t-il ensuite pu vérifier les chiffres dans le passage de discours préparé avant sa diffusion ?","Was the NSO subsequently able to check the figures in the prepared speech passage before dissemination?",O((1,"Oui","Yes"),(2,"Non, le passage n’a pas été reçu","No, the passage was not received"),(3,"Non, le passage a été reçu mais n’a pas été vérifié","No, the passage was received but not checked"),(4,"Le discours n’a pas été diffusé ou cette étape n’a pas eu lieu","The speech was not delivered or this stage did not occur"),(98,"Ne sait pas","Do not know")))],gate=IFIG,theme="Messages et publics",source="Questionnaire INS, p. 3"))
ins.append(S("QI08","Correction ou actualisation après transmission","Correction or updating after transmission",[
    F("QI08_Situation","Pendant la période de référence, avez-vous connaissance d’un appui déjà transmis qu’il a fallu corriger ou actualiser ?","During the reference period, do you know of support already provided that had to be corrected or updated?",O((1,"Oui, pour corriger une erreur","Yes, to correct an error"),(2,"Oui, pour actualiser une donnée","Yes, to update data"),(3,"Les deux situations ont été rencontrées","Both situations occurred"),(0,"Aucun épisode connu","No known instance"),(98,"Ne sait pas","Do not know"),(99,"Non applicable, aucun appui concerné","Not applicable, no relevant support"))),
    F("QI08_Actions","Pour le cas le plus récent, quelles actions ont été réalisées ? Plusieurs réponses possibles.","For the most recent case, what actions were taken? Multiple answers allowed.",O((1,"Alerte de l’équipe préparant le discours","Alerting the speech-preparation team"),(2,"Envoi d’une version corrigée avec sa source","Sending a corrected version with its source"),(3,"Conservation des versions et du motif de changement","Keeping versions and the reason for the change"),(4,"Demande de correction d’un passage déjà diffusé","Requesting correction of a passage already disseminated"),(5,"Vérification de la prise en compte de la correction","Checking that the correction was incorporated"),(0,"Aucune action connue","No known action"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know")),kind="multi",exclusive=[0,98],gate=eq("QI08_Situation",1,2,3)),other("QI08_Actions",multi=True)],
    note=L("Un appui initialement fourni avant la période peut être pris en compte si la correction intervient pendant celle-ci. Cette question reste accessible même si aucune nouvelle demande n’a été reçue.",
           "Support originally provided before the period may be included if the correction occurred during the period. This question remains available even if no new request was received."),theme="Actualisation et correction",source="Questionnaire INS, p. 4"))
devices=[("D1","Point d’entrée ou fonction identifiée pour recevoir les demandes","Identified contact point or function for receiving requests"),("D2","Consigne écrite de traitement, y compris des urgences","Written handling instructions, including for urgent requests"),("D3","Registre des demandes avec échéance, responsable et état de traitement","Request register with deadline, responsible officer and processing status"),("D4","Répertoire de chiffres avec sources et informations d’interprétation","Repository of figures with sources and interpretation information"),("D5","Trace du contrôle et de la validation statistique","Record of statistical checking and validation"),("D6","Conservation des versions et des corrections","Retention of versions and corrections"),("D7","Examen des délais, difficultés et retours des utilisateurs","Review of deadlines, difficulties and user feedback")]
DEV_OPTIONS=O((1,"Existe et a été utilisé pour ces appuis pendant la période","Exists and was used for this support during the period"),(2,"Existe mais n’a pas été utilisé pendant la période","Exists but was not used during the period"),(0,"N’existe pas","Does not exist"),(98,"Ne sait pas","Do not know"),(99,"Non applicable","Not applicable"))
ins.append(S("QI09","Organisation effectivement en place au 31 octobre 2026","Arrangements actually in place on 31 October 2026",[F("QI09_"+c,fr,en,DEV_OPTIONS) for c,fr,en in devices]+[A("QI09_Nouveaute","Pratique nouvelle après le 31 octobre, facultatif : dispositif, date de mise en place et une phrase","New practice after 31 October, optional: arrangement, implementation date and one sentence")],source="Questionnaire INS, p. 4"))
ins.append(S("QI10","Besoins prioritaires","Priority needs",priorities("QI10",INS_PRI)+[
    A("QI10_Difficulte","Pour la priorité 1, quelle difficulté réelle cela permettrait-il de résoudre ? Au plus 40 mots ; sinon, écrire « besoin anticipé ».","For priority 1, what actual difficulty would this resolve? Up to 40 words; otherwise write ‘anticipated need’.",max_words=40),
    T("QI10_Action","Première action possible avec les moyens existants","First action possible with existing resources"),
    T("QI10_Responsable","Fonction pouvant la conduire","Function that could lead the action"),
    T("QI10_Moyen","Moyen déjà disponible","Resource already available"),
    T("QI10_Obstacle","Principal obstacle","Main obstacle")],
    note=L("Classer au plus trois codes différents. Une ou deux priorités suffisent. 0 et 98 sont exclusifs des autres priorités. Indiquer « à déterminer » si nécessaire pour l’action, le responsable, les moyens ou l’obstacle.","Rank up to three different codes. One or two priorities are sufficient. Codes 0 and 98 exclude other priorities. Write ‘to be determined’ where necessary for the action, lead function, resources or obstacle."),source="Questionnaire INS, p. 4"))
ins.append(S("QI11","Un exemple ou document partageable","One shareable example or document",[
    F("QI11_Type","Exemple proposé, facultatif","Example provided, optional",O((1,"Lien public","Public link"),(2,"Extrait non confidentiel dont le partage est autorisé","Non-confidential extract authorised for sharing"),(0,"Aucun exemple partagé","No example shared"))),
    A("QI11_Reference","Titre ou description, date et lien public si disponible ; extrait autorisé le cas échéant","Title or description, date and public link if available; authorised extract where relevant",gate=eq("QI11_Type",1,2)),
    A("QI11_Apport","Ce que cet exemple montre et ce qui pourrait être adapté ailleurs, au plus 60 mots","What this example demonstrates and what could be adapted elsewhere, up to 60 words",max_words=60,gate=eq("QI11_Type",1,2))],
    note=L("Proposer un seul exemple : procédure, fiche de chiffres, modèle de demande, contrôle, correction ou discours public. Ne pas joindre de données individuelles, brouillon confidentiel, coordonnées personnelles ou contenu non autorisé. Un exemple antérieur peut illustrer une pratique, mais sa date doit rester visible.","Provide only one example: procedure, fact sheet, request template, check, correction or public speech. Do not provide individual-level data, confidential drafts, personal contact details or unauthorised content. An older example can illustrate a practice, but its date must remain visible."),source="Questionnaire INS, p. 5"))
ins.append(S("QI12","Attribution et contact facultatif","Attribution and optional contact",[
    F("QI12_Attribution","Modalité autorisée pour la contribution et l’exemple partagé","Permitted use of the contribution and shared example",ATTR,attribution=True),
    F("QI12_Suite","Accepteriez-vous un bref complément ou un test d’outil ?","Would you agree to a brief follow-up or a tool test?",FOLLOW)]+contacts("QI12"),note=ATTR_NOTE,source="Questionnaire INS, p. 5"))

UGATE=notin("QU02a",0)
UCASE=ALL(UGATE,eq("QU03_Cas",1))
users=[]
users.append(S("QU01","Accord de participation et profil","Participation agreement and profile",[
    consent("QU01"),T("QU01_Pays","Pays","Country"),T("QU01_Institution","Institution ou type d’institution","Institution or type of institution"),
    F("QU01_Statut","Périmètre","Scope",O((1,"Réponse consolidée de l’équipe ou du service","Consolidated team or service response"),(2,"Contribution individuelle fondée sur ma propre pratique","Individual contribution based on my own practice"))),
    F("QU01_Fonction","Fonction, sans nom personnel","Function, without a personal name",O((1,"Rédaction de discours ou conseil","Speechwriting or advisory work"),(2,"Communication","Communication"),(3,"Appui technique ou statistique","Technical or statistical support"),(4,"Coordination d’équipe","Team coordination"),(97,"Autre","Other"))),other("QU01_Fonction"),
    T("QU01_Reference_precedente","Si cet envoi complète une réponse antérieure, indiquer sa référence ou sa date","If this submission supplements an earlier response, give its reference or date")],
    note=L("Répondre pour une équipe ou un service préparant des discours, ou sur votre pratique personnelle si vous n’êtes pas mandaté pour l’équipe. Les informations portent sur les chiffres, sans évaluation politique des discours.","Respond for a speech-preparation team or service, or describe your own practice if you are not mandated to respond for the team. Information concerns figures, without any political assessment of speeches."),source="Questionnaire utilisateurs, p. 1"))
users.append(S("QU02","Activité pendant les douze mois de référence","Activity during the twelve-month reference period",[
    F("QU02a","À combien de discours comportant des chiffres votre équipe ou vous-même avez-vous contribué ?","How many speeches containing figures did your team or you contribute to?",VOL),
    F("QU02b","Avez-vous demandé un appui statistique à l’INS pour au moins un de ces discours ?","Did you request statistical support from the NSO for at least one of these speeches?",YES_NO+O((99,"Non applicable, aucun discours concerné","Not applicable, no relevant speech"))),
    F("QU02_Base","Base de la réponse","Basis for the response",BASIS)],
    note=L("Compter une intervention une seule fois, même si elle a fait l’objet de plusieurs versions. Si aucun discours n’est concerné, passer à QU07. Si le nombre est inconnu, poursuivre lorsqu’un cas réel est connu.","Count an intervention only once, even if several versions were prepared. If there were no relevant speeches, proceed to QU07. If the number is unknown, continue when a real case is known."),theme="Demande et urgence",source="Questionnaire utilisateurs, p. 1–2"))
users.append(S("QU03","Dernier discours préparé comportant des chiffres","Most recent speech prepared containing figures",[
    F("QU03_Cas","Un tel cas peut-il être décrit ?","Can such a case be described?",YES_NO),
    F("QU03_Mois","Mois et année de fin de préparation, MM/AAAA","Month and year preparation ended, MM/YYYY",kind="month",max_chars=7,gate=eq("QU03_Cas",1)),
    T("QU03_Theme","Thème général, sans nom de personne","General topic, without a person’s name",gate=eq("QU03_Cas",1)),
    F("QU03_Public","Public principal","Main audience",O((1,"Grand public","General public"),(2,"Parlement ou élus","Parliament or elected representatives"),(3,"Public technique ou professionnel","Technical or professional audience"),(4,"Communauté locale","Local community"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know")),gate=eq("QU03_Cas",1)),other("QU03_Public"),
    T("QU03_Langues","Langue ou langues du discours","Language(s) of the speech",gate=eq("QU03_Cas",1)),
    F("QU03_Origine","Origine du premier chiffre utilisé","Origin of the first figure used",O((1,"Source publiée de l’INS consultée directement","Published NSO source consulted directly"),(2,"Chiffre transmis directement par l’INS","Figure supplied directly by the NSO"),(3,"Autre administration nationale","Other national administration"),(4,"Organisation régionale ou internationale","Regional or international organisation"),(5,"Recherche ou source privée identifiée","Identified research or private source"),(6,"Discours ou document précédent sans retour à la source statistique","Previous speech or document, without consulting the statistical source"),(7,"Source non identifiée","Unidentified source"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know")),gate=eq("QU03_Cas",1)),other("QU03_Origine"),
    F("QU03_Delai","Si un appui a été demandé à l’INS pour ce cas, délai demandé","If NSO support was requested for this case, requested turnaround time",DELAYS+O((99,"Aucun appui demandé à l’INS pour ce cas","No NSO support requested for this case")),gate=eq("QU03_Cas",1)),
    F("QU03_Issue","Si un appui a été demandé, son issue","If support was requested, its outcome",O((1,"Chiffres reçus à temps","Figures received on time"),(2,"Chiffres reçus après l’échéance","Figures received after the deadline"),(3,"Aucun chiffre reçu","No figures received"),(4,"Chiffres reçus, sans échéance convenue","Figures received, with no agreed deadline"),(98,"Ne sait pas","Do not know"),(99,"Aucun appui demandé","No support requested")),gate=eq("QU03_Cas",1)),
    A("QU03_Exemple","Cas partageable, facultatif : lien vers le discours public ou bref extrait non confidentiel autorisé, avec sa date","Shareable case, optional: link to the public speech or brief authorised non-confidential extract, with its date",gate=eq("QU03_Cas",1))],
    note=L("Considérer le discours comportant des chiffres dont la préparation s’est achevée le plus récemment pendant la période, et non un exemple sélectionné pour sa qualité. Si aucun cas n’est décrit, passer à QU07. L’absence de lien ne remet pas en cause votre réponse.","Consider the speech containing figures whose preparation ended most recently during the period, rather than an example chosen for its quality. If no case can be described, proceed to QU07. The absence of a link does not invalidate your response."),gate=UGATE,theme="Messages et publics",source="Questionnaire utilisateurs, p. 2"))
meta_u=[("M1","Source permettant de retrouver le résultat","Source enabling the result to be located"),("M2","Date de publication ou version","Publication date or version"),("M3","Période de la valeur","Period of the value"),("M4","Définition de l’indicateur","Definition of the indicator"),("M5","Unité et base de calcul du taux, le cas échéant","Unit and calculation basis of the rate, where applicable"),("M6","Population et territoire couverts","Population and geographical coverage"),("M7","Limites de comparaison, incertitudes ou précautions utiles","Comparability limitations, uncertainties or relevant cautions")]
users.append(S("QU04","Informations disponibles pour comprendre ce chiffre","Information available to understand this figure",[F("QU04_"+c,fr,en,META_U) for c,fr,en in meta_u],note=L("Pour le premier chiffre indiqué en QU03, quelles informations aviez-vous effectivement à disposition lors de la préparation ?","For the first figure identified in QU03, what information did you actually have available during preparation?"),gate=UCASE,theme="Sources et métadonnées",source="Questionnaire utilisateurs, p. 2"))
users.append(S("QU05","Vérifications avant diffusion de ce discours","Checks before dissemination of this speech",[
    F("QU05a","Qui a effectivement vérifié les chiffres et leur interprétation ? Plusieurs réponses possibles.","Who actually checked the figures and their interpretation? Multiple answers allowed.",O((1,"Rédacteur ou équipe de communication","Speechwriter or communication team"),(2,"Conseiller technique ou statisticien de l’institution","Institution’s technical adviser or statistician"),(3,"Agent de l’INS","NSO staff member"),(4,"Autre organisme producteur","Other producing organisation"),(0,"Aucun contrôle connu","No known check"),(97,"Autre fonction","Other function"),(98,"Ne sait pas","Do not know")),kind="multi",exclusive=[0,98]),other("QU05a",multi=True),
    F("QU05b","L’INS a-t-il relu le passage contenant les chiffres dans le projet de discours ?","Did the NSO review the passage containing figures in the draft speech?",O((1,"Oui","Yes"),(2,"Non, il n’a pas été sollicité","No, it was not asked"),(3,"Non, il a été sollicité mais la relecture n’a pas eu lieu à temps","No, it was asked but the review did not take place in time"),(98,"Ne sait pas","Do not know"),(99,"Non applicable, cette étape n’a pas eu lieu","Not applicable, this stage did not occur"))),
    F("QU05c","Une référence à la source statistique figurait-elle dans la version diffusée ?","Did the disseminated version include a reference to the statistical source?",YES_NO+O((99,"Discours non diffusé ou version diffusée inconnue","Speech not disseminated or disseminated version unknown")))],gate=UCASE,theme="Vérification",source="Questionnaire utilisateurs, p. 2–3"))
users.append(S("QU06","Corrections et retour à l’INS","Corrections and feedback to the NSO",[
    F("QU06a","Pendant la période, avez-vous connaissance d’un chiffre ou d’une interprétation à corriger après finalisation d’un discours ?","During the period, do you know of a figure or interpretation requiring correction after a speech was finalised?",O((1,"Oui, correction avant diffusion","Yes, correction before dissemination"),(2,"Oui, correction après diffusion","Yes, correction after dissemination"),(3,"Les deux situations","Both situations"),(0,"Aucun épisode connu","No known instance"),(98,"Ne sait pas","Do not know"),(99,"Aucun discours concerné","No relevant speech"))),
    F("QU06_Actions","Pour le cas le plus récent, quelles actions ont été réalisées ? Plusieurs réponses possibles.","For the most recent case, what actions were taken? Multiple answers allowed.",O((1,"Retour à la source ou au producteur","Returning to the source or producer"),(2,"Version corrigée transmise à l’équipe","Corrected version sent to the team"),(3,"Conservation du changement et de son motif","Keeping a record of the change and its reason"),(4,"Rectificatif diffusé pour une erreur déjà publique","Correction issued for an already public error"),(5,"Vérification de la prise en compte","Checking that the correction was incorporated"),(0,"Aucune action connue","No known action"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know")),kind="multi",exclusive=[0,98],gate=eq("QU06a",1,2,3)),other("QU06_Actions",multi=True),
    F("QU06b","Pour le dernier appui demandé à l’INS pendant la période, lui avez-vous transmis un retour sur l’utilisation des chiffres ?","For the most recent support requested from the NSO during the period, did you provide feedback on the use of the figures?",O((1,"Oui, texte ou lien vers le discours","Yes, text or a link to the speech"),(2,"Oui, commentaire sur l’appui, sans texte ni lien","Yes, comments on the support, without text or link"),(3,"Les deux","Both"),(0,"Non","No"),(98,"Ne sait pas","Do not know"),(99,"Aucun appui demandé","No support requested")))],gate=UCASE,theme="Actualisation et correction",source="Questionnaire utilisateurs, p. 3"))
users.append(S("QU07","Appuis les plus utiles","Most useful forms of support",priorities("QU07",USER_PRI)+[
    A("QU07_Difficulte","Pour la priorité 1, citer une difficulté effectivement rencontrée, au plus 40 mots. À défaut, écrire « besoin anticipé ».","For priority 1, describe a difficulty actually experienced, up to 40 words. Otherwise write ‘anticipated need’.",max_words=40),
    A("QU07_Contrainte","Contrainte particulière liée au public, à la langue ou au contexte de communication, facultatif ; au plus 40 mots, aucune opinion politique demandée","Specific constraint related to the audience, language or communication context, optional; up to 40 words, no political opinion requested",max_words=40),
    A("QU07_Nouveaute","Pratique nouvelle après le 31 octobre, facultatif : changement et date","New practice after 31 October, optional: change and date")],note=L("Classer au plus trois codes différents. Les choix 0 et 98 excluent les autres priorités.","Rank up to three different codes. Options 0 and 98 exclude other priorities."),source="Questionnaire utilisateurs, p. 3"))
users.append(S("QU08","Attribution et contact facultatif","Attribution and optional contact",[
    F("QU08_Attribution","Modalité autorisée pour la contribution et le cas partagé","Permitted use of the contribution and shared case",ATTR,attribution=True),
    F("QU08_Suite","Acceptez-vous un complément ou un test ?","Do you agree to follow-up or a test?",FOLLOW)]+contacts("QU08"),note=ATTR_NOTE,source="Questionnaire utilisateurs, p. 3"))

partners=[]
partners.append(S("QR01","Participation et champ de la contribution","Participation and scope of the contribution",[
    consent("QR01"),T("QR01_Organisation","Organisation","Organisation"),
    F("QR01_Type","Type d’organisation","Type of organisation",O((1,"Communauté économique régionale (CER)","Regional Economic Community (REC)"),(2,"Organisation internationale","International organisation"),(97,"Autre","Other"))),other("QR01_Type"),
    T("QR01_Fonction","Fonction du service répondant, sans nom personnel","Function of the responding service, without a personal name"),
    F("QR01_Statut","Statut de la contribution","Contribution status",STATUS),
    A("QR01_Champ","Territoires ou institutions couverts par votre expérience, sans supposer la couverture de tous les membres","Territories or institutions covered by your experience, without assuming coverage of all members")],source="Guide, § 2, p. 1–2"))
partners.append(S("QR02","Appuis effectivement réalisés pendant la période","Support actually provided during the period",[
    F("QR02_Appui","Avez-vous réalisé ou suivi un appui lié à l’utilisation des statistiques dans les discours ?","Have you provided or monitored support related to the use of statistics in speeches?",YES_NO+O((99,"Hors de votre champ d’activité","Outside your area of activity"))),
    F("QR02_Activites","Si oui, cocher les activités réellement connues","If yes, select the activities actually known",O((1,"Guide ou modèle","Guide or template"),(2,"Formation ou accompagnement","Training or support"),(3,"Fourniture ou vérification de chiffres","Providing or checking figures"),(4,"Appui à une procédure nationale","Support for a national procedure"),(5,"Diffusion ou partage d’expériences","Dissemination or sharing experiences"),(97,"Autre","Other")),kind="multi",gate=eq("QR02_Appui",1)),other("QR02_Activites",multi=True),
    A("QR02_Cas","Décrire un cas en deux phrases : période, type d’institution bénéficiaire et réalisation observable. Ajouter une référence publique si disponible.","Describe one case in two sentences: period, type of beneficiary institution and observable output. Add a public reference if available.",gate=eq("QR02_Appui",1))],note=L("Ne pas interpréter une participation à une formation comme la preuve d’une réforme appliquée.","Participation in training must not be interpreted as proof that a reform has been implemented."),source="Guide, § 2, p. 2"))
partners.append(S("QR03","Pratiques transférables et conditions d’adaptation","Transferable practices and conditions for adaptation",[
    F("QR03_Absence","Si vous ne proposez aucune pratique, indiquer la situation ; sinon laisser ce champ vide","If you are not proposing a practice, indicate the situation; otherwise leave this field blank",O((0,"Aucune pratique documentée à proposer","No documented practice to propose"),(98,"Ne sait pas","Do not know"))),
    A("QR03_Pratiques","Au maximum deux pratiques : pratique ; source ou preuve ; contexte où elle a fonctionné ; condition nécessaire à son adaptation. Au plus 80 mots au total.","Up to two practices: practice; source or evidence; context in which it worked; condition required for adaptation. Up to 80 words in total.",max_words=80,gate=notin("QR03_Absence",0,98))],note=L("Citer seulement des pratiques effectivement observées ou documentées. Un exemple plus ancien peut être cité avec sa date ; il ne constitue pas un appui réalisé pendant la période.","Cite only practices actually observed or documented. An older example may be cited with its date; it is not support provided during the reference period."),source="Guide, § 2, p. 2"))
partners.append(S("QR04","Besoins prioritaires et contribution envisageable","Priority needs and possible contribution",[
    F("QR04_Besoins","Retenir au plus trois besoins","Select up to three needs",O((1,"Procédures communes adaptables","Adaptable common procedures"),(2,"Sources et fiches de chiffres","Sources and fact sheets"),(3,"Contrôle et interprétation","Checking and interpretation"),(4,"Communication et formulation","Communication and wording"),(5,"Formation","Training"),(6,"Partage d’expériences","Sharing experiences"),(7,"Adaptation linguistique","Language adaptation"),(97,"Autre","Other"),(0,"Aucun besoin identifié","No identified need"),(98,"Ne sait pas","Do not know")),kind="multi",max_choices=3,exclusive=[0,98]),other("QR04_Besoins",multi=True),
    F("QR04_Contribution","Quelle contribution votre organisation pourrait-elle envisager, sous réserve de sa décision interne ? Un choix.","What contribution might your organisation consider, subject to its internal decision? Select one.",O((1,"Relais de consultation","Relaying the consultation"),(2,"Documents publics","Public documents"),(3,"Commentaires techniques","Technical comments"),(4,"Mise en relation avec des utilisateurs volontaires","Contact with volunteer users"),(5,"Aucune contribution envisagée à ce stade","No contribution envisaged at this stage"),(97,"Autre","Other"),(98,"Ne sait pas","Do not know"))),other("QR04_Contribution"),
    A("QR04_Contrainte","Préciser une contrainte de moyens, de mandat ou de langue, si utile","Specify a resource, mandate or language constraint, if useful")],note=L("Cette réponse ne constitue pas un engagement de financement ou de personnel.","This response is not a commitment of funding or staff."),source="Guide, § 2, p. 2"))
partners.append(S("QR05","Attribution et suite éventuelle","Attribution and possible follow-up",[
    F("QR05_Attribution","Réutilisation autorisée","Authorised reuse",ATTR,attribution=True)]+contacts("QR05")+[
    A("QR05_Nouveaute","Pratique nouvelle après le 31 octobre, facultatif : description courte et date","New practice after 31 October, optional: brief description and date")],note=ATTR_NOTE,source="Guide, § 2, p. 2"))

short=[]
short.append(S("RA01","Participation et périmètre","Participation and scope",[
    consent("RA01"),T("RA01_Pays","Pays","Country"),T("RA01_Institution","Institution ou type d’institution","Institution or type of institution"),
    T("RA01_Fonction","Fonction","Function"),F("RA01_Statut","Réponse institutionnelle, de service ou individuelle","Institutional, service-level or individual response",STATUS)],source="Guide, § 3, p. 3"))
short.append(S("RA02","Dernière situation connue","Most recent known situation",[
    A("RA02_Cas","Décrivez une demande ou préparation de discours pendant la période : mois, thème, délai disponible, appui obtenu ou non. Si aucun cas n’est connu, l’indiquer. Trois lignes.","Describe a request or speech preparation during the period: month, topic, time available, support obtained or not. If no case is known, say so. Three lines.")],source="Guide, § 3, p. 3"))
short.append(S("RA03","Source et contrôle","Source and checking",[
    F("RA03_Source","Pour ce cas, pouvait-on retrouver la source du chiffre ?","For this case, could the source of the figure be located?",YES_NO+O((99,"Non applicable","Not applicable"))),
    T("RA03_Controle","Qui a vérifié le contenu avant transmission ? Fonction ou absence de contrôle connu.","Who checked the content before transmission? Function or absence of a known check.")],theme="Vérification",source="Guide, § 3, p. 3"))
short.append(S("RA04","Première amélioration","First improvement",[
    A("RA04_Amelioration","Quelle difficulté faut-il résoudre en priorité ? Quelle action serait possible avec les moyens existants et quelle fonction pourrait la conduire ? Trois lignes.","Which difficulty should be addressed first? What action would be possible with existing resources and which function could lead it? Three lines.")],source="Guide, § 3, p. 3"))
short.append(S("RA05","Réutilisation et suite","Reuse and follow-up",[
    F("RA05_Attribution","Restitution autorisée","Authorised reporting",ATTR,attribution=True),
    T("RA05_Lien","Lien public utile, facultatif","Useful public link, optional",max_chars=2000),
    T("RA05_Contact_fonction","Contact institutionnel pour un complément : fonction ou service, facultatif","Institutional follow-up contact: function or service, optional",private=True),
    F("RA05_Contact_courriel","Contact institutionnel pour un complément, facultatif (courriel)","Institutional contact for follow-up, optional (email)",kind="email",max_chars=254,private=True)],note=ATTR_NOTE,source="Guide, § 3, p. 3"))

FORMS={
    "ins":{"title":L("Instituts nationaux de la statistique","National statistical offices"),"duration":L("15 à 20 minutes","15 to 20 minutes"),"sections":ins,"consent":"QI01_Participation","attribution":"QI12_Attribution","institution":"QI01_INS","country":"QI01_Pays","status":"QI01_Statut","previous":"QI01_Reference_precedente"},
    "utilisateurs":{"title":L("Équipes préparant les discours","Speech-preparation teams"),"duration":L("10 à 15 minutes","10 to 15 minutes"),"sections":users,"consent":"QU01_Participation","attribution":"QU08_Attribution","institution":"QU01_Institution","country":"QU01_Pays","status":"QU01_Statut","previous":"QU01_Reference_precedente"},
    "partenaires":{"title":L("CER et organisations internationales","RECs and international organisations"),"duration":L("Environ 10 minutes","About 10 minutes"),"sections":partners,"consent":"QR01_Participation","attribution":"QR05_Attribution","institution":"QR01_Organisation","country":"QR01_Champ","status":"QR01_Statut"},
    "abrege":{"title":L("Réponse abrégée","Short response"),"duration":L("5 à 10 minutes","5 to 10 minutes"),"sections":short,"consent":"RA01_Participation","attribution":"RA05_Attribution","institution":"RA01_Institution","country":"RA01_Pays","status":"RA01_Statut"},
}

NOTICE=L(
"STATAFRIC et le consultant Mamadou Niang préparent des outils pratiques pour améliorer l’appui des INS aux discours des décideurs.\n\n"
"Période de référence : du 1er novembre 2025 au 31 octobre 2026. Décrire les pratiques effectivement connues ; signaler séparément toute nouveauté postérieure, avec sa date.\n\n"
"Participation volontaire. Les réponses sont destinées au consultant et aux personnes désignées par STATAFRIC pour la mission. Restitution agrégée ou anonymisée par défaut. Aucune attribution ou citation identifiable sans la permission indiquée à la dernière rubrique. Un cas très particulier peut rester reconnaissable. Les coordonnées facultatives sont séparées des données d’analyse.\n\n"
"Ne transmettre ni opinions politiques, données individuelles, coordonnées personnelles inutiles, ni document confidentiel. Utiliser des liens publics ou des extraits non confidentiels autorisés. Une contribution individuelle n’engage pas l’institution.\n\n"
"Une réponse partielle est utile. Les choix « aucun », « ne sait pas » et « non applicable » excluent les autres choix lorsqu’ils sont proposés dans une question à réponses multiples. Un blanc ne signifie ni « non » ni zéro. Aucune réponse substantielle n’est précochée.\n\n"
"Avant l’envoi final, vos réponses restent dans la session de l’application ; elles ne sont pas enregistrées dans la base. Un rechargement de page peut les perdre. Téléchargez un brouillon JSON pour reprendre plus tard. Après l’envoi confirmé, seule l’équipe autorisée peut consulter les réponses ; aucune liste de répondants n’est publique.",
"STATAFRIC and consultant Mamadou Niang are preparing practical tools to improve NSO support for decision-makers’ speeches.\n\n"
"Reference period: 1 November 2025 to 31 October 2026. Describe practices actually known; report any later developments separately, with their date.\n\n"
"Participation is voluntary. Responses are intended for the consultant and persons designated by STATAFRIC for this assignment. Reporting is aggregated or anonymised by default. No identifiable attribution or quotation is permitted without the permission selected in the last section. A very specific case may remain recognisable. Optional contact details are kept separate from analytical data.\n\n"
"Do not provide political opinions, individual-level data, unnecessary personal contact details or confidential documents. Use public links or authorised non-confidential extracts. An individual contribution does not commit the institution.\n\n"
"A partial response is useful. ‘None’, ‘Do not know’ and ‘Not applicable’, when offered in multiple-response questions, exclude the other choices. A blank is neither ‘No’ nor zero. No substantive response is preselected.\n\n"
"Before final submission, your responses stay in the application session; they are not stored in the database. Reloading the page may lose them. Download a JSON draft to resume later. After confirmed submission, only the authorised team can view responses; no respondent list is public.")


def text(value: dict[str,str] | str, lang: str="fr") -> str:
    return value.get(lang,value.get("fr","")) if isinstance(value,dict) else value


def iter_fields(form_id: str):
    for section in FORMS[form_id]["sections"]:
        for field in section["fields"]:
            yield section,field
