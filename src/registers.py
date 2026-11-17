"""Diagnostic registers. All changes are append-only, with optimistic concurrency.
Administrative schema is in French; original contributions retain their language.
"""
REGISTERS={
 'invitations':{'title':'Invitations et relais','columns':{
  'Pays':'text','INS relais':'text','ID relais':'text','ID invitation':'text','Institution':'text','Service':'text','Domaine':'text',
  'Profil':['ins','producteurs','utilisateurs','partenaires','abrege'],'Langue':['fr','en','ar','pt','es','Non connue'],
  'Sélection':['Directe','Relais INS','Spontanée'],'Motif de sélection':'long','Canal':['Courriel','Courrier','Téléphone','Autre'],
  'Date invitation':'date','Échéance':'date','Relance 1':'date','Relance 2':'date','Statut':['Préparé','Envoyé','Non remis','Relancé','Réponse reçue','Refus'],'ID réponse':'text','Limite ou adaptation':'long'}},
 'documents':{'title':'Documents et recherches multilingues','columns':{
  'ID document':'text','Titre original':'long','Institution':'text','Pays / périmètre':'text','Type':['Norme','Guide','Procédure','Publication statistique','Discours','Décision','Autre'],
  'Langue':['fr','en','ar','pt','es','sw','Autre'],'Date publication':'date','Version source':'text','Lien public ou emplacement autorisé':'text','Date consultation':'date','Pages ou passage':'long','Extrait original':'long','Traduction FR':'long',
  'Statut traduction':['Non nécessaire','À relire','Relue','Ambiguïté non résolue'],'Source effectivement lue':['Texte complet','Passage ciblé','Notice seulement','Non accessible'],
  'Requête et site exploré':'long','Méthode / relecteur traduction':'text','Décision':['À examiner','Retenu','Écarté','À compléter'],'Motif / limite':'long','ID famille multilingue':'text','Destination':['L2','Complément documentaire L3'],'Version livrable':'text'}},
 'discours':{'title':'Évaluation des extraits de discours','columns':{
  'ID discours':'text','ID extrait':'text','Pays':'text','Institution':'text','Langue':['fr','en','ar','pt','es','sw','Autre'],'Date discours':'date','Ordre affirmation':'text','Corpus':['Trois premières affirmations','Complément pédagogique','Hors période justifié','Décision chiffrée distincte'],
  'Motif de sélection':'long','Extrait original':'long','Traduction FR':'long','Statut du chiffre':['Résultat observé','Estimation','Prévision / projection','Objectif','Décision annoncée','À préciser'],
  'ID source de vérification':'text','Version disponible à la date du discours':'text','Version ultérieure distincte':'text',
  **{c:['Satisfait','Partiel','Difficulté constatée','Non applicable','Non vérifiable'] for c in ['Source','Période','Définition et champ','Unité et calcul','Comparabilité','Interprétation','Clarté','Limites et incertitude']},
  **{c+' — justification':'long' for c in ['Source','Période','Définition et champ','Unité et calcul','Comparabilité','Interprétation','Clarté','Limites et incertitude']},
  'Conclusion limitée à la preuve':'long','Destination':['L2','Complément documentaire L3']}},
 'constats':{'title':'Constats et recommandations','columns':{
  'ID constat':'text','Thème':['Demande','Priorisation','Sources','Vérification','Communication','Correction','Suivi','Coordination','Langues'],
  'Problème':'long','Statut preuve':['Déclaré','Documenté','Concordant','Contradictoire','Non établi'],
  'IDs réponses / documents / extraits':'long','Contrainte':'long','Action':'long','Responsable':'text','Moyens existants':'long','Échéance indicative':'text','Preuve de fonctionnement':'long',
  'Outil':['O1','O2','O3','O4','O5','O6','O7','O8','Transversal'],'Destination':['L2 documentaire','L3'],'Limites':'long'}},
 'traductions':{'title':'Traductions et décisions de codage','columns':{
  'ID réponse ou instrument':'text','Code champ':'text','Langue originale':['fr','en','ar','pt','es','sw','Autre'],'Texte original':'long','Traduction FR':'long',
  'Moyen utilisé':'text','Relecteur':'text','Date relecture':'date','Statut':['À traduire','Traduite non relue','Relue','Ambiguïté à clarifier'],
  'Divergence et arbitrage':'long','Code analytique retenu':'text','État retenu':['RENSEIGNE','NR','NSP','NA','NF','FILTRE_INDETERMINE','À contrôler'],
  'Autorisation attribution vérifiée':['Non','Oui','Non applicable'],'Version':'text'}},
 'tests':{'title':'Tests réels des outils O1–O8','columns':{
  'ID testeur pseudonymisé':'text','Institution / profil':'text','Profil':['INS','Autre producteur','Utilisateur','Partenaire','Contrôle interne'],
  'Version prototype':'text','Date test':'date','Langue essai':['fr','en','ar','pt','es','Autre'],'Langue support':'text',
  'Nature':['Essai utilisateur réel','Contrôle interne','Simulation technique'],'Tâche':['T1 Source','T2 Sens','T3 Comparaison','T4 Passage','T5 Vérification'],
  'Support public ou exercice identifié':'long','Consigne':'long','Réponse produite':'long','Temps déclaré ou mesuré':'text','Aide reçue':'long',
  'Résultat':['Réussite autonome','Réussite avec aide','Échec','Non observée'],'Difficulté':'long','Gravité':['Bloquante','Importante','Mineure','Aucune'],
  'Correction décidée':'long','Version corrigée':'text','ID contre-test':'text','Destination':['L3 préparation','L4']}},
 'observations':{'title':'Revue des livrables','columns':{
  'ID observation':'text','Auteur / profil autorisé':'text','Livrable':['L1','L2','L3','L4','L5'],'Version examinée':'text','Date réception':'date','Langue':'text','Observation':'long',
  'Décision':['À examiner','Retenue','Retenue en partie','Non retenue avec justification','À arbitrer'],'Justification':'long','Modification':'long','Localisation':'text','Responsable':'text',
  'Statut':['Ouverte','En cours','Traitée','Clôturée'],'Preuve de clôture':'long'}},
 'recette':{'title':'Contrôle des 25 combinaisons','columns':{
  'Parcours':['ins','producteurs','utilisateurs','partenaires','abrege'],'Langue':['fr','en','ar','pt','es'],'Version':'text','Date':'date','Vérificateur':'text',
  **{k:['À vérifier','Conforme','Anomalie','Non applicable'] for k in ['Sens et traduction','Notices et autorisations','Codes et filtres','Choix exclusifs','Changement de langue','Brouillon','Envoi partiel','Enregistrement distant','Accusé','Export UTF-8','Téléphone','Arabe RTL']},
  'Anomalie et correction':'long','Statut final':['À vérifier','Prête techniquement','Relecture linguistique effectuée','Autorisation de diffusion donnée'],'Preuve':'long'}}
}

def latest_registry(events,kind=None,include_tests=False):
    latest={}
    for e in events:
        if kind and e['kind']!=kind:continue
        key=e['record_id']
        if key not in latest or e['version']>latest[key]['version']:latest[key]=e
    return sorted([e for e in latest.values() if include_tests or not e.get('is_test',True)],key=lambda e:(e.get('created_at',''),e['record_id']))

def validate_registry(kind,data):
    if kind not in REGISTERS or not isinstance(data,dict):raise ValidationError('Registre inconnu.')
    allowed=REGISTERS[kind]['columns']
    if set(data)-set(allowed):raise ValidationError('Colonnes inconnues : '+', '.join(sorted(set(data)-set(allowed))))
    if not any(str(v).strip() for v in data.values()):raise ValidationError('Enregistrement vide.')
    for k,v in data.items():
        if not isinstance(v,str) or len(v)>12000 or '\x00' in v:raise ValidationError(k+' : texte invalide.')
        rule=allowed[k]
        if v and isinstance(rule,list) and v not in rule:raise ValidationError(k+' : modalité inconnue.')
        if v and rule=='date':
            try:datetime.strptime(v,'%Y-%m-%d')
            except ValueError as e:raise ValidationError(k+' : utiliser AAAA-MM-JJ.') from e
    if kind=='invitations' and data.get('Statut') in ('Envoyé','Relancé','Réponse reçue') and not data.get('Date invitation'):raise ValidationError('Date réelle d’invitation requise pour ce statut.')
    if kind=='discours':
        for c in ['Source','Période','Définition et champ','Unité et calcul','Comparabilité','Interprétation','Clarté','Limites et incertitude']:
            if data.get(c) and not data.get(c+' — justification'):raise ValidationError(c+' : renseigner la justification.')
    if kind=='constats' and data.get('Destination')=='L2 documentaire' and 'REP-' in data.get('IDs réponses / documents / extraits',''):
        raise ValidationError('Un constat L2 ne peut pas utiliser une réponse de consultation. Choisir L3 ou retirer cette source.')
    if kind=='recette' and data.get('Statut final')=='Autorisation de diffusion donnée':
        checks=[k for k in allowed if isinstance(allowed[k],list) and 'Conforme' in allowed[k]]
        if any(data.get(k) not in ('Conforme','Non applicable') for k in checks) or not data.get('Preuve') or not data.get('Vérificateur') or not data.get('Date'):
            raise ValidationError('Clore tous les contrôles et renseigner la preuve, le vérificateur et la date avant autorisation.')
    if kind=='observations' and data.get('Statut')=='Clôturée' and not data.get('Preuve de clôture'):raise ValidationError('Une preuve de clôture est nécessaire.')
    return data

def register_csv_templates():
    b=io.BytesIO()
    with zipfile.ZipFile(b,'w',zipfile.ZIP_DEFLATED) as z:
        for kind,meta in REGISTERS.items():z.writestr(kind+'.csv',csv_bytes([],list(meta['columns'])))
    return b.getvalue()

def descriptive_summary(records):
    # Only reviewed usable real responses; never count records as independent countries.
    included=[r for r in records if not r['payload'].get('is_test',True) and (r.get('review') or {}).get('status')=='Exploitable']
    result=[]
    for form_id,form in FORMS.items():
        group=[r for r in included if r['payload']['form_id']==form_id]
        inst={(str(r['payload']['answers'].get(form['country'],'')).strip().casefold(),str(r['payload']['answers'].get(form['institution'],'')).strip().casefold()) for r in group if r['payload']['answers'].get(form['institution'])}
        result.append({'Profil':form_id,'Contributions réelles exploitables':len(group),'Institutions déclarées distinctes (à harmoniser)':len(inst),'Sans institution déclarée':sum(not r['payload']['answers'].get(form['institution']) for r in group)})
    return result

def coverage_rows(records):
    """Descriptive coverage; no response rate or implicit institutional consolidation."""
    groups={}
    for r in records:
        p=r['payload']
        if p.get('is_test',True):continue
        form=FORMS.get(p.get('form_id'))
        if not form:continue
        a=p.get('answers',{});origin=a.get('RA01_Profil','') if p['form_id']=='abrege' else ''
        key=(str(a.get(form['country']) or 'Non renseigné').strip(),p['form_id'],str(origin),str(a.get(form.get('sector','')) or 'Non renseigné').strip(),p.get('language',''),p.get('schema_version',''))
        g=groups.setdefault(key,{'received':0,'usable':0,'institutions':set(),'missing':0,'individual':0})
        g['received']+=1;g['usable']+=(r.get('review') or {}).get('status')=='Exploitable'
        institution=str(a.get(form['institution']) or '').strip()
        if institution:g['institutions'].add(institution.casefold())
        else:g['missing']+=1
    return [dict(zip(['Pays / zone déclaré','Parcours','Profil RA déclaré','Domaine déclaré','Langue interface','Version'],k),**{'Contributions réelles reçues':v['received'],'Contributions classées exploitables':v['usable'],'Institutions déclarées distinctes (à harmoniser)':len(v['institutions']),'Sans institution déclarée':v['missing']}) for k,v in sorted(groups.items())]
