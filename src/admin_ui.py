"""Private diagnostic workspace. OIDC + allowlist before any data access."""
import streamlit as st
import time
from urllib.parse import urlencode,urlsplit,urlunsplit

def read_config():
    try:return dict(st.secrets)
    except FileNotFoundError:return {}

def authenticate(cfg,settings):
    if settings.mode=='local' and cfg.get('ALLOW_LOCAL_ADMIN') is True:
        st.warning('Test local seulement : aucune donnée réelle. Ne pas publier cette configuration.');return 'test-local'
    if settings.mode!='production':st.info('Administration désactivée en prévisualisation. Configurer les secrets et l’authentification.');st.stop()
    allowed={str(x).strip().casefold() for x in cfg.get('ADMIN_ALLOWED_EMAILS',[])}
    if not allowed or 'auth' not in cfg:st.error('Configurer auth et ADMIN_ALLOWED_EMAILS dans les secrets privés de cette application.');st.stop()
    if not st.user.is_logged_in:
        st.info('Accès réservé au consultant et aux personnes habilitées.')
        if st.button('Connexion sécurisée',type='primary'):st.login()
        st.stop()
    email=str(st.user.get('email','')).casefold();verified=st.user.get('email_verified') is True
    exp=st.user.get('exp')
    try:valid_expiry=exp is None or float(exp)>time.time()
    except (TypeError,ValueError):valid_expiry=False
    if email not in allowed or not verified or not valid_expiry:
        st.error('Compte non autorisé, adresse non vérifiée ou session expirée.')
        if st.button('Se déconnecter'):st.logout()
        st.stop()
    st.sidebar.caption(email)
    if st.sidebar.button('Se déconnecter'):
        for k in list(st.session_state):del st.session_state[k]
        st.logout();st.stop()
    return email

def refresh(store):
    # Only commit a complete successful read to session state.
    records=store.records();reviews=store.reviews();events=store.registry_events()
    st.session_state.update(admin_records=records,admin_reviews=reviews,admin_events=events,loaded_at=utcnow())

def render_register(store,actor,settings,events):
    kind=st.selectbox('Registre à renseigner',list(REGISTERS),format_func=lambda x:REGISTERS[x]['title'])
    include=st.checkbox('Afficher aussi les enregistrements fictifs',key='reg_include_tests')
    rows=latest_registry(events,kind,include)
    st.caption('Le consultant tient ce registre. Les modifications sont versionnées ; aucune réponse originale n’est remplacée.')
    if rows:st.dataframe([{**r['data'],'ID registre':r['record_id'],'Version':r['version'],'Test':r['is_test']} for r in rows],hide_index=True,use_container_width=True)
    selection=st.selectbox('Créer ou réviser une fiche',[None]+[r['record_id'] for r in rows],format_func=lambda x:'Nouvelle fiche' if x is None else x,key='edit_registry_'+kind)
    selected=next((r for r in rows if r['record_id']==selection),None);initial=selected['data'] if selected else {}
    spec=REGISTERS[kind]['columns']
    keytag=kind+'_'+(selection or 'new')+'_'+str(selected['version'] if selected else 0)
    values={}
    with st.form('registry_form_'+keytag):
        for name,rule in spec.items():
            val=initial.get(name,'');key='reg_'+keytag+'_'+name
            if isinstance(rule,list):values[name]=st.selectbox(name,['']+rule,index=(['']+rule).index(val) if val in ['']+rule else 0,key=key)
            elif rule=='long':values[name]=st.text_area(name,value=val,max_chars=12000,key=key)
            else:values[name]=st.text_input(name,value=val,placeholder='AAAA-MM-JJ' if rule=='date' else '',max_chars=1000,key=key)
        simulated=st.checkbox('Fiche fictive de recette — exclue des résultats réels',value=bool(selected['is_test']) if selected else settings.test_mode,disabled=settings.mode=='local')
        save=st.form_submit_button('Enregistrer une nouvelle version')
    if save:
        try:
            validate_registry(kind,values)
            is_test=settings.test_mode or simulated or (kind=='tests' and values.get('Nature')=='Simulation technique')
            store.save_registry(kind,selection or str(uuid4()),values,actor,selected['version'] if selected else 0,is_test)
            refresh(store)
        except ValidationError as exc:st.error(str(exc))
        except StorageError as exc:st.error('Conflit de version : actualiser avant de reprendre.' if str(exc)=='VERSION_CONFLICT' else 'Enregistrement non confirmé.')
        else:st.success('Version enregistrée.');st.rerun()
    with st.expander('Importer un registre CSV préparé hors ligne'):
        st.caption('Séparateur point-virgule, UTF-8. Utiliser les modèles fournis. L’import crée des fiches, il ne remplace pas les versions existantes. Les coordonnées ne doivent pas figurer ici.')
        upl=st.file_uploader('CSV du registre sélectionné',type=['csv'],key='import_'+kind)
        confirm=st.checkbox('Je confirme le registre, la provenance et le caractère réel ou fictif des lignes.',key='confirm_import_'+kind)
        if st.button('Contrôler puis importer',disabled=upl is None or not confirm,key='do_import_'+kind):
            try:
                raw=upl.getvalue()
                if len(raw)>5_000_000:raise ValidationError('Fichier trop volumineux.')
                imported=list(csv.DictReader(io.StringIO(raw.decode('utf-8-sig')),delimiter=';'))
                if len(imported)>500:raise ValidationError('Importer au plus 500 lignes par lot.')
                for r in imported:validate_registry(kind,r)
            except (ValueError,UnicodeError,ValidationError) as exc:st.error(str(exc))
            else:
                count=0
                try:
                    existing={r['data'].get('ID invitation') for r in rows if r['data'].get('ID invitation')}
                    for r in imported:
                        if kind=='invitations' and r.get('ID invitation') in existing:continue
                        store.save_registry(kind,str(uuid4()),r,actor,0,settings.test_mode);count+=1
                        if kind=='invitations' and r.get('ID invitation'):existing.add(r['ID invitation'])
                    refresh(store);st.success(f'{count} fiches ajoutées. Les identifiants d’invitation déjà présents ont été ignorés.')
                except StorageError:st.error(f'Import interrompu après {count} fiches confirmées. Actualiser avant de recommencer ; ne pas importer aveuglément le même lot.')

def main():
    st.set_page_config(page_title='HISWACA — Administration',page_icon='🔒',layout='wide')
    cfg=read_config()
    try:settings=Settings.from_mapping(cfg,admin=True)
    except ConfigurationError as exc:st.error(str(exc));st.stop()
    actor=authenticate(cfg,settings);store=make_store(settings,admin=True)
    if settings.mode=='production':
        try:campaign_check=store.admin_campaign()
        except StorageError:st.error('Impossible de vérifier la campagne administrée.');st.stop()
        if not campaign_check:st.error('Campagne introuvable. Vérifier CAMPAIGN_ID.');st.stop()
        if campaign_check.get('test_mode') is not None and campaign_check['test_mode']!=settings.test_mode:
            st.error('TEST_MODE ne concorde pas avec la campagne en base. Corriger les secrets avant de poursuivre.');st.stop()

    st.title('Diagnostic HISWACA — administration v'+APP_VERSION)
    if settings.test_mode:st.warning('Campagne de test. Les essais ne doivent jamais alimenter L2 ou L3.')
    if st.button('Charger / actualiser tous les registres',type='primary'):
        try:refresh(store)
        except StorageError:st.error('Lecture incomplète ou impossible. Aucun nouvel export partiel n’est présenté comme complet.')
    if 'loaded_at' in st.session_state:st.caption('Dernière lecture complète : '+st.session_state['loaded_at'])
    records=st.session_state.get('admin_records',[]);events=st.session_state.get('admin_events',[])
    tab=st.sidebar.radio('Espace de travail',['Contributions','Registres du diagnostic','Synthèse descriptive','Liens et invitations','Préparation et campagne'])
    if tab=='Contributions':
        fs=st.multiselect('Parcours',list(FORMS),default=list(FORMS));ls=st.multiselect('Langues',LANGUAGES,default=LANGUAGES)
        tests=st.checkbox('Inclure les réponses fictives',False)
        rows=[r for r in records if r['payload']['form_id'] in fs and r['payload']['language'] in ls and (tests or not r['payload'].get('is_test',True))]
        statuses=st.multiselect('Statuts de contrôle',['À examiner','Exploitable','À clarifier','Doublon','Hors champ'],default=['À examiner','Exploitable','À clarifier','Doublon','Hors champ'])
        rows=[r for r in rows if (r.get('review') or {}).get('status','À examiner') in statuses]
        st.metric('Contributions affichées',len(rows));st.caption('Un enregistrement n’est ni un INS distinct ni une position institutionnelle consolidée.')
        if rows:
            st.dataframe(contribution_rows(rows),hide_index=True,use_container_width=True)
            st.warning('Export interne : sans contacts structurés ne signifie pas anonyme. Vérifier noms, récits et autorisations avant diffusion.')
            st.download_button('Exporter les contributions filtrées (CSV + originaux JSON)',export_bundle(rows),file_name='HISWACA_contributions.zip',mime='application/zip')
            selected=st.selectbox('Contribution à examiner',[r['id'] for r in rows]);r=next(r for r in rows if r['id']==selected)
            with st.expander('Réponse originale analytique, jamais écrasée'):st.json(r['payload'])
            with st.form('review_'+selected):
                status=st.selectbox('Décision',['À examiner','Exploitable','À clarifier','Doublon','Hors champ']);note=st.text_area('Justification obligatoire',max_chars=5000)
                linked=st.selectbox('Lien vers la contribution conservée',[None]+[x['id'] for x in records if x['id']!=selected])
                save=st.form_submit_button('Ajouter cette décision')
            if save:
                if not note.strip() or status=='Doublon' and not linked:st.error('Justification et lien du doublon requis.')
                else:
                    try:store.add_review(selected,actor,status,note.strip(),linked or '');refresh(store)
                    except StorageError:st.error('Décision non confirmée.')
                    else:st.rerun()
            with st.expander('Historique des décisions'):st.dataframe([x for x in st.session_state.get('admin_reviews',[]) if x['response_id']==selected],use_container_width=True)
            with st.expander('Coordonnées facultatives — accès restreint'):
                consent=st.checkbox('Cet accès est nécessaire au suivi autorisé de la mission.')
                if st.button('Préparer les contacts filtrés',disabled=not consent):
                    try:
                        ids={r['id'] for r in rows};contacts=store.all_contacts()
                        data=[{'ID réponse':'REP-'+c['response_id'],'Champ':k,'Valeur':v} for c in contacts if c['response_id'] in ids for k,v in c['contact'].items()]
                        st.session_state['contact_download']=csv_bytes(data,['ID réponse','Champ','Valeur']);st.session_state['contact_scope']=sorted(ids)
                    except StorageError:st.error('Lecture impossible.')
                if not consent or st.session_state.get('contact_scope')!=sorted(r['id'] for r in rows):st.session_state.pop('contact_download',None)
                if 'contact_download' in st.session_state:st.download_button('Contacts séparés — ne pas publier',st.session_state['contact_download'],file_name='HISWACA_CONTACTS_RESTREINT.csv')
        else:st.info('Aucune réponse ne correspond aux filtres, ou données non encore chargées.')
    elif tab=='Registres du diagnostic':render_register(store,actor,settings,events)
    elif tab=='Synthèse descriptive':
        st.caption('Seules les réponses réelles classées Exploitable sont retenues. Aucun score de maturité, intervalle de sondage ou extrapolation.')
        st.dataframe(descriptive_summary(records),hide_index=True,use_container_width=True)
        coverage=coverage_rows(records)
        with st.expander('Couverture réelle par pays, profil, domaine, langue et version',expanded=True):
            st.caption('Effectifs descriptifs. Les noms d’institutions et pays doivent être harmonisés ; ne pas sommer des institutions répétées entre groupes. Le profil RA est un code de l’instrument, pas une réponse complète.')
            if coverage:
                st.dataframe(coverage,hide_index=True,use_container_width=True)
                st.download_button('Exporter la couverture',csv_bytes(coverage,list(coverage[0])),file_name='HISWACA_couverture_reelle.csv')
            else:st.info('Aucune contribution réelle chargée.')

        use=[r for r in records if not r['payload'].get('is_test',True) and (r.get('review') or {}).get('status')=='Exploitable']
        st.download_button('Export réel exploitable pour L3',export_bundle(use,{k:latest_registry(events,k) for k in REGISTERS}),file_name='HISWACA_L3_REEL.zip')
        fid=st.selectbox('Profil à analyser',list(FORMS));fields=[f for _,f in iter_fields(fid) if f['type'] in ('single','multi') and not f.get('private')]
        st.caption('Les fréquences ci-dessous utilisent uniquement la version actuelle du questionnaire ; les anciennes réponses sont conservées dans les exports, sans fusion implicite.')
        question=st.selectbox('Question',[f['id'] for f in fields],format_func=lambda x:x+' — '+text(next(f for f in fields if f['id']==x)['label']))
        field=next(f for f in fields if f['id']==question);group=[r for r in use if r['payload']['form_id']==fid and r['payload'].get('schema_version')==SCHEMA_VERSION]
        states=Counter(r['payload'].get('field_states',{}).get(question,'NON_PRESENT_VERSION') for r in group)
        counts=Counter(v for r in group for v in (r['payload']['answers'].get(question,[]) if isinstance(r['payload']['answers'].get(question),list) else [r['payload']['answers'].get(question)]) if v is not None)
        denominator=sum(r['payload'].get('field_states',{}).get(question)=='RENSEIGNE' for r in group)
        st.write({'Contributions du profil':len(group),'Réponses de fond':denominator,'États des informations':dict(states)})
        st.dataframe([{'Code':o['code'],'Modalité':text(o['label']),'Effectif':counts[o['code']],'Proportion parmi les réponses de fond':counts[o['code']]/denominator if denominator and o['code'] not in (98,99) else None} for o in field['options']],hide_index=True)
        st.caption('Pour les choix multiples, les proportions ne s’additionnent pas à 100 %. Les non-réponses restent distinctes. Harmoniser les noms institutionnels avant un décompte définitif.')
    elif tab=='Liens et invitations':
        base=st.text_input('URL HTTPS de la collecte',value=settings.collection_url)
        relay=st.text_input('Identifiant relais non secret, facultatif');inv=st.text_input('Identifiant invitation non secret, facultatif')
        parsed=urlsplit(base)
        if parsed.scheme=='https' and parsed.netloc and not parsed.username and not parsed.password:
            base=urlunsplit((parsed.scheme,parsed.netloc,parsed.path or '/','',''))
            for f in FORMS:
                with st.expander(text(FORMS[f]['title'])):
                    for lang in LANGUAGES:st.code(base+'?'+urlencode({'form':f,'lang':lang,**({'relay':relay} if relay else {}),**({'inv':inv} if inv else {})}),language=None)
        elif base:st.warning('URL HTTPS valide requise.')
        st.info('Chaque INS relaie QP à 3–8 producteurs et QU à 4–6 équipes utilisatrices. L’identifiant de relais ne donne aucun droit de lecture. Le module relais_ins.py prépare les messages et le relevé sans envoyer de courriel.')
    else:
        st.download_button('Modèles CSV vierges des huit registres',register_csv_templates(),file_name='HISWACA_registres_vierges.zip')
        dic=dictionary_rows();st.download_button('Dictionnaire des cinq langues',csv_bytes(dic,list(dic[0])),file_name='HISWACA_dictionnaire_5_langues.csv')
        matrix=latest_registry(events,'recette');pairs={(r['data'].get('Parcours'),r['data'].get('Langue')):r['data'].get('Statut final') for r in matrix}
        st.dataframe([{'Parcours':f,**{l:pairs.get((f,l),'À vérifier') for l in LANGUAGES}} for f in FORMS],hide_index=True)
        st.warning('L’existence des traductions et les tests automatisés ne valent pas relecture linguistique indépendante ni validation dans l’hébergement distant.')
        if settings.mode=='production':
            if st.button('Afficher la campagne'):
                try:st.json(store.admin_campaign())
                except StorageError:st.error('Lecture impossible.')
            action=st.selectbox('Action',['Fermer la collecte','Ouvrir la collecte']);confirm=st.checkbox('Je suis habilité à modifier la campagne. Les dates et le mode en base restent applicables.')
            if st.button('Appliquer',disabled=not confirm):
                try:store.set_active(action=='Ouvrir la collecte')
                except StorageError:st.error('Modification non confirmée.')
                else:st.success('État modifié. Vérifier les bornes de dates et TEST_MODE avant diffusion.')
        st.caption('La phase principale est du 19 au 26 novembre 2026 inclus, en UTC dans cette configuration. Les compléments tardifs nécessitent une décision explicite ; aucune prolongation tacite.')

if __name__=='__main__':main()
