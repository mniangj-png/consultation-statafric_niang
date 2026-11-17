"""Optional NSO relay tool. No database, no recipient-response access, no mail sending."""
import streamlit as st
from urllib.parse import urlencode,urlsplit,urlunsplit

def main():
    st.set_page_config(page_title='HISWACA — Relais INS',page_icon='🔗',layout='centered')
    lang=st.sidebar.selectbox('Langue / Language / اللغة',LANGUAGES,format_func=lambda l:LANG_NAMES[l],key='relay_language')
    if lang=='ar':st.markdown('<style>[data-testid="stMarkdownContainer"], [data-testid="stWidgetLabel"], textarea,input {direction:rtl;text-align:right;unicode-bidi:plaintext;} pre,code {direction:ltr;}</style>',unsafe_allow_html=True)
    st.title(ui('relay_title',lang));st.info(ui('relay_warning',lang));st.write(text(FORMS['ins']['relay_note'],lang));st.caption(ui('dates',lang))
    st.caption(ui('draft_warning',lang))
    base=st.text_input(ui('base_url',lang));ns=st.text_input(text(next(f for _,f in iter_fields('ins') if f['id']=='QI01_INS')['label'],lang));country=st.text_input(text(next(f for _,f in iter_fields('ins') if f['id']=='QI01_Pays')['label'],lang));relay=st.text_input(ui('relay_id',lang))
    rows=st.session_state.setdefault('relay_rows',[])
    with st.expander(ui('relay_resume',lang)):
        upl=st.file_uploader(ui('relay_resume',lang),type=['csv'])
        if st.button(ui('relay_import',lang),disabled=upl is None):
            try:
                if len(upl.getvalue())>2_000_000:raise ValueError()
                imported=list(csv.DictReader(io.StringIO(upl.getvalue().decode('utf-8-sig')),delimiter=';'))
                if len(imported)>200:raise ValueError()
                for row in imported:
                    validate_registry('invitations',row)
                    if row.get('Profil') not in ('producteurs','utilisateurs') or not row.get('ID invitation'):raise ValueError()
                existing={r['ID invitation'] for r in rows}
                for row in imported:
                    if row['ID invitation'] not in existing:rows.append(row);existing.add(row['ID invitation'])
            except (ValueError,ValidationError,UnicodeDecodeError,KeyError):st.error(ui('invalid_file',lang))

    with st.form('invitation_form'):
        profile=st.selectbox(ui('form',lang),['producteurs','utilisateurs'],format_func=lambda x:text(FORMS[x]['title'],lang))
        institution=st.text_input(ui('institution',lang));sector=st.text_input(ui('sector',lang))
        target_lang=st.selectbox(ui('language',lang),LANGUAGES,format_func=lambda l:LANG_NAMES[l],index=LANGUAGES.index(lang))
        sent_date=st.text_input(ui('date_sent',lang),placeholder='2026-11-19')
        submit=st.form_submit_button(ui('add_invite',lang))
    parsed=urlsplit(base)
    valid=parsed.scheme=='https' and bool(parsed.netloc) and not parsed.username and not parsed.password
    if submit:
        try:
            if not valid or not institution.strip():raise ValueError()
            if sent_date:datetime.strptime(sent_date,'%Y-%m-%d')
        except ValueError:st.error(ui('error_invite',lang))
        else:
            invitation_id='INV-'+uuid4().hex[:16]
            normalized=urlunsplit((parsed.scheme,parsed.netloc,parsed.path or '/','',''))
            link=normalized+'?'+urlencode({'form':profile,'lang':target_lang,'relay':relay,'inv':invitation_id})
            row={k:'' for k in REGISTERS['invitations']['columns']};row.update({'Pays':country,'INS relais':ns,'ID relais':relay,'ID invitation':invitation_id,'Institution':institution,'Domaine':sector,'Profil':profile,'Langue':target_lang,'Sélection':'Relais INS','Canal':'Courriel','Date invitation':sent_date,'Échéance':'2026-11-26','Statut':'Envoyé' if sent_date else 'Préparé'})
            rows.append(row);st.session_state['relay_last_link']=link;st.session_state['relay_last_lang']=target_lang
    if 'relay_last_link' in st.session_state:
        link=st.session_state['relay_last_link'];ll=st.session_state['relay_last_lang']
        st.subheader(ui('link',lang));st.code(link,language=None)
        st.text_area(ui('invite',lang),value=ui('invitation_text',ll,link=link),height=250,key='message_'+link)
    if rows:
        st.caption(ui('relay_count',lang,p=sum(x['Profil']=='producteurs' for x in rows),u=sum(x['Profil']=='utilisateurs' for x in rows)))
        # Headers come from the technical import contract. Display translated core fields separately.
        st.dataframe([{ui('institution',lang):r['Institution'],ui('form',lang):text(FORMS[r['Profil']]['title'],lang),ui('language',lang):LANG_NAMES[r['Langue']],ui('date_sent',lang):r['Date invitation']} for r in rows],hide_index=True)
        st.download_button(ui('list_download',lang),csv_bytes(rows,list(REGISTERS['invitations']['columns'])),file_name='HISWACA_invitations_INS.csv',mime='text/csv')

if __name__=='__main__':main()
