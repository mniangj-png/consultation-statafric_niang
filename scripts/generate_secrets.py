"""Generate local campaign tokens, never publish the generated directory."""
from pathlib import Path
import argparse,secrets,hashlib,os,json,re

def main():
    p=argparse.ArgumentParser(description='Générer une configuration privée de campagne, fermée par défaut.')
    p.add_argument('--mode',choices=['test','reel'],default='test');p.add_argument('--sortie',default='private_config');p.add_argument('--campaign')
    a=p.parse_args();campaign=a.campaign or ('discours-2026-v2-test' if a.mode=='test' else 'discours-2026-v2')
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}',campaign):p.error('Identifiant de campagne invalide.')
    root=Path(a.sortie);root.mkdir(parents=True,exist_ok=True);out=root/campaign
    if out.exists():p.error('Ce dossier existe. Ne pas écraser un jeton existant : utiliser un nouveau nom.')
    out.mkdir(mode=0o700);token=secrets.token_urlsafe(48);cookie=secrets.token_urlsafe(48);sha=hashlib.sha256(token.encode()).hexdigest();test=a.mode=='test'
    config='''# FICHIER PRIVÉ : à copier dans les secrets Streamlit, JAMAIS dans GitHub.
APP_MODE = "production"
TEST_MODE = %s
CAMPAIGN_ID = %s
SUPABASE_URL = "https://REMPLACER.supabase.co"
SUPABASE_PUBLIC_KEY = "REMPLACER_PAR_LA_CLE_ANON_PUBLIQUE"
WRITER_TOKEN = %s
CONTACT_EMAIL = "REMPLACER_PAR_LE_CONTACT_INSTITUTIONNEL"
NOTICE_CONTROLLER = "À confirmer avec STATAFRIC"
RETENTION_NOTICE = "À confirmer avant ouverture"
DEPLOYMENT_APPROVED = false
APPROVED_LANGUAGES = []
# Compléter les notices dans les cinq langues avant toute collecte réelle.
[notice]
fr = "À compléter : responsable, hébergeur, lieu, accès, conservation et contact."
en = "To complete: controller, host, location, access, retention and contact."
ar = "يستكمل: المسؤول والاستضافة والموقع والصلاحيات والاحتفاظ وجهة الاتصال."
pt = "Completar: responsável, alojamento, localização, acesso, conservação e contacto."
es = "Completar: responsable, alojamiento, ubicación, acceso, conservación y contacto."
'''%('true' if test else 'false',json.dumps(campaign),json.dumps(token))
    start='2026-11-16 00:00:00+00' if test else '2026-11-19 00:00:00+00';end='2026-12-31 23:59:59+00' if test else '2026-11-26 23:59:59.999999+00'
    sql=f"-- Configuration générée : campagne fermée. Pas de jeton en clair dans ce SQL.\nINSERT INTO public.diag_campaigns(id,writer_token_hash,active,opens_at,closes_at,max_responses,test_mode)\nVALUES ('{campaign}','{sha}',false,'{start}','{end}',10000,{'true' if test else 'false'});\n"
    values={'collecte_secrets.toml':config,'campagne.sql':sql,'cookie_secret_admin.txt':cookie+'\n'}
    for name,value in values.items():
        f=out/name;f.write_text(value,encoding='utf-8');os.chmod(f,0o600)
    print('Fichiers créés dans',out.resolve());print('Aucune base distante modifiée. Compléter puis appliquer la configuration selon DEPLOIEMENT.md. Les secrets ne sont pas affichés.')
if __name__=='__main__':main()
