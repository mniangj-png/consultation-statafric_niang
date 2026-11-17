"""Restricted analytical backup, NOT a PostgreSQL snapshot or automated restore."""
import argparse,sys,json,os,zipfile,io,tomllib
from pathlib import Path
from datetime import datetime,timezone
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from src.runtime import Settings,make_store,StorageError,ConfigurationError,export_bundle

def main():
    p=argparse.ArgumentParser(description='Export analytique restreint, sans transmission ni modification de la base.')
    p.add_argument('--secrets',required=True,help='Chemin local privé du TOML administrateur, jamais une clé dans la ligne de commande.')
    p.add_argument('--sortie',default='exports');p.add_argument('--contacts',action='store_true',help='Inclure les contacts seulement si nécessaire et autorisé.')
    a=p.parse_args();cfg=tomllib.loads(Path(a.secrets).read_text(encoding='utf-8'));settings=Settings.from_mapping(cfg,admin=True)
    if settings.mode!='production':p.error('Cette commande est réservée à la base distante administrée.')
    store=make_store(settings,admin=True)
    try:
        records=store.records();reviews=store.reviews();events=store.registry_events();campaign=store.admin_campaign()
        contacts=store.all_contacts() if a.contacts else None
    except StorageError:
        print('Export interrompu : lecture distante incomplète. Aucun fichier partiel livré.',file=sys.stderr);return 1
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,'w',zipfile.ZIP_DEFLATED) as z:
        for name,obj in [('reponses',records),('historique_controles',reviews),('historique_registres',events),('campagne_sans_secrets',campaign)]:
            z.writestr(name+'.json',json.dumps(obj,ensure_ascii=False,indent=2))
        if contacts is not None:z.writestr('CONTACTS_RESTREINT.json',json.dumps(contacts,ensure_ascii=False,indent=2))
        z.writestr('LISEZ_MOI.txt','Export interne à accès limité. Lectures successives, pas un instantané transactionnel PostgreSQL. Ne remplace pas une sauvegarde de base ni un test de restauration. Contient les essais et l’historique ; filtrer pour analyse. Les contacts ne sont inclus que si l’option explicite a été donnée. Ne pas publier.\n')
    out=Path(a.sortie);out.mkdir(parents=True,exist_ok=True)
    f=out/('HISWACA_backup_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.zip')
    with f.open('xb') as h:h.write(buf.getvalue())
    os.chmod(f,0o600);print('Export complet enregistré dans',f.resolve());return 0
if __name__=='__main__':raise SystemExit(main())
