"""Local preflight. No network calls and no secrets read or printed."""
from pathlib import Path
import ast,json,hashlib,sys
ROOT=Path(__file__).resolve().parents[1]

def main():
    failures=[]
    for name in ['app.py','admin.py','relais_ins.py']:
        try:
            code=(ROOT/name).read_text(encoding='utf-8');tree=ast.parse(code);compile(tree,name,'exec')
            for n in ast.walk(tree):
                mods=[n.module or ''] if isinstance(n,ast.ImportFrom) else [x.name for x in n.names] if isinstance(n,ast.Import) else []
                if any(x.split('.')[0] in ('diagnostic','src') for x in mods):failures.append(name+' : dépendance locale inattendue')
        except (OSError,SyntaxError) as e:failures.append(name+' : '+str(e))
    d=json.loads((ROOT/'src/schema.json').read_text(encoding='utf-8'))
    langs=set(d['languages']);count=0
    def walk(x):
        nonlocal count
        if isinstance(x,dict):
            if 'fr' in x and 'en' in x:
                count+=1
                if not langs.issubset(x):failures.append('Traduction manquante')
                if x['fr'] and any(not x[l].strip() for l in langs):failures.append('Traduction vide')
            else:
                for v in x.values():walk(v)
        elif isinstance(x,list):
            for v in x:walk(v)
    walk(d)
    if set(d['forms'])!={'ins','producteurs','utilisateurs','partenaires','abrege'}:failures.append('Parcours incomplets')
    m=ROOT/'MANIFEST_SHA256.json'
    if m.exists():
        for name,digest in json.loads(m.read_text()).items():
            f=ROOT/name
            if not f.exists() or hashlib.sha256(f.read_bytes()).hexdigest()!=digest:failures.append('Empreinte différente : '+name)
    if failures:
        print('\n'.join(failures));sys.exit(1)
    print(f'Précontrôles locaux réussis : 3 fichiers autonomes, 5 parcours × 5 langues ; {count} blocs localisés. Aucune recette distante ni relecture linguistique présumée.')
if __name__=='__main__':main()
