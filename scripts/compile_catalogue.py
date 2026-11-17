"""Compile the reviewed source catalogue; never translates responses at runtime."""
import json,csv,pathlib,sys
ROOT=pathlib.Path(__file__).resolve().parents[1]
base=json.loads((ROOT/'src/schema_base.json').read_text())
cat=json.loads((ROOT/'src/catalogue_fr_en.json').read_text())
rows={int(r[0]):r[1:] for r in csv.reader((ROOT/'src/translations.tsv').open(),delimiter='\t')}
if set(rows)!=set(range(len(cat))):raise ValueError('Catalogue incomplet')
by_fr={x['fr']:{'es':rows[x['id']][0].replace('\\n','\n'),'pt':rows[x['id']][1].replace('\\n','\n'),'ar':rows[x['id']][2].replace('\\n','\n')} for x in cat}
def walk(x):
 if isinstance(x,dict):
  if 'fr' in x and 'en' in x:
   x.update(by_fr[x['fr']])
   if x['fr'] and any(not x[l].strip() for l in base['languages']):raise ValueError(x['fr'])
  else:
   for v in x.values():walk(v)
 elif isinstance(x,list):
  for v in x:walk(v)
walk(base)
# Preserve the language codes explicitly used in QP01 (also applied to management fields).
for form in base['forms'].values():
 for s in form['sections']:
  for f in s['fields']:
   if f['id'].endswith('_Langue_texte'):
    for o in f['options']:o['code']={1:'fr',2:'en',3:'ar',4:'pt',5:'es'}[o['code']]
# Explicit producer wording, not NSO wording copied from QI.
f=next(f for s in base['forms']['producteurs']['sections'] for f in s['fields'] if f['id']=='QP07b')
f['label']={'fr':'Votre organisme a-t-il pu vérifier le passage de discours avant diffusion ?','en':'Was your organisation able to check the speech passage before dissemination?','es':'¿Pudo su organismo comprobar el pasaje del discurso antes de su difusión?','pt':'O seu organismo pôde verificar o trecho do discurso antes da divulgação?','ar':'هل تمكنت مؤسستكم من مراجعة مقطع الخطاب قبل نشره؟'}
(ROOT/'src/schema.json').write_text(json.dumps(base,ensure_ascii=False,indent=2))
print('Catalogue compilé :',len(cat),'entrées en cinq langues.')
