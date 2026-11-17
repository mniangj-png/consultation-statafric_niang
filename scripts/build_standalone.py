"""Build readable single-file Streamlit applications. Run after catalogue changes."""
import pathlib,json,hashlib
ROOT=pathlib.Path(__file__).resolve().parents[1]
parts=['from __future__ import annotations\n']
schema=(ROOT/'src/schema.json').read_text();assert "'''" not in schema
parts.append("import json\nSCHEMA=json.loads(r'''"+schema+"''')\n")
for name in ['ui_catalog.py','core.py','storage.py','registers.py']:
    source=(ROOT/'src'/name).read_text().replace('from __future__ import annotations\n','')
    parts.append('\n# --- '+name+' ---\n'+source)
runtime='\n'.join(parts)
(ROOT/'src/runtime.py').write_text(runtime)
(ROOT/'src/__init__.py').write_text('')
for target,ui in [('app.py','app_ui.py'),('admin.py','admin_ui.py'),('relais_ins.py','relay_ui.py')]:
    source=ROOT/'src'/ui
    if source.exists():
        code='"""HISWACA 2.0.0 — fichier autonome, construit depuis src/. Aucun import diagnostic."""\n'+runtime+'\n'+source.read_text()
        compile(code,target,'exec');(ROOT/target).write_text(code)
print('Applications autonomes construites.')
