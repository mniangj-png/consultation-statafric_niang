"""Build standalone applications without free-standing module strings."""
from pathlib import Path
import ast, json
ROOT = Path(__file__).resolve().parents[1]
def module_source(path):
    source = path.read_text(encoding='utf-8').replace('from __future__ import annotations\n', '')
    lines = source.splitlines(keepends=True)
    for node in reversed(ast.parse(source).body):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            lines[node.lineno-1:node.end_lineno] = ['# Documentation interne non affichee.\n']
    return ''.join(lines)
schema = (ROOT/'src/schema.json').read_text(encoding='utf-8')
assert "\'\'\'" not in schema
parts = ['from __future__ import annotations\n', "import json\nSCHEMA=json.loads(r\'\'\'"+schema+"\'\'\')\n"]
for name in ['ui_catalog.py', 'core.py', 'storage.py', 'registers.py']:
    parts.append('\n# --- '+name+' ---\n'+module_source(ROOT/'src'/name))
runtime = '\n'.join(parts)
(ROOT/'src/runtime.py').write_text(runtime, encoding='utf-8')
for target, name in [('app.py','app_ui.py'),('admin.py','admin_ui.py'),('relais_ins.py','relay_ui.py')]:
    code = '# HISWACA 2.1.0 — autonome ; aucun import diagnostic.\n'+runtime+'\n'+module_source(ROOT/'src'/name)
    compile(code, target, 'exec')
    (ROOT/target).write_text(code, encoding='utf-8')
print('Applications autonomes 2.1.0 construites.')
