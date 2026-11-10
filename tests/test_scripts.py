import hashlib
from pathlib import Path
import subprocess
import sys
import tomllib

ROOT=Path(__file__).parents[1]

def test_generator_private_outputs_and_consistent_hash(tmp_path):
    command=[sys.executable,str(ROOT/'scripts/generate_secrets.py'),'--output',str(tmp_path),'--access-code']
    result=subprocess.run(command,check=True,capture_output=True,text=True)
    cfg=tomllib.loads((tmp_path/'collecte.secrets.private.toml').read_text())
    token=cfg['WRITER_TOKEN'];assert len(token)>=32
    sql=(tmp_path/'campaign.private.sql').read_text()
    assert hashlib.sha256(token.encode()).hexdigest() in sql
    assert token not in sql and token not in result.stdout and token not in result.stderr
    access=(tmp_path/'invitation_code.private.txt').read_text().strip()
    assert hashlib.sha256(access.encode()).hexdigest() in sql
    assert cfg['TEST_MODE'] is True and cfg['DEPLOYMENT_APPROVED'] is False
    before=(tmp_path/'collecte.secrets.private.toml').read_text()
    repeat=subprocess.run(command,capture_output=True,text=True)
    assert repeat.returncode!=0
    assert (tmp_path/'collecte.secrets.private.toml').read_text()==before

def test_examples_are_valid_toml():
    for p in (ROOT/'docs').glob('*.example.toml'):
        cfg=tomllib.loads(p.read_text());assert cfg['APP_MODE'] in ['local','production']

def test_main_config_valid_toml():
    cfg=tomllib.loads((ROOT/'.streamlit/config.toml').read_text())
    assert cfg['server']['enableXsrfProtection'] and cfg['server']['enableCORS']
    assert cfg['browser']['gatherUsageStats'] is False
