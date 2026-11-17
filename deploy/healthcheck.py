"""Liveness only: does not certify database access or successful submissions."""
import os
from urllib.request import urlopen
try:
    port = int(os.environ.get('PORT', '8501'))
    with urlopen('http://127.0.0.1:' + str(port) + '/_stcore/health', timeout=4) as response:
        if response.status != 200 or response.read(64).strip().lower() != b'ok':
            raise ValueError('unhealthy')
except Exception:
    raise SystemExit(1)
