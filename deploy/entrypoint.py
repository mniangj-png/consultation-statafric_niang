"""Launch a single Streamlit process; no keep-alive traffic or local fallback."""
import os
import sys
from pathlib import Path


def main():
    name = os.environ.get('APP_FILE', 'app.py')
    if name not in {'app.py', 'admin.py', 'relais_ins.py'}:
        raise SystemExit('APP_FILE must be app.py, admin.py or relais_ins.py')
    try:
        port = int(os.environ.get('PORT', '8501'))
        if not 1024 <= port <= 65535:
            raise ValueError
    except ValueError:
        raise SystemExit('PORT must be between 1024 and 65535')
    if not Path('/app', name).is_file():
        raise SystemExit('Application file not found')
    argv = [sys.executable, '-m', 'streamlit', 'run', '/app/' + name,
            '--server.address=0.0.0.0', '--server.port=' + str(port),
            '--server.headless=true', '--server.fileWatcherType=none',
            '--server.enableXsrfProtection=true', '--server.enableCORS=true',
            '--runner.magicEnabled=false', '--browser.gatherUsageStats=false']
    os.execv(sys.executable, argv)


if __name__ == '__main__':
    main()
