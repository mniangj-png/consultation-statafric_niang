#!/usr/bin/env python3
"""Génère localement les secrets de campagne, sans les afficher.

Utilisation : python scripts/generate_secrets.py
Fichiers sous .local/ (exclus de Git). Ne pas exécuter dans GitHub Actions.
Aucun compte externe n'est créé et aucune donnée n'est envoyée.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import secrets


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',default='.local',help='Dossier privé local, jamais dans GitHub')
    parser.add_argument('--access-code',action='store_true',help='Créer aussi un code facultatif pour les invitations')
    args=parser.parse_args()
    out=Path(args.output)
    files=['collecte.secrets.private.toml','campaign.private.sql','oidc_cookie.private.txt']
    if args.access_code: files.append('invitation_code.private.txt')
    if any((out/name).exists() for name in files):
        parser.error('Un fichier existe déjà. Rien n’a été remplacé. Sauvegardez-le dans un espace sécurisé avant une nouvelle génération.')
    out.mkdir(parents=True,exist_ok=True)
    token=secrets.token_urlsafe(48)
    code=secrets.token_urlsafe(12) if args.access_code else None
    def write(name,content):
        path=out/name
        with path.open('x',encoding='utf-8') as handle: handle.write(content)
        try: path.chmod(0o600)
        except OSError: pass # Windows : vérifier les autorisations du dossier.
    template=Path(__file__).resolve().parents[1]/'docs/collecte.secrets.example.toml'
    text=template.read_text(encoding='utf-8').replace('GENERER_UN_JETON_SERVEUR_ALEATOIRE',token)
    write(files[0],text)
    codehash="'"+hashlib.sha256(code.encode()).hexdigest()+"'" if code else 'NULL'
    sql=f'''-- PRIVÉ : exécuter dans le SQL Editor ; ne jamais publier dans Git.
INSERT INTO public.diag_campaigns(id,writer_token_hash,access_code_hash,active,opens_at,closes_at,max_responses)
VALUES ('discours-2026','{hashlib.sha256(token.encode()).hexdigest()}',{codehash},false,
        '2026-11-10T00:00:00Z','2026-12-14T23:59:59Z',3000);
-- Dates techniques proposées : ajuster avant diffusion. Aucune ouverture automatique.
'''
    write(files[1],sql)
    write(files[2],secrets.token_urlsafe(48)+'\n')
    if code:write('invitation_code.private.txt',code+'\n')
    print('Fichiers privés créés dans : '+str(out.resolve()))
    print('Aucun secret affiché. Complétez les paramètres, puis copiez-les uniquement dans les secrets Streamlit / le SQL Editor.')
    print('Ne régénérez pas le jeton après ouverture sans mettre à jour simultanément la base et les secrets de collecte.')

if __name__=='__main__':main()
