"""Persistance : Supabase en production, SQLite explicitement local pour les tests.

Aucun repli silencieux vers un fichier local si le stockage distant échoue.
L'application publique ne possède qu'une clé publique Supabase et un jeton
serveur donnant accès à deux RPC limitées (état de campagne et soumission).
"""
from __future__ import annotations
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
from uuid import UUID,uuid4
import requests


class StorageError(RuntimeError):
    pass


class ConfigurationError(ValueError):
    pass


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def canonical(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(",",":"))


def digest(payload,contacts):
    return hashlib.sha256(canonical([payload,contacts]).encode()).hexdigest()


@dataclass
class Settings:
    mode: str="preview"
    url: str=""
    public_key: str=""
    admin_key: str=""
    writer_token: str=""
    campaign: str="discours-2026"
    test_mode: bool=True
    contact_email: str=""
    notice_controller: str="STATAFRIC — consultant et personnes désignées pour cette mission"
    retention_notice: str="Durée de conservation et hébergement à confirmer avant ouverture."
    collection_url: str=""
    local_path: str=".local/diagnostic_test.sqlite3"

    @classmethod
    def from_mapping(cls, config, admin=False):
        obj=cls(mode=str(config.get("APP_MODE","preview")),url=str(config.get("SUPABASE_URL","")),
                public_key=str(config.get("SUPABASE_PUBLIC_KEY","")),admin_key=str(config.get("SUPABASE_ADMIN_KEY","")),
                writer_token=str(config.get("WRITER_TOKEN","")),campaign=str(config.get("CAMPAIGN_ID","discours-2026")),
                test_mode=bool(config.get("TEST_MODE",True)),contact_email=str(config.get("CONTACT_EMAIL","")),
                notice_controller=str(config.get("NOTICE_CONTROLLER",cls.notice_controller)),
                retention_notice=str(config.get("RETENTION_NOTICE",cls.retention_notice)),
                collection_url=str(config.get("COLLECTION_URL","")),local_path=str(config.get("LOCAL_DB_PATH",cls.local_path)))
        if obj.mode not in ["preview","local","production"]:
            raise ConfigurationError("APP_MODE doit être preview, local ou production.")
        if obj.mode=="production":
            from urllib.parse import urlparse
            parsed=urlparse(obj.url)
            # Service choisi pour cette version ; pas de point de sortie arbitraire.
            if parsed.scheme!="https" or not parsed.hostname or not parsed.hostname.endswith(".supabase.co") or parsed.path not in ["","/"] or parsed.query or parsed.fragment or parsed.username:
                raise ConfigurationError("SUPABASE_URL doit être l’URL HTTPS du projet Supabase (*.supabase.co).")
            if admin:
                if not obj.admin_key:
                    raise ConfigurationError("Configurer SUPABASE_ADMIN_KEY uniquement dans l’application d’administration.")
            else:
                if obj.admin_key:
                    raise ConfigurationError("Retirer SUPABASE_ADMIN_KEY des secrets de l’application publique.")
                if not obj.public_key or len(obj.writer_token)<32:
                    raise ConfigurationError("Configurer la clé publique Supabase et un WRITER_TOKEN d’au moins 32 caractères.")
                if not obj.contact_email or config.get("DEPLOYMENT_APPROVED") is not True or not config.get("RETENTION_NOTICE"):
                    raise ConfigurationError("Renseigner CONTACT_EMAIL et RETENTION_NOTICE puis confirmer DEPLOYMENT_APPROVED avant la collecte réelle.")
        if obj.mode=="local":
            obj.test_mode=True
        return obj


class LocalStore:
    """Tests locaux uniquement : toutes les réponses sont marquées is_test."""
    def __init__(self,path):
        self.path=str(path)
        Path(path).parent.mkdir(parents=True,exist_ok=True)
        with self._db() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS responses (
                  id TEXT PRIMARY KEY, receipt TEXT UNIQUE NOT NULL, received_at TEXT NOT NULL,
                  content_hash TEXT NOT NULL, payload TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS contacts (
                  response_id TEXT PRIMARY KEY REFERENCES responses(id) ON DELETE CASCADE, contact TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS reviews (
                  id TEXT PRIMARY KEY, response_id TEXT NOT NULL REFERENCES responses(id),
                  created_at TEXT NOT NULL, actor TEXT NOT NULL, status TEXT NOT NULL,
                  note TEXT NOT NULL, linked_to TEXT NOT NULL);
            """)
    @contextmanager
    def _db(self):
        db=sqlite3.connect(self.path,timeout=10)
        db.row_factory=sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()
    def campaign_info(self):
        return {"active":True,"requires_access_code":False,"opens_at":None,"closes_at":None,"mode":"local"}
    def submit(self,payload,contacts,access_code=""):
        if payload.get("is_test") is not True:
            raise StorageError("Le stockage local est réservé aux réponses de test.")
        uid=str(UUID(payload["submission_id"]))
        h=digest(payload,contacts)
        try:
            with self._db() as db:
                db.execute("BEGIN IMMEDIATE")
                row=db.execute("SELECT * FROM responses WHERE id=?",(uid,)).fetchone()
                if row:
                    if row["content_hash"]!=h:
                        raise StorageError("Identifiant déjà utilisé avec un contenu différent. Ne pas écraser la contribution initiale.")
                    return {"receipt":row["receipt"],"received_at":row["received_at"],"duplicate":True}
                receipt="REP-"+uid
                date=utcnow()
                db.execute("INSERT INTO responses VALUES (?,?,?,?,?)",(uid,receipt,date,h,canonical(payload)))
                if contacts:
                    db.execute("INSERT INTO contacts VALUES (?,?)",(uid,canonical(contacts)))
                return {"receipt":receipt,"received_at":date,"duplicate":False}
        except sqlite3.Error as exc:
            raise StorageError("Échec du stockage local ; aucun accusé de réception fiable n’est disponible.") from exc
    def records(self):
        with self._db() as db:
            rows=db.execute("SELECT * FROM responses ORDER BY received_at,id").fetchall()
            return [{"id":r["id"],"receipt":r["receipt"],"received_at":r["received_at"],"payload":json.loads(r["payload"]),
                     "review":self.latest_review(r["id"])} for r in rows]
    def latest_review(self,uid):
        with self._db() as db:
            r=db.execute("SELECT * FROM reviews WHERE response_id=? ORDER BY created_at DESC,id DESC LIMIT 1",(uid,)).fetchone()
            return dict(r) if r else None
    def all_contacts(self):
        with self._db() as db:
            return [{"response_id":r["response_id"],"contact":json.loads(r["contact"])} for r in db.execute("SELECT * FROM contacts")]
    def add_review(self,uid,actor,status,note,linked_to=""):
        with self._db() as db:
            db.execute("INSERT INTO reviews VALUES (?,?,?,?,?,?,?)",(str(uuid4()),uid,utcnow(),actor,status,note,linked_to))
    def reviews(self):
        with self._db() as db:
            return [dict(r) for r in db.execute("SELECT * FROM reviews ORDER BY created_at,id")]


class SupabaseStore:
    def __init__(self,settings: Settings,admin=False):
        self.s=settings
        self.admin=admin
        self.key=settings.admin_key if admin else settings.public_key
    def _request(self,method,path,body=None,params=None):
        headers={"apikey":self.key,"Content-Type":"application/json","Prefer":"return=representation"}
        if self.key.startswith("eyJ"):
            headers["Authorization"]="Bearer "+self.key
        try:
            response=requests.request(method,self.s.url.rstrip("/")+"/rest/v1/"+path,
                headers=headers,json=body,params=params,timeout=(5,25),allow_redirects=False)
        except requests.RequestException as exc:
            raise StorageError("NETWORK_UNCERTAIN") from exc
        if not 200<=response.status_code<300:
            # Ne jamais afficher une réponse brute pouvant inclure des détails SQL.
            message=""
            try:
                error_body=response.json()
                message=str(error_body.get("message","")) if isinstance(error_body,dict) else ""
            except ValueError: pass
            known=["COLLECTION_CLOSED","INVALID_ACCESS_CODE","SUBMISSION_CONFLICT","QUOTA_REACHED","INVALID_SUBMISSION","INVALID_WRITER"]
            safe=next((code for code in known if code in message),"REMOTE_ERROR")
            raise StorageError(safe)
        try:
            return response.json() if response.content else None
        except ValueError as exc:
            raise StorageError("REMOTE_ERROR") from exc
    def campaign_info(self):
        result=self._request("POST","rpc/diag_campaign_info",{"p_campaign":self.s.campaign,"p_writer_token":self.s.writer_token})
        if not isinstance(result,dict) or type(result.get("active")) is not bool:
            raise StorageError("REMOTE_ERROR")
        return result
    def submit(self,payload,contacts,access_code=""):
        result=self._request("POST","rpc/diag_submit",{"p_campaign":self.s.campaign,"p_writer_token":self.s.writer_token,
                    "p_access_code":access_code,"p_payload":payload,"p_contacts":contacts})
        if not isinstance(result,dict) or result.get("receipt")!="REP-"+payload["submission_id"] or not isinstance(result.get("received_at"),str):
            raise StorageError("NETWORK_UNCERTAIN")
        try:
            datetime.fromisoformat(result["received_at"].replace("Z","+00:00"))
        except ValueError as exc:
            raise StorageError("NETWORK_UNCERTAIN") from exc
        return result
    def _all(self,table,select="*",extra=None):
        if not self.admin:
            raise StorageError("L’application publique ne peut pas lire les réponses.")
        cutoff=utcnow()
        out=[]; offset=0
        while True:
            params={"select":select,"limit":500,"offset":offset,"order":"created_at.asc,id.asc", "created_at":"lte."+cutoff,**(extra or {})}
            rows=self._request("GET",table,params=params)
            if not isinstance(rows,list):
                raise StorageError("REMOTE_ERROR")
            out.extend(rows)
            if len(rows)<500: break
            offset+=500
            if offset>50_000:
                raise StorageError("Export trop volumineux : utiliser un export administrateur de la base.")
        return out
    def records(self):
        rows=self._all("diag_responses",extra={"campaign_id":"eq."+self.s.campaign})
        review_rows=self.reviews()
        latest={}
        for r in review_rows: latest[r["response_id"]]=r
        return [{"id":r["id"],"receipt":r["receipt"],"received_at":r["created_at"],"payload":r["payload"],"review":latest.get(r["id"])} for r in rows]
    def reviews(self):
        return self._all("diag_reviews",extra={"campaign_id":"eq."+self.s.campaign})
    def all_contacts(self):
        return self._all("diag_contacts",select="id,response_id,contact,created_at",extra={"campaign_id":"eq."+self.s.campaign})
    def add_review(self,uid,actor,status,note,linked_to=""):
        if not self.admin: raise StorageError("Accès refusé.")
        return self._request("POST","diag_reviews",{"id":str(uuid4()),"campaign_id":self.s.campaign,"response_id":uid,
                        "actor":actor,"status":status,"note":note,"linked_to":linked_to or None})
    def set_active(self,active: bool):
        if not self.admin: raise StorageError("Accès refusé.")
        return self._request("PATCH","diag_campaigns",{"active":active},params={"id":"eq."+self.s.campaign})
    def admin_campaign(self):
        if not self.admin: raise StorageError("Accès refusé.")
        rows=self._request("GET","diag_campaigns",params={"id":"eq."+self.s.campaign,"select":"id,active,opens_at,closes_at,max_responses"})
        return rows[0] if rows else None


def make_store(settings: Settings,admin=False):
    if settings.mode=="production": return SupabaseStore(settings,admin)
    if settings.mode=="local": return LocalStore(settings.local_path)
    return None
