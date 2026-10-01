#!/usr/bin/env python3
"""agent-mesh — API-based agent org endpoint (portable, stdlib-only).

Single-file HTTP server so agents can check in, get work, report status,
and upload results. Role hierarchy + per-agent API keys managed via web UI.

Run:  python3 mesh_server.py --data ~/.local/state/agent-mesh --port 4850
Spec: SCHEMA.md in this directory (contract-first; code conforms to it).
"""
import argparse
import hashlib
import hmac
import json
import mimetypes
import os
import re
import secrets
import shutil
import sqlite3
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

VERSION = "0.1"
HEARTBEAT_WINDOW_S = 90          # seen within this window => online
MAX_UPLOAD_BYTES = 200 * 1024 * 1024   # 200 MB per artifact
ROLES = ("orchestrator", "planner", "worker", "qa", "reviewer", "observer")
ROLE_RANK = {r: i for i, r in enumerate(
    ["observer", "reviewer", "qa", "worker", "planner", "orchestrator"])}
TASK_STATUSES = ("queued", "claimed", "in_progress", "done", "failed",
                 "cancelled", "approved", "rejected")

# role -> capability flags
CAP_DISPATCH = {"orchestrator", "planner"}
CAP_PULL = {"orchestrator", "planner", "worker", "qa"}
CAP_RESULT = {"orchestrator", "planner", "worker", "qa"}
CAP_REVIEW = {"orchestrator", "qa", "reviewer"}


def now():
    return time.time()


def sha256_hex(b):
    return hashlib.sha256(b).hexdigest()


def gen_key():
    return "mesh_" + secrets.token_urlsafe(24)


def gen_admin_token():
    return "adm_" + secrets.token_urlsafe(24)


# ---------------------------------------------------------------- storage
class Store:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        os.makedirs(os.path.join(data_dir, "artifacts"), exist_ok=True)
        self.db_path = os.path.join(data_dir, "mesh.db")
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._migrate()

    def _migrate(self):
        c = self.conn
        c.executescript("""
        CREATE TABLE IF NOT EXISTS agents(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'worker',
            caps TEXT NOT NULL DEFAULT '[]',
            key_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'online',
            created_at REAL NOT NULL,
            last_seen REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS tasks(
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'generic',
            spec TEXT NOT NULL DEFAULT '{}',
            priority INTEGER NOT NULL DEFAULT 3,
            status TEXT NOT NULL DEFAULT 'queued',
            created_by TEXT,
            assigned_to TEXT,
            deadline REAL,
            retry TEXT NOT NULL DEFAULT '{"max":3,"backoff_s":10}',
            result TEXT,
            artifacts TEXT NOT NULL DEFAULT '[]',
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
        CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assigned_to);
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            actor TEXT,
            type TEXT NOT NULL,
            task_id TEXT,
            detail TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id);
        CREATE TABLE IF NOT EXISTS artifacts(
            id TEXT PRIMARY KEY,
            task_id TEXT,
            name TEXT NOT NULL,
            size INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            uploaded_by TEXT,
            ts REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
        """)
        c.commit()

    # -- helpers
    def q(self, sql, args=(), one=False):
        with self._lock:
            cur = self.conn.execute(sql, args)
            rows = cur.fetchall()
            self.conn.commit()
            if one:
                return rows[0] if rows else None
            return rows

    def q1(self, sql, args=()):
        with self._lock:
            cur = self.conn.execute(sql, args)
            row = cur.fetchone()
            self.conn.commit()
            return row

    def ex(self, sql, args=()):
        with self._lock:
            cur = self.conn.execute(sql, args)
            self.conn.commit()
            return cur.lastrowid

    # -- meta
    def get_meta(self, key, default=None):
        r = self.q1("SELECT value FROM meta WHERE key=?", (key,))
        return r["value"] if r else default

    def set_meta(self, key, value):
        self.ex("INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)))

    # -- agents
    def add_agent(self, aid, name, role, caps, key_hash):
        t = now()
        self.ex("INSERT INTO agents(id,name,role,caps,key_hash,status,"
                "created_at,last_seen) VALUES(?,?,?,?,?,'online',?,?)",
                (aid, name, role, json.dumps(caps), key_hash, t, t))

    def get_agent(self, aid):
        return self.q1("SELECT * FROM agents WHERE id=?", (aid,))

    def agent_by_key_hash(self, key_hash):
        return self.q1("SELECT * FROM agents WHERE key_hash=?", (key_hash,))

    def list_agents(self):
        return self.q("SELECT * FROM agents ORDER BY created_at DESC")

    def touch_agent(self, aid):
        self.ex("UPDATE agents SET last_seen=? WHERE id=?", (now(), aid))

    def update_agent(self, aid, **fields):
        allowed = {"name", "role", "caps", "status", "key_hash"}
        sets, args = [], []
        for k, v in fields.items():
            if k in allowed:
                sets.append(f"{k}=?")
                args.append(v)
        if sets:
            args.append(aid)
            self.ex(f"UPDATE agents SET {', '.join(sets)} WHERE id=?", args)

    def delete_agent(self, aid):
        self.ex("DELETE FROM agents WHERE id=?", (aid,))

    # -- tasks
    def add_task(self, tid, title, kind, spec, priority, created_by,
                 assigned_to, deadline, retry):
        t = now()
        self.ex("INSERT INTO tasks(id,title,kind,spec,priority,status,"
                "created_by,assigned_to,deadline,retry,result,artifacts,"
                "created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?,?,?,"
                "NULL,'[]',?,?)",
                (tid, title, kind, json.dumps(spec), priority, created_by,
                 assigned_to, deadline, json.dumps(retry), t, t))

    def get_task(self, tid):
        return self.q1("SELECT * FROM tasks WHERE id=?", (tid,))

    def list_tasks(self, status=None, assigned_to=None, created_by=None,
                   limit=50):
        sql = "SELECT * FROM tasks"
        where, args = [], []
        if status:
            where.append("status=?"); args.append(status)
        if assigned_to:
            where.append("assigned_to=?"); args.append(assigned_to)
        if created_by:
            where.append("created_by=?"); args.append(created_by)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY priority ASC, created_at ASC LIMIT ?"
        args.append(limit)
        return self.q(sql, args)

    def update_task(self, tid, **fields):
        allowed = {"title", "kind", "spec", "priority", "status",
                   "assigned_to", "deadline", "retry", "result", "artifacts"}
        sets, args = [], []
        for k, v in fields.items():
            if k in allowed:
                sets.append(f"{k}=?")
                args.append(v)
        if sets:
            sets.append("updated_at=?")
            args.append(now())
            args.append(tid)
            self.ex(f"UPDATE tasks SET {', '.join(sets)} WHERE id=?", args)

    def next_pullable(self, caller_role):
        """Next queued task a caller may claim: unassigned first, then own."""
        if caller_role in CAP_PULL:
            r = self.q1("SELECT * FROM tasks WHERE status='queued' AND "
                        "(assigned_to IS NULL OR assigned_to='') "
                        "ORDER BY priority ASC, created_at ASC LIMIT 1")
            if r:
                return r
        return self.q1("SELECT * FROM tasks WHERE status='queued' "
                       "ORDER BY priority ASC, created_at ASC LIMIT 1")

    def count_queued(self, assignee=None):
        if assignee:
            r = self.q1("SELECT COUNT(*) n FROM tasks WHERE status IN "
                        "('queued','claimed','in_progress') AND assigned_to=?",
                        (assignee,))
        else:
            r = self.q1("SELECT COUNT(*) n FROM tasks WHERE status='queued'")
        return r["n"] if r else 0

    # -- events
    def add_event(self, actor, etype, task_id=None, detail=None):
        self.ex("INSERT INTO events(ts,actor,type,task_id,detail) "
                "VALUES(?,?,?,?,?)",
                (now(), actor, etype, task_id,
                 json.dumps(detail) if detail is not None else None))

    def list_events(self, task_id=None, actor=None, etype=None, limit=50):
        sql = "SELECT * FROM events"
        where, args = [], []
        if task_id:
            where.append("task_id=?"); args.append(task_id)
        if actor:
            where.append("actor=?"); args.append(actor)
        if etype:
            where.append("type=?"); args.append(etype)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        return self.q(sql, args)

    # -- artifacts
    def add_artifact(self, aid, task_id, name, size, digest, uploader):
        self.ex("INSERT INTO artifacts(id,task_id,name,size,sha256,"
                "uploaded_by,ts) VALUES(?,?,?,?,?,?,?)",
                (aid, task_id, name, size, digest, uploader, now()))

    def get_artifact(self, aid):
        return self.q1("SELECT * FROM artifacts WHERE id=?", (aid,))

    def list_artifacts(self, task_id=None, limit=50):
        sql = "SELECT * FROM artifacts"
        args = []
        if task_id:
            sql += " WHERE task_id=?"
            args.append(task_id)
        sql += " ORDER BY ts DESC LIMIT ?"
        args.append(limit)
        return self.q(sql, args)

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------- domain
class Mesh:
    def __init__(self, store):
        self.store = store
        self.admin_token = store.get_meta("admin_token")
        if not self.admin_token:
            self.admin_token = gen_admin_token()
            store.set_meta("admin_token", self.admin_token)
            print(f"[agent-mesh] generated admin token: {self.admin_token}",
                  file=sys.stderr)
            print("[agent-mesh] (shown once — store it; the UI needs it)",
                  file=sys.stderr)

    # -- auth
    def auth_agent(self, header_value):
        """Return agent row for a Bearer key, or None."""
        if not header_value or not header_value.startswith("Bearer "):
            return None
        key = header_value[7:].strip()
        kh = sha256_hex(key.encode())
        row = self.store.agent_by_key_hash(kh)
        if not row:
            return None
        if row["status"] == "disabled":
            return None
        return row

    def auth_admin(self, header_value):
        if not header_value or not header_value.startswith("Bearer "):
            return False
        return hmac.compare_digest(header_value[7:].strip(), self.admin_token)

    def auth_is_admin(self, header_value):
        """True if the credential is the admin token OR an agent holding the
        'admin' capability (a trusted agent granted console access)."""
        if self.auth_admin(header_value):
            return True
        row = self.auth_agent(header_value)
        if row:
            caps = json.loads(row["caps"]) if row["caps"] else []
            if "admin" in caps:
                return True
        return False

    def effective_status(self, row):
        if row["status"] == "disabled":
            return "disabled"
        if now() - row["last_seen"] <= HEARTBEAT_WINDOW_S:
            return "online"
        return "offline"

    # -- serialization
    def agent_pub(self, row):
        caps = json.loads(row["caps"]) if row["caps"] else []
        return {
            "id": row["id"], "name": row["name"], "role": row["role"],
            "caps": caps, "created_at": row["created_at"],
            "last_seen": row["last_seen"],
            "status": self.effective_status(row),
        }

    def task_pub(self, row):
        spec = json.loads(row["spec"]) if row["spec"] else {}
        retry = json.loads(row["retry"]) if row["retry"] else {}
        arts = json.loads(row["artifacts"]) if row["artifacts"] else []
        result = json.loads(row["result"]) if row["result"] else None
        return {
            "id": row["id"], "title": row["title"], "kind": row["kind"],
            "spec": spec, "priority": row["priority"],
            "status": row["status"], "created_by": row["created_by"],
            "assigned_to": row["assigned_to"], "deadline": row["deadline"],
            "retry": retry, "result": result, "artifacts": arts,
            "created_at": row["created_at"], "updated_at": row["updated_at"],
        }

    def event_pub(self, row):
        detail = json.loads(row["detail"]) if row["detail"] else None
        return {"ts": row["ts"], "actor": row["actor"], "type": row["type"],
                "task_id": row["task_id"], "detail": detail}

    def artifact_pub(self, row):
        return {"id": row["id"], "task_id": row["task_id"],
                "name": row["name"], "size": row["size"],
                "sha256": row["sha256"], "uploaded_by": row["uploaded_by"],
                "ts": row["ts"]}

    # -- permission checks (raise PermissionError / ValueError)
    def require_role(self, agent, roles, action):
        if agent["role"] not in roles:
            raise PermissionError(f"role '{agent['role']}' cannot {action}")

    def can_touch_task(self, agent, task_row, need="related"):
        role = agent["role"]
        rank = ROLE_RANK.get(role, 0)
        if role == "orchestrator":
            return True
        creator = task_row["created_by"]
        assignee = task_row["assigned_to"]
        if need == "assignee":
            return agent["id"] == assignee
        if need == "creator_or_higher":
            if agent["id"] == creator:
                return True
            if creator:
                cr = self.store.get_agent(creator)
                if cr and ROLE_RANK.get(cr["role"], 0) > rank:
                    return False  # creator outranks caller -> deny
            return rank >= 4  # planner+
        # related: creator, assignee, planner+ (rank>=4), or a reviewer/qa
        # (they must be able to open tasks in order to review them).
        if role in CAP_REVIEW:
            return True
        return agent["id"] in (creator, assignee) or rank >= 4


# ---------------------------------------------------------------- http
class Handler(BaseHTTPRequestHandler):
    server_version = "agent-mesh/" + VERSION
    mesh: Mesh = None      # injected by factory
    store: Store = None

    # ---- plumbing
    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(),
                                        fmt % args))

    def _send_json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _err(self, status, msg):
        self._send_json({"detail": msg}, status)

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return b""
        if length > MAX_UPLOAD_BYTES:
            raise ValueError("payload too large")
        return self.rfile.read(length)

    def _json_body(self):
        raw = self._read_body()
        if not raw:
            return {}
        try:
            return json.loads(raw.decode())
        except Exception:
            raise ValueError("invalid JSON body")

    def _auth(self, need_admin=False):
        h = self.headers.get("Authorization", "")
        if need_admin:
            # Admin-gated: the admin token OR an agent with the 'admin' cap.
            if not self.mesh.auth_is_admin(h):
                raise PermissionError("admin token or admin-capable agent required")
            return None
        # Agent-auth endpoints: an agent key identifies the caller. The admin
        # token / an admin-capable agent is also accepted (a human operator or
        # trusted agent viewing the board) as a top-privilege read principal.
        agent = self.mesh.auth_agent(h)
        if not agent:
            if self.mesh.auth_is_admin(h):
                return {"id": "admin", "name": "admin", "role": "orchestrator",
                        "caps": [], "status": "online"}
            raise PermissionError("valid agent API key or admin token required")
        return agent

    # ---- routing
    ROUTES = [
        ("GET",    r"^/api/health$",                          "ep_health"),
        ("POST",   r"^/api/agents/register$",                 "ep_register"),
        ("POST",   r"^/api/agents/join$",                     "ep_join"),
        ("GET",    r"^/api/agents$",                           "ep_agents"),
        ("GET",    r"^/api/agents/me$",                        "ep_me"),
        ("PATCH",  r"^/api/agents/me$",                        "ep_patch_me"),
        ("POST",   r"^/api/agents/checkin$",                   "ep_checkin"),
        ("DELETE", r"^/api/agents/(?P<id>[^/]+)$",             "ep_del_agent"),
        ("POST",   r"^/api/tasks$",                            "ep_create_task"),
        ("GET",    r"^/api/tasks$",                            "ep_list_tasks"),
        ("GET",    r"^/api/tasks/(?P<id>[^/]+)$",              "ep_get_task"),
        ("GET",    r"^/api/work/pull$",                        "ep_pull"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/start$",        "ep_start"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/progress$",     "ep_progress"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/result$",       "ep_result"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/cancel$",       "ep_cancel"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/review$",       "ep_review"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/requeue$",      "ep_requeue"),
        ("POST",   r"^/api/artifacts$",                        "ep_upload"),
        ("GET",    r"^/api/artifacts$",                        "ep_list_art"),
        ("GET",    r"^/api/artifacts/(?P<id>[^/]+)$",          "ep_get_art"),
        ("GET",    r"^/api/events$",                           "ep_events"),
        ("GET",    r"^/api/admin/keys$",                       "ep_admin_keys"),
        ("POST",   r"^/api/admin/keys$",                       "ep_admin_issue"),
        ("DELETE", r"^/api/admin/keys/(?P<id>[^/]+)$",         "ep_admin_revoke"),
        ("PATCH",  r"^/api/admin/agents/(?P<id>[^/]+)$",       "ep_admin_patch"),
        ("GET",    r"^/api/admin/stats$",                      "ep_admin_stats"),
        ("GET",    r"^/$",                                     "ep_ui"),
    ]

    def _dispatch(self, method):
        path = urlparse(self.path).path
        # Base-path prefix (e.g. "/agent-mesh") for mounting under a reverse
        # proxy on a shared host. When set it is REQUIRED: requests that don't
        # carry the prefix get a clean 404, so a path the proxy doesn't route
        # to us can never silently hit mesh at its root.
        prefix = getattr(Handler, "base_path", "") or ""
        if prefix:
            if not (path == prefix or path.startswith(prefix + "/")):
                return self._err(404, "not found")
            path = path[len(prefix):] or "/"
        try:
            for m, pat, fn in self.ROUTES:
                if m != method:
                    continue
                mm = re.match(pat, path)
                if mm:
                    getattr(self, fn)(mm.groupdict())
                    return
            # static assets for the UI
            if method == "GET" and path.startswith("/static/"):
                return self._serve_static(path)
            self._err(404, "not found")
        except PermissionError as e:
            self._err(403, str(e))
        except ValueError as e:
            self._err(422, str(e))
        except KeyError as e:
            self._err(422, f"missing field: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            self._err(500, f"internal error: {e}")

    def error_content_type(self, request_path):
        # Return JSON (not the default HTML) for errors on API paths, so a
        # reverse proxy / client sees a machine-readable 404/501 instead of a
        # page when a path doesn't match.
        return "application/json" if request_path.startswith("/api/") \
            else super().error_content_type(request_path)

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PATCH(self):
        self._dispatch("PATCH")

    def do_DELETE(self):
        self._dispatch("DELETE")

    # ---- endpoints
    def ep_health(self, g):
        agents = len(self.store.list_agents())
        queued = self.store.count_queued()
        self._send_json({"ok": True, "version": VERSION,
                         "agents": agents, "tasks_queued": queued})

    def ep_register(self, g):
        self._auth(need_admin=True)
        body = self._json_body()
        name = (body.get("name") or "").strip()
        role = body.get("role") or "worker"
        if not name:
            raise ValueError("name required")
        if role not in ROLES:
            raise ValueError(f"role must be one of {list(ROLES)}")
        caps = body.get("caps") or []
        if not isinstance(caps, list):
            raise ValueError("caps must be a list")
        aid = (body.get("id") or "").strip() or \
            f"{name.lower().replace(' ', '-')}-{uuid.uuid4().hex[:8]}"
        if self.store.get_agent(aid):
            raise ValueError(f"agent id '{aid}' already exists")
        key = gen_key()
        self.store.add_agent(aid, name, role, caps, sha256_hex(key.encode()))
        self.store.add_event("admin", "agent.registered", None,
                             {"agent": aid, "role": role})
        self._send_json({"agent": self.mesh.agent_pub(self.store.get_agent(aid)),
                         "api_key": key,
                         "note": "store this key now — it is shown only once"},
                        201)

    def ep_join(self, g):
        # Open self-service join: a new box registers itself as an 'observer'
        # (read-only, cannot pull/dispatch/review). The admin then promotes it
        # to a real role from the console. This is how a remote agent checks in
        # without holding an admin token.
        body = self._json_body()
        name = (body.get("name") or "").strip()
        if not name:
            raise ValueError("name required")
        caps = body.get("caps") or []
        if not isinstance(caps, list):
            raise ValueError("caps must be a list")
        aid = (body.get("id") or "").strip() or \
            f"{name.lower().replace(' ', '-')}-{uuid.uuid4().hex[:8]}"
        if self.store.get_agent(aid):
            raise ValueError(f"agent id '{aid}' already exists — ask admin to rekey")
        key = gen_key()
        self.store.add_agent(aid, name, "observer", caps, sha256_hex(key.encode()))
        self.store.touch_agent(aid)
        self.store.add_event(aid, "agent.joined", None,
                             {"agent": aid, "note": "self-join as observer"})
        self._send_json({"agent": self.mesh.agent_pub(self.store.get_agent(aid)),
                         "api_key": key,
                         "note": "joined as 'observer'; admin must assign a role"},
                        201)

    def ep_agents(self, g):
        self._auth()
        items = [self.mesh.agent_pub(r) for r in self.store.list_agents()]
        self._send_json({"items": items})

    def ep_me(self, g):
        agent = self._auth()
        self._send_json(self.mesh.agent_pub(agent))

    def ep_patch_me(self, g):
        agent = self._auth()
        body = self._json_body()
        fields = {}
        if "name" in body and body["name"]:
            fields["name"] = str(body["name"]).strip()
        if "caps" in body:
            if not isinstance(body["caps"], list):
                raise ValueError("caps must be a list")
            fields["caps"] = json.dumps(body["caps"])
        if fields:
            self.store.update_agent(agent["id"], **fields)
            self.store.add_event(agent["id"], "agent.updated", None, fields)
        self._send_json(self.mesh.agent_pub(self.store.get_agent(agent["id"])))

    def ep_checkin(self, g):
        agent = self._auth()
        body = self._json_body() if self.headers.get("Content-Length") else {}
        self.store.touch_agent(agent["id"])
        pending = self.store.count_queued(assignee=agent["id"])
        self.store.add_event(agent["id"], "agent.checkin", None,
                             {"load": body.get("load"),
                              "queue_depth": body.get("queue_depth")})
        self._send_json({"ok": True, "ts": now(), "pending_tasks": pending})

    def ep_del_agent(self, g):
        self._auth(need_admin=True)
        aid = g["id"]
        if not self.store.get_agent(aid):
            self._err(404, "unknown agent")
            return
        self.store.delete_agent(aid)
        self.store.add_event("admin", "agent.deleted", None, {"agent": aid})
        self._send_json({"ok": True})

    def ep_create_task(self, g):
        agent = self._auth()
        self.mesh.require_role(agent, CAP_DISPATCH, "create tasks")
        body = self._json_body()
        title = (body.get("title") or "").strip()
        if not title:
            raise ValueError("title required")
        kind = body.get("kind") or "generic"
        spec = body.get("spec") or {}
        priority = int(body.get("priority", 3))
        if not 0 <= priority <= 5:
            raise ValueError("priority must be 0-5")
        deadline = body.get("deadline")
        assigned_to = body.get("assigned_to")
        retry = body.get("retry") or {"max": 3, "backoff_s": 10}
        tid = "task-" + uuid.uuid4().hex[:12]
        self.store.add_task(tid, title, kind, spec, priority, agent["id"],
                            assigned_to, deadline, retry)
        self.store.add_event(agent["id"], "task.created", tid,
                             {"title": title, "priority": priority})
        self._send_json(self.mesh.task_pub(self.store.get_task(tid)), 201)

    def ep_list_tasks(self, g):
        self._auth()
        q = parse_qs(urlparse(self.path).query)
        items = self.store.list_tasks(
            status=(q.get("status") or [None])[0],
            assigned_to=(q.get("assigned_to") or [None])[0],
            created_by=(q.get("created_by") or [None])[0],
            limit=min(int((q.get("limit") or ["50"])[0]), 200))
        self._send_json({"items": [self.mesh.task_pub(r) for r in items]})

    def ep_get_task(self, g):
        agent = self._auth()
        row = self.store.get_task(g["id"])
        if not row:
            self._err(404, "unknown task")
            return
        if not self.mesh.can_touch_task(agent, row, "related"):
            self._err(403, "not related to this task")
            return
        self._send_json(self.mesh.task_pub(row))

    def ep_pull(self, g):
        agent = self._auth()
        self.mesh.require_role(agent, CAP_PULL, "pull work")
        row = self.store.next_pullable(agent["role"])
        if not row:
            self._send_json({"task": None})
            return
        self.store.update_task(row["id"], assigned_to=agent["id"],
                               status="claimed")
        self.store.touch_agent(agent["id"])
        self.store.add_event(agent["id"], "task.claimed", row["id"],
                             {"title": row["title"]})
        self._send_json({"task": self.mesh.task_pub(self.store.get_task(row["id"]))})

    def _get_task_or_404(self, tid):
        row = self.store.get_task(tid)
        if not row:
            self._err(404, "unknown task")
        return row

    def ep_start(self, g):
        agent = self._auth()
        row = self._get_task_or_404(g["id"])
        if not self.mesh.can_touch_task(agent, row, "assignee"):
            self._err(403, "only the assignee can start this task")
            return
        if row["status"] not in ("claimed", "queued"):
            raise ValueError(f"cannot start from status '{row['status']}'")
        self.store.update_task(row["id"], status="in_progress")
        self.store.add_event(agent["id"], "task.started", row["id"])
        self._send_json(self.mesh.task_pub(self.store.get_task(row["id"])))

    def ep_progress(self, g):
        agent = self._auth()
        row = self._get_task_or_404(g["id"])
        if not self.mesh.can_touch_task(agent, row, "assignee"):
            self._err(403, "only the assignee can post progress")
            return
        body = self._json_body()
        pct = body.get("pct")
        note = body.get("note")
        if pct is not None and not 0 <= float(pct) <= 100:
            raise ValueError("pct must be 0-100")
        self.store.add_event(agent["id"], "task.progress", row["id"],
                             {"pct": pct, "note": note})
        self.store.touch_agent(agent["id"])
        self._send_json({"ok": True, "ts": now()})

    def ep_result(self, g):
        agent = self._auth()
        row = self._get_task_or_404(g["id"])
        if not self.mesh.can_touch_task(agent, row, "assignee"):
            self._err(403, "only the assignee can submit a result")
            return
        body = self._json_body()
        status = body.get("status")
        if status not in ("ok", "failed", "partial"):
            raise ValueError("status must be ok|failed|partial")
        final = "done" if status == "ok" else "failed"
        result = {"status": status, "output": body.get("output"),
                  "error": body.get("error")}
        self.store.update_task(row["id"], status=final,
                               result=json.dumps(result))
        self.store.add_event(agent["id"], "task.result", row["id"], result)
        self._send_json(self.mesh.task_pub(self.store.get_task(row["id"])))

    def ep_cancel(self, g):
        agent = self._auth()
        row = self._get_task_or_404(g["id"])
        if not self.mesh.can_touch_task(agent, row, "creator_or_higher"):
            self._err(403, "cannot cancel this task")
            return
        if row["status"] not in ("queued", "claimed", "in_progress"):
            raise ValueError(f"cannot cancel from status '{row['status']}'")
        self.store.update_task(row["id"], status="cancelled")
        self.store.add_event(agent["id"], "task.cancelled", row["id"])
        self._send_json(self.mesh.task_pub(self.store.get_task(row["id"])))

    def ep_review(self, g):
        agent = self._auth()
        # Review requires a genuine reviewer role. The bare-admin pseudo
        # principal (id 'admin', from the admin token) has no real role, so it
        # is denied here — approving must be done by an actual qa/reviewer/
        # orchestrator agent (or an admin-capable agent that holds one).
        if agent["id"] == "admin":
            raise PermissionError(
                "review requires a qa/reviewer/orchestrator agent key")
        self.mesh.require_role(agent, CAP_REVIEW, "review tasks")
        row = self._get_task_or_404(g["id"])
        if row["status"] not in ("done", "failed"):
            raise ValueError("can only review done/failed tasks")
        body = self._json_body()
        verdict = body.get("verdict")
        if verdict not in ("approved", "rejected"):
            raise ValueError("verdict must be approved|rejected")
        self.store.update_task(row["id"], status=verdict)
        self.store.add_event(agent["id"], "task.review", row["id"],
                             {"verdict": verdict, "note": body.get("note")})
        self._send_json(self.mesh.task_pub(self.store.get_task(row["id"])))

    def ep_requeue(self, g):
        agent = self._auth()
        self.mesh.require_role(agent, CAP_DISPATCH, "requeue tasks")
        row = self._get_task_or_404(g["id"])
        if row["status"] not in ("failed", "cancelled", "rejected"):
            raise ValueError(f"cannot requeue from status '{row['status']}'")
        self.store.update_task(row["id"], status="queued", assigned_to=None,
                               result=None)
        self.store.add_event(agent["id"], "task.requeued", row["id"])
        self._send_json(self.mesh.task_pub(self.store.get_task(row["id"])))

    # ---- artifacts
    def ep_upload(self, g):
        agent = self._auth()
        ctype = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in ctype:
            raise ValueError("use multipart/form-data with field 'file'")
        boundary = None
        for part in ctype.split(";"):
            part = part.strip()
            if part.startswith("boundary="):
                boundary = part[9:].strip('"')
        if not boundary:
            raise ValueError("missing multipart boundary")
        raw = self._read_body()
        fields, file_name, file_data = {}, None, None
        for seg in raw.split(b"--" + boundary.encode()):
            seg = seg.strip(b"\r\n")
            if not seg or seg == b"--":
                continue
            head, _, content = seg.partition(b"\r\n\r\n")
            headers = {}
            for line in head.split(b"\r\n"):
                if b":" in line:
                    k, v = line.split(b":", 1)
                    headers[k.strip().lower().decode()] = v.strip().decode()
            disp = headers.get("content-disposition", "")
            name_m = re.search(r'name="([^"]*)"', disp)
            fname_m = re.search(r'filename="([^"]*)"', disp)
            if not name_m:
                continue
            field = name_m.group(1)
            if fname_m:
                file_name = fname_m.group(1)
                file_data = content
            else:
                fields[field] = content.decode(errors="replace")
        if file_data is None:
            raise ValueError("no 'file' part in upload")
        task_id = fields.get("task_id")
        if task_id:
            trow = self.store.get_task(task_id)
            if not trow:
                self._err(404, "unknown task_id")
                return
            if not self.mesh.can_touch_task(agent, trow, "assignee"):
                self._err(403, "not the assignee of that task")
                return
        name = fields.get("name") or file_name or "artifact"
        aid = "art-" + uuid.uuid4().hex[:12]
        dest = os.path.join(self.store.data_dir, "artifacts", aid)
        with open(dest, "wb") as f:
            f.write(file_data)
        digest = sha256_hex(file_data)
        self.store.add_artifact(aid, task_id, name, len(file_data), digest,
                                agent["id"])
        # link to task
        if task_id:
            trow = self.store.get_task(task_id)
            arts = json.loads(trow["artifacts"]) if trow["artifacts"] else []
            arts.append(aid)
            self.store.update_task(task_id, artifacts=json.dumps(arts))
        self.store.add_event(agent["id"], "artifact.uploaded", task_id,
                             {"artifact": aid, "name": name,
                              "size": len(file_data)})
        self._send_json(self.mesh.artifact_pub(self.store.get_artifact(aid)),
                        201)

    def ep_list_art(self, g):
        self._auth()
        q = parse_qs(urlparse(self.path).query)
        items = self.store.list_artifacts(
            task_id=(q.get("task_id") or [None])[0])
        self._send_json({"items": [self.mesh.artifact_pub(r)
                                   for r in items]})

    def ep_get_art(self, g):
        self._auth()
        row = self.store.get_artifact(g["id"])
        if not row:
            self._err(404, "unknown artifact")
            return
        path = os.path.join(self.store.data_dir, "artifacts", row["id"])
        if not os.path.exists(path):
            self._err(404, "artifact file missing on disk")
            return
        data = open(path, "rb").read()
        self.send_response(200)
        self.send_header("Content-Type",
                         mimetypes.guess_type(row["name"])[0]
                         or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Content-Disposition",
                         f'attachment; filename="{row["name"]}"')
        self.end_headers()
        self.wfile.write(data)

    def ep_events(self, g):
        self._auth()
        q = parse_qs(urlparse(self.path).query)
        items = self.store.list_events(
            task_id=(q.get("task_id") or [None])[0],
            actor=(q.get("actor") or [None])[0],
            etype=(q.get("type") or [None])[0],
            limit=min(int((q.get("limit") or ["50"])[0]), 200))
        self._send_json({"items": [self.mesh.event_pub(r) for r in items]})

    # ---- admin
    def ep_admin_keys(self, g):
        self._auth(need_admin=True)
        items = []
        for r in self.store.list_agents():
            pub = self.mesh.agent_pub(r)
            pub["key_prefix"] = r["key_hash"][:12]
            pub["has_key"] = bool(r["key_hash"])
            items.append(pub)
        self._send_json({"items": items})

    def ep_admin_issue(self, g):
        self._auth(need_admin=True)
        body = self._json_body()
        aid = body.get("agent_id")
        row = self.store.get_agent(aid) if aid else None
        if not row:
            self._err(404, "unknown agent_id")
            return
        key = gen_key()
        self.store.update_agent(aid, key_hash=sha256_hex(key.encode()),
                                status="online")
        self.store.add_event("admin", "key.issued", None, {"agent": aid})
        self._send_json({"agent": aid, "api_key": key,
                         "note": "old key revoked; shown only once"})

    def ep_admin_revoke(self, g):
        self._auth(need_admin=True)
        aid = g["id"]
        row = self.store.get_agent(aid)
        if not row:
            self._err(404, "unknown agent")
            return
        self.store.update_agent(aid, key_hash="", status="disabled")
        self.store.add_event("admin", "key.revoked", None, {"agent": aid})
        self._send_json({"ok": True})

    def ep_admin_patch(self, g):
        self._auth(need_admin=True)
        aid = g["id"]
        row = self.store.get_agent(aid)
        if not row:
            self._err(404, "unknown agent")
            return
        body = self._json_body()
        fields = {}
        if "role" in body:
            if body["role"] not in ROLES:
                raise ValueError(f"role must be one of {list(ROLES)}")
            fields["role"] = body["role"]
        if "status" in body:
            if body["status"] not in ("online", "offline", "disabled"):
                raise ValueError("status must be online|offline|disabled")
            fields["status"] = body["status"]
        if "caps" in body:
            caps = body["caps"]
            if not isinstance(caps, list) or not all(isinstance(c, str) for c in caps):
                raise ValueError("caps must be a list of strings")
            fields["caps"] = json.dumps(caps)
        if fields:
            self.store.update_agent(aid, **fields)
            self.store.add_event("admin", "agent.role_changed", None,
                                 {"agent": aid, **fields})
        self._send_json(self.mesh.agent_pub(self.store.get_agent(aid)))

    def ep_admin_stats(self, g):
        self._auth(need_admin=True)
        agents = self.store.list_agents()
        by_role = {}
        for a in agents:
            by_role[a["role"]] = by_role.get(a["role"], 0) + 1
        tasks = self.store.q("SELECT status, COUNT(*) n FROM tasks "
                             "GROUP BY status")
        by_status = {r["status"]: r["n"] for r in tasks}
        total_tasks = sum(by_status.values())
        done = by_status.get("done", 0) + by_status.get("approved", 0)
        self._send_json({
            "agents_total": len(agents), "agents_by_role": by_role,
            "tasks_total": total_tasks, "tasks_by_status": by_status,
            "completion_rate": round(done / total_tasks, 3) if total_tasks else 0,
        })

    # ---- web UI
    def ep_ui(self, g):
        base = getattr(Handler, "base_path", "") or ""
        html = UI_HTML.replace("__VERSION__", VERSION).replace("__BASE__", base)
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, path):
        # everything is inline in UI_HTML; nothing external
        self._err(404, "not found")


# ---------------------------------------------------------------- UI
UI_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>agent-mesh console</title>
<style>
:root{--bg:#fafafa;--fg:#1d1d1f;--mut:#6e6e73;--acc:#0a84ff;--card:#fff;
--bd:#e5e5ea;--ok:#30d158;--warn:#ff9f0a;--bad:#ff453a}
*{box-sizing:border-box;margin:0}
body{font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
background:var(--bg);color:var(--fg);padding:0 24px 40px}
h1{font-size:20px;font-weight:600}
h2{font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:.04em;
color:var(--mut);margin-bottom:12px}
.sub{color:var(--mut);margin-bottom:20px}
.topbar{display:flex;align-items:center;justify-content:space-between;
padding:18px 0 14px;border-bottom:1px solid var(--bd);margin-bottom:18px;flex-wrap:wrap;gap:10px}
.brand{display:flex;align-items:baseline;gap:10px}
.brand .v{color:var(--mut);font-size:12px}
nav{display:flex;gap:4px;flex-wrap:wrap}
nav button{font:inherit;border:1px solid transparent;background:none;color:var(--mut);
border-radius:8px;padding:6px 12px;cursor:pointer;font-weight:500}
nav button:hover{background:#f0f0f2;color:var(--fg)}
nav button.active{background:var(--fg);color:#fff}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:16px}
.card{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:16px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;color:var(--mut);font-weight:500;padding:6px 8px;border-bottom:1px solid var(--bd)}
td{padding:7px 8px;border-bottom:1px solid var(--bd);vertical-align:top}
tr:last-child td{border-bottom:none}
tr.clickable{cursor:pointer}tr.clickable:hover{background:#f7f7f9}
.pill{display:inline-block;padding:1px 8px;border-radius:999px;font-size:11px;font-weight:500;white-space:nowrap}
.p-online{background:#e8f8ec;color:#1a7f37}.p-offline{background:#f0f0f2;color:var(--mut)}
.p-disabled{background:#ffe5e3;color:#c41e14}
.s-queued{background:#fff4e0;color:#9a6700}.s-in_progress,.s-claimed{background:#e5f1ff;color:#0a57d0}
.s-done,.s-approved{background:#e8f8ec;color:#1a7f37}.s-failed,.s-rejected{background:#ffe5e3;color:#c41e14}
.s-cancelled{background:#f0f0f2;color:var(--mut)}
button{font:inherit;border:1px solid var(--bd);background:var(--card);border-radius:8px;
padding:5px 11px;cursor:pointer;font-weight:500}
button:hover{border-color:var(--acc);color:var(--acc)}
button.primary{background:var(--acc);border-color:var(--acc);color:#fff}
button.primary:hover{filter:brightness(.94);color:#fff}
button.danger{color:var(--bad)}button.danger:hover{border-color:var(--bad);color:var(--bad)}
button.sm{padding:2px 8px;font-size:12px}
input,select,textarea{font:inherit;padding:6px 8px;border:1px solid var(--bd);border-radius:8px;width:100%}
textarea{resize:vertical;min-height:60px;font-family:ui-monospace,Menlo,monospace;font-size:12px}
.row{display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap}
.row .grow{flex:1;min-width:160px}
.mono{font-family:ui-monospace,Menlo,monospace;font-size:12px}
.key{background:#f5f5f7;padding:2px 6px;border-radius:6px}
.flash{position:fixed;top:16px;right:16px;background:var(--fg);color:#fff;
padding:10px 16px;border-radius:10px;opacity:0;transition:opacity .3s;pointer-events:none;z-index:9;max-width:340px}
.flash.show{opacity:1}
.stat{font-size:26px;font-weight:600}
.statlabel{color:var(--mut);font-size:12px}
.stats{display:flex;gap:28px;margin-bottom:16px;flex-wrap:wrap}
.lock{max-width:420px;margin:80px auto;text-align:center}
.lock input{margin:12px 0}
.ev{font-size:12px;color:var(--mut)}
.ev b{color:var(--fg);font-weight:500}
details summary{cursor:pointer;color:var(--mut);font-size:12px}
pre{background:#f5f5f7;padding:8px;border-radius:8px;font-size:11px;overflow:auto;max-height:220px;white-space:pre-wrap;word-break:break-word}
.kv{display:grid;grid-template-columns:120px 1fr;gap:4px 12px;font-size:13px}
.kv dt{color:var(--mut)}
.kv dd{margin:0;word-break:break-word}
.backlink{color:var(--acc);cursor:pointer;font-size:13px;display:inline-block;margin-bottom:12px}
.empty{color:var(--mut);font-size:13px;padding:8px 0}
.filters{display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap}
.filters select{width:auto}
.a11y:focus-visible{outline:2px solid var(--acc);outline-offset:2px}
</style></head><body>
<div id="app"></div>
<div class="flash" id="flash"></div>
<script>
const V="__VERSION__";
const BASE="__BASE__";   // base-path prefix when mounted under a reverse proxy
let ADMIN=sessionStorage.getItem("mesh_adm")||null;
const $=s=>document.querySelector(s);
const esc=s=>String(s??"").replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const ROLES=["orchestrator","planner","worker","qa","reviewer","observer"];
const KINDS=["code","research","docs","ops","test","generic"];
async function api(path,{method="GET",body,admin=false}={}){
  const h={"Content-Type":"application/json"};
  const admTok=sessionStorage.getItem("mesh_adm");
  // Prefer a saved agent key; otherwise fall back to the admin token so an
  // operator (no agent key) can still read the board via admin auth.
  const tok=localStorage.getItem("mesh_agent_key")||admTok||(admin?admTok:null);
  if(tok)h.Authorization="Bearer "+tok;
  const r=await fetch(BASE+path,{method,headers:h,body:body?JSON.stringify(body):undefined});
  let d={};try{d=await r.json()}catch{}
  if(!r.ok)throw new Error(d.detail||("HTTP "+r.status));
  return d;
}
function flash(msg,bad){const f=$("#flash");f.textContent=msg;f.style.background=bad?"var(--bad)":"var(--fg)";f.classList.add("show");clearTimeout(f._t);f._t=setTimeout(()=>f.classList.remove("show"),3200)}
function ago(ts){if(!ts)return"-";const s=Math.floor(Date.now()/1000-ts);if(s<5)return"now";if(s<60)return s+"s";if(s<3600)return Math.floor(s/60)+"m";if(s<86400)return Math.floor(s/3600)+"h";return Math.floor(s/86400)+"d"}
function pill(cls,txt){return `<span class="pill ${cls}">${esc(txt)}</span>`}
function statusPill(s){return pill("s-"+s,s)}
function agentPill(s){return pill("p-"+s,s)}
function roleOpts(sel){return ROLES.map(r=>`<option ${r===sel?"selected":""}>${r}</option>`).join("")}
function kindOpts(sel){return KINDS.map(k=>`<option ${k===sel?"selected":""}>${k}</option>`).join("")}
function go(h){location.hash=h}

/* ---------------- lock screen ---------------- */
function renderLock(){
  $("#app").innerHTML=`<div class="lock card">
    <h1>agent-mesh console</h1><div class="sub">v${V} · admin access</div>
    <div class="row grow" style="max-width:320px;margin:12px auto"><input id="adm" placeholder="admin token (adm_…)" type="password" class="a11y"></div>
    <div class="row" style="justify-content:center"><button class="primary" onclick="doUnlock()">Unlock</button>
    <span style="color:var(--mut);font-size:12px">printed at server first-run / in env</span></div>
    <hr style="border:none;border-top:1px solid var(--bd);margin:18px 0">
    <div style="font-size:12px;color:var(--mut)">Agent console — paste an agent API key. A key with the <b>admin</b> capability opens the full console (as that agent, so its role still gates review):</div>
    <div class="row grow" style="max-width:320px;margin:12px auto"><input id="akey" placeholder="agent key (mesh_…)"></div>
    <div class="row" style="justify-content:center"><button onclick="unlockAsAgent()">Unlock as agent</button></div>
  </div>`;
  setTimeout(()=>$("#adm").focus(),50);
}
function doUnlock(){const v=$("#adm").value.trim();if(!v)return;ADMIN=v;sessionStorage.setItem("mesh_adm",v);boot()}
async function unlockAsAgent(){
  const k=$("#akey").value.trim();if(!k)return flash("enter an agent key",1);
  try{
    const r=await fetch("/api/agents/me",{headers:{Authorization:"Bearer "+k}});
    if(!r.ok)throw new Error((await r.json()).detail||("HTTP "+r.status));
    const me=await r.json();
    localStorage.setItem("mesh_agent_key",k);
    sessionStorage.removeItem("mesh_adm");
    ADMIN=null;                 // act as the agent, not the admin token
    boot();
    flash("unlocked as "+me.name+" ("+me.role+")");
  }catch(e){flash("agent unlock failed: "+e.message,1)}
}
function saveAkey(){const k=$("#akey").value.trim();if(k){localStorage.setItem("mesh_agent_key",k);flash("agent key saved");setTimeout(()=>location.reload(),500)}}
function logout(){ADMIN=null;sessionStorage.removeItem("mesh_adm");localStorage.removeItem("mesh_agent_key");renderLock()}

/* ---------------- shell ---------------- */
function shell(active,title,inner){
  return `<div class="topbar">
    <div class="brand"><h1>agent-mesh</h1><span class="v">v${V}</span></div>
    <nav>
      <button class="${active==='dash'?'active':''}" onclick="go('#/')" >Dashboard</button>
      <button class="${active==='agents'?'active':''}" onclick="go('#/agents')">Agents</button>
      <button class="${active==='tasks'?'active':''}" onclick="go('#/tasks')">Tasks</button>
      <button class="${active==='events'?'active':''}" onclick="go('#/events')">Events</button>
      <button class="${active==='artifacts'?'active':''}" onclick="go('#/artifacts')">Artifacts</button>
    </nav>
    <div style="display:flex;align-items:center;gap:10px">
      <span class="ev" id="whoami"></span>
      <button class="sm" onclick="logout()">lock</button>
    </div>
  </div>
  <div style="height:18px"></div>
  <h2>${esc(title)}</h2>
  <div id="page">${inner}</div>`;
}

/* ---------------- data cache for auto-refresh ---------------- */
let CACHE={};
async function loadAll(){
  try{
    const [agents,tasks,events,stats]=await Promise.all([
      api("/api/admin/keys",{admin:true}),
      api("/api/tasks?limit=200"),
      api("/api/events?limit=60"),
      api("/api/admin/stats",{admin:true})]);
    CACHE={agents:agents.items,tasks:tasks.items,events:events.items,stats};
    rerenderPage();
  }catch(e){flash(e.message,1)}
}
function rerenderPage(){route(false)}

/* ---------------- pages ---------------- */
function pageDash(){
  const S=CACHE.stats||{},T=CACHE.tasks||[],E=CACHE.events||[];
  const roleCounts=Object.entries(S.agents_by_role||{}).map(([r,n])=>`${r}:${n}`).join(" · ")||"—";
  const active=(S.tasks_by_status?.in_progress||0)+(S.tasks_by_status?.claimed||0);
  const done=(S.tasks_by_status?.done||0)+(S.tasks_by_status?.approved||0);
  const recent=T.slice(0,6);
  return shell("dash","Dashboard",`
   <div class="stats">
     <div><div class="stat">${S.tasks_by_status?.queued||0}</div><div class="statlabel">queued</div></div>
     <div><div class="stat">${active}</div><div class="statlabel">active</div></div>
     <div><div class="stat">${done}</div><div class="statlabel">done/approved</div></div>
     <div><div class="stat">${S.tasks_by_status?.failed||0}</div><div class="statlabel">failed</div></div>
     <div><div class="stat">${S.agents_total||0}</div><div class="statlabel">agents (${esc(roleCounts)})</div></div>
   </div>
   <div class="grid">
     <div class="card"><h2>Recent tasks</h2>
       <table><tr><th>title</th><th>status</th><th>assignee</th><th>updated</th></tr>
       ${recent.map(t=>`<tr class="clickable" onclick="go('#/task/${t.id}')"><td>${esc(t.title)}</td><td>${statusPill(t.status)}</td><td class="mono">${esc(t.assigned_to||"—")}</td><td>${ago(t.updated_at)}</td></tr>`).join("")||'<tr><td colspan=4 class="empty">no tasks yet</td></tr>'}
       </table>
       <div style="margin-top:10px"><button class="sm" onclick="go('#/tasks')">all tasks →</button></div>
     </div>
     <div class="card"><h2>Live events</h2>
       ${E.slice(0,12).map(evLine).join("")||'<div class="empty">no events</div>'}
       <div style="margin-top:10px"><button class="sm" onclick="go('#/events')">full log →</button></div>
     </div>
   </div>`);
}
function evLine(e){return `<div class="ev"><b>${ago(e.ts)}</b> · <b>${esc(e.actor||"?")}</b> · ${esc(e.type)}${e.task_id?` <span class="mono">${esc(e.task_id)}</span>`:""}</div>`}

function pageAgents(){
  const A=CACHE.agents||[];
  return shell("agents","Agents & keys",`
   <div class="card">
     <div class="row">
       <input id="na" class="grow" placeholder="new agent name">
       <select id="nr" style="width:auto">${roleOpts("worker")}</select>
       <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--mut)"><input type="checkbox" id="ncap" style="width:auto"> admin cap</label>
       <button class="primary" onclick="regAgent()">Register</button>
     </div>
     <table><tr><th>name</th><th>role</th><th>status</th><th>seen</th><th>key</th><th>console</th><th>actions</th></tr>
     ${A.map(a=>{const hasAdmin=(a.caps||[]).includes("admin");return `<tr>
       <td>${esc(a.name)}<div class="ev mono">${esc(a.id)}</div></td>
       <td><select onchange="setRole('${esc(a.id)}',this.value)" style="width:auto">${roleOpts(a.role)}</select></td>
       <td>${agentPill(a.status)}</td><td>${ago(a.last_seen)}</td>
       <td class="mono key">${esc(a.key_prefix||"—")}${a.has_key?"":" ⚠ no-key"}</td>
       <td><button class="sm ${hasAdmin?'primary':''}" onclick="toggleAdminCap('${esc(a.id)}',${!hasAdmin})">${hasAdmin?'admin ✓':'grant'}</button></td>
       <td style="white-space:nowrap"><button class="sm" onclick="issueKey('${esc(a.id)}')">rekey</button>
           <button class="sm danger" onclick="revokeKey('${esc(a.id)}')">revoke</button>
           <button class="sm danger" onclick="delAgent('${esc(a.id)}')">delete</button></td>
     </tr>`}).join("")||'<tr><td colspan=7 class="empty">no agents registered</td></tr>'}
     </table>
   </div>`);
}

function pageTasks(){
  const T=CACHE.tasks||[];
  return shell("tasks","Task board",`
   <div class="card">
     <div class="row">
       <input id="tt" class="grow" placeholder="task title">
       <select id="tk" style="width:auto">${kindOpts("code")}</select>
       <input id="tp" type="number" min="0" max="5" value="3" style="width:64px" title="priority 0-5 (lower = more urgent)">
       <button class="primary" onclick="mkTask()">Create task</button>
     </div>
     <div class="filters">
       <select id="tf" onchange="renderTaskFilter()" style="width:auto">
         <option value="">all statuses</option>
         ${["queued","claimed","in_progress","done","failed","cancelled","approved","rejected"].map(s=>`<option>${s}</option>`).join("")}
       </select>
       <span class="ev" id="tcount"></span>
     </div>
     <table id="tasktable"><tr><th>title</th><th>kind</th><th>status</th><th>assignee</th><th>prio</th><th>created</th><th>updated</th></tr>
     ${taskRows(T)}
     </table>
   </div>`);
}
function taskRows(T){
  const f=$("#tf")?$("#tf").value:"";
  const list=f?T.filter(t=>t.status===f):T;
  if($("#tcount"))$("#tcount").textContent=list.length+" of "+T.length;
  return list.map(t=>`<tr class="clickable" onclick="go('#/task/${t.id}')">
     <td>${esc(t.title)}<div class="ev mono">${esc(t.id)}</div></td>
     <td>${esc(t.kind)}</td><td>${statusPill(t.status)}</td>
     <td class="mono">${esc(t.assigned_to||"—")}</td><td>${t.priority}</td>
     <td>${ago(t.created_at)}</td><td>${ago(t.updated_at)}</td></tr>`).join("")
     ||'<tr><td colspan=7 class="empty">no tasks match</td></tr>';
}
function renderTaskFilter(){
  const body=$("#tasktable").querySelectorAll("tr").length; // noop guard
  // re-render just the rows
  const tbl=$("#tasktable");
  const head=tbl.querySelector("tr");
  tbl.innerHTML="";tbl.appendChild(head);
  tmpDiv(taskRows(CACHE.tasks||[])).childNodes.forEach(n=>tbl.appendChild(n));
}
function tmpDiv(html){const d=document.createElement("div");d.innerHTML=html;return d}

async function pageTask(id){
  let t;
  try{t=await api("/api/tasks/"+id)}catch(e){return shell("tasks","Task not found",`<div class="card"><div class="empty">${esc(e.message)}</div><span class="backlink" onclick="go('#/tasks')">← back to tasks</span></div>`)}
  const arts=t.artifacts||[];
  const evs=(CACHE.events||[]).filter(e=>e.task_id===id);
  const canReview=["done","failed"].includes(t.status);
  return shell("tasks",esc(t.title),`
   <span class="backlink" onclick="go('#/tasks')">← all tasks</span>
   <div class="grid">
     <div class="card"><h2>Details</h2>
       <dl class="kv">
         <dt>id</dt><dd class="mono">${esc(t.id)}</dd>
         <dt>kind</dt><dd>${esc(t.kind)}</dd>
         <dt>status</dt><dd>${statusPill(t.status)}</dd>
         <dt>priority</dt><dd>${t.priority}</dd>
         <dt>created by</dt><dd class="mono">${esc(t.created_by||"—")}</dd>
         <dt>assigned to</dt><dd class="mono">${esc(t.assigned_to||"—")}</dd>
         <dt>deadline</dt><dd>${t.deadline?new Date(t.deadline*1000).toLocaleString():"—"}</dd>
         <dt>created</dt><dd>${ago(t.created_at)}</dd>
         <dt>updated</dt><dd>${ago(t.updated_at)}</dd>
       </dl>
       <h2 style="margin-top:16px">Spec</h2>
       <pre>${esc(JSON.stringify(t.spec||{},null,1))}</pre>
       ${t.result?`<h2 style="margin-top:16px">Result</h2><pre>${esc(JSON.stringify(t.result,null,1))}</pre>`:""}
     </div>
     <div class="card"><h2>Actions</h2>
       <div class="row">
         ${t.status==="queued"||t.status==="claimed"?`<button class="primary" onclick="taskAct('${t.id}','start')">Start</button>`:""}
         ${["queued","claimed","in_progress"].includes(t.status)?`<button class="danger" onclick="taskAct('${t.id}','cancel')">Cancel</button>`:""}
         ${["failed","cancelled","rejected"].includes(t.status)?`<button onclick="taskAct('${t.id}','requeue')">Requeue</button>`:""}
       </div>
       ${canReview?`<h2 style="margin-top:8px">Review</h2>
         <div class="row">
           <button class="primary" onclick="taskReview('${t.id}','approved')">Approve</button>
           <button class="danger" onclick="taskReview('${t.id}','rejected')">Reject</button>
           <input id="rnote" class="grow" placeholder="note (optional)">
         </div>`:`<div class="ev">Review available when task is done or failed.</div>`}
       <h2 style="margin-top:16px">Artifacts (${arts.length})</h2>
       ${arts.length?`<table><tr><th>name</th><th>size</th><th>sha256</th><th></th></tr>
         ${arts.map(aid=>artRow(aid)).join("")}</table>`:'<div class="empty">none uploaded</div>'}
     </div>
     <div class="card"><h2>Task events</h2>
       ${evs.map(evLine).join("")||'<div class="empty">no events for this task</div>'}
     </div>
   </div>`);
}
function artRow(aid){
  const a=(CACHE.artifacts||[]).find(x=>x.id===aid);
  if(!a)return `<tr><td class="mono">${esc(aid)}</td><td colspan=3 class="ev">loading…</td></tr>`;
  return `<tr><td>${esc(a.name)}<div class="ev mono">${esc(a.id)}</div></td>
    <td>${fmtSize(a.size)}</td><td class="mono">${esc((a.sha256||"").slice(0,12))}…</td>
    <td><a href="${BASE}/api/artifacts/${esc(a.id)}" download style="color:var(--acc)">download</a></td></tr>`;
}
function fmtSize(n){if(n<1024)return n+" B";if(n<1048576)return (n/1024).toFixed(1)+" KB";return (n/1048576).toFixed(1)+" MB"}

function pageEvents(){
  const E=CACHE.events||[];
  return shell("events","Event audit log",`
   <div class="card">
     <div class="filters">
       <select id="ef-actor" onchange="reloadEvents()" style="width:auto"><option value="">all actors</option>${[...new Set(E.map(e=>e.actor).filter(Boolean))].map(a=>`<option>${esc(a)}</option>`).join("")}</select>
       <select id="ef-type" onchange="reloadEvents()" style="width:auto"><option value="">all types</option>${[...new Set(E.map(e=>e.type))].map(t=>`<option>${esc(t)}</option>`).join("")}</select>
       <input id="ef-task" placeholder="filter task id…" style="width:220px" onkeyup="if(event.key==='Enter')reloadEvents()">
     </div>
     <div id="evlist">${E.map(evFull).join("")||'<div class="empty">no events</div>'}</div>
   </div>`);
}
function evFull(e){return `<div class="ev" style="margin-bottom:6px"><b>${ago(e.ts)}</b> · <b>${esc(e.actor||"?")}</b> · ${esc(e.type)}${e.task_id?` <span class="mono">${esc(e.task_id)}</span>`:""}${e.detail?`<details><summary>detail</summary><pre>${esc(JSON.stringify(e.detail,null,1))}</pre></details>`:""}</div>`}
async function reloadEvents(){
  const actor=$("#ef-actor").value,type=$("#ef-type").value,task=$("#ef-task").value.trim();
  const q=new URLSearchParams();if(actor)q.set("actor",actor);if(type)q.set("type",type);if(task)q.set("task_id",task);
  try{const d=await api("/api/events?"+q.toString());CACHE.events=d.items;
    $("#evlist").innerHTML=d.items.map(evFull).join("")||'<div class="empty">no events</div>'}catch(e){flash(e.message,1)}
}

function pageArtifacts(){
  $("#app").innerHTML = shell("artifacts","Artifacts",`
   <div class="card" id="artcard"><div class="empty">loading…</div></div>`);
  // fill async (must run AFTER the shell is in the DOM)
  (async()=>{try{const d=await api("/api/artifacts");CACHE.artifacts=d.items;
    const el=$("#artcard");if(!el)return;
    el.innerHTML=`<table><tr><th>name</th><th>task</th><th>size</th><th>sha256</th><th>by</th><th>uploaded</th><th></th></tr>
      ${d.items.map(a=>`<tr><td>${esc(a.name)}<div class="ev mono">${esc(a.id)}</div></td>
        <td class="mono">${a.task_id?`<span class="backlink" style="display:inline" onclick="go('#/task/${esc(a.task_id)}')">${esc(a.task_id)}</span>`:"—"}</td>
        <td>${fmtSize(a.size)}</td><td class="mono">${esc((a.sha256||"").slice(0,12))}…</td>
        <td class="mono">${esc(a.uploaded_by||"—")}</td><td>${ago(a.ts)}</td>
        <td><a href="${BASE}/api/artifacts/${esc(a.id)}" download style="color:var(--acc)">download</a></td></tr>`).join("")
        ||'<tr><td colspan=7 class="empty">no artifacts uploaded</td></tr>'}
      </table>`;
  }catch(e){const el=$("#artcard");if(el)el.innerHTML=`<div class="empty">${esc(e.message)}</div>`}})();
  return null;
}

/* ---------------- router ---------------- */
function route(refresh=true){
  if(!hasSession()){renderLock();return}
  const h=location.hash||"#/";
  if(refresh)loadAll();
  let m;
  if(h==="#/"||h===""){$("#app").innerHTML=pageDash()}
  else if(h==="#/agents"){$("#app").innerHTML=pageAgents()}
  else if(h==="#/tasks"){$("#app").innerHTML=pageTasks()}
  else if(m=h.match(/^#\/task\/([^/]+)$/)){$("#app").innerHTML='<div class="empty">loading…</div>';pageTask(decodeURIComponent(m[1])).then(html=>{$("#app").innerHTML=html})}
  else if(h==="#/events"){$("#app").innerHTML=pageEvents()}
  else if(h==="#/artifacts"){pageArtifacts()}
  else{$("#app").innerHTML=pageDash()}
  const who=$("#whoami");if(who)who.textContent=ADMIN?"admin":"agent";
}

/* ---------------- actions ---------------- */
async function regAgent(){
  const name=$("#na").value.trim(),role=$("#nr").value;
  const caps=$("#ncap")&&$("#ncap").checked?["admin"]:[];
  if(!name)return flash("name required",1);
  try{const d=await api("/api/agents/register",{method:"POST",admin:true,body:{name,role,caps}});
    flash("registered "+d.agent.id+(caps.length?" (admin cap)":""));
    window.prompt("API key (copy now — shown once):",d.api_key);
    loadAll();}catch(e){flash(e.message,1)}
}
async function issueKey(id){try{const d=await api("/api/admin/keys",{method:"POST",admin:true,body:{agent_id:id}});window.prompt("New key (old revoked):",d.api_key);loadAll()}catch(e){flash(e.message,1)}}
async function revokeKey(id){if(!confirm("Revoke key for "+id+"?"))return;try{await api("/api/admin/keys/"+id,{method:"DELETE",admin:true});flash("revoked");loadAll()}catch(e){flash(e.message,1)}}
async function delAgent(id){if(!confirm("Delete agent "+id+"? This removes it and revokes its key."))return;try{await api("/api/agents/"+id,{method:"DELETE",admin:true});flash("deleted");loadAll()}catch(e){flash(e.message,1)}}
async function setRole(id,role){try{await api("/api/admin/agents/"+id,{method:"PATCH",admin:true,body:{role}});flash("role → "+role);loadAll()}catch(e){flash(e.message,1)}}
async function toggleAdminCap(id,grant){
  // Read current caps, add/remove 'admin', write back.
  try{
    const list=await api("/api/admin/keys",{admin:true});
    const a=list.items.find(x=>x.id===id);if(!a)throw new Error("agent not found");
    let caps=a.caps||[];
    caps=grant?Array.from(new Set([...caps,"admin"])):caps.filter(c=>c!=="admin");
    await api("/api/admin/agents/"+id,{method:"PATCH",admin:true,body:{caps}});
    flash(grant?"admin cap granted":"admin cap removed");loadAll();
  }catch(e){flash(e.message,1)}
}
async function mkTask(){
  const title=$("#tt").value.trim(),kind=$("#tk").value,priority=parseInt($("#tp").value||"3",10);
  if(!title)return flash("title required",1);
  try{await api("/api/tasks",{method:"POST",body:{title,kind,priority,spec:{}}});flash("task created");loadAll()}catch(e){flash(e.message,1)}
}
async function taskAct(id,act){
  try{await api(`/api/tasks/${id}/${act}`,{method:"POST",admin:true,body:{}});flash(act+" ok");loadAll();
    if(location.hash.startsWith("#/task/"))pageTask(id).then(html=>{$("#app").innerHTML=shell("tasks","",html.replace(/<span class="backlink.*?<\/span>/,""))})}
  catch(e){flash(e.message,1)}
}
async function taskReview(id,verdict){
  const note=($("#rnote")?$("#rnote").value.trim():"");
  // Review must be done as a real reviewer agent (qa/reviewer/orchestrator).
  // Use the agent identity when present; otherwise the admin token is rejected
  // server-side with a clear message.
  const asAgent=!!localStorage.getItem("mesh_agent_key");
  try{await api(`/api/tasks/${id}/review`,{method:"POST",admin:!asAgent,body:{verdict,note}});flash(verdict+" ok");loadAll();
    pageTask(id).then(html=>{$("#app").innerHTML=html})}
  catch(e){flash(e.message,1)}
}

/* ---------------- boot ---------------- */
function hasSession(){return !!(ADMIN||localStorage.getItem("mesh_agent_key"))}
function boot(){
  if(!hasSession()){renderLock();return}
  route(true);
  clearInterval(window._rt);window._rt=setInterval(()=>{if(hasSession())loadAll()},5000);
}
window.addEventListener("hashchange",()=>route(false));
boot();
</script></body></html>
"""


# ---------------------------------------------------------------- client
class MeshClient:
    """Tiny convenience client any agent can embed. Stdlib-only.

    mc = MeshClient("http://127.0.0.1:4850", "mesh_...")
    mc.checkin()
    task = mc.pull()
    ...
    mc.report(task["id"], "ok", output={"commit": "abc"})
    """

    def __init__(self, base_url, api_key, timeout=30):
        self.base = base_url.rstrip("/")
        self.key = api_key
        self.timeout = timeout

    def _req(self, method, path, body=None):
        import urllib.request
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self.key)
        if data:
            req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read().decode())

    def health(self):
        import urllib.request
        with urllib.request.urlopen(self.base + "/api/health",
                                    timeout=self.timeout) as r:
            return json.loads(r.read().decode())

    def me(self):
        return self._req("GET", "/api/agents/me")

    def checkin(self, load=None, queue_depth=None):
        body = {}
        if load is not None:
            body["load"] = load
        if queue_depth is not None:
            body["queue_depth"] = queue_depth
        return self._req("POST", "/api/agents/checkin", body or None)

    def pull(self):
        d = self._req("GET", "/api/work/pull")
        return d.get("task")

    def create_task(self, title, kind="generic", spec=None, priority=3,
                    assigned_to=None):
        return self._req("POST", "/api/tasks",
                         {"title": title, "kind": kind,
                          "spec": spec or {}, "priority": priority,
                          "assigned_to": assigned_to})

    def start(self, task_id):
        return self._req("POST", f"/api/tasks/{task_id}/start")

    def progress(self, task_id, pct=None, note=None):
        return self._req("POST", f"/api/tasks/{task_id}/progress",
                         {"pct": pct, "note": note})

    def report(self, task_id, status, output=None, error=None):
        return self._req("POST", f"/api/tasks/{task_id}/result",
                         {"status": status, "output": output, "error": error})

    def upload(self, task_id, path, name=None):
        import urllib.request
        boundary = "----mesh" + secrets.token_hex(8)
        with open(path, "rb") as f:
            file_bytes = f.read()
        fname = name or os.path.basename(path)
        body = (f"--{boundary}\r\nContent-Disposition: form-data; "
                f'name="task_id"\r\n\r\n{task_id}\r\n'
                f"--{boundary}\r\nContent-Disposition: form-data; "
                f'name="file"; filename="{fname}"\r\n'
                f"Content-Type: application/octet-stream\r\n\r\n").encode() \
            + file_bytes + f"\r\n--{boundary}--\r\n".encode()
        req = urllib.request.Request(self.base + "/api/artifacts", data=body,
                                     method="POST")
        req.add_header("Authorization", "Bearer " + self.key)
        req.add_header("Content-Type",
                       f"multipart/form-data; boundary={boundary}")
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read().decode())


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="agent-mesh endpoint")
    ap.add_argument("--data", default=os.path.expanduser(
        "~/.local/state/agent-mesh"), help="state dir (db + artifacts)")
    ap.add_argument("--port", type=int,
                    default=int(os.environ.get("MESH_PORT", "4850")))
    ap.add_argument("--host", default=os.environ.get("MESH_HOST", "127.0.0.1"))
    ap.add_argument("--base-path", default=os.environ.get("MESH_BASE_PATH", ""),
                    help="URL prefix when mounted under a reverse proxy, "
                         "e.g. '/agent-mesh' (no trailing slash)")
    args = ap.parse_args()

    # Normalize base path: '' or '/' -> ''; else leading-slash, no trailing.
    bp = args.base_path.strip()
    if bp in ("", "/"):
        bp = ""
    else:
        if not bp.startswith("/"):
            bp = "/" + bp
        bp = bp.rstrip("/")

    store = Store(args.data)
    mesh = Mesh(store)
    if os.environ.get("MESH_ADMIN_TOKEN"):
        mesh.admin_token = os.environ["MESH_ADMIN_TOKEN"]
        store.set_meta("admin_token", mesh.admin_token)

    Handler.mesh = mesh
    Handler.store = store
    Handler.base_path = bp
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    shown = f"http://{args.host}:{args.port}{bp}"
    print(f"[agent-mesh] serving on {shown} (data: {args.data})", file=sys.stderr)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        store.close()


if __name__ == "__main__":
    main()
