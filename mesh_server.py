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
STALE_TASK_S = 300               # assigned task untouched for this long => stale (reassign candidate)
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


# ---------------------------------------------------------------------------
# SSE change-notification hub (instant console updates)
#
# A tiny pub/sub: any state mutation calls notify_change(), which wakes every
# connected /api/stream subscriber so the browser can re-fetch fresh data
# immediately instead of waiting for its 5s poll. One shared condition variable
# is enough — subscribers just need to know "something changed", not what.
# ---------------------------------------------------------------------------
class ChangeHub:
    def __init__(self):
        self._cv = threading.Condition()

    def subscribe(self):
        """Return a per-connection waiter. Call .wait(timeout) to block until a
        change or timeout; call .close() on disconnect."""
        with self._cv:
            waiters = getattr(self, "_waiters", None)
            if waiters is None:
                waiters = self._waiters = []
            w = {"alive": True}
            waiters.append(w)
            return w

    def close(self, w):
        with self._cv:
            try:
                self._waiters.remove(w)
            except ValueError:
                pass
            w["alive"] = False

    def notify_change(self):
        with self._cv:
            self._cv.notify_all()


HUB = ChangeHub()


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
        CREATE TABLE IF NOT EXISTS projects(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            context TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'active',
            owner_agent TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
        CREATE TABLE IF NOT EXISTS tasks(
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'generic',
            spec TEXT NOT NULL DEFAULT '{}',
            priority INTEGER NOT NULL DEFAULT 3,
            status TEXT NOT NULL DEFAULT 'queued',
            created_by TEXT,
            assigned_to TEXT,
            project_id TEXT,
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
        CREATE TABLE IF NOT EXISTS messages(
            id TEXT PRIMARY KEY,
            from_agent TEXT NOT NULL,
            to_agent TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'note',
            payload TEXT NOT NULL DEFAULT '{}',
            correlation_id TEXT,
            status TEXT NOT NULL DEFAULT 'unread',
            reply_to TEXT,
            ts REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_messages_to ON messages(to_agent);
        CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_agent);
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
        """)
        # Migration: add project_id to tasks for DBs created before projects.
        cols = [r["name"] for r in c.execute("PRAGMA table_info(tasks)")]
        if "project_id" not in cols:
            c.execute("ALTER TABLE tasks ADD COLUMN project_id TEXT")
        # Index lives here (not in the CREATE script) so it works whether the
        # column was just added or existed from a fresh schema.
        c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_project "
                  "ON tasks(project_id)")
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

    # -- A2A messages
    def add_message(self, mid, from_agent, to_agent, mtype, payload,
                    correlation_id=None, status="unread", reply_to=None):
        self.ex("INSERT INTO messages(id,from_agent,to_agent,type,payload,"
                "correlation_id,status,reply_to,ts) VALUES(?,?,?,?,?,?,?,?,?)",
                (mid, from_agent, to_agent, mtype, json.dumps(payload),
                 correlation_id, status, reply_to, now()))

    def get_message(self, mid):
        return self.q1("SELECT * FROM messages WHERE id=?", (mid,))

    def inbox(self, agent_id, limit=50, unread_only=False):
        sql = ("SELECT * FROM messages WHERE to_agent=? "
               + ("AND status='unread' " if unread_only else "")
               + "ORDER BY ts DESC LIMIT ?")
        return self.q(sql, (agent_id, limit))

    def mark_read(self, mid):
        self.ex("UPDATE messages SET status='read' WHERE id=? AND status='unread'",
                (mid,))

    def count_unread(self, agent_id):
        r = self.q1("SELECT COUNT(*) n FROM messages WHERE to_agent=? "
                    "AND status='unread'", (agent_id,))
        return r["n"] if r else 0

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
                 assigned_to, deadline, retry, project_id=None):
        t = now()
        self.ex("INSERT INTO tasks(id,title,kind,spec,priority,status,"
                "created_by,assigned_to,project_id,deadline,retry,result,"
                "artifacts,created_at,updated_at) VALUES(?,?,?,?,?,'queued',"
                "?,?,?,?,?,NULL,'[]',?,?)",
                (tid, title, kind, json.dumps(spec), priority, created_by,
                 assigned_to, project_id, deadline, json.dumps(retry), t, t))

    def get_task(self, tid):
        return self.q1("SELECT * FROM tasks WHERE id=?", (tid,))

    def list_tasks(self, status=None, assigned_to=None, created_by=None,
                   project_id=None, limit=50):
        sql = "SELECT * FROM tasks"
        where, args = [], []
        if status:
            where.append("status=?"); args.append(status)
        if assigned_to:
            where.append("assigned_to=?"); args.append(assigned_to)
        if created_by:
            where.append("created_by=?"); args.append(created_by)
        if project_id:
            where.append("project_id=?"); args.append(project_id)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY priority ASC, created_at ASC LIMIT ?"
        args.append(limit)
        return self.q(sql, args)

    def update_task(self, tid, **fields):
        allowed = {"title", "kind", "spec", "priority", "status",
                   "assigned_to", "project_id", "deadline", "retry",
                   "result", "artifacts"}
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
            HUB.notify_change()   # task state changed — wake SSE subscribers

    def next_pullable(self, caller_role, caller_id):
        """Next queued task a caller may claim.

        Assignment model: the ORCHESTRATOR assigns work; workers only execute
        what is explicitly assigned to THEM. This stops agents from grabbing
        arbitrary queue items (the 'took all the tasks and did nothing' bug).

        - Workers/QA: only tasks where assigned_to == caller.
        - Orchestrator/planner: may also pick up unassigned tasks (they're the
          ones dispatching), so an orchestrator running its own worker loop can
          drain work it created but forgot to assign.
        """
        if caller_role in ("orchestrator", "planner"):
            r = self.q1("SELECT * FROM tasks WHERE status='queued' AND "
                        "(assigned_to IS NULL OR assigned_to='' OR "
                        "assigned_to=?) ORDER BY priority ASC, created_at ASC "
                        "LIMIT 1", (caller_id,))
            return r
        # worker / qa: strictly their own assignments
        return self.q1("SELECT * FROM tasks WHERE status='queued' AND "
                       "assigned_to=? ORDER BY priority ASC, created_at ASC "
                       "LIMIT 1", (caller_id,))

    def count_queued(self, assignee=None):
        if assignee:
            r = self.q1("SELECT COUNT(*) n FROM tasks WHERE status IN "
                        "('queued','claimed','in_progress') AND assigned_to=?",
                        (assignee,))
        else:
            r = self.q1("SELECT COUNT(*) n FROM tasks WHERE status='queued'")
        return r["n"] if r else 0

    # -- projects
    def add_project(self, pid, name, description, context, owner_agent):
        t = now()
        self.ex("INSERT INTO projects(id,name,description,context,status,"
                "owner_agent,created_at,updated_at) VALUES(?,?,?,?, 'active',"
                "?,?,?)",
                (pid, name, description, json.dumps(context), owner_agent,
                 t, t))

    def get_project(self, pid):
        return self.q1("SELECT * FROM projects WHERE id=?", (pid,))

    def list_projects(self, status=None, limit=50):
        sql = "SELECT * FROM projects"
        where, args = [], []
        if status:
            where.append("status=?"); args.append(status)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        args.append(limit)
        return self.q(sql, args)

    def update_project(self, pid, **fields):
        allowed = {"name", "description", "context", "status", "owner_agent"}
        sets, args = [], []
        for k, v in fields.items():
            if k in allowed:
                sets.append(f"{k}=?")
                args.append(v)
        if sets:
            sets.append("updated_at=?")
            args.append(now())
            args.append(pid)
            self.ex(f"UPDATE projects SET {', '.join(sets)} WHERE id=?", args)

    def project_task_counts(self, pid):
        rows = self.q("SELECT status, COUNT(*) n FROM tasks WHERE project_id="
                      "? GROUP BY status", (pid,))
        return {r["status"]: r["n"] for r in rows}

    # -- deletion (admin) ---------------------------------------------------
    def task_artifact_ids(self, tid):
        """Return the artifact ids attached to a task (for disk cleanup).
        The task's artifacts column stores a JSON list of id strings."""
        row = self.q1("SELECT artifacts FROM tasks WHERE id=?", (tid,))
        if not row or not row["artifacts"]:
            return []
        try:
            arts = json.loads(row["artifacts"])
        except Exception:
            return []
        out = []
        for a in arts:
            if isinstance(a, str):
                out.append(a)
            elif isinstance(a, dict) and a.get("id"):
                out.append(a["id"])
        return out

    def delete_task(self, tid):
        """Delete a task + its artifacts (rows). Caller removes artifact files."""
        self.ex("DELETE FROM artifacts WHERE task_id=?", (tid,))
        self.ex("DELETE FROM tasks WHERE id=?", (tid,))

    def delete_project(self, pid):
        """Delete a project + all its tasks + their artifacts (rows).
        Returns the list of artifact ids so the caller can remove their files."""
        art_ids = []
        for t in self.q("SELECT id FROM tasks WHERE project_id=?", (pid,)):
            art_ids.extend(self.task_artifact_ids(t["id"]))
        self.ex("DELETE FROM artifacts WHERE task_id IN "
                "(SELECT id FROM tasks WHERE project_id=?)", (pid,))
        self.ex("DELETE FROM tasks WHERE project_id=?", (pid,))
        self.ex("DELETE FROM projects WHERE id=?", (pid,))
        return art_ids

    # -- events
    def add_event(self, actor, etype, task_id=None, detail=None):
        self.ex("INSERT INTO events(ts,actor,type,task_id,detail) "
                "VALUES(?,?,?,?,?)",
                (now(), actor, etype, task_id,
                 json.dumps(detail) if detail is not None else None))
        HUB.notify_change()   # wake SSE subscribers — something changed

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
            "assigned_to": row["assigned_to"],
            "project_id": row["project_id"] if "project_id" in row.keys()
            else None,
            "deadline": row["deadline"],
            "retry": retry, "result": result, "artifacts": arts,
            "created_at": row["created_at"], "updated_at": row["updated_at"],
        }

    def project_pub(self, row, with_counts=False):
        ctx = json.loads(row["context"]) if row["context"] else {}
        out = {
            "id": row["id"], "name": row["name"],
            "description": row["description"], "context": ctx,
            "status": row["status"], "owner_agent": row["owner_agent"],
            "created_at": row["created_at"], "updated_at": row["updated_at"],
        }
        if with_counts:
            out["tasks"] = self.store.project_task_counts(row["id"])
        return out

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
        return self._auth_from(self.headers.get("Authorization", ""),
                               need_admin=need_admin)

    def _auth_from(self, h, need_admin=False):
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
        ("DELETE", r"^/api/projects/(?P<id>[^/]+)$",            "ep_del_project"),
        ("DELETE", r"^/api/tasks/(?P<id>[^/]+)$",               "ep_del_task"),
        ("POST",   r"^/api/tasks$",                            "ep_create_task"),
        ("GET",    r"^/api/tasks$",                            "ep_list_tasks"),
        ("GET",    r"^/api/tasks/(?P<id>[^/]+)$",              "ep_get_task"),
        ("GET",    r"^/api/projects$",                         "ep_list_projects"),
        ("POST",   r"^/api/projects$",                         "ep_create_project"),
        ("GET",    r"^/api/projects/(?P<id>[^/]+)$",           "ep_get_project"),
        ("PATCH",  r"^/api/projects/(?P<id>[^/]+)$",           "ep_update_project"),
        ("GET",    r"^/api/work/pull$",                        "ep_pull"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/start$",        "ep_start"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/progress$",     "ep_progress"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/result$",       "ep_result"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/cancel$",       "ep_cancel"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/review$",       "ep_review"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/requeue$",      "ep_requeue"),
        ("POST",   r"^/api/tasks/(?P<id>[^/]+)/reassign$",     "ep_reassign"),
        ("POST",   r"^/api/artifacts$",                        "ep_upload"),
        ("GET",    r"^/api/artifacts$",                        "ep_list_art"),
        ("GET",    r"^/api/artifacts/(?P<id>[^/]+)$",          "ep_get_art"),
        ("GET",    r"^/api/events$",                           "ep_events"),
        ("GET",    r"^/api/admin/keys$",                       "ep_admin_keys"),
        ("POST",   r"^/api/admin/keys$",                       "ep_admin_issue"),
        ("DELETE", r"^/api/admin/keys/(?P<id>[^/]+)$",         "ep_admin_revoke"),
        ("PATCH",  r"^/api/admin/agents/(?P<id>[^/]+)$",       "ep_admin_patch"),
        ("POST",   r"^/api/admin/join-key$",                   "ep_admin_joinkey"),
        ("POST",   r"^/api/orch/spawn-member$",                "ep_spawn_member"),
        ("GET",    r"^/api/orch/swarm-view$",                  "ep_swarm_view"),
        ("GET",    r"^/api/messages$",                         "ep_inbox"),
        ("POST",   r"^/api/messages$",                         "ep_send_msg"),
        ("POST",   r"^/api/messages/(?P<id>[^/]+)/read$",      "ep_mark_read"),
        ("GET",    r"^/api/messages/stream$",                  "ep_messages_stream"),
        ("GET",    r"^/api/stream$",                            "ep_sse_stream"),
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
        # Gated self-service join: a new box presents a JOIN KEY (provisioned by
        # the admin in the console) to register itself as an 'observer'. The
        # admin then promotes it to a real role. This keeps /join from being an
        # open door on a public domain while still letting a remote agent check
        # in without holding the full admin token.
        body = self._json_body()
        jk = (body.get("join_key") or "").strip()
        if not jk:
            raise PermissionError("join_key required")
        expected = self.store.get_meta("join_key_hash")
        if not expected or not hmac.compare_digest(sha256_hex(jk.encode()), expected):
            raise PermissionError("invalid join key")
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
                             {"agent": aid, "note": "joined with join-key as observer"})
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

    def _remove_artifact_files(self, art_ids):
        """Best-effort removal of artifact files from disk."""
        removed = 0
        for aid in art_ids:
            p = os.path.join(self.store.data_dir, "artifacts", aid)
            try:
                if os.path.isfile(p):
                    os.remove(p); removed += 1
            except OSError:
                pass
        return removed

    def ep_del_task(self, g):
        # Admin-only deletion (distinct from role-gated cancel/requeue). Removes
        # the task row + its artifacts (rows and files). Events are kept as an
        # audit trail.
        self._auth(need_admin=True)
        tid = g["id"]
        if not self.store.get_task(tid):
            self._err(404, "unknown task")
            return
        art_ids = self.store.task_artifact_ids(tid)
        self.store.delete_task(tid)
        removed = self._remove_artifact_files(art_ids)
        self.store.add_event("admin", "task.deleted", tid,
                             {"artifacts_removed": removed})
        self._send_json({"ok": True, "artifacts_removed": removed})

    def ep_del_project(self, g):
        # Admin-only. Deletes the project + all its tasks + their artifacts.
        self._auth(need_admin=True)
        pid = g["id"]
        if not self.store.get_project(pid):
            self._err(404, "unknown project")
            return
        n_tasks = len(self.store.q("SELECT id FROM tasks WHERE project_id=?",
                                   (pid,)))
        art_ids = self.store.delete_project(pid)
        removed = self._remove_artifact_files(art_ids)
        self.store.add_event("admin", "project.deleted", None,
                             {"project": pid, "tasks_removed": n_tasks,
                              "artifacts_removed": removed})
        self._send_json({"ok": True, "tasks_removed": n_tasks,
                         "artifacts_removed": removed})

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
        project_id = body.get("project_id")
        retry = body.get("retry") or {"max": 3, "backoff_s": 10}
        if project_id and not self.store.get_project(project_id):
            raise ValueError(f"unknown project_id '{project_id}'")
        tid = "task-" + uuid.uuid4().hex[:12]
        self.store.add_task(tid, title, kind, spec, priority, agent["id"],
                            assigned_to, deadline, retry, project_id)
        self.store.add_event(agent["id"], "task.created", tid,
                             {"title": title, "priority": priority,
                              "project_id": project_id})
        self._send_json(self.mesh.task_pub(self.store.get_task(tid)), 201)

    def ep_list_tasks(self, g):
        self._auth()
        q = parse_qs(urlparse(self.path).query)
        items = self.store.list_tasks(
            status=(q.get("status") or [None])[0],
            assigned_to=(q.get("assigned_to") or [None])[0],
            created_by=(q.get("created_by") or [None])[0],
            project_id=(q.get("project_id") or [None])[0],
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

    # ---- projects
    def ep_list_projects(self, g):
        self._auth()
        q = parse_qs(urlparse(self.path).query)
        items = self.store.list_projects(
            status=(q.get("status") or [None])[0],
            limit=min(int((q.get("limit") or ["50"])[0]), 200))
        self._send_json({"items": [self.mesh.project_pub(r, with_counts=True)
                                   for r in items]})

    def ep_create_project(self, g):
        agent = self._auth()
        self.mesh.require_role(agent, CAP_DISPATCH, "create projects")
        body = self._json_body()
        name = (body.get("name") or "").strip()
        if not name:
            raise ValueError("name required")
        description = body.get("description") or ""
        context = body.get("context") or {}
        if not isinstance(context, dict):
            raise ValueError("context must be an object")
        pid = "proj-" + uuid.uuid4().hex[:12]
        self.store.add_project(pid, name, description, context, agent["id"])
        self.store.add_event(agent["id"], "project.created", None,
                             {"project": pid, "name": name})
        self._send_json(self.mesh.project_pub(self.store.get_project(pid),
                                              with_counts=True), 201)

    def ep_get_project(self, g):
        self._auth()
        row = self.store.get_project(g["id"])
        if not row:
            self._err(404, "unknown project")
            return
        out = self.mesh.project_pub(row, with_counts=True)
        tasks = self.store.list_tasks(project_id=row["id"], limit=200)
        out["task_items"] = [self.mesh.task_pub(t) for t in tasks]
        self._send_json(out)

    def ep_update_project(self, g):
        agent = self._auth()
        self.mesh.require_role(agent, CAP_DISPATCH, "update projects")
        row = self.store.get_project(g["id"])
        if not row:
            self._err(404, "unknown project")
            return
        body = self._json_body()
        fields = {}
        if "name" in body and body["name"]:
            fields["name"] = str(body["name"]).strip()
        if "description" in body:
            fields["description"] = body["description"]
        if "context" in body:
            if not isinstance(body["context"], dict):
                raise ValueError("context must be an object")
            fields["context"] = json.dumps(body["context"])
        if "status" in body:
            if body["status"] not in ("active", "paused", "done", "cancelled"):
                raise ValueError("status must be active|paused|done|cancelled")
            fields["status"] = body["status"]
        if fields:
            self.store.update_project(row["id"], **fields)
            self.store.add_event(agent["id"], "project.updated", None,
                                 {"project": row["id"], **{
                                     k: v for k, v in fields.items()
                                     if k != "context"}})
        self._send_json(self.mesh.project_pub(self.store.get_project(row["id"]),
                                              with_counts=True))

    def ep_pull(self, g):
        agent = self._auth()
        self.mesh.require_role(agent, CAP_PULL, "pull work")
        row = self.store.next_pullable(agent["role"], agent["id"])
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

    def ep_reassign(self, g):
        # Orchestrator moves a task to a different agent — used when an assigned
        # worker doesn't pick it up in a reasonable time (stale). Resets the task
        # to queued under the new assignee so the new worker can pull it.
        agent = self._auth()
        self.mesh.require_role(agent, CAP_DISPATCH, "reassign tasks")
        row = self._get_task_or_404(g["id"])
        body = self._json_body()
        to = (body.get("to") or "").strip()
        if not to:
            raise ValueError("to required")
        target = self.store.get_agent(to)
        if not target:
            raise KeyError(f"no such agent: {to}")
        # Only reassign work that hasn't been finished/reviewed.
        if row["status"] in ("done", "approved", "rejected"):
            raise ValueError(f"cannot reassign a task in status '{row['status']}'")
        old = row["assigned_to"]
        # Reset to queued under the new owner so they can pull it fresh.
        self.store.update_task(row["id"], status="queued", assigned_to=to,
                               result=None)
        self.store.add_event(agent["id"], "task.reassigned", row["id"],
                             {"from": old, "to": to})
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

    def ep_swarm_view(self, g):
        # Orchestrator's active-management view: who's online, what each agent
        # is currently doing, what's unassigned (needs dispatch), and who's idle
        # (online but no active task). This is what lets the master keep its
        # attention on the project and keep agents busy instead of idle.
        caller = self._auth()
        self.mesh.require_role(caller, CAP_DISPATCH, "view swarm")
        now = time.time()
        agents = []
        for a in self.store.list_agents():
            online = (now - a["last_seen"]) <= HEARTBEAT_WINDOW_S
            # what is this agent actively working on?
            active = self.store.q1(
                "SELECT id,title,status FROM tasks WHERE assigned_to=? AND "
                "status IN ('claimed','in_progress') ORDER BY updated_at DESC "
                "LIMIT 1", (a["id"],))
            queued_for = self.store.count_queued(assignee=a["id"])
            agents.append({
                "id": a["id"], "name": a["name"], "role": a["role"],
                "online": online,
                "last_seen_s_ago": round(now - a["last_seen"]),
                "current_task": ({"id": active["id"], "title": active["title"],
                                  "status": active["status"]} if active else None),
                "queued_for_them": queued_for,
                "idle": online and not active and queued_for == 0,
            })
        # work awaiting assignment (the orchestrator should dispatch these)
        unassigned = self.store.q(
            "SELECT id,title,priority,project_id FROM tasks WHERE status='queued' "
            "AND (assigned_to IS NULL OR assigned_to='') "
            "ORDER BY priority ASC, created_at ASC LIMIT 50")
        # STALE tasks: assigned but not picked up / not finished within the
        # window — candidates for reassignment to another agent.
        stale_cutoff = now - STALE_TASK_S
        stale = self.store.q(
            "SELECT id,title,status,assigned_to,updated_at FROM tasks WHERE "
            "status IN ('queued','claimed') AND assigned_to IS NOT NULL AND "
            "assigned_to!='' AND updated_at < ? ORDER BY updated_at ASC LIMIT 50",
            (stale_cutoff,))
        by_status = {r["status"]: r["n"] for r in self.store.q(
            "SELECT status, COUNT(*) n FROM tasks GROUP BY status")}
        self._send_json({
            "agents": agents,
            "unassigned_tasks": [dict(r) for r in unassigned],
            "stale_tasks": [dict(r) for r in stale],
            "stale_after_s": STALE_TASK_S,
            "tasks_by_status": by_status,
            "idle_agents": [a["id"] for a in agents if a["idle"]],
            "offline_agents": [a["id"] for a in agents if not a["online"]],
        })

    def ep_admin_joinkey(self, g):
        # Issue (or rotate) the join key. Returns the plaintext ONCE; stored
        # hashed. New agents present it to POST /api/agents/join.
        self._auth(need_admin=True)
        jk = "join_" + secrets.token_urlsafe(24)
        self.store.set_meta("join_key_hash", sha256_hex(jk.encode()))
        self.store.add_event("admin", "join_key.issued", None, {})
        self._send_json({"join_key": jk,
                         "note": "hand this to new agents; shown only once"})

    def ep_spawn_member(self, g):
        # Orchestrator-gated: mint a NEW swarm member on demand (the "agents
        # create bots/members" capability). Only orchestrator/planner roles may
        # spawn, and the spawned role is capped at 'worker' unless the caller is
        # a full orchestrator — so a planner can't mint another orchestrator.
        agent = self._auth()
        self.mesh.require_role(agent, CAP_DISPATCH, "spawn members")
        body = self._json_body()
        name = (body.get("name") or "").strip()
        if not name:
            raise ValueError("name required")
        role = body.get("role") or "worker"
        if role not in ROLES:
            raise ValueError(f"role must be one of {list(ROLES)}")
        # A non-orchestrator dispatcher cannot mint reviewer/orchestrator roles.
        if agent["role"] != "orchestrator" and role in ("reviewer", "qa",
                                                        "orchestrator"):
            raise PermissionError(
                "only an orchestrator can spawn that role")
        caps = body.get("caps") or []
        if not isinstance(caps, list) or not all(isinstance(c, str)
                                                  for c in caps):
            raise ValueError("caps must be a list of strings")
        aid = (body.get("id") or "").strip() or \
            f"{name.lower().replace(' ', '-')}-{uuid.uuid4().hex[:8]}"
        if self.store.get_agent(aid):
            raise ValueError(f"agent id '{aid}' already exists")
        key = gen_key()
        self.store.add_agent(aid, name, role, caps, sha256_hex(key.encode()))
        self.store.touch_agent(aid)
        self.store.add_event(agent["id"], "member.spawned", None,
                             {"agent": aid, "role": role, "by": agent["id"]})
        self._send_json({"agent": self.mesh.agent_pub(self.store.get_agent(aid)),
                         "api_key": key,
                         "note": "spawned member; key shown only once"}, 201)

    # ---- A2A messaging (over the mesh)
    def _msg_pub(self, row):
        # rowid is aliased as 'mid' by the stream query (it's a SQLite
        # pseudo-column not returned by SELECT *); fall back gracefully.
        return {"id": row["id"], "rowid": row["mid"] if "mid" in row.keys() else None,
                "from": row["from_agent"], "to": row["to_agent"],
                "type": row["type"], "payload": json.loads(row["payload"] or "{}"),
                "correlation_id": row["correlation_id"], "status": row["status"],
                "reply_to": row["reply_to"], "ts": row["ts"]}

    def ep_send_msg(self, g):
        # Any authenticated agent can message any other mesh member. This is the
        # peer-to-peer channel that was missing: agents no longer have to route
        # everything through the master for non-task chatter / coordination.
        caller = self._auth()
        body = self._json_body()
        to = (body.get("to") or "").strip()
        if not to:
            raise ValueError("to required")
        target = self.store.get_agent(to)
        if not target:
            raise KeyError(f"no such agent: {to}")
        mtype = body.get("type") or "note"
        payload = body.get("payload") or {}
        if not isinstance(payload, dict):
            raise ValueError("payload must be an object")
        mid = f"msg-{uuid.uuid4().hex}"
        self.store.add_message(mid, caller["id"], to, mtype, payload,
                               correlation_id=body.get("correlation_id"),
                               reply_to=body.get("reply_to"))
        self.store.add_event(caller["id"], "a2a.sent", None,
                             {"to": to, "type": mtype, "msg": mid})
        self._send_json({"ok": True, "id": mid, "delivered": True}, 201)

    def ep_inbox(self, g):
        caller = self._auth()
        unread_only = self.headers.get("X-Read", "") == "unread"
        q = parse_qs(urlparse(self.path).query)
        limit = min(int((q.get("limit") or ["50"])[0]), 200)
        rows = self.store.inbox(caller["id"], limit=limit,
                                unread_only=unread_only)
        self._send_json({
            "items": [self._msg_pub(r) for r in rows],
            "unread": self.store.count_unread(caller["id"]),
        })

    def ep_mark_read(self, g):
        caller = self._auth()
        row = self.store.get_message(g["id"])
        if not row:
            raise KeyError("no such message")
        if row["to_agent"] != caller["id"] and not self.mesh.auth_is_admin(
                self.headers.get("Authorization", "")):
            raise PermissionError("not your message")
        self.store.mark_read(g["id"])
        self._send_json({"ok": True, "id": g["id"]})

    def ep_messages_stream(self, g):
        # Long-poll: hold the connection until a NEW message arrives for this
        # agent (or timeout). This is what turns pull-based A2A into near-push:
        # an agent opens ONE stream and stays connected; a message lands within
        # ~1s of being sent instead of waiting for the next inbox poll.
        # Reverse-proxy friendly: bounded wait (default 25s < typical 30s proxy
        # idle timeout), so no SSE/websocket needed. The client just re-issues
        # with last_id to resume where it left off.
        caller = self._auth()
        q = parse_qs(urlparse(self.path).query)
        try:
            last_id = int((q.get("last_id") or ["0"])[0])
        except ValueError:
            raise ValueError("last_id must be an integer rowid")
        timeout = min(float((q.get("timeout") or ["25"])[0]), 55)
        limit = min(int((q.get("limit") or ["50"])[0]), 200)

        # Newer rows than last_id (messages.rowid is INTEGER AUTOINCREMENT, so
        # numeric comparison gives strict recency ordering). rowid is a SQLite
        # pseudo-column not exposed by SELECT *, so alias it explicitly.
        rows = self.store.q("SELECT *, rowid AS mid FROM messages "
                            "WHERE to_agent=? AND rowid>? ORDER BY rowid ASC LIMIT ?",
                            (caller["id"], last_id, limit))
        if rows:
            return self._send_json({
                "items": [self._msg_pub(r) for r in rows],
                "last_id": max(r["mid"] for r in rows),
                "unread": self.store.count_unread(caller["id"]),
                "timed_out": False,
            })

        # Nothing new yet — hold up to `timeout` seconds for one arrival.
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(remaining, 0.4))
            rows = self.store.q("SELECT *, rowid AS mid FROM messages "
                                "WHERE to_agent=? AND rowid>? ORDER BY rowid ASC LIMIT ?",
                                (caller["id"], last_id, limit))
            if rows:
                return self._send_json({
                    "items": [self._msg_pub(r) for r in rows],
                    "last_id": max(r["mid"] for r in rows),
                    "unread": self.store.count_unread(caller["id"]),
                    "timed_out": False,
                })
        self._send_json({"items": [], "last_id": last_id,
                         "unread": self.store.count_unread(caller["id"]),
                         "timed_out": True})

    def ep_sse_stream(self, g):
        # Server-Sent Events: instant console updates. The browser opens this
        # once; we hold the connection and push a `change` event whenever mesh
        # state mutates (via HUB.notify_change). The client re-fetches data on
        # each ping. A periodic keepalive comment prevents proxies from
        # closing an idle stream. Auth: Bearer header OR ?token= (EventSource
        # can't set headers, so the console uses the query param).
        q = parse_qs(urlparse(self.path).query)
        h = self.headers.get("Authorization", "")
        if not h:
            tok = (q.get("token") or [""])[0]
            if tok:
                h = "Bearer " + tok
        agent = self._auth_from(h)   # raises 403 if no valid key/admin
        try:
            keepalive = min(float((q.get("keepalive") or ["15"])[0]), 55)
        except ValueError:
            keepalive = 15.0
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")   # disable proxy buffering
        self.end_headers()
        w = HUB.subscribe()
        try:
            # initial hello so the client knows the stream is live
            self.wfile.write(b"event: hello\ndata: {}\n\n")
            self.wfile.flush()
            last_keepalive = time.monotonic()
            while True:
                with HUB._cv:
                    changed = HUB._cv.wait(timeout=1.0)
                now_mono = time.monotonic()
                if changed:
                    self.wfile.write(b"event: change\ndata: {}\n\n")
                    self.wfile.flush()
                    last_keepalive = now_mono
                elif now_mono - last_keepalive >= keepalive:
                    # SSE comment line — keeps the connection alive, ignored by clients
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    last_keepalive = now_mono
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass   # client disconnected
        finally:
            HUB.close(w)

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
UI_HTML = r"""<!doctype html>
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
/* ---- onboarding / help ---- */
.intro{background:#f0f7ff;border:1px solid #cfe4ff;border-radius:10px;padding:10px 14px;
margin-bottom:16px;font-size:13px;color:#274b6d;line-height:1.55;max-width:900px}
.intro b{color:#12324f}
.intro code{background:#e2eefc;padding:1px 5px;border-radius:5px;font-size:12px}
.tip{position:relative;display:inline-flex;align-items:center;justify-content:center;
width:15px;height:15px;border-radius:50%;background:var(--bd);color:var(--mut);
font-size:10px;font-weight:700;cursor:help;vertical-align:middle;margin-left:6px}
.tip:hover .tipbox,.tip:focus .tipbox{opacity:1;pointer-events:auto}
.tipbox{position:absolute;bottom:calc(100% + 8px);left:50%;transform:translateX(-50%);
width:250px;background:var(--fg);color:#fff;font-size:12px;font-weight:400;line-height:1.5;
padding:9px 11px;border-radius:9px;opacity:0;pointer-events:none;transition:opacity .15s;z-index:20;
box-shadow:0 6px 20px rgba(0,0,0,.18)}
.tipbox::after{content:"";position:absolute;top:100%;left:50%;transform:translateX(-50%);
border:6px solid transparent;border-top-color:var(--fg)}
.helplink{font:inherit;border:none;background:none;color:var(--acc);cursor:pointer;
font-size:13px;font-weight:500;padding:5px 8px;border-radius:8px}
.helplink:hover{background:#eaf3ff}
.modal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.4);z-index:50;
display:flex;align-items:flex-start;justify-content:center;padding:40px 16px;overflow:auto}
.modal{background:var(--card);border-radius:14px;max-width:680px;width:100%;padding:22px 24px;
box-shadow:0 20px 60px rgba(0,0,0,.3)}
.modal h3{font-size:16px;margin-bottom:4px}
.modal .sub{margin-bottom:14px}
.modal h4{font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:var(--mut);
margin:16px 0 6px}
.modal p,.modal li{font-size:13px;line-height:1.55;color:var(--fg)}
.modal ul{padding-left:18px;margin:4px 0}
.modal code{background:#f5f5f7;padding:1px 5px;border-radius:5px;font-size:12px}
.modal .close-x{float:right;border:none;background:none;font-size:20px;cursor:pointer;color:var(--mut);line-height:1}
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
// Inline "?" tooltip: <span class="tip">?<span class="tipbox">explanation</span></span>
function tip(html){return `<span class="tip" tabindex="0" aria-label="help">?<span class="tipbox">${html}</span></span>`}
// Full "How it works" help modal, opened from the top bar.
function openHelp(){
  const el=document.createElement("div");el.className="modal-backdrop";
  el.innerHTML=`<div class="modal">
    <button class="close-x" onclick="this.closest('.modal-backdrop').remove()" aria-label="close">×</button>
    <h3>How agent-mesh works</h3>
    <div class="sub">A quick mental model for new admins.</div>
    <h4>The big picture</h4>
    <p>agent-mesh is a <b>work channel</b> for a team of AI agents (and you). One box runs the
    <b>orchestrator</b> (this endpoint); other boxes join as <b>agents</b>. Agents check in, get
    assigned work, report progress, and upload results. You steer it all from this console.</p>
    <h4>Roles — who can do what</h4>
    <ul>
      <li><b>orchestrator / planner</b> — create &amp; assign tasks, spawn members, view the swarm.</li>
      <li><b>worker</b> — executes tasks <i>assigned to them</i>. Can't grab others' work.</li>
      <li><b>qa / reviewer</b> — review &amp; approve/reject finished work.</li>
      <li><b>observer</b> — read-only; what new agents start as until you promote them.</li>
    </ul>
    <h4>The task lifecycle</h4>
    <p><code>queued → claimed → in_progress → done/failed</code>, then optionally
    <code>approved / rejected</code> by a qa/reviewer. A task only moves when the right role acts on it.</p>
    <h4>Typical workflow</h4>
    <ul>
      <li><b>Onboard an agent:</b> Agents page → issue a <b>join key</b> → hand it to the new box → it appears as <i>observer</i> → set its role in the table.</li>
      <li><b>Give work:</b> create a task (Tasks page or a project) and <b>assign it</b> to a worker. Workers only pull what's assigned to them.</li>
      <li><b>Watch it happen:</b> Dashboard shows live counts + who's working what; Events is the audit log.</li>
      <li><b>Review:</b> when a task is done, a qa/reviewer approves or rejects it from the task page.</li>
    </ul>
    <h4>Keys &amp; security</h4>
    <p>Each agent has one <code>mesh_…</code> API key (shown once at creation). <b>Rekey</b> issues a
    new one and kills the old instantly (use if a key leaks). The <b>admin token</b> unlocks this
    console; the <b>join key</b> lets new boxes enroll (issuing a new one invalidates the old).</p>
    <h4>Live updates</h4>
    <p>Pages auto-refresh every ~5s. Your typed form text is preserved across refreshes.</p>
  </div>`;
  document.body.appendChild(el);
  el.addEventListener("click",e=>{if(e.target===el)el.remove()});
}

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
      <button class="${active==='projects'?'active':''}" onclick="go('#/projects')">Projects</button>
      <button class="${active==='agents'?'active':''}" onclick="go('#/agents')">Agents</button>
      <button class="${active==='tasks'?'active':''}" onclick="go('#/tasks')">Tasks</button>
      <button class="${active==='events'?'active':''}" onclick="go('#/events')">Events</button>
      <button class="${active==='artifacts'?'active':''}" onclick="go('#/artifacts')">Artifacts</button>
    </nav>
    <div style="display:flex;align-items:center;gap:10px">
      <button class="helplink" onclick="openHelp()">? Help</button>
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
    const [agents,tasks,events,stats,projects]=await Promise.all([
      api("/api/admin/keys",{admin:true}),
      api("/api/tasks?limit=200"),
      api("/api/events?limit=60"),
      api("/api/admin/stats",{admin:true}),
      api("/api/projects?limit=100")]);
    CACHE={agents:agents.items,tasks:tasks.items,events:events.items,stats,
           projects:projects.items};
    refreshData();   // update only the data regions — never touch form inputs
  }catch(e){flash(e.message,1)}
}
// Non-destructive auto-refresh: re-render just the #refresh region of the
// current page (tables/lists/stats). Form fields live OUTSIDE #refresh, so
// typed text survives the 5s poll. Falls back to a full route() if the page
// has no refresh region (e.g. async detail pages still loading).
function refreshData(){
  const h=location.hash||"#/";let m;
  const set=(id,html)=>{const el=$(id);if(el)el.innerHTML=html};
  if(h==="#/"||h===""){
    const S=CACHE.stats||{},T=CACHE.tasks||[],E=CACHE.events||[];
    const active=(S.tasks_by_status?.in_progress||0)+(S.tasks_by_status?.claimed||0);
    const done=(S.tasks_by_status?.done||0)+(S.tasks_by_status?.approved||0);
    set("#dashstats",`
     <div class="stats">
       <div><div class="stat">${S.tasks_by_status?.queued||0}</div><div class="statlabel">queued</div></div>
       <div><div class="stat">${active}</div><div class="statlabel">active</div></div>
       <div><div class="stat">${done}</div><div class="statlabel">done/approved</div></div>
       <div><div class="stat">${S.tasks_by_status?.failed||0}</div><div class="statlabel">failed</div></div>
       <div><div class="stat">${S.agents_total||0}</div><div class="statlabel">agents (${esc(Object.entries(S.agents_by_role||{}).map(([r,n])=>`${r}:${n}`).join(" · ")||"—")})</div></div>
     </div>`);
    set("#recent",T.slice(0,6).map(t=>`<tr class="clickable" onclick="go('#/task/${t.id}')"><td>${esc(t.title)}</td><td>${statusPill(t.status)}</td><td class="mono">${esc(t.assigned_to||"—")}</td><td>${ago(t.updated_at)}</td></tr>`).join("")||'<tr><td colspan=4 class="empty">no tasks yet</td></tr>');
    set("#evlive",E.slice(0,12).map(evLine).join("")||'<div class="empty">no events</div>');
  }
  else if(h==="#/projects"){
    const P=CACHE.projects||[];
    set("#projlist",P.map(p=>{const t=p.tasks||{};const total=Object.values(t).reduce((a,b)=>a+b,0);
        return `<div class="card">
          <div style="display:flex;justify-content:space-between;align-items:start">
            <div><b style="font-size:15px">${esc(p.name)}</b><div class="ev mono">${esc(p.id)}</div></div>
            ${projStatusPill(p.status)}
          </div>
          ${p.description?`<div class="ev" style="margin:8px 0">${esc(p.description)}</div>`:""}
          <div class="stats" style="margin:10px 0">
            <div><div class="stat" style="font-size:18px">${total}</div><div class="statlabel">tasks</div></div>
            <div><div class="stat" style="font-size:18px">${(t.in_progress||0)+(t.claimed||0)}</div><div class="statlabel">active</div></div>
            <div><div class="stat" style="font-size:18px">${(t.done||0)+(t.approved||0)}</div><div class="statlabel">done</div></div>
            <div><div class="stat" style="font-size:18px">${(t.failed||0)+(t.rejected||0)}</div><div class="statlabel">failed</div></div>
          </div>
          <div style="display:flex;gap:8px">
            <button class="sm primary" onclick="go('#/project/${esc(p.id)}')">open →</button>
            <button class="sm" onclick="closeProject('${esc(p.id)}','done')">mark done</button>
            <button class="sm danger" onclick="closeProject('${esc(p.id)}','cancelled')">cancel</button>
            ${ADMIN?`<button class="sm danger" onclick="delProject('${esc(p.id)}')" title="Permanently delete this project, all its tasks, and their artifacts (admin only). Cannot be undone.">delete</button>`:""}
          </div>
        </div>`}).join("")||'<div class="card"><div class="empty">no projects yet</div></div>');
  }
  else if(m=h.match(/^#\/project\/([^/]+)$/)){
    const pid=decodeURIComponent(m[1]);
    const p=(CACHE.projects||[]).find(x=>x.id===pid);
    if(p&&$("#ptaskbody")){
      const items=p.task_items||CACHE.tasks.filter(t=>t.project_id===pid);
      $("#ptaskbody").innerHTML=(items.map(t=>`<tr class="clickable" onclick="go('#/task/${t.id}')"><td>${esc(t.title)}</td><td>${statusPill(t.status)}</td><td class="mono">${esc(t.assigned_to||"—")}</td><td>${t.priority}</td></tr>`).join("")||'<tr><td colspan=4 class="empty">no tasks</td></tr>');
      const th=$("#ptaskcount");if(th)th.textContent=`Tasks (${items.length})`;
    }
  }
  else if(h==="#/agents"){
    const A=CACHE.agents||[];
    set("#agentrows",A.map(a=>{const hasAdmin=(a.caps||[]).includes("admin");return `<tr>
       <td>${esc(a.name)}<div class="ev mono">${esc(a.id)}</div></td>
       <td><select onchange="setRole('${esc(a.id)}',this.value)" style="width:auto">${roleOpts(a.role)}</select></td>
       <td>${agentPill(a.status)}</td><td>${ago(a.last_seen)}</td>
       <td class="mono key">${esc(a.key_prefix||"—")}${a.has_key?"":" ⚠ no-key"}</td>
       <td><button class="sm ${hasAdmin?'primary':''}" onclick="toggleAdminCap('${esc(a.id)}',${!hasAdmin})">${hasAdmin?'admin ✓':'grant'}</button></td>
       <td style="white-space:nowrap"><button class="sm" onclick="issueKey('${esc(a.id)}')">rekey</button>
           <button class="sm danger" onclick="revokeKey('${esc(a.id)}')">revoke</button>
           <button class="sm danger" onclick="delAgent('${esc(a.id)}')">delete</button></td>
     </tr>`}).join("")||'<tr><td colspan=7 class="empty">no agents registered</td></tr>');
  }
  else if(h==="#/tasks"){
    renderTaskFilter();  // updates #tasktable rows + #tcount, keeps filter select
  }
  else if(m=h.match(/^#\/task\/([^/]+)$/)){
    const tid=decodeURIComponent(m[1]);
    const evs=(CACHE.events||[]).filter(e=>e.task_id===tid);
    set("#taskevents",evs.map(evLine).join("")||'<div class="empty">no events for this task</div>');
  }
  else if(h==="#/events"){
    reloadEvents();
  }
  // artifacts page pulls its own data on demand; nothing to refresh here.
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
   <div class="intro">Live view of the swarm. <b>Queued</b> = waiting to be assigned, <b>active</b> = claimed or in progress,
    <b>done/approved</b> = finished (and accepted), <b>failed</b> = needs a look. Click any recent task to see its detail and act on it.
    New here? Hit <b>? Help</b> top-right for the full walkthrough.</div>
   <div id="dashstats" class="stats">
    <div><div class="stat">${S.tasks_by_status?.queued||0}</div><div class="statlabel">queued</div></div>
    <div><div class="stat">${active}</div><div class="statlabel">active</div></div>
    <div><div class="stat">${done}</div><div class="statlabel">done/approved</div></div>
    <div><div class="stat">${S.tasks_by_status?.failed||0}</div><div class="statlabel">failed</div></div>
    <div><div class="stat">${S.agents_total||0}</div><div class="statlabel">agents (${esc(roleCounts)})</div></div>
   </div>
   <div class="grid">
     <div class="card"><h2>Recent tasks</h2>
       <table><tr><th>title</th><th>status</th><th>assignee</th><th>updated</th></tr>
       <tbody id="recent">${recent.map(t=>`<tr class="clickable" onclick="go('#/task/${t.id}')"><td>${esc(t.title)}</td><td>${statusPill(t.status)}</td><td class="mono">${esc(t.assigned_to||"—")}</td><td>${ago(t.updated_at)}</td></tr>`).join("")||'<tr><td colspan=4 class="empty">no tasks yet</td></tr>'}</tbody>
       </table>
       <div style="margin-top:10px"><button class="sm" onclick="go('#/tasks')">all tasks →</button></div>
     </div>
     <div class="card"><h2>Live events</h2>
       <div id="evlive">${E.slice(0,12).map(evLine).join("")||'<div class="empty">no events</div>'}</div>
       <div style="margin-top:10px"><button class="sm" onclick="go('#/events')">full log →</button></div>
     </div>
   </div>`);
}
function evLine(e){return `<div class="ev"><b>${ago(e.ts)}</b> · <b>${esc(e.actor||"?")}</b> · ${esc(e.type)}${e.task_id?` <span class="mono">${esc(e.task_id)}</span>`:""}</div>`}

function pageAgents(){
  const A=CACHE.agents||[];
  return shell("agents","Agents & keys",`
   <div class="intro">Every agent in the swarm and its API key. <b>Onboard a new box:</b> issue a join key, hand it to the box,
    it shows up as <i>observer</i>, then set its role in the table. Or register an agent directly below.
    ${tip("A worker only executes tasks assigned to it. A qa/reviewer can approve finished work. An orchestrator/planner creates & assigns tasks. Observer is read-only.")} Roles control what each agent may do.</div>
   <div class="card" style="margin-bottom:16px">
     <h2>Join key (for new agents)${tip("New boxes run ./install.sh guest with this key + your base URL. They enroll as 'observer'. Issuing a new key invalidates the old one — so rotate after onboarding batches.")}</h2>
     <div class="row">
       <span class="ev" style="flex:1">Hand this to a new box so it can join the swarm. It lands as <b>observer</b> — assign a real role in the table below once it appears.</span>
       <button class="primary" onclick="issueJoinKey()">Issue / rotate join key</button>
     </div>
   </div>
   <div class="card">
     <h2>Register an agent directly${tip("Creates an agent right now and shows its API key ONCE. Use this for agents you're setting up by hand; use the join key for remote boxes that self-enroll.")}</h2>
     <div class="row">
       <input id="na" class="grow" placeholder="new agent name">
       <select id="nr" style="width:auto">${roleOpts("worker")}</select>
       <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--mut)" title="Lets this agent's key open the full admin console (as itself)."><input type="checkbox" id="ncap" style="width:auto"> admin cap</label>
       <button class="primary" onclick="regAgent()">Register</button>
     </div>
     <table><tr><th>name</th><th>role${tip("Change anytime. Takes effect immediately — e.g. promote a new observer to worker so it can start pulling assigned tasks.")}</th><th>status${tip("online = checked in within the last 90s. offline = no recent heartbeat (agent down or not running its worker loop).")}</th><th>seen</th><th>key${tip("The agent's API key prefix. The full key is shown only once at creation; 'rekey' issues a fresh one (old dies instantly).")}</th><th>console${tip("Grant/remove the admin capability so this agent's key can open the full console.")}</th><th>actions</th></tr>
     <tbody id="agentrows">${A.map(a=>{const hasAdmin=(a.caps||[]).includes("admin");return `<tr>
       <td>${esc(a.name)}<div class="ev mono">${esc(a.id)}</div></td>
       <td><select onchange="setRole('${esc(a.id)}',this.value)" style="width:auto">${roleOpts(a.role)}</select></td>
       <td>${agentPill(a.status)}</td><td>${ago(a.last_seen)}</td>
       <td class="mono key">${esc(a.key_prefix||"—")}${a.has_key?"":" ⚠ no-key"}</td>
       <td><button class="sm ${hasAdmin?'primary':''}" onclick="toggleAdminCap('${esc(a.id)}',${!hasAdmin})" title="Toggle the admin capability for this agent's key">${hasAdmin?'admin ✓':'grant'}</button></td>
       <td style="white-space:nowrap"><button class="sm" onclick="issueKey('${esc(a.id)}')" title="Issue a new API key; the old one stops working immediately">rekey</button>
           <button class="sm danger" onclick="revokeKey('${esc(a.id)}')" title="Revoke this agent's key (it can no longer authenticate)">revoke</button>
           <button class="sm danger" onclick="delAgent('${esc(a.id)}')" title="Remove the agent entirely (and its key)">delete</button></td>
     </tr>`}).join("")||'<tr><td colspan=7 class="empty">no agents registered yet — issue a join key above or register one below</td></tr>'}</tbody>
     </table>
   </div>`);
}

function projStatusPill(s){const m={active:"s-claimed",paused:"s-cancelled",done:"s-done",cancelled:"s-cancelled"};return pill(m[s]||"s-queued",s)}
function pageProjects(){
  const P=CACHE.projects||[];
  return shell("projects","Projects",`
   <div class="intro">A project groups related tasks (e.g. one feature or app) and carries shared context like the repo to work in.
    Create a project, open it, and add tasks — then assign those tasks to workers from inside the project.
    ${tip("Context is free-form JSON shown to agents working the project's tasks — put the repo path, branch, or any shared facts here.")}</div>
   <div class="card" style="margin-bottom:16px">
     <h2>New project${tip("Groups tasks under one name + shared context. You'll add and assign tasks from inside the project page.")}</h2>
     <div class="row">
       <input id="pjname" class="grow" placeholder="project name">
       <input id="pjctx" class="grow" placeholder='context JSON e.g. {"repo":"~/Work/x"}' title='Optional shared context for this project, as JSON. Shown to agents working its tasks.'>
       <button class="primary" onclick="createProject()">Create</button>
     </div>
   </div>
   <div class="grid" id="projlist">
     ${P.map(p=>{const t=p.tasks||{};const total=Object.values(t).reduce((a,b)=>a+b,0);
        return `<div class="card">
          <div style="display:flex;justify-content:space-between;align-items:start">
            <div><b style="font-size:15px">${esc(p.name)}</b><div class="ev mono">${esc(p.id)}</div></div>
            ${projStatusPill(p.status)}
          </div>
          ${p.description?`<div class="ev" style="margin:8px 0">${esc(p.description)}</div>`:""}
          <div class="stats" style="margin:10px 0">
            <div><div class="stat" style="font-size:18px">${total}</div><div class="statlabel">tasks</div></div>
            <div><div class="stat" style="font-size:18px">${(t.in_progress||0)+(t.claimed||0)}</div><div class="statlabel">active</div></div>
            <div><div class="stat" style="font-size:18px">${(t.done||0)+(t.approved||0)}</div><div class="statlabel">done</div></div>
            <div><div class="stat" style="font-size:18px">${(t.failed||0)+(t.rejected||0)}</div><div class="statlabel">failed</div></div>
          </div>
          <div style="display:flex;gap:8px">
            <button class="sm primary" onclick="go('#/project/${esc(p.id)}')">open →</button>
            <button class="sm" onclick="closeProject('${esc(p.id)}','done')">mark done</button>
            <button class="sm danger" onclick="closeProject('${esc(p.id)}','cancelled')">cancel</button>
            ${ADMIN?`<button class="sm danger" onclick="delProject('${esc(p.id)}')" title="Permanently delete this project, all its tasks, and their artifacts (admin only). Cannot be undone.">delete</button>`:""}
          </div>
        </div>`}).join("")||'<div class="card"><div class="empty">no projects yet</div></div>'}
   </div>`);
}
async function pageProject(id){
  let p;
  try{p=await api("/api/projects/"+id)}catch(e){return shell("projects","Project not found",`<div class="card"><div class="empty">${esc(e.message)}</div><span class="backlink" onclick="go('#/projects')">← back to projects</span></div>`)}
  const items=p.task_items||[];
  return shell("projects",esc(p.name),`
   <span class="backlink" onclick="go('#/projects')">← all projects</span>
   <div class="grid">
     <div class="card"><h2>Details</h2>
       <dl class="kv">
         <dt>id</dt><dd class="mono">${esc(p.id)}</dd>
         <dt>status</dt><dd>${projStatusPill(p.status)}</dd>
         <dt>owner</dt><dd class="mono">${esc(p.owner_agent||"—")}</dd>
         <dt>created</dt><dd>${ago(p.created_at)}</dd>
         <dt>updated</dt><dd>${ago(p.updated_at)}</dd>
       </dl>
       ${p.description?`<h2 style="margin-top:14px">Description</h2><div>${esc(p.description)}</div>`:""}
       <h2 style="margin-top:14px">Context</h2><pre>${esc(JSON.stringify(p.context||{},null,1))}</pre>
       <div style="margin-top:14px;display:flex;gap:8px">
         <button class="sm" onclick="closeProject('${esc(p.id)}','done')">mark done</button>
         <button class="sm" onclick="closeProject('${esc(p.id)}','paused')">pause</button>
         <button class="sm danger" onclick="closeProject('${esc(p.id)}','cancelled')">cancel</button>
       </div>
     </div>
     <div class="card"><h2 id="ptaskcount">Tasks (${items.length})</h2>
       <div class="row">
         <input id="pttitle" class="grow" placeholder="new task title">
         <select id="ptkind" style="width:auto">${KINDS.map(k=>`<option>${k}</option>`).join("")}</select>
         <input id="ptprio" type="number" min="0" max="5" value="3" style="width:56px">
         <button class="sm primary" onclick="addTaskToProject('${esc(p.id)}')">add</button>
       </div>
       <table id="ptasktable"><thead><tr><th>title</th><th>status</th><th>assignee</th><th>prio</th></tr></thead>
       <tbody id="ptaskbody">${items.map(t=>`<tr class="clickable" onclick="go('#/task/${t.id}')"><td>${esc(t.title)}</td><td>${statusPill(t.status)}</td><td class="mono">${esc(t.assigned_to||"—")}</td><td>${t.priority}</td></tr>`).join("")||'<tr><td colspan=4 class="empty">no tasks</td></tr>'}</tbody>
       </table>
     </div>
   </div>`);
}

function pageTasks(){
  const T=CACHE.tasks||[];
  return shell("tasks","Task board",`
   <div class="intro">All work in the swarm. Create a task here, then <b>assign it to a worker</b> (open the task → set assignee) —
    workers only pull tasks assigned to them, so an unassigned task just sits in <i>queued</i>.
    Click any row for detail, actions (start/cancel/requeue), and review.</div>
   <div class="card">
     <div class="row">
       <input id="tt" class="grow" placeholder="task title">
       <select id="tk" style="width:auto" title="What kind of work: code, research, docs, ops, test, or generic">${kindOpts("code")}</select>
       <input id="tp" type="number" min="0" max="5" value="3" style="width:64px" title="priority 0-5 (lower = more urgent)">
       <button class="primary" onclick="mkTask()" title="Creates a queued task. Assign it to a worker from its detail page so someone will pick it up.">Create task</button>
     </div>
     <div class="filters">
       <select id="tf" onchange="renderTaskFilter()" style="width:auto">
         <option value="">all statuses</option>
         ${["queued","claimed","in_progress","done","failed","cancelled","approved","rejected"].map(s=>`<option>${s}</option>`).join("")}
       </select>
       <span class="ev" id="tcount"></span>
     </div>
     <table id="tasktable"><thead><tr><th>title</th><th>kind</th><th>status</th><th>assignee</th><th>prio</th><th>created</th><th>updated</th></tr></thead>
     <tbody id="taskbody">${taskRows(T)}</tbody>
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
// Targeted refresh: update ONLY the #taskbody rows (and the count), never the
// header or the filter select. This is what stops the table from collapsing /
// losing formatting on every 5s auto-refresh.
function renderTaskFilter(){
  const body=$("#taskbody");
  if(!body)return;
  body.innerHTML=taskRows(CACHE.tasks||[]);
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
     <div class="card"><h2>Actions${tip("These act on the task's state. Start moves it to in_progress; Cancel stops it; Requeue puts a failed/cancelled task back to queued so it can be tried again.")}</h2>
       <div class="row">
         ${t.status==="queued"||t.status==="claimed"?`<button class="primary" onclick="taskAct('${t.id}','start')" title="Mark this task as actively being worked (in_progress)">Start</button>`:""}
         ${["queued","claimed","in_progress"].includes(t.status)?`<button class="danger" onclick="taskAct('${t.id}','cancel')" title="Stop this task; it won't be worked further">Cancel</button>`:""}
         ${["failed","cancelled","rejected"].includes(t.status)?`<button onclick="taskAct('${t.id}','requeue')" title="Put this task back to queued so it can be attempted again">Requeue</button>`:""}
         ${ADMIN?`<button class="danger" onclick="delTask('${t.id}')" title="Permanently delete this task and its artifacts (admin only). This cannot be undone — Cancel/Requeue are safer for stopping work.">Delete</button>`:""}
       </div>
       ${canReview?`<h2 style="margin-top:8px">Review${tip("Only a qa/reviewer/orchestrator can approve or reject. Approve accepts the finished work; Reject sends it back (use the note to say why).")}</h2>
         <div class="row">
           <button class="primary" onclick="taskReview('${t.id}','approved')" title="Accept this finished task as done well">Approve</button>
           <button class="danger" onclick="taskReview('${t.id}','rejected')" title="Send this task back — it didn't meet the bar">Reject</button>
           <input id="rnote" class="grow" placeholder="note (optional)" title="Optional note recorded with your review decision">
         </div>`:`<div class="ev">Review becomes available once the task is <b>done</b> or <b>failed</b>.</div>`}
       <h2 style="margin-top:16px">Artifacts (${arts.length})${tip("Files the agent uploaded as results (builds, reports, etc.). Click download to grab them.")}</h2>
       ${arts.length?`<table><tr><th>name</th><th>size</th><th>sha256</th><th></th></tr>
         ${arts.map(aid=>artRow(aid)).join("")}</table>`:'<div class="empty">none uploaded</div>'}
     </div>
     <div class="card"><h2>Task events</h2>
       <div id="taskevents">${evs.map(evLine).join("")||'<div class="empty">no events for this task</div>'}</div>
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
   <div class="intro">A running record of everything that happened — who did what, when. Useful for tracing a task's history or debugging why something is in a given state.
    Filter by actor (who), type (what kind of event), or task id.</div>
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
   <div class="intro">Files agents uploaded as task results — builds, reports, datasets, etc. Each is tied to the task that produced it. Click <b>download</b> to grab one.</div>
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
  else if(h==="#/projects"){$("#app").innerHTML=pageProjects()}
  else if(m=h.match(/^#\/project\/([^/]+)$/)){$("#app").innerHTML='<div class="empty">loading…</div>';pageProject(decodeURIComponent(m[1])).then(html=>{$("#app").innerHTML=html})}
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
async function issueJoinKey(){
  try{const d=await api("/api/admin/join-key",{method:"POST",admin:true,body:{}});
    window.prompt("JOIN KEY (hand to new agents; old one is now invalid)"+String.fromCharCode(10,10)+"./install.sh <orchestrator-url> "+d.join_key, d.join_key);
    flash("join key issued");}catch(e){flash(e.message,1)}
}
async function createProject(){
  const name=$("#pjname").value.trim();if(!name)return flash("name required",1);
  let ctx={};const raw=$("#pjctx").value.trim();
  if(raw){try{ctx=JSON.parse(raw)}catch(e){return flash("context must be valid JSON",1)}}
  try{await api("/api/projects",{method:"POST",body:{name,description:"",context:ctx}});
    flash("project created");loadAll();}catch(e){flash(e.message,1)}
}
async function closeProject(id,status){
  try{await api("/api/projects/"+id,{method:"PATCH",body:{status}});
    flash("project "+status);loadAll();}catch(e){flash(e.message,1)}
}
async function addTaskToProject(pid){
  const title=$("#pttitle").value.trim();if(!title)return flash("title required",1);
  const kind=$("#ptkind").value,priority=parseInt($("#ptprio").value||"3",10);
  try{await api("/api/tasks",{method:"POST",body:{title,kind,priority,project_id:pid,spec:{}}});
    flash("task added");loadAll();
    pageProject(pid).then(html=>{$("#app").innerHTML=html});}catch(e){flash(e.message,1)}
}
async function issueKey(id){try{const d=await api("/api/admin/keys",{method:"POST",admin:true,body:{agent_id:id}});window.prompt("New key (old revoked):",d.api_key);loadAll()}catch(e){flash(e.message,1)}}
async function revokeKey(id){if(!confirm("Revoke key for "+id+"?"))return;try{await api("/api/admin/keys/"+id,{method:"DELETE",admin:true});flash("revoked");loadAll()}catch(e){flash(e.message,1)}}
async function delAgent(id){if(!confirm("Delete agent "+id+"? This removes it and revokes its key."))return;try{await api("/api/agents/"+id,{method:"DELETE",admin:true});flash("deleted");loadAll()}catch(e){flash(e.message,1)}}
async function delTask(id){if(!confirm("Permanently delete task "+id+" and its artifacts? This cannot be undone."))return;try{const d=await api("/api/tasks/"+id,{method:"DELETE",admin:true});flash("task deleted"+(d.artifacts_removed?" ("+d.artifacts_removed+" artifact(s) removed)":""));location.hash="#/tasks";loadAll()}catch(e){flash(e.message,1)}}
async function delProject(id){if(!confirm("Permanently delete project "+id+", ALL of its tasks, and their artifacts? This cannot be undone."))return;try{const d=await api("/api/projects/"+id,{method:"DELETE",admin:true});flash("project deleted"+(d.tasks_removed?" ("+d.tasks_removed+" task(s), "+d.artifacts_removed+" artifact(s) removed)":""));location.hash="#/projects";loadAll()}catch(e){flash(e.message,1)}}
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
// Instant updates via Server-Sent Events: subscribe once; on each `change`
// ping, re-fetch data immediately. The 5s poll stays as a fallback in case the
// stream drops (proxy timeout, network blip) so the console never goes stale.
let _es=null,_sseAlive=false;
function connectStream(){
  if(_es){try{_es.close()}catch{};_es=null}
  if(!hasSession())return;
  const tok=localStorage.getItem("mesh_agent_key")||sessionStorage.getItem("mesh_adm");
  // EventSource can't set an Authorization header, so pass the token as a
  // query param; the server accepts either for this endpoint.
  const es=new EventSource(BASE+"/api/stream?token="+encodeURIComponent(tok||""));
  _es=es;
  es.addEventListener("hello",()=>{_sseAlive=true});
  es.addEventListener("change",()=>{if(hasSession())loadAll()});
  es.onerror=()=>{_sseAlive=false;   // EventSource auto-reconnects; poll covers gaps
    try{es.close()}catch{};_es=null;
    setTimeout(connectStream,3000);};
}
function boot(){
  if(!hasSession()){renderLock();return}
  route(true);
  clearInterval(window._rt);window._rt=setInterval(()=>{if(hasSession())loadAll()},5000);
  connectStream();
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

    # -- A2A messaging (over the mesh)
    def send_msg(self, to, text=None, mtype="note", payload=None,
                 correlation_id=None, reply_to=None):
        body = {"to": to, "type": mtype,
                "payload": payload if payload is not None else {"text": text or ""}}
        if correlation_id:
            body["correlation_id"] = correlation_id
        if reply_to:
            body["reply_to"] = reply_to
        return self._req("POST", "/api/messages", body)

    def inbox(self, limit=50, unread_only=False):
        import urllib.request
        url = self.base + f"/api/messages?limit={limit}"
        req = urllib.request.Request(url, method="GET")
        req.add_header("Authorization", "Bearer " + self.key)
        if unread_only:
            req.add_header("X-Read", "unread")
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read().decode())

    def mark_read(self, msg_id):
        return self._req("POST", f"/api/messages/{msg_id}/read")

    def stream(self, last_id=0, timeout=25, limit=50):
        # Long-poll: blocks until a new message arrives or `timeout` seconds.
        import urllib.request
        url = (self.base + f"/api/messages/stream?last_id={last_id}"
               f"&timeout={timeout}&limit={limit}")
        req = urllib.request.Request(url, method="GET")
        req.add_header("Authorization", "Bearer " + self.key)
        with urllib.request.urlopen(req, timeout=self.timeout + 10) as r:
            return json.loads(r.read().decode())

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
