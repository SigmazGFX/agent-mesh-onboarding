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

VERSION = "0.9"
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


# ---------------------------------------------------------------------------
# Chat hub — broadcast admin chat messages in real time to all SSE listeners.
# Unlike ChangeHub (which just says "something changed"), ChatHub carries the
# actual message payload so chat clients receive it inline with no refetch.
# ---------------------------------------------------------------------------
class ChatHub:
    def __init__(self):
        self._lock = threading.Lock()   # protects _subs list only
        self._subs: list = []

    def subscribe(self):
        """Return a per-connection subscriber dict with its own Condition."""
        import collections
        # Each sub has its own independent Condition so broadcast can notify
        # without holding the list lock (avoids deadlock).
        sub = {"q": collections.deque(), "cv": threading.Condition()}
        with self._lock:
            self._subs.append(sub)
        return sub

    def unsubscribe(self, sub):
        with self._lock:
            try:
                self._subs.remove(sub)
            except ValueError:
                pass

    def broadcast(self, msg: dict):
        """Enqueue message on every subscriber and wake their wait loop."""
        data = json.dumps(msg)
        with self._lock:
            snapshot = list(self._subs)
        # Notify outside the list lock so subscribe/unsubscribe can proceed
        for sub in snapshot:
            with sub["cv"]:
                sub["q"].append(data)
                sub["cv"].notify_all()


CHAT_HUB = ChatHub()


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
            artifact_storage TEXT NOT NULL DEFAULT '{"type":"local"}',
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
        CREATE TABLE IF NOT EXISTS chat_messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT NOT NULL,
            sender_name TEXT NOT NULL DEFAULT 'admin',
            text TEXT NOT NULL,
            ts REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS artifact_targets(
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            config TEXT NOT NULL DEFAULT '{}',
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS project_requests(
            id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            text TEXT NOT NULL,
            kind TEXT NOT NULL DEFAULT 'feature',
            submitted_by TEXT,
            task_id TEXT,
            created_at REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_proj_req_project
            ON project_requests(project_id);
        """)
        # Migration: add project_id to tasks for DBs created before projects.
        cols = [r["name"] for r in c.execute("PRAGMA table_info(tasks)")]
        if "project_id" not in cols:
            c.execute("ALTER TABLE tasks ADD COLUMN project_id TEXT")
        c.execute("CREATE INDEX IF NOT EXISTS idx_tasks_project "
                  "ON tasks(project_id)")
        # Migration: add artifact_storage to projects for DBs before v0.8.
        pcols = [r["name"] for r in c.execute("PRAGMA table_info(projects)")]
        if "artifact_storage" not in pcols:
            c.execute("ALTER TABLE projects ADD COLUMN artifact_storage TEXT "
                      "NOT NULL DEFAULT '{\"type\":\"local\"}'")
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

    def get_agent_by_name(self, name):
        """Case-insensitive lookup by friendly name (names are meant to be unique)."""
        return self.q1("SELECT * FROM agents WHERE lower(name)=lower(?)", (name,))

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
    def add_project(self, pid, name, description, context, owner_agent,
                    artifact_storage=None):
        t = now()
        storage = json.dumps(artifact_storage or {"type": "local"})
        self.ex("INSERT INTO projects(id,name,description,context,status,"
                "owner_agent,artifact_storage,created_at,updated_at) "
                "VALUES(?,?,?,?,'active',?,?,?,?)",
                (pid, name, description, json.dumps(context), owner_agent,
                 storage, t, t))

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
        allowed = {"name", "description", "context", "status", "owner_agent",
                   "artifact_storage"}
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

    def list_artifacts_for_project(self, project_id):
        """Return all artifacts whose task belongs to the given project."""
        return self.q(
            "SELECT a.* FROM artifacts a "
            "JOIN tasks t ON a.task_id=t.id "
            "WHERE t.project_id=? ORDER BY a.ts ASC",
            (project_id,))

    # -- artifact targets (external push destinations)
    def list_artifact_targets(self):
        return self.q("SELECT * FROM artifact_targets ORDER BY created_at ASC")

    def get_artifact_target(self, tid):
        return self.q1("SELECT * FROM artifact_targets WHERE id=?", (tid,))

    def add_artifact_target(self, tid, name, atype, config):
        t = now()
        self.ex("INSERT INTO artifact_targets(id,name,type,config,enabled,created_at) "
                "VALUES(?,?,?,?,1,?)",
                (tid, name, atype, json.dumps(config), t))

    def update_artifact_target(self, tid, **fields):
        allowed = {"name", "type", "config", "enabled"}
        sets, args = [], []
        for k, v in fields.items():
            if k in allowed:
                sets.append(f"{k}=?")
                args.append(v)
        if sets:
            args.append(tid)
            self.ex(f"UPDATE artifact_targets SET {', '.join(sets)} WHERE id=?", args)

    def delete_artifact_target(self, tid):
        self.ex("DELETE FROM artifact_targets WHERE id=?", (tid,))

    # -- admin chat
    def add_chat_message(self, sender, sender_name, text):
        t = now()
        self.ex("INSERT INTO chat_messages(sender,sender_name,text,ts) "
                "VALUES(?,?,?,?)", (sender, sender_name, text, t))
        row_id = self.q1("SELECT last_insert_rowid() id")["id"]
        return {"id": row_id, "sender": sender, "sender_name": sender_name,
                "text": text, "ts": t}

    def list_chat_messages(self, limit=100):
        return list(self.q(
            "SELECT id,sender,sender_name,text,ts FROM chat_messages "
            "ORDER BY ts DESC LIMIT ?", (limit,)))

    # -- auto-planning helpers
    def best_agent_for_role(self, role):
        """Return the online agent best suited for a role (online first, then offline)."""
        t = now()
        # Prefer online agents with the given role; fallback to any with that role.
        r = self.q1(
            "SELECT * FROM agents WHERE role=? AND status!='disabled' "
            "ORDER BY (last_seen > ?) DESC, last_seen DESC LIMIT 1",
            (role, t - HEARTBEAT_WINDOW_S))
        return r

    # -- project requests (admin free-text → task)
    def add_project_request(self, rid, project_id, text, kind, submitted_by,
                            task_id=None):
        t = now()
        self.ex("INSERT INTO project_requests(id,project_id,text,kind,"
                "submitted_by,task_id,created_at) VALUES(?,?,?,?,?,?,?)",
                (rid, project_id, text, kind, submitted_by, task_id, t))

    def list_project_requests(self, project_id, limit=50):
        return self.q("SELECT * FROM project_requests WHERE project_id=? "
                      "ORDER BY created_at DESC LIMIT ?",
                      (project_id, limit))

    def project_activity(self, project_id):
        """Return a summary dict: last_activity ts, last_task snippet, counts
        by status, and recent request count. Used to enrich project cards."""
        counts_rows = self.q(
            "SELECT status, COUNT(*) n FROM tasks WHERE project_id=? "
            "GROUP BY status", (project_id,))
        counts = {r["status"]: r["n"] for r in counts_rows}
        last_task = self.q1(
            "SELECT title, status, updated_at FROM tasks WHERE project_id=? "
            "ORDER BY updated_at DESC LIMIT 1", (project_id,))
        rc_row = self.q1(
            "SELECT COUNT(*) n FROM project_requests WHERE project_id=?",
            (project_id,))
        req_count = rc_row["n"] if rc_row else 0
        last_activity = (last_task["updated_at"] if last_task else None)
        return {
            "counts": counts,
            "last_task": {"title": last_task["title"],
                          "status": last_task["status"]} if last_task else None,
            "last_activity": last_activity,
            "request_count": req_count,
        }

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

    def request_pub(self, row):
        return {"id": row["id"], "project_id": row["project_id"],
                "text": row["text"], "kind": row["kind"],
                "submitted_by": row["submitted_by"], "task_id": row["task_id"],
                "created_at": row["created_at"]}

    def project_pub(self, row, with_counts=False, with_activity=False):
        ctx = json.loads(row["context"]) if row["context"] else {}
        # Expose storage type only — never return plaintext credentials
        raw_storage = (json.loads(row["artifact_storage"])
                       if row["artifact_storage"] else {"type": "local"})
        storage_pub = {"type": raw_storage.get("type", "local")}
        # Include non-secret config fields so the UI can display context
        for k, v in raw_storage.items():
            if k in ("type", "repo", "branch", "path", "org", "project",
                     "feed", "local_path", "package"):
                storage_pub[k] = v
        out = {
            "id": row["id"], "name": row["name"],
            "description": row["description"], "context": ctx,
            "status": row["status"], "owner_agent": row["owner_agent"],
            "artifact_storage": storage_pub,
            "created_at": row["created_at"], "updated_at": row["updated_at"],
        }
        if with_counts:
            out["tasks"] = self.store.project_task_counts(row["id"])
        if with_activity:
            out["activity"] = self.store.project_activity(row["id"])
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

    def artifact_target_pub(self, row):
        cfg = json.loads(row["config"]) if row["config"] else {}
        # Redact secrets before returning to client
        safe_cfg = {k: ("***" if "token" in k.lower() or "secret" in k.lower()
                        or "password" in k.lower() else v)
                    for k, v in cfg.items()}
        return {"id": row["id"], "name": row["name"], "type": row["type"],
                "config": safe_cfg, "enabled": bool(row["enabled"]),
                "created_at": row["created_at"]}

    def _project_storage(self, project_id, store):
        """Return the full (secrets-included) artifact_storage dict for a project."""
        if not project_id:
            return {"type": "local"}
        row = store.get_project(project_id)
        if not row or not row["artifact_storage"]:
            return {"type": "local"}
        try:
            return json.loads(row["artifact_storage"])
        except Exception:
            return {"type": "local"}

    def push_artifact_to_targets(self, artifact_row, file_path, store,
                                 project_id=None):
        """Push artifact to the project's configured storage destination.
        Always async — never blocks the upload response.
        project_id: resolved from the task if not passed directly.
        """
        import threading as _threading
        # Resolve project from task if not given
        if not project_id:
            task_id = artifact_row.get("task_id") if isinstance(
                artifact_row, dict) else artifact_row["task_id"]
            if task_id:
                trow = store.get_task(task_id)
                if trow:
                    project_id = trow["project_id"]
        t = _threading.Thread(
            target=self._push_worker,
            args=(dict(artifact_row), file_path, store, project_id),
            daemon=True)
        t.start()

    def _push_worker(self, artifact, file_path, store, project_id):
        storage = self._project_storage(project_id, store)
        ttype = storage.get("type", "local")
        # Inject project_id so push methods can build structured paths
        cfg = {**storage, "_project_id": project_id or ""}
        try:
            if ttype == "github":
                self._push_github(artifact, file_path, cfg)
            elif ttype == "ado":
                self._push_ado(artifact, file_path, cfg)
            elif ttype == "local":
                self._push_local_structured(artifact, file_path, store,
                                            project_id, storage)
            store.add_event("system", "artifact.pushed",
                            artifact.get("task_id"),
                            {"artifact": artifact["id"],
                             "type": ttype, "project": project_id})
        except Exception as exc:
            store.add_event("system", "artifact.push_failed",
                            artifact.get("task_id"),
                            {"artifact": artifact["id"], "type": ttype,
                             "error": str(exc)})

    def _push_local_structured(self, artifact, file_path, store, project_id,
                               cfg):
        """Mirror artifact into a structured local directory tree.

        Layout: <local_path or data_dir>/projects/<project_id>/<task_id>/<name>
        This gives the master node a clean browsable tree of every project's
        outputs without touching the flat artifacts/<id> primary store.
        """
        base = cfg.get("local_path") or store.data_dir
        task_id = artifact.get("task_id") or "no-task"
        proj_dir = os.path.join(base, "projects",
                                project_id or "unscoped", task_id)
        os.makedirs(proj_dir, exist_ok=True)
        dest = os.path.join(proj_dir, artifact["name"])
        # Avoid collision: suffix with artifact id prefix if name already exists
        if os.path.exists(dest):
            stem, ext = os.path.splitext(artifact["name"])
            dest = os.path.join(proj_dir, f"{stem}_{artifact['id'][:8]}{ext}")
        import shutil as _shutil
        _shutil.copy2(file_path, dest)

    def _push_github(self, artifact, file_path, cfg):
        """Commit artifact to a GitHub repo on behalf of the master node.

        Path layout in the repo:
          <path_prefix>/<project_id>/<task_id>/<filename>
        so every project's outputs are organized by project then task.
        """
        import urllib.request as _ur
        import base64 as _b64
        token = cfg.get("token", "")
        repo = cfg.get("repo", "")          # e.g. "owner/repo"
        branch = cfg.get("branch", "main")
        base_path = cfg.get("path", "artifacts/").rstrip("/")
        # Build structured path: base/project_id/task_id/filename
        project_id = cfg.get("_project_id", "")
        task_id = artifact.get("task_id") or "no-task"
        dest_path = "/".join(filter(None, [base_path, project_id, task_id,
                                           artifact["name"]]))
        if not token or not repo:
            raise ValueError("github target missing token or repo")
        with open(file_path, "rb") as f:
            content = _b64.b64encode(f.read()).decode()
        # Check if file already exists (need sha for update)
        api_url = f"https://api.github.com/repos/{repo}/contents/{dest_path}"
        get_req = _ur.Request(api_url, method="GET")
        get_req.add_header("Authorization", f"token {token}")
        get_req.add_header("Accept", "application/vnd.github+json")
        sha = None
        try:
            with _ur.urlopen(get_req, timeout=30) as r:
                existing = json.loads(r.read().decode())
                sha = existing.get("sha")
        except Exception:
            pass
        body = {"message": f"artifact: {artifact['name']} (mesh {artifact['id']})",
                "content": content, "branch": branch}
        if sha:
            body["sha"] = sha
        put_req = _ur.Request(api_url, data=json.dumps(body).encode(), method="PUT")
        put_req.add_header("Authorization", f"token {token}")
        put_req.add_header("Content-Type", "application/json")
        put_req.add_header("Accept", "application/vnd.github+json")
        with _ur.urlopen(put_req, timeout=30) as r:
            r.read()

    def _push_ado(self, artifact, file_path, cfg):
        """Push artifact to Azure DevOps Artifacts feed or repository."""
        import urllib.request as _ur
        import base64 as _b64
        pat = cfg.get("pat", "")
        org = cfg.get("org", "")
        project = cfg.get("project", "")
        feed = cfg.get("feed", "")
        mesh_project_id = cfg.get("_project_id", "")
        task_id = artifact.get("task_id") or "no-task"
        # Package name: <feed-package>/<project_id>/<task_id> — keeps outputs
        # organized by project and task inside the ADO feed.
        base_pkg = cfg.get("package", feed + "-artifacts")
        pkg_name = "-".join(filter(None,
                                   [base_pkg, mesh_project_id[:12], task_id[:12],
                                    artifact["name"].replace(" ", "-")]))
        if not pat or not org or not project or not feed:
            raise ValueError("ado target missing pat, org, project, or feed")
        credentials = _b64.b64encode(f":{pat}".encode()).decode()
        # Upload as a Universal Package (simplest ADO artifacts push)
        url = (f"https://pkgs.dev.azure.com/{org}/{project}/_apis/packaging/feeds/"
               f"{feed}/upack/packages/{pkg_name}/versions/{artifact['id'][:12]}"
               f"?api-version=7.0")
        with open(file_path, "rb") as f:
            data = f.read()
        req = _ur.Request(url, data=data, method="PUT")
        req.add_header("Authorization", f"Basic {credentials}")
        req.add_header("Content-Type", "application/octet-stream")
        with _ur.urlopen(req, timeout=60) as r:
            r.read()

    # -- storage helpers
    def verify_storage_target(self, storage):
        """Validate credentials for a storage target by doing a lightweight
        live check against the real API.  Raises ValueError with a human-readable
        message on failure so the UI can surface it before any data is moved."""
        import urllib.request as _ur
        stype = storage.get("type", "local")
        if stype == "github":
            token = storage.get("token", "")
            repo  = storage.get("repo", "")
            if not token or not repo:
                raise ValueError("GitHub token and repo are required")
            req = _ur.Request(
                f"https://api.github.com/repos/{repo}",
                method="GET")
            req.add_header("Authorization", f"token {token}")
            req.add_header("Accept", "application/vnd.github+json")
            try:
                with _ur.urlopen(req, timeout=10) as r:
                    data = json.loads(r.read().decode())
            except Exception as exc:
                raise ValueError(f"GitHub credential check failed: {exc}")
            if not data.get("full_name"):
                raise ValueError("GitHub: repo not found or token lacks access")
        elif stype == "ado":
            pat     = storage.get("pat", "")
            org     = storage.get("org", "")
            project = storage.get("project", "")
            feed    = storage.get("feed", "")
            if not all([pat, org, project, feed]):
                raise ValueError("ADO org, project, feed and PAT are all required")
            import base64 as _b64
            creds = _b64.b64encode(f":{pat}".encode()).decode()
            url = (f"https://feeds.dev.azure.com/{org}/{project}/_apis/packaging/"
                   f"feeds/{feed}?api-version=7.0")
            req = _ur.Request(url, method="GET")
            req.add_header("Authorization", f"Basic {creds}")
            try:
                with _ur.urlopen(req, timeout=10) as r:
                    data = json.loads(r.read().decode())
            except Exception as exc:
                raise ValueError(f"ADO credential check failed: {exc}")
            if not data.get("id"):
                raise ValueError("ADO: feed not found or PAT lacks access")
        # local needs no verification

    def migrate_artifacts_to_storage(self, project_id, new_storage, store):
        """Walk all artifacts belonging to project_id's tasks and push each one
        to new_storage.  Returns (pushed, skipped, errors) counts."""
        pushed = skipped = 0
        errors = []
        artifacts = [dict(a) for a in
                     store.list_artifacts_for_project(project_id)]
        new_cfg = {**new_storage, "_project_id": project_id}
        for art in artifacts:
            file_path = os.path.join(store.data_dir, "artifacts", art["id"])
            if not os.path.exists(file_path):
                skipped += 1
                continue
            try:
                stype = new_storage.get("type", "local")
                if stype == "github":
                    self._push_github(art, file_path, new_cfg)
                elif stype == "ado":
                    self._push_ado(art, file_path, new_cfg)
                else:
                    self._push_local_structured(art, file_path, store,
                                                project_id, new_storage)
                pushed += 1
            except Exception as exc:
                errors.append({"artifact": art["id"],
                               "name": art["name"], "error": str(exc)})
        return pushed, skipped, errors

    # -- permission checks (raise PermissionError / ValueError)
    def notify_planner(self, by_agent_id, project_id, project_name, task_id,
                       message_text):
        """Find the best available planner/orchestrator and send an A2A dispatch
        message. Used both by project creation and by ep_project_request so the
        logic stays in one place. No-ops silently if no planner is available."""
        planner = (self.store.best_agent_for_role("planner") or
                   self.store.best_agent_for_role("orchestrator"))
        if not planner:
            return
        mid = f"msg-{uuid.uuid4().hex}"
        self.store.add_message(
            mid, by_agent_id, planner["id"], "task.dispatch",
            {"project_id": project_id, "project_name": project_name,
             "task_id": task_id, "text": message_text})

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
            # Admin-gated: the admin token OR any registered agent key.
            # Agents are trusted principals — they operate on behalf of remote
            # admins and need to be able to post alerts / read chat history.
            if self.mesh.auth_is_admin(h):
                return {"id": "admin", "name": "admin", "role": "orchestrator",
                        "status": "online"}
            agent = self.mesh.auth_agent(h)
            if agent:
                return dict(agent)   # convert sqlite3.Row → plain dict
            raise PermissionError("admin token or admin-capable agent required")
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
        ("POST",   r"^/api/projects/(?P<id>[^/]+)/request$",   "ep_project_request"),
        ("POST",   r"^/api/projects/(?P<id>[^/]+)/migrate-storage$",
                                                              "ep_migrate_storage"),
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
        ("GET",    r"^/api/admin/chat$",                       "ep_chat_history"),
        ("POST",   r"^/api/admin/chat$",                       "ep_chat_post"),
        ("GET",    r"^/api/admin/chat/stream$",                "ep_chat_stream"),
        ("GET",    r"^/api/admin/stats$",                      "ep_admin_stats"),
        ("GET",    r"^/api/admin/artifact-targets$",           "ep_list_targets"),
        ("POST",   r"^/api/admin/artifact-targets$",           "ep_create_target"),
        ("PATCH",  r"^/api/admin/artifact-targets/(?P<id>[^/]+)$", "ep_update_target"),
        ("DELETE", r"^/api/admin/artifact-targets/(?P<id>[^/]+)$", "ep_delete_target"),
        # Node (guest) chat — available on any mode; /node-chat page only in guest mode
        ("GET",    r"^/api/node/ping$",                        "ep_node_ping"),
        ("GET",    r"^/api/node/chat$",                        "ep_node_chat_history"),
        ("POST",   r"^/api/node/chat$",                        "ep_node_chat_post"),
        ("GET",    r"^/api/node/chat/stream$",                 "ep_node_chat_stream"),
        ("GET",    r"^/node-chat$",                            "ep_node_chat_ui"),
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
        # Names are meant to be unique for easy identification.
        existing = self.store.get_agent_by_name(name)
        if existing:
            raise ValueError(
                f"agent name '{name}' is already taken by {existing['id']} — "
                f"choose a unique friendly name")
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
        # Friendly names are meant to be unique so agents are easy to identify.
        # Reusing one is almost always a mistake (e.g. installer defaulting to
        # the hostname), so reject it and tell the caller to pick another.
        existing = self.store.get_agent_by_name(name)
        if existing:
            raise ValueError(
                f"agent name '{name}' is already taken by {existing['id']} — "
                f"choose a unique friendly name (or ask admin to remove the old one)")
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

        # Enrich response: current role, active task, unread messages, and
        # orders summary so the agent knows exactly what it should be doing.
        active_task = self.store.q1(
            "SELECT id, title, kind, status, project_id, spec FROM tasks "
            "WHERE assigned_to=? AND status IN ('queued','claimed','in_progress') "
            "ORDER BY priority ASC, updated_at ASC LIMIT 1",
            (agent["id"],))
        unread = self.store.count_unread(agent["id"])

        orders = None
        if active_task:
            orders = {
                "task_id": active_task["id"],
                "title": active_task["title"],
                "kind": active_task["kind"],
                "status": active_task["status"],
                "project_id": active_task["project_id"],
                "spec": json.loads(active_task["spec"]) if active_task["spec"] else {},
                "instruction": (
                    f"You are a '{agent['role']}'. Your current assignment is: "
                    f"'{active_task['title']}' (kind={active_task['kind']}, "
                    f"status={active_task['status']}). Pull it via GET /api/work/pull "
                    f"and execute according to the spec."
                ),
            }
        elif pending == 0:
            orders = {
                "instruction": (
                    f"You are a '{agent['role']}'. No tasks are currently assigned "
                    f"to you. Check back periodically or listen for A2A messages."
                )
            }

        self._send_json({
            "ok": True,
            "ts": now(),
            "pending_tasks": pending,
            "role": agent["role"],
            "agent_id": agent["id"],
            "unread_messages": unread,
            "orders": orders,
        })

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
        self._send_json({"items": [self.mesh.project_pub(r, with_counts=True,
                                                          with_activity=True)
                                   for r in items]})

    @staticmethod
    def _validate_artifact_storage(raw):
        """Validate and normalise the artifact_storage dict from the request.
        Returns the storage dict ready for DB storage (with plaintext secrets).
        Raises ValueError for invalid input.
        """
        if not isinstance(raw, dict):
            raise ValueError("artifact_storage must be an object")
        stype = (raw.get("type") or "local").lower()
        if stype not in ("github", "ado", "local"):
            raise ValueError("artifact_storage.type must be github|ado|local")
        storage = {"type": stype}
        if stype == "github":
            repo = (raw.get("repo") or "").strip()
            token = (raw.get("token") or "").strip()
            if not repo:
                raise ValueError("artifact_storage.repo required for github")
            if not token:
                raise ValueError("artifact_storage.token required for github")
            storage["repo"] = repo
            storage["token"] = token
            storage["branch"] = (raw.get("branch") or "main").strip()
            storage["path"] = (raw.get("path") or "artifacts/").strip()
        elif stype == "ado":
            for f in ("org", "project", "feed", "pat"):
                v = (raw.get(f) or "").strip()
                if not v:
                    raise ValueError(f"artifact_storage.{f} required for ado")
                storage[f] = v
            if raw.get("package"):
                storage["package"] = raw["package"].strip()
        elif stype == "local":
            if raw.get("local_path"):
                storage["local_path"] = raw["local_path"].strip()
        return storage

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
        # Artifact storage: validated and stored with full credentials server-side
        raw_storage = body.get("artifact_storage") or {"type": "local"}
        artifact_storage = self._validate_artifact_storage(raw_storage)
        pid = "proj-" + uuid.uuid4().hex[:12]
        self.store.add_project(pid, name, description, context, agent["id"],
                               artifact_storage)
        self.store.add_event(agent["id"], "project.created", None,
                             {"project": pid, "name": name,
                              "storage_type": artifact_storage["type"]})

        # -- Auto-planning: find the best available planner or orchestrator
        # and create a "Plan project" task assigned to them so the swarm
        # immediately starts decomposing the work.
        planner = (self.store.best_agent_for_role("planner") or
                   self.store.best_agent_for_role("orchestrator"))
        if planner:
            plan_tid = "task-" + uuid.uuid4().hex[:12]
            plan_spec = {
                "project_name": name,
                "project_description": description,
                "project_context": context,
                "instructions": (
                    "Decompose this project into concrete tasks. For each task "
                    "include: title, kind (code/research/docs/ops/test/generic), "
                    "priority (1=urgent … 5=low), a clear spec, and the role "
                    "best suited to execute it (worker/qa/reviewer). "
                    "Submit your plan as the task result using output.tasks "
                    "(a list of task objects). The orchestrator will then "
                    "create and assign those tasks automatically."
                ),
            }
            self.store.add_task(plan_tid, f"Plan: {name}", "planning", plan_spec,
                                1, agent["id"], planner["id"], None,
                                {"max": 2, "backoff_s": 30}, pid)
            self.store.add_event(agent["id"], "project.plan_task_created", plan_tid,
                                 {"project": pid, "assigned_to": planner["id"]})
            # Notify the planner via A2A message
            self.mesh.notify_planner(
                agent["id"], pid, name, plan_tid,
                f"New project '{name}' created. You have been assigned "
                f"the planning task ({plan_tid}). Pull it and decompose "
                f"the project into actionable tasks."
            )

        self._send_json(self.mesh.project_pub(self.store.get_project(pid),
                                              with_counts=True), 201)

    def ep_get_project(self, g):
        self._auth()
        row = self.store.get_project(g["id"])
        if not row:
            self._err(404, "unknown project")
            return
        out = self.mesh.project_pub(row, with_counts=True, with_activity=True)
        tasks = self.store.list_tasks(project_id=row["id"], limit=200)
        out["task_items"] = [self.mesh.task_pub(t) for t in tasks]
        out["requests"] = [self.mesh.request_pub(r)
                           for r in self.store.list_project_requests(row["id"])]
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
        if "artifact_storage" in body and body["artifact_storage"]:
            raw_new = body["artifact_storage"]
            if not isinstance(raw_new, dict):
                raise ValueError("artifact_storage must be an object")
            # Merge: preserve stored secrets if UI sends "***"
            existing_raw = (json.loads(row["artifact_storage"])
                            if row["artifact_storage"] else {"type": "local"})
            merged = {**existing_raw}
            for k, v in raw_new.items():
                if v != "***":
                    merged[k] = v
            # Re-validate the merged result
            fields["artifact_storage"] = json.dumps(
                self._validate_artifact_storage(merged))
        if fields:
            self.store.update_project(row["id"], **fields)
            self.store.add_event(agent["id"], "project.updated", None,
                                 {"project": row["id"], **{
                                     k: v for k, v in fields.items()
                                     if k not in ("context", "artifact_storage")}})
        self._send_json(self.mesh.project_pub(self.store.get_project(row["id"]),
                                              with_counts=True))

    def ep_project_request(self, g):
        """POST /api/projects/{id}/request
        Admin submits a free-text request (feature / enhancement / bugfix).
        Body: {text: str, kind?: "feature"|"enhancement"|"bugfix"}
        Creates a task of that kind on the project, fires the planning agent
        if one is available, and records the request for audit.
        Returns: {request_id, task_id}
        """
        agent = self._auth(need_admin=True)
        row = self.store.get_project(g["id"])
        if not row:
            self._err(404, "unknown project")
            return
        body = self._json_body()
        text = (body.get("text") or "").strip()
        if not text:
            self._err(422, "text is required")
            return
        kind = (body.get("kind") or "feature").lower()
        if kind not in ("feature", "enhancement", "bugfix"):
            self._err(422, "kind must be feature|enhancement|bugfix")
            return

        # Build a task title from the request text (truncate if long)
        MAX = 80
        title = text if len(text) <= MAX else text[:MAX - 1] + "…"

        # Create the task on this project
        pid = row["id"]
        tid = "task-" + uuid.uuid4().hex[:12]
        self.store.add_task(
            tid, title, kind, {"request_text": text},
            2, agent["id"], None, None, {"max": 3, "backoff_s": 10}, pid)

        # Record the request for audit / display
        rid = "req-" + uuid.uuid4().hex[:12]
        self.store.add_project_request(rid, pid, text, kind,
                                       submitted_by=agent["id"], task_id=tid)

        # Notify a planner/orchestrator about the new task
        self.mesh.notify_planner(
            agent["id"], pid, row["name"], tid,
            f"Admin request ({kind}) added to project '{row['name']}': "
            f"{title}. Task {tid} is queued — pull and act on it."
        )

        self.store.add_event(agent["id"], "project.request", tid,
                             {"project_id": pid, "kind": kind,
                              "request_id": rid})
        self._send_json({"request_id": rid, "task_id": tid}, 201)

    def ep_migrate_storage(self, g):
        """POST /api/projects/{id}/migrate-storage
        Admin-only.  Validates credentials, copies all existing project
        artifacts to the new destination, then updates artifact_storage
        on the project.

        Body: {target: {type, ...credentials}}
          - type "github": repo, branch?, path?, token
          - type "ado":    org, project, feed, pat
          - type "local":  local_path?

        Response: {pushed, skipped, errors: [...], storage_type}
        """
        agent = self._auth(need_admin=True)
        row = self.store.get_project(g["id"])
        if not row:
            self._err(404, "unknown project")
            return
        body = self._json_body()
        target = body.get("target")
        if not isinstance(target, dict):
            self._err(422, "'target' object is required")
            return

        # Merge with existing stored secrets so callers can send "***" for
        # fields they don't want to replace (same pattern as ep_update_project)
        existing_raw = (json.loads(row["artifact_storage"])
                        if row["artifact_storage"] else {"type": "local"})
        merged = {**existing_raw}
        for k, v in target.items():
            if v != "***":
                merged[k] = v

        try:
            new_storage = self._validate_artifact_storage(merged)
        except ValueError as exc:
            self._err(422, str(exc))
            return

        # Credential verification — do a live API call before moving any data
        try:
            self.mesh.verify_storage_target(new_storage)
        except ValueError as exc:
            self._err(422, f"Credential check failed: {exc}")
            return

        # Copy existing artifacts to the new destination
        pushed, skipped, errors = self.mesh.migrate_artifacts_to_storage(
            row["id"], new_storage, self.store)

        # Only update the project's storage config if we could reach the target
        # (even partial success — some artifacts may not exist on disk)
        self.store.update_project(
            row["id"],
            artifact_storage=json.dumps(new_storage))
        self.store.add_event(
            agent.get("id", "admin"), "project.storage_migrated", None,
            {"project_id": row["id"],
             "new_type": new_storage["type"],
             "pushed": pushed, "skipped": skipped,
             "error_count": len(errors)})
        HUB.notify_change()
        self._send_json({
            "pushed":       pushed,
            "skipped":      skipped,
            "errors":       errors,
            "storage_type": new_storage["type"],
        })

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

        # -- Delegation: if this was a planning task that succeeded, auto-create
        # the downstream tasks described in output.tasks.
        if (final == "done" and row["kind"] == "planning"
                and row["project_id"] and status == "ok"):
            output = body.get("output") or {}
            task_plan = output.get("tasks") or []
            if isinstance(task_plan, list):
                self._delegate_planned_tasks(
                    agent, row["project_id"], task_plan)

        self._send_json(self.mesh.task_pub(self.store.get_task(row["id"])))

    # Role -> best agent assignment priority for auto-delegation
    _ROLE_PREF = {
        "code":     ["worker", "planner", "orchestrator"],
        "research": ["worker", "planner"],
        "docs":     ["worker", "planner"],
        "ops":      ["worker", "planner"],
        "test":     ["qa", "worker"],
        "qa":       ["qa", "worker"],
        "review":   ["reviewer", "qa", "orchestrator"],
        "generic":  ["worker", "planner"],
        "planning": ["planner", "orchestrator"],
    }

    def _delegate_planned_tasks(self, creator_agent, project_id, task_plan):
        """Auto-create tasks from a planner's output.tasks list.

        Each item in task_plan may contain: title, kind, priority, spec,
        role (preferred role for assignee), assigned_to (explicit agent id).
        Creates tasks assigned to the best available agent for each role.
        """
        orch = (self.store.best_agent_for_role("orchestrator") or
                self.store.best_agent_for_role("planner"))
        creator_id = creator_agent["id"] if creator_agent else (
            orch["id"] if orch else None)

        for item in task_plan:
            if not isinstance(item, dict):
                continue
            title = (item.get("title") or "").strip()
            if not title:
                continue
            kind = item.get("kind") or "generic"
            priority = int(item.get("priority") or 3)
            spec = item.get("spec") or {}
            preferred_role = item.get("role") or kind

            # Explicit assignee overrides auto-assignment
            assigned_to = item.get("assigned_to")
            if not assigned_to:
                roles_to_try = self._ROLE_PREF.get(preferred_role,
                                                    ["worker", "planner"])
                for role in roles_to_try:
                    candidate = self.store.best_agent_for_role(role)
                    if candidate:
                        assigned_to = candidate["id"]
                        break

            tid = "task-" + uuid.uuid4().hex[:12]
            self.store.add_task(tid, title, kind, spec, priority,
                                creator_id, assigned_to, None,
                                {"max": 3, "backoff_s": 10}, project_id)
            self.store.add_event(creator_id or "system", "task.delegated", tid,
                                 {"project": project_id, "kind": kind,
                                  "assigned_to": assigned_to})
            # Notify the assigned agent
            if assigned_to:
                mid = f"msg-{uuid.uuid4().hex}"
                self.store.add_message(
                    mid, creator_id or "system", assigned_to, "task.dispatch",
                    {"project_id": project_id, "task_id": tid,
                     "text": (f"Task assigned: '{title}' ({kind}, prio {priority}). "
                              f"Pull it when ready.")})

        self.store.add_event(creator_id or "system", "project.tasks_delegated",
                             None, {"project": project_id,
                                    "count": len(task_plan)})

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
        # Resolve project_id from task so the push goes to the right destination
        project_id = None
        if task_id:
            trow2 = self.store.get_task(task_id)
            if trow2:
                project_id = trow2["project_id"]
        self.store.add_event(agent["id"], "artifact.uploaded", task_id,
                             {"artifact": aid, "name": name,
                              "size": len(file_data), "project": project_id})
        art_row = self.store.get_artifact(aid)
        # Push to project-scoped storage destination asynchronously
        self.mesh.push_artifact_to_targets(art_row, dest, self.store,
                                           project_id=project_id)
        self._send_json(self.mesh.artifact_pub(art_row),
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

    # -- artifact targets (external push destinations) --
    def ep_list_targets(self, g):
        self._auth(need_admin=True)
        items = [self.mesh.artifact_target_pub(r)
                 for r in self.store.list_artifact_targets()]
        self._send_json({"items": items})

    def ep_create_target(self, g):
        self._auth(need_admin=True)
        body = self._json_body()
        name = (body.get("name") or "").strip()
        if not name:
            raise ValueError("name required")
        atype = (body.get("type") or "").strip().lower()
        if atype not in ("github", "ado", "local"):
            raise ValueError("type must be github|ado|local")
        config = body.get("config") or {}
        if not isinstance(config, dict):
            raise ValueError("config must be an object")
        tid = "tgt-" + uuid.uuid4().hex[:10]
        self.store.add_artifact_target(tid, name, atype, config)
        self.store.add_event("admin", "artifact_target.created", None,
                             {"target": tid, "name": name, "type": atype})
        self._send_json(
            self.mesh.artifact_target_pub(self.store.get_artifact_target(tid)),
            201)

    def ep_update_target(self, g):
        self._auth(need_admin=True)
        tid = g["id"]
        row = self.store.get_artifact_target(tid)
        if not row:
            self._err(404, "unknown artifact target")
            return
        body = self._json_body()
        fields = {}
        if "name" in body and body["name"]:
            fields["name"] = str(body["name"]).strip()
        if "type" in body:
            if body["type"] not in ("github", "ado", "local"):
                raise ValueError("type must be github|ado|local")
            fields["type"] = body["type"]
        if "config" in body:
            if not isinstance(body["config"], dict):
                raise ValueError("config must be an object")
            # Merge: preserve existing secrets if new config sends "***"
            existing = json.loads(row["config"]) if row["config"] else {}
            merged = {**existing}
            for k, v in body["config"].items():
                if v != "***":
                    merged[k] = v
            fields["config"] = json.dumps(merged)
        if "enabled" in body:
            fields["enabled"] = 1 if body["enabled"] else 0
        if fields:
            self.store.update_artifact_target(tid, **fields)
            self.store.add_event("admin", "artifact_target.updated", None,
                                 {"target": tid})
        self._send_json(
            self.mesh.artifact_target_pub(self.store.get_artifact_target(tid)))

    def ep_delete_target(self, g):
        self._auth(need_admin=True)
        tid = g["id"]
        if not self.store.get_artifact_target(tid):
            self._err(404, "unknown artifact target")
            return
        self.store.delete_artifact_target(tid)
        self.store.add_event("admin", "artifact_target.deleted", None,
                             {"target": tid})
        self._send_json({"ok": True})

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

    # ---- admin chat --------------------------------------------------------

    def ep_chat_history(self, g):
        """GET /api/admin/chat — return recent chat messages, newest last."""
        self._auth(need_admin=True)
        q = parse_qs(urlparse(self.path).query)
        limit = min(int((q.get("limit") or ["100"])[0]), 500)
        rows = self.store.list_chat_messages(limit)
        msgs = [{"id": r["id"], "sender": r["sender"],
                 "sender_name": r["sender_name"],
                 "text": r["text"], "ts": r["ts"]}
                for r in reversed(rows)]   # oldest first for display
        self._send_json({"messages": msgs})

    def ep_chat_post(self, g):
        """POST /api/admin/chat — broadcast a chat message.
        Body: {text: str, sender_name?: str}
        Admin token or admin-cap agent. Any connected agent may also post if it
        has the admin cap (remote admin agents can send alerts to the master).
        """
        agent = self._auth(need_admin=True)
        body = self._json_body()
        text = (body.get("text") or "").strip()
        if not text:
            self._err(422, "text is required")
            return
        if len(text) > 2000:
            self._err(422, "text too long (max 2000 chars)")
            return
        # Determine display name: prefer body override, then agent name, then "admin"
        sender_name = (body.get("sender_name") or "").strip()
        if not sender_name:
            sender_name = agent.get("name") or "admin"
        sender = agent.get("id") or "admin"

        msg = self.store.add_chat_message(sender, sender_name, text)
        CHAT_HUB.broadcast(msg)   # push to all live chat SSE listeners
        self._send_json(msg, 201)

    def ep_chat_stream(self, g):
        """GET /api/admin/chat/stream — SSE stream of live chat messages.
        Auth: Bearer header OR ?token= query param (EventSource limitation).
        Each event: event:message  data:<json>
        """
        q = parse_qs(urlparse(self.path).query)
        h = self.headers.get("Authorization", "")
        if not h:
            tok = (q.get("token") or [""])[0]
            if tok:
                h = "Bearer " + tok
        self._auth_from(h, need_admin=True)   # raises 403 if not admin

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        sub = CHAT_HUB.subscribe()
        try:
            self.wfile.write(b"event: hello\ndata: {}\n\n")
            self.wfile.flush()
            last_ka = time.monotonic()
            while True:
                # Wait up to 1s for a new message; cv.wait releases the lock
                with sub["cv"]:
                    sub["cv"].wait(timeout=1.0)
                    # Drain while still holding the lock (safe deque access)
                    pending = []
                    while sub["q"]:
                        pending.append(sub["q"].popleft())
                # Write outside the lock so broadcast is never blocked by I/O
                for data in pending:
                    self.wfile.write(
                        f"event: message\ndata: {data}\n\n".encode())
                if pending:
                    self.wfile.flush()
                # Keepalive every 15s
                if time.monotonic() - last_ka >= 15:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    last_ka = time.monotonic()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            CHAT_HUB.unsubscribe(sub)

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

    # ---- node (guest) chat endpoints ----------------------------------------
    # These use a plain mesh_ agent key (no admin-cap required).
    # They mirror the admin chat channel so guest node operators can read and
    # post to the same broadcast feed visible in the master admin console.

    def ep_node_ping(self, g):
        """GET /api/node/ping — public no-auth ping so the guest chat page can
        verify connectivity and discover the server mode."""
        mode = getattr(Handler, "node_mode", "master")
        self._send_json({"ok": True, "version": VERSION, "mode": mode})

    def ep_node_chat_history(self, g):
        """GET /api/node/chat — chat history readable with any valid mesh_ key."""
        self._auth()   # any registered agent key; no admin-cap needed
        q = parse_qs(urlparse(self.path).query)
        limit = min(int((q.get("limit") or ["100"])[0]), 500)
        rows = self.store.list_chat_messages(limit)
        msgs = [{"id": r["id"], "sender": r["sender"],
                 "sender_name": r["sender_name"],
                 "text": r["text"], "ts": r["ts"]}
                for r in reversed(rows)]
        self._send_json({"messages": msgs})

    def ep_node_chat_post(self, g):
        """POST /api/node/chat — post to the shared broadcast channel with any
        valid mesh_ key.  Body: {text: str, sender_name?: str}"""
        agent = self._auth()   # any registered agent key
        body = self._json_body()
        text = (body.get("text") or "").strip()
        if not text:
            self._err(422, "text is required")
            return
        if len(text) > 2000:
            self._err(422, "text too long (max 2000 chars)")
            return
        sender_name = (body.get("sender_name") or "").strip() or \
                      agent["name"] or "agent"
        sender = agent["id"] or "agent"
        msg = self.store.add_chat_message(sender, sender_name, text)
        CHAT_HUB.broadcast(msg)
        self._send_json(msg, 201)

    def ep_node_chat_stream(self, g):
        """GET /api/node/chat/stream — SSE live chat for guest agents.
        Auth: Bearer header OR ?token= (EventSource limitation)."""
        q = parse_qs(urlparse(self.path).query)
        h = self.headers.get("Authorization", "")
        if not h:
            tok = (q.get("token") or [""])[0]
            if tok:
                h = "Bearer " + tok
        self._auth_from(h)   # any valid agent key

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache, no-transform")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        sub = CHAT_HUB.subscribe()
        try:
            self.wfile.write(b"event: hello\ndata: {}\n\n")
            self.wfile.flush()
            last_ka = time.monotonic()
            while True:
                with sub["cv"]:
                    sub["cv"].wait(timeout=1.0)
                    pending = []
                    while sub["q"]:
                        pending.append(sub["q"].popleft())
                for data in pending:
                    self.wfile.write(
                        f"event: message\ndata: {data}\n\n".encode())
                if pending:
                    self.wfile.flush()
                if time.monotonic() - last_ka >= 15:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    last_ka = time.monotonic()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            CHAT_HUB.unsubscribe(sub)

    def ep_node_chat_ui(self, g):
        """GET /node-chat — standalone guest chat page.
        Only served when --mode guest; returns 404 on master nodes."""
        mode = getattr(Handler, "node_mode", "master")
        if mode != "guest":
            self._err(404, "not found")
            return
        base = getattr(Handler, "base_path", "") or ""
        # Extract the logo base64 from UI_HTML so we don't duplicate 21KB
        logo_b64 = getattr(Handler, "_logo_b64", "")
        html = (NODE_CHAT_HTML
                .replace("__VERSION__", VERSION)
                .replace("__BASE__", base)
                .replace("__LOGO_B64__", logo_b64))
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---- web UI (master only — hidden on guest nodes)
    def ep_ui(self, g):
        if getattr(Handler, "node_mode", "master") == "guest":
            # Guest nodes expose /node-chat, not the admin console
            self._err(404, "admin console not available on guest nodes — "
                          "see /node-chat for the guest chat interface")
            return
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


# Guest node chat — standalone page served at /node-chat (--mode guest only).
# Uses any valid mesh_ agent key; no admin token required.
# Key auto-detected from localStorage("mesh_node_key") or ?key= URL param.
NODE_CHAT_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>agent-mesh node chat v__VERSION__</title>
<style>
:root{
  --canvas:#09090b;
  --surf:#111113;
  --surf2:#18181b;
  --surf3:#1f1f23;
  --bd:#27272a;
  --bd2:#3f3f46;
  --fg:#f4f4f5;
  --fg2:#a1a1aa;
  --fg3:#71717a;
  --acc:#3b82f6;
  --acc-lo:rgba(59,130,246,.12);
  --bad:#ef4444;
  --ok:#22c55e;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--canvas);color:var(--fg);font:14px/1.5 system-ui,sans-serif;
     display:flex;flex-direction:column;height:100vh;overflow:hidden}

/* ── login screen ─────────────────────────────────────────── */
#login-screen{
  flex:1;display:flex;align-items:center;justify-content:center;padding:24px
}
.login-card{
  background:var(--surf);border:1px solid var(--bd);border-radius:12px;
  padding:36px 32px;width:100%;max-width:420px;display:flex;flex-direction:column;gap:16px
}
.login-card .brand{text-align:center;padding-bottom:8px}
.login-card .brand img{width:130px;display:block;margin:0 auto 10px}
.login-card .brand h2{font-size:20px;font-weight:700;font-style:italic;
  color:var(--fg);letter-spacing:-.3px}
.login-card .brand p{font-size:12px;color:var(--fg3);margin-top:3px}
.login-card label{font-size:12px;color:var(--fg2);font-weight:500;
  display:block;margin-bottom:4px}
.login-card input{
  width:100%;padding:9px 11px;background:var(--surf2);border:1px solid var(--bd2);
  border-radius:7px;color:var(--fg);font:inherit;outline:none
}
.login-card input:focus{border-color:var(--acc);
  box-shadow:0 0 0 3px rgba(59,130,246,.15)}
.login-card input::placeholder{color:var(--fg3)}
.login-err{font-size:12px;color:var(--bad);min-height:16px;text-align:center}
.login-card button{
  width:100%;padding:10px;background:var(--acc);color:#fff;border:none;
  border-radius:7px;font:600 14px/1 inherit;cursor:pointer;margin-top:4px
}
.login-card button:hover{background:#2563eb}
.login-card button:disabled{opacity:.5;cursor:default}
.login-card .hint{font-size:11px;color:var(--fg3);text-align:center;line-height:1.6}
.login-card .hint code{font-family:monospace;background:var(--surf3);
  padding:1px 5px;border-radius:3px;font-size:11px}

/* ── chat screen ──────────────────────────────────────────── */
#chat-screen{flex:1;display:none;flex-direction:column;overflow:hidden}
#chat-header{
  display:flex;align-items:center;gap:12px;padding:12px 20px;
  background:var(--surf);border-bottom:1px solid var(--bd);flex-shrink:0
}
#chat-header img{width:64px}
#chat-header .info{flex:1}
#chat-header .info h2{font-size:15px;font-weight:600;font-style:italic}
#chat-header .info p{font-size:11px;color:var(--fg3);margin-top:2px}
#conn-dot{width:8px;height:8px;border-radius:50%;background:#cbd5e1;flex-shrink:0}
#logout-btn{
  padding:5px 12px;background:var(--surf2);border:1px solid var(--bd2);
  border-radius:6px;color:var(--fg2);font:inherit;font-size:12px;cursor:pointer
}
#logout-btn:hover{background:var(--surf3)}
#chat-msgs{
  flex:1;overflow-y:auto;display:flex;flex-direction:column;
  gap:8px;padding:16px 20px;background:var(--canvas)
}
.msg-wrap{display:flex;flex-direction:column;gap:3px;max-width:68%}
.msg-wrap.me{align-self:flex-end;align-items:flex-end}
.msg-wrap.them{align-self:flex-start;align-items:flex-start}
.msg-bubble{
  padding:9px 13px;border-radius:14px;font-size:14px;line-height:1.5;
  word-break:break-word;white-space:pre-wrap;border:1px solid var(--bd)
}
.msg-wrap.me   .msg-bubble{background:var(--acc);color:#fff;
  border-color:var(--acc);border-bottom-right-radius:4px}
.msg-wrap.them .msg-bubble{background:var(--surf);
  border-bottom-left-radius:4px}
.msg-meta{font-size:11px;color:var(--fg3);padding:0 2px}
.msg-sys{align-self:center;font-size:11px;color:var(--fg3);
  font-style:italic;padding:4px 0}
#chat-footer{
  display:flex;gap:10px;align-items:flex-end;padding:12px 20px;
  background:var(--surf);border-top:1px solid var(--bd);flex-shrink:0
}
#chat-inp{
  flex:1;padding:9px 11px;background:var(--surf2);border:1px solid var(--bd2);
  border-radius:7px;color:var(--fg);font:inherit;font-size:14px;outline:none;
  resize:none;min-height:40px;max-height:120px;line-height:1.5
}
#chat-inp:focus{border-color:var(--acc);box-shadow:0 0 0 3px rgba(59,130,246,.15)}
#chat-inp::placeholder{color:var(--fg3)}
#send-btn{
  padding:9px 18px;background:var(--acc);color:#fff;border:none;
  border-radius:7px;font:600 14px/1 inherit;cursor:pointer;flex-shrink:0
}
#send-btn:hover{background:#2563eb}
#send-btn:disabled{opacity:.45;cursor:default}
</style>
</head><body>

<!-- ── LOGIN SCREEN ─────────────────────────────────────── -->
<div id="login-screen">
<div class="login-card">
  <div class="brand">
    <img id="login-logo" src="" alt="agent-mesh">
    <h2>agent-mesh</h2>
    <p id="login-sub">node chat · v__VERSION__ · Sigmaz Technologies</p>
  </div>
  <div>
    <label for="key-inp">Master URL</label>
    <input id="url-inp" type="url" placeholder="https://your-master-server/agent-mesh" autocomplete="off">
  </div>
  <div>
    <label for="key-inp">Agent API Key</label>
    <input id="key-inp" type="password" placeholder="mesh_…" autocomplete="off"
           oninput="clearErr()" onkeydown="if(event.key==='Enter')connect()">
  </div>
  <div id="login-err" class="login-err"></div>
  <button id="conn-btn" onclick="connect()">Connect</button>
  <p class="hint">
    Your key is in <code>~/.config/agent-mesh/config.json</code><br>
    or ask your admin to register you and share the key.
  </p>
</div>
</div>

<!-- ── CHAT SCREEN ─────────────────────────────────────── -->
<div id="chat-screen">
  <div id="chat-header">
    <img id="chat-logo" src="" alt="">
    <div class="info">
      <h2 id="agent-name">agent-mesh</h2>
      <p id="agent-info">connecting…</p>
    </div>
    <div id="conn-dot" title="stream status"></div>
    <button id="logout-btn" onclick="logout()">Disconnect</button>
  </div>
  <div id="chat-msgs"></div>
  <div id="chat-footer">
    <textarea id="chat-inp" rows="1" placeholder="Message all nodes… (Enter to send, Ctrl+Enter for new line)"
      oninput="autosize(this)" onkeydown="keydown(event)"></textarea>
    <button id="send-btn" onclick="send()">Send</button>
  </div>
</div>

<script>
(function(){
// ── logo (same base64 transparent PNG used in the admin console)
const LOGO_SRC = "data:image/png;base64,__LOGO_B64__";
document.getElementById("login-logo").src = LOGO_SRC;
document.getElementById("chat-logo").src  = LOGO_SRC;

const BASE_KEY    = "mesh_node_base";
const AGENT_KEY   = "mesh_node_key";
const NAME_KEY    = "mesh_node_name";
let _base = "", _key = "", _agentName = "", _es = null, _myId = null;

// ── auto-fill from localStorage / URL param
(function init(){
  const p = new URLSearchParams(location.search);
  const storedBase = localStorage.getItem(BASE_KEY) || "";
  const storedKey  = localStorage.getItem(AGENT_KEY) || "";
  const storedName = localStorage.getItem(NAME_KEY)  || "";
  const urlKey     = p.get("key") || "";
  const urlBase    = p.get("base") || "";

  document.getElementById("url-inp").value = urlBase || storedBase;
  document.getElementById("key-inp").value = urlKey  || storedKey;
  if(storedName) document.getElementById("agent-name").textContent = storedName;

  // If both URL params present, auto-connect
  if((urlKey || storedKey) && (urlBase || storedBase)){
    // slight delay so the page paints first
    setTimeout(connect, 80);
  }
})();

function clearErr(){ document.getElementById("login-err").textContent = ""; }
function showErr(m){ document.getElementById("login-err").textContent = m; }
function esc(s){ const d=document.createElement("div");d.textContent=s;return d.innerHTML; }
function scrollBottom(){ const b=document.getElementById("chat-msgs");if(b)b.scrollTop=b.scrollHeight; }
function ts(unix){ return new Date(unix*1000).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"}); }

async function connect(){
  const btn = document.getElementById("conn-btn");
  btn.disabled = true;
  clearErr();

  _base = (document.getElementById("url-inp").value || "").trim().replace(/\/+$/,"");
  _key  = (document.getElementById("key-inp").value || "").trim();

  if(!_base){ showErr("Enter the master server URL."); btn.disabled=false; return; }
  if(!_key || !_key.startsWith("mesh_")){
    showErr("Enter a valid agent key (starts with mesh_).");
    btn.disabled=false; return;
  }

  // Verify key by calling /api/agents/me
  try{
    const me = await apiFetch("GET", "/api/agents/me");
    _agentName = me.name || "agent";
    _myId      = me.id   || "";
    localStorage.setItem(BASE_KEY, _base);
    localStorage.setItem(AGENT_KEY, _key);
    localStorage.setItem(NAME_KEY, _agentName);
    showChatScreen(me);
    loadHistory();
    openStream();
  } catch(e){
    showErr(e.message || "Connection failed. Check URL and key.");
    btn.disabled = false;
  }
}

function showChatScreen(me){
  document.getElementById("login-screen").style.display = "none";
  const cs = document.getElementById("chat-screen");
  cs.style.display = "flex";
  document.getElementById("agent-name").textContent = me.name || "agent";
  document.getElementById("agent-info").textContent =
    "role: " + (me.role||"?") + " · " + _base;
  document.getElementById("chat-inp").focus();
}

async function loadHistory(){
  try{
    const d = await apiFetch("GET", "/api/node/chat?limit=200");
    const box = document.getElementById("chat-msgs");
    box.innerHTML = "";
    (d.messages||[]).forEach(appendMsg);
    scrollBottom();
  } catch(e){ appendSys("Could not load history: "+e.message); }
}

function openStream(){
  if(_es){ try{_es.close()}catch{} _es=null; }
  const dot = document.getElementById("conn-dot");
  _es = new EventSource(_base + "/api/node/chat/stream?token=" + encodeURIComponent(_key));
  _es.addEventListener("hello", ()=>{ dot.style.background="var(--ok)"; });
  _es.addEventListener("message", e=>{
    try{ appendMsg(JSON.parse(e.data)); scrollBottom(); }catch{}
  });
  _es.onerror = ()=>{
    dot.style.background = "var(--bad)";
    try{_es.close()}catch{} _es=null;
    // Reconnect if still on the chat screen
    if(document.getElementById("chat-screen").style.display !== "none")
      setTimeout(openStream, 3000);
  };
}

function appendMsg(m){
  const box = document.getElementById("chat-msgs");
  if(!box) return;
  if(document.querySelector('[data-mid="'+m.id+'"]')) return; // dedupe
  const isMe = (m.sender === _myId);
  const wrap = document.createElement("div");
  wrap.className = "msg-wrap " + (isMe ? "me" : "them");
  wrap.setAttribute("data-mid", m.id);
  wrap.innerHTML =
    '<div class="msg-bubble">'+esc(m.text)+'</div>' +
    '<div class="msg-meta">'+esc(m.sender_name)+' · '+ts(m.ts)+'</div>';
  box.appendChild(wrap);
}

function appendSys(t){
  const box = document.getElementById("chat-msgs");
  if(!box) return;
  const d = document.createElement("div");
  d.className = "msg-sys";
  d.textContent = t;
  box.appendChild(d);
}

async function send(){
  const inp = document.getElementById("chat-inp");
  const btn = document.getElementById("send-btn");
  if(!inp) return;
  const text = inp.value.trim();
  if(!text) return;
  inp.value = ""; autosize(inp);
  btn.disabled = true;
  try{
    const msg = await apiFetch("POST", "/api/node/chat", {text});
    if(msg && msg.id){ appendMsg(msg); scrollBottom(); }
  } catch(e){ appendSys("Send failed: "+e.message); }
  btn.disabled = false;
  inp.focus();
}

function keydown(e){
  if(e.key==="Enter" && !e.ctrlKey && !e.metaKey && !e.shiftKey){
    e.preventDefault(); send();
  }
}
function autosize(el){ el.style.height="auto"; el.style.height=Math.min(el.scrollHeight,120)+"px"; }

function logout(){
  if(_es){ try{_es.close()}catch{} _es=null; }
  localStorage.removeItem(AGENT_KEY);
  localStorage.removeItem(BASE_KEY);
  localStorage.removeItem(NAME_KEY);
  document.getElementById("chat-screen").style.display = "none";
  document.getElementById("login-screen").style.display = "flex";
  document.getElementById("key-inp").value = "";
  document.getElementById("url-inp").value = "";
  document.getElementById("chat-msgs").innerHTML = "";
  document.getElementById("conn-dot").style.background = "#cbd5e1";
}

async function apiFetch(method, path, body){
  const url = _base + path;
  const opts = {
    method,
    headers: { "Authorization": "Bearer " + _key }
  };
  if(body !== undefined){
    opts.body = JSON.stringify(body);
    opts.headers["Content-Type"] = "application/json";
  }
  const r = await fetch(url, opts);
  let d;
  try{ d = await r.json(); } catch{ d = {}; }
  if(!r.ok) throw new Error(d.detail || ("HTTP "+r.status));
  return d;
}
})();
</script>
</body></html>"""

# ---------------------------------------------------------------- UI
UI_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>agent-mesh console v__VERSION__</title>
<style>
/* ─── Design tokens (21st.dev dark system) ─────────────────────────── */
:root{
  --canvas:#09090b;    /* outermost shell */
  --surf:#111113;      /* card / panel surface */
  --surf2:#18181b;     /* raised card / input bg */
  --surf3:#1f1f23;     /* hover states / selected rows */
  --bd:#27272a;        /* structural borders */
  --bd2:#3f3f46;       /* stronger border (inputs, focus rings) */
  --fg:#f4f4f5;        /* primary text */
  --fg2:#a1a1aa;       /* secondary / muted text */
  --fg3:#71717a;       /* placeholder / disabled */
  --acc:#3b82f6;       /* primary action — blue */
  --acc-h:#2563eb;     /* hover variant */
  --acc-lo:#1d3461;    /* tinted bg for badges/accents */
  --ok:#22c55e;        --ok-lo:#14301e;
  --warn:#f59e0b;      --warn-lo:#2d200a;
  --bad:#ef4444;       --bad-lo:#2d1010;
  --purple:#a855f7;    --purple-lo:#2a1540;
}

/* ─── Reset & base ──────────────────────────────────────────────────── */
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--canvas)}
body{
  font:13px/1.6 -apple-system,BlinkMacSystemFont,"Inter","Segoe UI",sans-serif;
  background:var(--canvas);color:var(--fg);
}
/* #app is the full-page flex container */
#app{display:flex;min-height:100vh}

/* ─── Sidebar layout ────────────────────────────────────────────────── */
#sidebar{
  width:220px;flex-shrink:0;
  background:var(--surf);border-right:1px solid var(--bd);
  display:flex;flex-direction:column;
  /* sticky within the flex row so it stays while content scrolls */
  position:sticky;top:0;height:100vh;overflow-y:auto;
  align-self:flex-start;
}
#main-content{
  flex:1;min-width:0;
  padding:28px 28px 48px;
  min-height:100vh;
}
.brand{padding:20px 16px 14px;border-bottom:1px solid var(--bd);margin-bottom:8px}
.brand h1{font-size:16px;font-weight:700;font-style:italic;color:var(--fg);letter-spacing:-.01em}
.brand .v{display:block;font-size:10px;color:var(--fg3);margin-top:3px;font-weight:400;letter-spacing:.01em}
.brand .co{color:var(--fg3);font-size:10px;font-weight:500}
nav{display:flex;flex-direction:column;gap:2px;padding:0 8px}
.nav-section{font-size:10px;font-weight:600;color:var(--fg3);
  text-transform:uppercase;letter-spacing:.08em;padding:12px 8px 4px}
nav button{
  font:inherit;border:none;background:none;color:var(--fg2);
  border-radius:8px;padding:8px 10px;cursor:pointer;font-weight:500;
  text-align:left;width:100%;display:flex;align-items:center;gap:8px;
  font-size:13px;transition:background .12s,color .12s;
}
nav button:hover{background:var(--surf3);color:var(--fg)}
nav button.active{background:var(--acc-lo);color:var(--acc);font-weight:600}
nav button .nav-icon{font-size:14px;opacity:.8;width:18px;text-align:center;flex-shrink:0}
.sidebar-footer{margin-top:auto;padding:12px 8px;border-top:1px solid var(--bd);display:flex;flex-direction:column;gap:4px}
.sidebar-footer button{font-size:12px;padding:7px 10px;color:var(--fg3)}
.sidebar-footer button:hover{color:var(--fg);background:var(--surf3)}

/* ─── Page titles ───────────────────────────────────────────────────── */
h1{font-size:22px;font-weight:700;color:var(--fg);letter-spacing:-.02em;margin-bottom:4px}
h2{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;
  color:var(--fg3);margin-bottom:12px}
.page-title{margin-bottom:20px}
.page-title .sub{font-size:13px;color:var(--fg2);margin-top:4px}
.sub{color:var(--fg2);font-size:13px}

/* ─── Cards ─────────────────────────────────────────────────────────── */
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:16px;margin-bottom:16px}
.card{
  background:var(--surf);border:1px solid var(--bd);border-radius:12px;
  padding:20px;transition:border-color .15s;
}
.card:hover{border-color:var(--bd2)}

/* ─── Stat cards (dashboard) ────────────────────────────────────────── */
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:20px}
.stat-card{background:var(--surf);border:1px solid var(--bd);border-radius:12px;
  padding:16px 18px;transition:border-color .15s}
.stat-card:hover{border-color:var(--bd2)}
.stat{font-size:28px;font-weight:700;color:var(--fg);letter-spacing:-.02em;line-height:1}
.statlabel{font-size:11px;color:var(--fg3);margin-top:6px;font-weight:500;text-transform:uppercase;letter-spacing:.04em}
/* legacy .stats flex — keep working for pages that use it */
div.stats:not(.stats-grid){display:flex;gap:24px;flex-wrap:wrap;margin-bottom:16px}

/* ─── Tables ─────────────────────────────────────────────────────────── */
table{width:100%;border-collapse:collapse;font-size:13px}
thead tr{border-bottom:1px solid var(--bd)}
th{text-align:left;color:var(--fg3);font-weight:500;font-size:11px;
  text-transform:uppercase;letter-spacing:.05em;padding:8px 10px}
td{padding:10px 10px;border-bottom:1px solid var(--bd);vertical-align:top;color:var(--fg2)}
td:first-child{color:var(--fg)}
tr:last-child td{border-bottom:none}
tr.clickable{cursor:pointer;transition:background .1s}
tr.clickable:hover{background:var(--surf3)}
tr.clickable:hover td{color:var(--fg)}

/* ─── Status & role pills ───────────────────────────────────────────── */
.pill{display:inline-flex;align-items:center;padding:2px 9px;border-radius:999px;
  font-size:11px;font-weight:600;white-space:nowrap;letter-spacing:.01em}
.p-online{background:#14301e;color:#4ade80}
.p-offline{background:var(--surf2);color:var(--fg3)}
.p-disabled{background:var(--bad-lo);color:#f87171}
.s-queued{background:var(--warn-lo);color:#fbbf24}
.s-in_progress,.s-claimed{background:var(--acc-lo);color:#60a5fa}
.s-done,.s-approved{background:var(--ok-lo);color:#4ade80}
.s-failed,.s-rejected{background:var(--bad-lo);color:#f87171}
.s-cancelled{background:var(--surf2);color:var(--fg3)}
.s-planning{background:var(--purple-lo);color:#c084fc}

/* ─── Buttons ───────────────────────────────────────────────────────── */
button{
  font:inherit;border:1px solid var(--bd2);background:var(--surf2);color:var(--fg2);
  border-radius:8px;padding:6px 14px;cursor:pointer;font-weight:500;font-size:13px;
  transition:background .12s,border-color .12s,color .12s;white-space:nowrap;
}
button:hover{background:var(--surf3);border-color:var(--bd2);color:var(--fg)}
button:active{background:var(--surf3)}
button.primary{background:var(--acc);border-color:var(--acc);color:#fff;font-weight:600}
button.primary:hover{background:var(--acc-h);border-color:var(--acc-h);color:#fff}
button.danger{border-color:var(--bd2);color:var(--bad)}
button.danger:hover{border-color:var(--bad);background:var(--bad-lo);color:#f87171}
button.sm{padding:3px 10px;font-size:12px;border-radius:6px}
button:disabled{opacity:.4;cursor:not-allowed}

/* ─── Inputs ────────────────────────────────────────────────────────── */
input,select,textarea{
  font:inherit;padding:7px 10px;
  border:1px solid var(--bd2);border-radius:8px;
  background:var(--surf2);color:var(--fg);
  width:100%;transition:border-color .12s,box-shadow .12s;
  -webkit-appearance:none;
}
input::placeholder,textarea::placeholder{color:var(--fg3)}
input:focus,select:focus,textarea:focus{
  outline:none;border-color:var(--acc);
  box-shadow:0 0 0 3px rgba(59,130,246,.2);
}
select option{background:var(--surf2);color:var(--fg)}
textarea{resize:vertical;min-height:64px;
  font-family:ui-monospace,Menlo,"Cascadia Code",monospace;font-size:12px;line-height:1.5}

/* ─── Layout helpers ────────────────────────────────────────────────── */
.row{display:flex;gap:8px;align-items:center;margin-bottom:10px;flex-wrap:wrap}
.row .grow{flex:1;min-width:160px}
.field-lbl{font-size:12px;color:var(--fg3);white-space:nowrap;min-width:120px;
  padding-right:8px;display:flex;align-items:center;gap:4px}
.mono{font-family:ui-monospace,Menlo,"Cascadia Code",monospace;font-size:12px;color:var(--fg2)}
.key{background:var(--surf2);border:1px solid var(--bd);padding:1px 7px;border-radius:6px;
  font-family:ui-monospace,monospace;font-size:12px;color:var(--fg2)}
.ev{font-size:12px;color:var(--fg3);line-height:1.5}
.ev b{color:var(--fg2);font-weight:500}
.empty{color:var(--fg3);font-size:13px;padding:12px 0;text-align:center}
.backlink{color:var(--acc);cursor:pointer;font-size:13px;display:inline-flex;
  align-items:center;gap:4px;margin-bottom:14px;opacity:.9}
.backlink:hover{opacity:1}

/* ─── Filters bar ───────────────────────────────────────────────────── */
.filters{display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap;align-items:center}
.filters select{width:auto;padding:5px 10px;font-size:12px}
.filters input{width:auto;padding:5px 10px;font-size:12px}

/* ─── Code / pre ────────────────────────────────────────────────────── */
pre{background:var(--surf2);border:1px solid var(--bd);padding:10px 12px;border-radius:8px;
  font-size:11px;overflow:auto;max-height:240px;white-space:pre-wrap;word-break:break-word;
  color:var(--fg2);line-height:1.6}
.kv{display:grid;grid-template-columns:120px 1fr;gap:5px 14px;font-size:13px}
.kv dt{color:var(--fg3);font-size:12px}
.kv dd{margin:0;word-break:break-word;color:var(--fg2)}
details summary{cursor:pointer;color:var(--fg3);font-size:12px;list-style:none}

/* ─── Flash toast ───────────────────────────────────────────────────── */
.flash{
  position:fixed;bottom:24px;right:24px;
  background:var(--surf);border:1px solid var(--bd2);color:var(--fg);
  padding:12px 18px;border-radius:10px;
  opacity:0;transition:opacity .25s,transform .25s;
  transform:translateY(8px);
  pointer-events:none;z-index:99;max-width:360px;
  box-shadow:0 8px 32px rgba(0,0,0,.5);
  font-size:13px;font-weight:500;
}
.flash.show{opacity:1;transform:translateY(0)}
.flash.err{border-color:var(--bad);color:#f87171}

/* ─── Lock screen ───────────────────────────────────────────────────── */
.lock{
  max-width:440px;margin:80px auto;
  background:var(--surf);border:1px solid var(--bd);border-radius:16px;
  padding:36px 32px;text-align:center;
}
.lock h1{font-size:20px;margin-bottom:4px}
.lock .sub{font-size:13px;color:var(--fg2);margin-bottom:24px}
.lock input{margin:6px 0;text-align:left}
.lock hr{border:none;border-top:1px solid var(--bd);margin:20px 0}

/* ─── Intro / onboarding banner ─────────────────────────────────────── */
.intro{
  background:var(--surf2);border:1px solid var(--bd2);border-radius:10px;
  padding:12px 16px;margin-bottom:16px;font-size:13px;color:var(--fg2);
  line-height:1.55;max-width:960px;
}
.intro b{color:var(--fg)}
.intro code{background:var(--surf3);border:1px solid var(--bd);padding:1px 5px;border-radius:5px;font-size:12px;color:var(--fg2)}

/* ─── Progress bars ─────────────────────────────────────────────────── */
.prog-bar{height:4px;background:var(--surf3);border-radius:2px;overflow:hidden;margin:6px 0}
.prog-bar-fill{height:100%;background:var(--acc);border-radius:2px;transition:width .3s}

/* ─── Tooltip ───────────────────────────────────────────────────────── */
.tip{position:relative;display:inline-flex;align-items:center;justify-content:center;
  width:15px;height:15px;border-radius:50%;background:var(--surf3);color:var(--fg3);
  font-size:10px;font-weight:700;cursor:help;vertical-align:middle;margin-left:6px;flex-shrink:0}
.tip:hover .tipbox,.tip:focus .tipbox{opacity:1;pointer-events:auto}
.tipbox{position:absolute;bottom:calc(100% + 8px);left:50%;transform:translateX(-50%);
  width:260px;background:#18181b;border:1px solid var(--bd2);color:var(--fg2);
  font-size:12px;font-weight:400;line-height:1.5;
  padding:10px 12px;border-radius:10px;opacity:0;pointer-events:none;
  transition:opacity .15s;z-index:40;box-shadow:0 8px 32px rgba(0,0,0,.5)}
.tipbox::after{content:"";position:absolute;top:100%;left:50%;transform:translateX(-50%);
  border:6px solid transparent;border-top-color:#27272a}

/* ─── Help & modal ───────────────────────────────────────────────────── */
.helplink{font:inherit;border:none;background:none;color:var(--fg3);cursor:pointer;
  font-size:12px;font-weight:500;padding:6px 10px;border-radius:8px;
  display:flex;align-items:center;gap:5px;width:100%;transition:background .12s,color .12s}
.helplink:hover{background:var(--surf3);color:var(--fg)}
.modal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:50;
  display:flex;align-items:flex-start;justify-content:center;padding:40px 16px;overflow:auto;
  backdrop-filter:blur(4px)}
.modal{
  background:var(--surf);border:1px solid var(--bd2);border-radius:16px;
  max-width:680px;width:100%;padding:24px 26px;
  box-shadow:0 24px 64px rgba(0,0,0,.6);
}
.modal h3{font-size:17px;font-weight:700;margin-bottom:4px;color:var(--fg)}
.modal .sub{margin-bottom:14px;color:var(--fg2)}
.modal h4{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--fg3);
  margin:16px 0 6px;font-weight:600}
.modal p,.modal li{font-size:13px;line-height:1.6;color:var(--fg2)}
.modal ul{padding-left:18px;margin:5px 0}
.modal code{background:var(--surf2);border:1px solid var(--bd);
  padding:1px 6px;border-radius:5px;font-size:12px;color:var(--fg2)}
.modal .close-x{float:right;border:none;background:none;font-size:22px;
  cursor:pointer;color:var(--fg3);line-height:1;padding:0 2px}
.modal .close-x:hover{color:var(--fg)}
/* help-page section (tinted) */
.help-page-section{background:var(--acc-lo);border:1px solid #2a4a8a;border-radius:10px;
  padding:14px 16px;margin-bottom:4px}
.help-page-section h4{font-size:11px;text-transform:uppercase;letter-spacing:.05em;
  color:#60a5fa;margin:14px 0 5px;font-weight:600}
.help-page-section h4:first-child{margin-top:0}
.help-page-section p,.help-page-section li{font-size:13px;line-height:1.6;color:#93c5fd}
.help-page-section ul{padding-left:18px;margin:4px 0}
.help-page-section pre{font-size:11px;background:rgba(0,0,0,.3);border:1px solid #2a4a8a;
  padding:8px;border-radius:6px;overflow-x:auto;line-height:1.5;
  white-space:pre-wrap;word-break:break-all;color:#93c5fd}
.help-page-section code{background:rgba(59,130,246,.18);border:none;padding:1px 5px;
  border-radius:4px;font-size:12px;color:#93c5fd}
/* accordion */
.help-accordion details{border-top:1px solid var(--bd);padding:5px 0}
.help-accordion details:last-child{border-bottom:1px solid var(--bd)}
.help-accordion summary{cursor:pointer;font-size:13px;padding:5px 0;
  user-select:none;list-style:none;color:var(--fg2);font-weight:500}
.help-accordion summary::before{content:"▶ ";font-size:9px;color:var(--fg3);
  display:inline-block;transition:transform .15s;margin-right:2px}
.help-accordion details[open] summary::before{transform:rotate(90deg)}
.help-accordion details p,.help-accordion details li{font-size:13px;line-height:1.55;color:var(--fg2)}
.help-accordion details ul{padding-left:18px;margin:6px 0}
.help-accordion details pre{font-size:11px;background:var(--surf2);padding:8px;border-radius:6px;
  border:1px solid var(--bd);overflow-x:auto;white-space:pre-wrap;word-break:break-all;color:var(--fg2)}
.help-accordion details code{background:var(--surf2);border:1px solid var(--bd);
  padding:1px 5px;border-radius:4px;font-size:12px;color:var(--fg2)}
.help-accordion details>*:not(summary){padding:6px 0 4px 14px}

/* ─── Misc utilities ────────────────────────────────────────────────── */
.a11y:focus-visible{outline:2px solid var(--acc);outline-offset:2px}
.storage-tag{display:inline-flex;align-items:center;gap:4px;font-size:11px;
  padding:2px 8px;border-radius:6px;background:var(--surf2);
  border:1px solid var(--bd);color:var(--fg3);font-weight:500}

/* ─── Responsive ────────────────────────────────────────────────────── */
@media(max-width:768px){
  #app{flex-direction:column}
  #sidebar{width:100%;height:auto;position:static;border-right:none;border-bottom:1px solid var(--bd);overflow:visible}
  #main-content{padding:16px 16px 40px;min-height:unset}
  nav{flex-direction:row;flex-wrap:wrap;padding:4px 8px 8px}
  nav button{width:auto;padding:6px 10px;font-size:12px}
  .nav-section{display:none}
  .brand{padding:14px 16px 10px}
}
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
// Context-aware help modal. Shows a page-specific section first, then
// collapsible general reference sections below it.
const HELP_PAGES={
  dash:{
    title:"Dashboard",
    body:`
    <p>The dashboard is your real-time swarm overview. Numbers update automatically every ~5 seconds via a live stream — no manual refresh needed.</p>
    <h4>What the stats mean</h4>
    <ul>
      <li><b>queued</b> — tasks created but not yet claimed by a worker. If this number keeps growing and active stays at zero, check that workers are running and tasks are assigned to a specific worker (unassigned tasks sit here forever).</li>
      <li><b>active</b> — tasks currently claimed or in progress. This is your "working right now" count.</li>
      <li><b>done/approved</b> — finished work. "done" means the worker submitted a result; "approved" means a reviewer accepted it. Both count here.</li>
      <li><b>failed</b> — tasks the worker could not complete. Click into them from Recent tasks to see the error and decide whether to requeue or delete.</li>
      <li><b>agents</b> — total registered agents, broken down by role. If this reads zero, no agents have enrolled yet — go to the Agents page to issue a join key.</li>
    </ul>
    <h4>Recent tasks</h4>
    <p>Shows the 6 most recently updated tasks. Click any row to open the full task detail where you can start, cancel, requeue, review, or delete it.</p>
    <h4>Live events</h4>
    <p>A rolling feed of the last 12 system events — task state changes, agent check-ins, artifact uploads, and more. Click <b>full log →</b> to see the complete filterable audit log.</p>
    <h4>Why are my queued tasks not moving?</h4>
    <p>Common reasons: (1) No worker agents are running. (2) The tasks are not assigned to any specific worker — go to the task detail and set the assignee. (3) The assigned worker's API key was revoked. Check the Agents page for offline agents.</p>`
  },
  projects:{
    title:"Projects",
    body:`
    <p>A project is a container for related tasks — one feature, one app, one sprint. It holds a name, description, shared context (passed to every agent working its tasks), and an artifact storage config.</p>
    <h4>Creating a project</h4>
    <ul>
      <li><b>Name</b> — displayed on all cards and included in planner messages. Be specific: "Warehouse Inventory v2 — reporting module" beats "Project 1".</li>
      <li><b>Description</b> — shown on the card. A one-sentence summary of the goal.</li>
      <li><b>Context JSON</b> — free-form JSON passed to every agent assigned to this project's tasks. Put things every agent needs: <code>{"repo":"~/Work/inventory","branch":"feature/v2","stack":"Python/FastAPI"}</code>. Must be valid JSON (double-quoted keys and strings).</li>
      <li><b>Artifact storage</b> — where the master node stores completed work files. See the storage section below.</li>
    </ul>
    <h4>What happens after you create a project?</h4>
    <p>The server immediately creates a <code>planning</code> task for the project and sends an A2A message to the first available planner or orchestrator agent. That agent picks up the planning task, reads your project name and context, and creates the specific work tasks. If no planner is available, the planning task stays queued until one comes online.</p>
    <h4>Project status</h4>
    <ul>
      <li><b>active</b> — work is ongoing. Tasks can be added and assigned.</li>
      <li><b>paused</b> — temporarily on hold. Tasks stay but no new planning is triggered.</li>
      <li><b>done</b> — work is finished. Record kept, nothing deleted.</li>
      <li><b>cancelled</b> — abandoned. Record kept, nothing deleted.</li>
    </ul>
    <p><b>mark done / cancel</b> only changes the status label — they never delete tasks or artifacts. Use <b>delete</b> (admin only) to permanently remove everything.</p>
    <h4>Artifact storage — which option should I choose?</h4>
    <ul>
      <li><b>Local filesystem</b> — simplest option. Files are saved under the server's data directory at <code>projects/&lt;project-id&gt;/&lt;task-id&gt;/&lt;filename&gt;</code>. Good for single-machine setups or when you don't need external version control.</li>
      <li><b>GitHub</b> — the master node commits each uploaded artifact to your repo via the GitHub API. Workers never need git credentials or repo access. Requires a Personal Access Token with <em>Contents: write</em> scope on the target repo. The token is stored server-side only and never returned to the browser.</li>
      <li><b>Azure DevOps</b> — the master node publishes each artifact as a Universal Package to your ADO feed. Requires a PAT with <em>Packaging: read &amp; write</em> scope.</li>
    </ul>
    <p><b>Migrating storage later:</b> open the project, click <b>⎇ Migrate storage…</b> in the Details card. The server verifies your new credentials before copying anything, then copies all existing artifacts to the new destination and updates the config.</p>`
  },
  agents:{
    title:"Agents &amp; Keys",
    body:`
    <p>Agents are the workers, planners, reviewers, and orchestrators in your swarm. Each has a role, a status, and an API key.</p>
    <h4>Join key vs. direct registration — which should I use?</h4>
    <ul>
      <li><b>Join key</b> — best for remote boxes that run an install/enroll script. The box calls <code>POST /api/agents/enroll</code> with the join key and auto-registers itself as an <em>observer</em>. You then promote it to the right role in the table. <em>One join key is reusable for multiple agents</em> — you don't need to issue a new key for every agent. However, issuing a new join key immediately invalidates the old one, so rotate it <em>after</em> all boxes in a batch have enrolled, not between each one.</li>
      <li><b>Register directly</b> — best for agents you're setting up by hand (a script, a Devin session, a local process). The server creates the agent immediately and shows its API key <em>once</em>. Copy it before closing the dialog — it cannot be retrieved later.</li>
    </ul>
    <h4>Roles in detail</h4>
    <ul>
      <li><b>orchestrator</b> — highest trust. Can create tasks, assign them to any agent, change task status, and send A2A messages. Typically your top-level coordinator agent.</li>
      <li><b>planner</b> — like orchestrator but focused on decomposing goals into tasks. Can create and assign tasks. Cannot approve reviews.</li>
      <li><b>worker</b> — pulls tasks assigned to it, executes them, uploads results, marks tasks done or failed. Cannot create tasks or assign work to others.</li>
      <li><b>qa / reviewer</b> — can approve or reject tasks that are in <em>done</em> or <em>failed</em> state. Cannot execute tasks.</li>
      <li><b>observer</b> — read-only. Cannot pull tasks, create tasks, or upload artifacts. This is the default role when an agent enrolls via the join key. Promote it once you've confirmed the agent is the right one.</li>
    </ul>
    <h4>Changing a role</h4>
    <p>Use the role dropdown in the table. The change takes effect immediately — the next API call the agent makes will be checked against the new role. You don't need to restart anything.</p>
    <h4>Key management — rekey vs. revoke vs. delete</h4>
    <ul>
      <li><b>rekey</b> — generates a brand-new API key and immediately invalidates the old one. Use this if a key is compromised or you need to rotate credentials. The agent can continue working once it has the new key.</li>
      <li><b>revoke</b> — deactivates the current key. The agent can no longer authenticate, but its record (task history, events) stays in the system. Use when you want to temporarily block an agent without deleting its history.</li>
      <li><b>delete</b> — permanently removes the agent and its key. The agent's past task assignments remain in the task records, but the agent itself is gone. Use to clean up retired agents.</li>
    </ul>
    <h4>Admin capability</h4>
    <p>Granting "admin cap" lets that agent's <code>mesh_…</code> key open the full admin console — identical access to the <code>adm_…</code> token. Only grant this to agents you fully control, such as your own orchestrator script running on the master node. Never grant it to worker agents on remote machines.</p>
    <h4>Why is an agent showing "offline"?</h4>
    <p>An agent goes offline when it hasn't sent a heartbeat in the last 90 seconds. Common causes: (1) the agent process stopped or crashed on the remote box, (2) network connectivity to the master node was lost, (3) the agent's key was revoked. Check the remote box's logs and the Events page for recent activity from that agent.</p>`
  },
  tasks:{
    title:"Tasks",
    body:`
    <p>A task is a unit of work assigned to one agent. It moves through a fixed lifecycle and every state change is recorded in the audit log.</p>
    <h4>Task lifecycle</h4>
    <p style="font-family:monospace;font-size:13px;background:var(--card);padding:8px;border-radius:4px;border:1px solid var(--bd)">
      queued → claimed → in_progress → done | failed<br>
      &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↓<br>
      &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;approved | rejected
    </p>
    <ul>
      <li><b>queued</b> — created, waiting. Will not move until a worker is assigned and pulls it.</li>
      <li><b>claimed</b> — a worker called <code>GET /api/work/pull</code> and got this task. It hasn't started yet.</li>
      <li><b>in_progress</b> — worker called <code>POST /api/tasks/{id}/start</code>. Active work is happening.</li>
      <li><b>done</b> — worker submitted a result. Awaiting review (or considered complete if no review process).</li>
      <li><b>failed</b> — worker reported it could not complete the task. Open the task to see the result payload for the error message, then decide to requeue or delete.</li>
      <li><b>cancelled</b> — stopped by an admin or orchestrator. Record kept.</li>
      <li><b>approved</b> — a qa/reviewer accepted the result. Final positive state.</li>
      <li><b>rejected</b> — a qa/reviewer sent it back. The worker should correct and resubmit (or the admin can requeue it).</li>
    </ul>
    <h4>Why is my task stuck in "queued"?</h4>
    <p>A task stays queued until: (1) it is assigned to a specific worker agent ID, AND (2) that worker calls the pull endpoint. Open the task detail and check the <em>assigned to</em> field. If it's blank, the task will never be picked up automatically — set the assignee to a worker's agent ID.</p>
    <h4>Work kinds</h4>
    <ul>
      <li><b>code</b> — write or modify source code files</li>
      <li><b>research</b> — investigate, compare, or summarise findings</li>
      <li><b>docs</b> — write or update documentation</li>
      <li><b>ops</b> — deploy, configure infrastructure, run shell commands</li>
      <li><b>test</b> — write or execute tests, report coverage</li>
      <li><b>generic</b> — any other work</li>
      <li><b>planning</b> — auto-created when a project is created; picked up by planners to decompose the project into tasks</li>
    </ul>
    <h4>Priority</h4>
    <p>0 = most urgent, 5 = lowest. Workers receive their assigned tasks sorted by priority ascending (0 first). If a worker has 10 queued tasks, the priority-0 ones are presented first. Default is 3.</p>
    <h4>Start / Cancel / Requeue — when to use each</h4>
    <ul>
      <li><b>Start</b> — manually moves a queued/claimed task to in_progress. Normally workers do this automatically. Use it to manually mark a task active when a worker began work outside the normal pull flow.</li>
      <li><b>Cancel</b> — stops the task. The worker will stop receiving it on the next pull. Record kept — use Cancel if you might want to try again later.</li>
      <li><b>Requeue</b> — puts a failed/cancelled/rejected task back to queued. Use after fixing the root cause of a failure, or after a reject to let the worker try again.</li>
      <li><b>Delete</b> (admin) — permanently removes the task and all its uploaded artifacts. Irreversible. Prefer Cancel/Requeue unless you're cleaning up old data.</li>
    </ul>`
  },
  task_detail:{
    title:"Task detail",
    body:`
    <p>The task detail page shows everything about one task and lets you act on it directly.</p>
    <h4>Fields explained</h4>
    <ul>
      <li><b>spec</b> — the full task specification as JSON. This is what the assigned worker receives when it pulls the task. It includes the title, context, and any additional fields set at creation.</li>
      <li><b>result</b> — the JSON payload the worker submitted when it marked the task done or failed. Contains the agent's output, any error message, and metadata.</li>
      <li><b>assigned to</b> — the agent ID of the worker responsible for this task. If blank, the task will not be picked up. Orchestrators can reassign tasks via the API (<code>POST /api/tasks/{id}/reassign</code>).</li>
      <li><b>deadline</b> — optional Unix timestamp. Shown in local time. The system does not auto-fail tasks past their deadline — it's informational for agents and planners.</li>
      <li><b>priority</b> — 0 (most urgent) to 5 (lowest). Workers see their tasks in this order.</li>
    </ul>
    <h4>Review</h4>
    <p>Review is only available when the task is in <b>done</b> or <b>failed</b> status. Only agents with the <b>qa</b>, <b>reviewer</b>, or <b>orchestrator</b> role can approve or reject. The admin token alone cannot review — you need to log in as an agent with one of those roles, or grant the admin-cap to a reviewer agent and use its key.</p>
    <p>When rejecting: always fill in the <b>review note</b> to tell the worker what needs to change. The note is stored with the rejection event and visible to the worker on its next pull.</p>
    <h4>Artifacts</h4>
    <p>Files the worker uploaded via <code>POST /api/artifacts</code> while working this task. Click <b>download</b> to retrieve the file. The sha256 hash lets you verify integrity. Artifacts are also pushed to the project's configured storage (GitHub/ADO/local mirror) asynchronously after upload.</p>
    <h4>Task events</h4>
    <p>Every state change, artifact upload, and review action is logged here with a timestamp and actor. If something went wrong, this is the first place to look.</p>`
  },
  events:{
    title:"Event audit log",
    body:`
    <p>Every action in the system produces an immutable event record. Events cannot be edited or deleted — they are the authoritative record of what happened and when.</p>
    <h4>Common event types</h4>
    <ul>
      <li><b>task.created</b> — a new task was added (includes creator and project)</li>
      <li><b>task.claimed</b> — a worker pulled the task from the queue</li>
      <li><b>task.started</b> — worker marked the task in_progress</li>
      <li><b>task.done / task.failed</b> — worker submitted a result</li>
      <li><b>task.cancelled / task.requeued</b> — admin/orchestrator acted on the task</li>
      <li><b>task.approved / task.rejected</b> — reviewer acted on a finished task</li>
      <li><b>artifact.uploaded</b> — a file was uploaded for a task</li>
      <li><b>artifact.pushed / artifact.push_failed</b> — the master node attempted to push the file to GitHub/ADO</li>
      <li><b>agent.checkin</b> — a running agent sent its heartbeat (every ~60s)</li>
      <li><b>agent.registered / agent.rekeyed / agent.deleted</b> — key management actions</li>
      <li><b>project.created / project.request / project.storage_migrated</b> — project lifecycle</li>
      <li><b>chat.message</b> — an admin chat message was posted</li>
    </ul>
    <h4>How to trace a task's full history</h4>
    <p>Paste the task ID into the <b>filter task ID</b> box and press Enter. You'll see every event that touched that task in chronological order — who created it, which worker claimed it, when it started, what happened when it finished, and whether it was reviewed.</p>
    <h4>How to find all failed tasks quickly</h4>
    <p>Use the <b>type</b> filter and select <code>task.failed</code>. You'll see every failure with the actor (which worker reported it) and timestamp. Click through to the task detail to see the result payload and error message.</p>
    <h4>Filtering tips</h4>
    <ul>
      <li>Filters are combined (AND): filtering by actor <em>and</em> type shows events where both match.</li>
      <li>The task ID filter is a prefix match — paste the first few characters if you don't have the full ID.</li>
      <li>The actor filter lists all actors seen in the current event window. If an agent isn't shown, there are no recent events from it.</li>
    </ul>`
  },
  artifacts:{
    title:"Artifacts",
    body:`
    <p>Artifacts are files agents uploaded as task results — compiled builds, reports, datasets, generated code, and so on. Each artifact is permanently linked to the task that produced it.</p>
    <h4>How artifacts get here</h4>
    <p>Workers call <code>POST /api/artifacts</code> (multipart form, <code>file</code> + <code>task_id</code> fields) while executing a task. The master node saves the file locally, records it here, and then asynchronously pushes it to the project's configured storage (GitHub/ADO/local mirror).</p>
    <h4>Integrity verification</h4>
    <p>Every artifact has a <b>sha256</b> hash computed on upload. The first 12 characters are shown in the table. After downloading, you can verify: <code>sha256sum &lt;file&gt;</code> (Linux/Mac) or <code>Get-FileHash &lt;file&gt;</code> (PowerShell) and compare to the full hash stored in the DB.</p>
    <h4>Downloading artifacts</h4>
    <p>Click <b>download</b> to retrieve the file directly from the master node. The URL is <code>GET /api/artifacts/{id}</code> — you can also fetch it programmatically with an agent key or admin token in the <code>Authorization: Bearer</code> header.</p>
    <h4>Are artifacts deleted when I delete a task?</h4>
    <p>Yes — deleting a task via the admin UI or API permanently removes the task record <em>and</em> all its uploaded artifact files from disk. The deletion count is shown in the confirmation flash message. If you only want to stop the task without losing files, use <b>Cancel</b> instead.</p>`
  },
  settings:{
    title:"Artifact Targets (Settings)",
    body:`
    <p>Global artifact push targets. Every time a worker uploads a file, it is saved locally on the master node first. Targets configured here cause an additional copy to be pushed to an external system automatically, for every upload across all projects.</p>
    <p><em>Note: per-project storage (set when creating a project) takes precedence over these global targets for that project's tasks. Global targets are a fallback for tasks that aren't part of any project.</em></p>
    <h4>GitHub target — fields explained</h4>
    <ul>
      <li><b>owner/repo</b> — the GitHub repository in <code>owner/repo</code> format, e.g. <code>acme/build-artifacts</code>. The repo must already exist.</li>
      <li><b>branch</b> — the branch to commit into. Defaults to <code>main</code>. The branch must exist in the repo.</li>
      <li><b>path prefix</b> — a folder path inside the repo where files are committed. Include the trailing slash: <code>artifacts/</code>. Files are placed at <code>&lt;prefix&gt;/&lt;project-id&gt;/&lt;task-id&gt;/&lt;filename&gt;</code>.</li>
      <li><b>PAT</b> — a GitHub Personal Access Token. Under <em>Settings → Developer settings → Personal access tokens (classic)</em>, create a token with at minimum <em>repo</em> scope (or <em>Contents: write</em> for fine-grained tokens). The token is stored server-side only and never returned to the browser. Shown as <code>***</code> after saving.</li>
    </ul>
    <h4>Azure DevOps target — fields explained</h4>
    <ul>
      <li><b>Organisation</b> — the part after <code>dev.azure.com/</code> in your ADO URL.</li>
      <li><b>Project</b> — the ADO project containing the feed.</li>
      <li><b>Feed</b> — the name of the Universal Packages feed. The feed must already exist in ADO.</li>
      <li><b>PAT</b> — an ADO Personal Access Token with <em>Packaging (read &amp; write)</em> scope.</li>
    </ul>
    <h4>Updating a secret (PAT / token)</h4>
    <p>To rotate a PAT without changing other fields: enter the new token value and save. To keep the existing token unchanged, leave the field showing <code>***</code> — the server preserves the stored value. This applies to both global targets and per-project storage.</p>
    <h4>Enabling / disabling a target</h4>
    <p>The <b>on/off</b> toggle pauses pushes to that target without deleting the config. Useful for temporarily disabling a target while rotating credentials or during maintenance.</p>`
  },
  chat:{
    title:"Admin Chat",
    body:`
    <p>A persistent, real-time message channel between all admin console users and remote agent nodes. Messages are stored in the master node's database and survive server restarts.</p>
    <h4>Who can post?</h4>
    <ul>
      <li>Anyone logged into the console with an admin token</li>
      <li>Any agent whose key has been granted the <b>admin cap</b></li>
      <li>Any registered agent key (even without admin cap) — useful for remote monitoring agents to post status alerts</li>
    </ul>
    <h4>Posting from a remote agent (no console access needed)</h4>
    <pre style="font-size:11px;background:var(--card);padding:8px;border-radius:4px;border:1px solid var(--bd)">curl -X POST https://your-server/api/admin/chat \\
  -H "Authorization: Bearer &lt;agent-key&gt;" \\
  -H "Content-Type: application/json" \\
  -d '{"text":"Node 3 degraded — 2 of 5 workers offline","sender_name":"Node 3 Monitor"}'</pre>
    <h4>sender_name</h4>
    <p>Optional display name shown next to the message. If omitted, defaults to the agent's registered name (or "admin" for the admin token). Useful for giving remote monitoring scripts a recognisable identity in the chat.</p>
    <h4>Keyboard shortcuts</h4>
    <ul>
      <li><b>Ctrl+Enter</b> (or <b>⌘+Enter</b> on Mac) — send the message</li>
      <li><b>Enter</b> — add a new line (does not send)</li>
    </ul>
    <h4>Real-time delivery</h4>
    <p>Messages are delivered to all open chat panels instantly via Server-Sent Events — no polling or manual refresh needed. If the connection drops, the panel auto-reconnects in 3 seconds and reloads history to catch up on any missed messages.</p>`
  }
};

// General reference sections shown at the bottom of every help modal
const HELP_GENERAL=`
  <details><summary><b>The big picture</b></summary>
    <p>agent-mesh is a <b>work channel</b> for a team of AI agents. One machine runs the master node (this server); other machines enroll as agents. Agents check in, get assigned tasks, execute them, upload results, and report progress. You steer the whole system from this console.</p>
    <p>The master node is the single point of coordination — it holds all task state, agent keys, artifacts, and events. Agents never talk directly to each other; everything goes through the master node's HTTP API.</p>
  </details>
  <details><summary><b>Roles &amp; permissions</b></summary>
    <ul>
      <li><b>orchestrator</b> — create &amp; assign tasks, full task management, A2A messaging</li>
      <li><b>planner</b> — decompose goals into tasks, create &amp; assign tasks</li>
      <li><b>worker</b> — pull assigned tasks, execute, upload artifacts, report done/failed</li>
      <li><b>qa / reviewer</b> — approve or reject finished work</li>
      <li><b>observer</b> — read-only; default role for newly enrolled agents</li>
    </ul>
    <p>Roles are changed instantly from the Agents page — no restart needed.</p>
  </details>
  <details><summary><b>Task lifecycle</b></summary>
    <p><code>queued → claimed → in_progress → done | failed → approved | rejected</code></p>
    <p>A task only moves to <em>claimed</em> when a worker with it assigned calls <code>GET /api/work/pull</code>. From there the worker controls progress. A qa/reviewer acts at the <em>done/failed</em> stage.</p>
    <p>Admins can manually move tasks via Start, Cancel, and Requeue buttons on the task detail page.</p>
  </details>
  <details><summary><b>Keys &amp; security</b></summary>
    <ul>
      <li><b>Admin token</b> (<code>adm_…</code>) — printed once at server first-run. Grants full console access. Does not expire. Store it in a secrets manager.</li>
      <li><b>Agent key</b> (<code>mesh_…</code>) — issued at agent creation or after a rekey. Shown once only. If lost, use <b>rekey</b> to generate a new one (old key dies immediately).</li>
      <li><b>Join key</b> — used by new boxes to self-enroll. <em>One join key can be reused for multiple agents</em> — you do not need to issue a new one per agent. Issuing a new join key invalidates the old one, so wait until a batch of agents has finished enrolling before rotating it.</li>
      <li><b>Admin cap</b> — grants an agent's <code>mesh_…</code> key the same full-console access as the admin token. Only grant to fully trusted agents.</li>
    </ul>
  </details>
  <details><summary><b>Artifact storage</b></summary>
    <p>Every uploaded artifact is saved locally on the master node first (flat store at <code>data/artifacts/&lt;id&gt;</code>). A structured mirror copy is then written to the project's configured storage backend:</p>
    <ul>
      <li><b>Local</b> — mirrored at <code>data/projects/&lt;project-id&gt;/&lt;task-id&gt;/&lt;filename&gt;</code></li>
      <li><b>GitHub</b> — committed to the configured repo at <code>&lt;prefix&gt;/&lt;project-id&gt;/&lt;task-id&gt;/&lt;filename&gt;</code> via the GitHub API</li>
      <li><b>Azure DevOps</b> — published as a Universal Package to the configured feed</li>
    </ul>
    <p>Storage can be changed after project creation using <b>⎇ Migrate storage…</b> on the project detail page. Credentials are verified before any data is moved.</p>
  </details>
  <details><summary><b>Live updates &amp; auto-refresh</b></summary>
    <p>The console maintains a Server-Sent Events connection to the master node. Any state change (task update, agent check-in, artifact upload) wakes all connected browsers immediately. A 5-second polling fallback ensures the UI stays current even if the SSE stream drops.</p>
    <p>Your typed form text is preserved across auto-refreshes — only the data regions (tables, stats, event lists) are updated, never the input fields.</p>
  </details>`;

function openHelp(){
  // Determine which page the user is currently on
  const h=location.hash||"#/";
  let pageKey="dash";
  if(h==="#/agents") pageKey="agents";
  else if(h==="#/tasks") pageKey="tasks";
  else if(h.startsWith("#/task/")) pageKey="task_detail";
  else if(h==="#/events") pageKey="events";
  else if(h==="#/artifacts") pageKey="artifacts";
  else if(h==="#/settings") pageKey="settings";
  else if(h==="#/chat") pageKey="chat";
  else if(h==="#/projects"||h.startsWith("#/project/")) pageKey="projects";

  const page=HELP_PAGES[pageKey]||HELP_PAGES.dash;

  const el=document.createElement("div");el.className="modal-backdrop";
  el.innerHTML=`<div class="modal" style="max-width:680px">
    <button class="close-x" onclick="this.closest('.modal-backdrop').remove()" aria-label="close">×</button>
    <h3>Help — ${page.title}</h3>
    <div class="sub" style="margin-bottom:12px">Context-aware guide for the page you're on. General reference sections are below.</div>
    <div class="help-page-section">${page.body}</div>
    <hr style="border:none;border-top:1px solid var(--bd);margin:18px 0">
    <div class="sub" style="margin-bottom:8px;font-weight:600">General reference</div>
    <div class="help-accordion">${HELP_GENERAL}</div>
  </div>`;
  document.body.appendChild(el);
  el.addEventListener("click",e=>{if(e.target===el)el.remove()});
}

/* ---------------- lock screen ---------------- */
function renderLock(errMsg=""){
  // Remove sidebar if present (user was logged in, token expired)
  const sb=document.getElementById("sidebar");if(sb)sb.remove();
  const mc=document.getElementById("main-content");if(mc)mc.remove();
  // Center the lock card inside #app
  const app=$("#app");
  app.style.cssText="display:flex;align-items:flex-start;justify-content:center;padding:60px 16px;min-height:100vh";
  app.innerHTML=`<div class="lock">
    <img src="data:image/png;base64,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" alt="Sigmaz Technologies" style="width:220px;height:auto;margin-bottom:20px;display:block;margin-left:auto;margin-right:auto">
    <h1>agent-mesh</h1>
    <div class="sub">v${V} · admin console</div>
    ${errMsg?`<div style="color:var(--bad);font-size:13px;background:var(--bad-lo);
      border:1px solid var(--bad);border-radius:8px;padding:8px 12px;margin:10px 0;text-align:left">${esc(errMsg)}</div>`:""}
    <div style="font-size:12px;color:var(--fg3);margin:16px 0 6px;text-align:left;line-height:1.6">
      <b style="color:var(--fg2)">Admin token</b> — full read/write access.<br>
      Printed to the server console on first start-up. Starts with <code style="background:var(--surf2);padding:1px 5px;border-radius:4px;font-size:11px">adm_</code>.
    </div>
    <input id="adm" placeholder="adm_…" type="password" class="a11y"
      style="margin-bottom:8px" onkeydown="if(event.key==='Enter')doUnlock()">
    <button id="unlock-btn" class="primary" onclick="doUnlock()" style="width:100%;margin-bottom:4px">Unlock as admin</button>
    <hr style="border:none;border-top:1px solid var(--bd);margin:20px 0">
    <div style="font-size:12px;color:var(--fg3);margin-bottom:6px;text-align:left;line-height:1.6">
      <b style="color:var(--fg2)">Agent key</b> — opens the console as a specific agent.<br>
      Role still gates what you can do (e.g. only <code style="background:var(--surf2);padding:1px 5px;border-radius:4px;font-size:11px">qa</code> can approve tasks).
    </div>
    <input id="akey" placeholder="mesh_…" style="margin-bottom:8px"
      onkeydown="if(event.key==='Enter')unlockAsAgent()">
    <button id="agent-unlock-btn" onclick="unlockAsAgent()" style="width:100%">Unlock as agent</button>
  </div>`;
  setTimeout(()=>$("#adm").focus(),50);
}
async function doUnlock(){
  const v=$("#adm").value.trim();
  if(!v)return;
  const btn=$("#unlock-btn");
  if(btn){btn.disabled=true;btn.textContent="Checking…";}
  try{
    // Validate the token server-side before storing it or rendering any pages
    const r=await fetch(BASE+"/api/admin/stats",{headers:{Authorization:"Bearer "+v}});
    if(r.status===403||r.status===401){
      renderLock("Invalid token — check the value printed at server startup.");
      return;
    }
    if(!r.ok){
      renderLock("Server error ("+r.status+") — try again.");
      return;
    }
    // Token is valid — store and boot
    ADMIN=v;
    sessionStorage.setItem("mesh_adm",v);
    boot();
  }catch(e){
    renderLock("Cannot reach server — is agent-mesh running?");
  }
}
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

/* helper: set page from async shell result (shell() returns null if updated in-place) */
function setApp(html){
  if(html!=null){$("#app").innerHTML=html;}
  // else: shell() already updated sidebar + #page in-place
}
function refreshPage(html){
  // Used by async reload calls — prefer in-place update if sidebar is mounted
  if(html==null)return; // shell updated in-place already
  const pg=document.getElementById("page");
  if(pg)pg.innerHTML=html; // html here is the inner content, not a full shell
  else setApp(html);
}

/* ---------------- shell (sidebar layout) ---------------- */
function navBtn(id,icon,label,active){
  const href=id==='dash'?'#/':'#/'+id;
  return `<button class="${active===id?'active':''}" onclick="go('${href}')">
    <span class="nav-icon">${icon}</span>${label}</button>`;
}
function buildSidebar(active){
  return `<div id="sidebar">
    <div class="brand">
      <h1><i>agent-mesh</i></h1>
      <span class="v">v${V} &middot; <span class="co">Sigmaz Technologies</span></span>
    </div>
    <div class="nav-section">Workspace</div>
    <nav>
      ${navBtn('dash','⬡','Dashboard',active)}
      ${navBtn('projects','◈','Projects',active)}
      ${navBtn('tasks','✦','Tasks',active)}
      ${navBtn('agents','◎','Agents',active)}
    </nav>
    <div class="nav-section">Logs</div>
    <nav>
      ${navBtn('events','◉','Events',active)}
      ${navBtn('artifacts','▣','Artifacts',active)}
    </nav>
    ${ADMIN?`<div class="nav-section">Admin</div>
    <nav>
      ${navBtn('settings','⚙','Settings',active)}
      ${navBtn('chat','✉','Chat',active)}
    </nav>`:''}
    <div class="sidebar-footer">
      <span id="whoami" style="padding:4px 10px;font-size:11px;color:var(--fg3)"></span>
      <button class="helplink" onclick="openHelp()">
        <span style="font-size:13px">?</span> Help &amp; docs
      </button>
      <button class="helplink" onclick="logout()">
        <span style="font-size:13px">⏻</span> Lock console
      </button>
    </div>
  </div>`;
}
function shell(active,title,inner){
  // Mount the full layout (sidebar + content) into #app.
  // On subsequent calls, update sidebar active state and swap only #page.
  const sidebar=document.getElementById("sidebar");
  if(sidebar){
    // Sidebar already mounted — update it in place and replace page content
    sidebar.outerHTML=buildSidebar(active);
    const pg=document.getElementById("page");
    if(pg){pg.innerHTML=inner;return null;}
  }
  // First render — return the full markup to be set as #app innerHTML
  return buildSidebar(active)+
    `<div id="main-content"><div id="page">${inner}</div></div>`;
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
       <div class="stat-card"><div class="stat">${S.tasks_by_status?.queued||0}</div><div class="statlabel">Queued</div></div>
       <div class="stat-card"><div class="stat" style="color:var(--acc)">${active}</div><div class="statlabel">Active</div></div>
       <div class="stat-card"><div class="stat" style="color:var(--ok)">${done}</div><div class="statlabel">Done / Approved</div></div>
       <div class="stat-card"><div class="stat" style="color:var(--bad)">${S.tasks_by_status?.failed||0}</div><div class="statlabel">Failed</div></div>
       <div class="stat-card"><div class="stat">${S.agents_total||0}</div><div class="statlabel">Agents · ${esc(Object.entries(S.agents_by_role||{}).map(([r,n])=>`${r}:${n}`).join(" · ")||"—")}</div></div>
     </div>`);
    set("#recent",T.slice(0,6).map(t=>`<tr class="clickable" onclick="go('#/task/${t.id}')"><td>${esc(t.title)}</td><td>${statusPill(t.status)}</td><td class="mono">${esc(t.assigned_to||"—")}</td><td>${ago(t.updated_at)}</td></tr>`).join("")||'<tr><td colspan=4 class="empty">no tasks yet</td></tr>');
    set("#evlive",E.slice(0,12).map(evLine).join("")||'<div class="empty">no events</div>');
  }
  else if(h==="#/projects"){
    const P=CACHE.projects||[];
    set("#projlist",P.map(p=>projCard(p)).join("")||'<div class="card"><div class="empty">no projects yet</div></div>');
  }
  else if(m=h.match(/^#\/project\/([^/]+)$/)){
    const pid=decodeURIComponent(m[1]);
    const p=(CACHE.projects||[]).find(x=>x.id===pid);
    if(p&&$("#ptaskbody")){
      const items=p.task_items||CACHE.tasks.filter(t=>t.project_id===pid);
      $("#ptaskbody").innerHTML=(items.map(t=>`<tr class="clickable" onclick="go('#/task/${t.id}')"><td>${esc(t.title)}</td><td class="ev">${esc(t.kind)}</td><td>${statusPill(t.status)}</td><td class="mono">${esc(t.assigned_to||"—")}</td><td>${t.priority}</td><td>${ago(t.updated_at)}</td></tr>`).join("")||'<tr><td colspan=6 class="empty">no tasks</td></tr>');
      const th=$("#ptaskcount");if(th)th.textContent=`Task history (${items.length})`;
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
    <div class="stat-card"><div class="stat">${S.tasks_by_status?.queued||0}</div><div class="statlabel">Queued</div></div>
    <div class="stat-card"><div class="stat" style="color:var(--acc)">${active}</div><div class="statlabel">Active</div></div>
    <div class="stat-card"><div class="stat" style="color:var(--ok)">${done}</div><div class="statlabel">Done / Approved</div></div>
    <div class="stat-card"><div class="stat" style="color:var(--bad)">${S.tasks_by_status?.failed||0}</div><div class="statlabel">Failed</div></div>
    <div class="stat-card"><div class="stat">${S.agents_total||0}</div><div class="statlabel">Agents · ${esc(roleCounts)||"—"}</div></div>
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
     <h2>Register an agent directly${tip("Creates an agent right now and shows its API key ONCE. Copy it immediately — it cannot be retrieved later. Use this when setting up an agent by hand (e.g. a script or Devin session). Use the join key above for remote boxes that self-enroll via the install script.")}</h2>
     <div class="row" style="flex-wrap:wrap;gap:8px;align-items:center">
       <input id="na" class="grow" placeholder="Agent name — e.g. 'worker-node-2' or 'qa-bot'" title="A human-readable label for this agent. Shown in task assignments and the agents table. Pick something descriptive so you know which machine or role it represents.">
       <div style="display:flex;flex-direction:column;gap:2px">
         <select id="nr" style="width:auto" title="The agent's starting role. Controls what it's allowed to do: orchestrator/planner can create and assign tasks; worker pulls and executes tasks; qa/reviewer can approve results; observer is read-only.">${roleOpts("worker")}</select>
         <span style="font-size:10px;color:var(--fg3)">role (controls permissions)</span>
       </div>
       <label style="display:flex;flex-direction:column;gap:2px;font-size:12px;color:var(--fg3);cursor:pointer" title="Grants this agent's key the same full-console access as the admin token. Only grant to highly trusted agents (e.g. your own orchestrator). Regular workers should NOT have this.">
         <span style="display:flex;align-items:center;gap:4px"><input type="checkbox" id="ncap" style="width:auto"> admin cap</span>
         <span style="font-size:10px">full console access</span>
       </label>
       <button class="primary" onclick="regAgent()">Register &amp; show key</button>
     </div>
     <table><tr>
       <th>name / id</th>
       <th>role${tip("The agent's current role — change it here any time. Takes effect immediately.\n• orchestrator / planner — can create tasks and assign them to others\n• worker — pulls tasks assigned to it and executes them\n• qa / reviewer — can approve or reject finished work\n• observer — read-only; cannot pull or submit tasks")}</th>
       <th>status${tip("online = agent checked in within the last 90 seconds (its heartbeat loop is running).\noffline = no recent heartbeat — the agent process is likely stopped or unreachable.\nIf an agent shows offline unexpectedly, check that its worker loop is still running on the remote box.")}</th>
       <th>last seen</th>
       <th>key${tip("The first 8 characters of this agent's API key. The full key is only shown once at creation or after a 'rekey'.\n• rekey — issues a brand-new key; the old one stops working immediately. Use if a key leaks.\n• revoke — deactivates the key without deleting the agent record.\nStore keys securely — anyone with a key can act as that agent.")}</th>
       <th>console${tip("Whether this agent's key can open the full admin console.\n'grant' = enable admin access for this agent's key.\n'admin ✓' = already enabled — click to remove it.\nOnly grant to agents you fully trust (e.g. your own orchestrator script).")}</th>
       <th>actions</th></tr>
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
// Render a small storage-type tag for display in project cards and detail pages.
function storageTag(s){
  if(!s)return'<span class="pill">local</span>';
  const icons={github:"⎇ GitHub",ado:"☁ ADO",local:"📁 local"};
  const label=icons[s.type]||s.type;
  const detail=s.type==="github"?` (${esc(s.repo||"")})`:
               s.type==="ado"?` (${esc(s.org||"")}/${esc(s.feed||"")})`:
               s.local_path?` (${esc(s.local_path)})`:"";
  return `<span class="pill s-queued" style="font-size:11px">${label}</span>${detail?`<span class="ev" style="margin-left:4px">${detail}</span>`:""}`;
}
// Compact progress bar: done|approved vs total tasks
function projProgressBar(t){
  const total=Object.values(t||{}).reduce((a,b)=>a+b,0);
  if(!total)return'';
  const done=(t.done||0)+(t.approved||0);
  const active=(t.in_progress||0)+(t.claimed||0);
  const failed=(t.failed||0)+(t.rejected||0);
  const queued=t.queued||0;
  const pct=Math.round(done/total*100);
  return `<div style="margin:8px 0 2px 0;display:flex;align-items:center;gap:8px">
    <div style="flex:1;height:6px;background:var(--surf3);border-radius:3px;overflow:hidden">
      <div style="height:100%;width:${pct}%;background:var(--ok);transition:width .3s"></div>
    </div>
    <span class="ev" style="white-space:nowrap">${done}/${total} done</span>
  </div>
  <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:4px">
    ${active?`<span class="pill s-claimed" style="font-size:11px">${active} active</span>`:''}
    ${queued?`<span class="pill s-queued" style="font-size:11px">${queued} queued</span>`:''}
    ${failed?`<span class="pill s-failed" style="font-size:11px">${failed} failed</span>`:''}
  </div>`;
}
// Last-activity line for project cards
function projLastActivity(a){
  if(!a)return'';
  const lt=a.last_task;
  const la=a.last_activity;
  const rc=a.request_count||0;
  const parts=[];
  if(la)parts.push(`<span class="ev">Last activity ${ago(la)}</span>`);
  if(lt)parts.push(`<span class="ev" title="${esc(lt.title)}">${esc(lt.title.length>40?lt.title.slice(0,38)+'…':lt.title)} <span class="pill s-${lt.status==='done'?'done':lt.status==='failed'?'failed':'claimed'}" style="font-size:10px">${lt.status}</span></span>`);
  if(rc)parts.push(`<span class="pill s-planning">${rc} request${rc>1?'s':''}</span>`);
  return parts.length?`<div style="display:flex;flex-wrap:wrap;gap:6px;margin:4px 0">${parts.join('')}</div>`:'';
}
// Shared project card renderer (used in both pageProjects and refreshData)
function projCard(p){
  const t=p.tasks||{};
  return `<div class="card">
    <div style="display:flex;justify-content:space-between;align-items:start">
      <div><b style="font-size:15px">${esc(p.name)}</b>${planningBadge(p)}<div class="ev mono">${esc(p.id)}</div></div>
      ${projStatusPill(p.status)}
    </div>
    <div style="margin:4px 0 2px 0">${storageTag(p.artifact_storage)}</div>
    ${p.description?`<div class="ev" style="margin:4px 0">${esc(p.description)}</div>`:""}
    ${projProgressBar(t)}
    ${projLastActivity(p.activity)}
    <div style="display:flex;gap:8px;align-items:center;margin-top:10px">
      <button class="sm primary" onclick="go('#/project/${esc(p.id)}')">open →</button>
      <button class="sm" onclick="closeProject('${esc(p.id)}','done')">mark done</button>
      <button class="sm danger" onclick="closeProject('${esc(p.id)}','cancelled')">cancel</button>
      ${ADMIN?`<button class="sm danger" onclick="delProject('${esc(p.id)}')">delete</button>`:""}
    </div>
  </div>`;
}
// Show a "planning" badge on a project card when a planning task is queued/active.
function planningBadge(project){
  const items=project.task_items||(CACHE.tasks||[]).filter(t=>t.project_id===project.id);
  const planTask=items.find(t=>t.kind==="planning");
  if(!planTask)return"";
  if(["queued","claimed","in_progress"].includes(planTask.status))
    return `<span class="pill s-claimed" style="margin-left:6px">planning…</span>`;
  if(planTask.status==="done")
    return `<span class="pill s-done" style="margin-left:6px">planned</span>`;
  return"";
}
function pageProjects(){
  const P=CACHE.projects||[];
  return shell("projects","Projects",`
   <div class="intro">A project groups related tasks (e.g. one feature or app) and carries shared context like the repo to work in.
    Create a project, open it, and add tasks — then assign those tasks to workers from inside the project.
    ${tip("Context is free-form JSON shown to agents working the project's tasks — put the repo path, branch, or any shared facts here.")}</div>
   <div class="intro" style="margin-top:8px">
    <b>Project card buttons:</b>
    <b>open →</b> go to the project's page to view/add/assign its tasks ·
    <b>mark done</b> set status to <i>done</i> (work finished; keeps the record) ·
    <b>cancel</b> set status to <i>cancelled</i> (abandon; keeps the record) ·
    <b>delete</b> (admin) permanently remove the project + all its tasks + files — use to prune finished projects.
    ${tip("'mark done' and 'cancel' only change the status — they keep everything for reference. 'delete' is the only one that removes data, and it's admin-only.")}
   </div>
   <div class="card" style="margin-bottom:16px">
     <h2>New project${tip("A project groups related tasks under a shared name, description, and context. Create the project first, then open it to add tasks, submit requests, and track progress.")}</h2>
     <div class="row" style="flex-wrap:wrap;gap:8px">
       <div style="display:flex;flex-direction:column;gap:2px;flex:1;min-width:160px">
         <input id="pjname" class="grow" placeholder="Project name — e.g. 'Warehouse Inventory v2'" title="A short, descriptive name for the project. Shown on all cards and used in task planning messages sent to agents.">
         <span style="font-size:10px;color:var(--fg3)">required · used in agent planning messages</span>
       </div>
       <div style="display:flex;flex-direction:column;gap:2px;flex:2;min-width:200px">
         <input id="pjdesc" class="grow" placeholder="Description — e.g. 'Rebuild inventory reporting module'" title="Optional longer description of the project's goal. Helps human operators and agents understand what this project is about.">
         <span style="font-size:10px;color:var(--fg3)">optional · shown on project cards</span>
       </div>
     </div>
     <div class="row" style="margin-top:8px">
       <div style="display:flex;flex-direction:column;gap:2px;flex:1">
         <input id="pjctx" class="grow" placeholder='Context JSON — e.g. {"repo":"~/Work/inventory","branch":"main","stack":"Python/Flask"}' title='Optional shared context sent to every agent working this project&apos;s tasks. Use it to pass the repo path, branch, tech stack, or any facts every agent needs. Must be valid JSON (use double-quotes).'>
         <span style="font-size:10px;color:var(--fg3)">optional · valid JSON · e.g. {"repo":"~/Work/x","branch":"main"} — passed to every agent on this project</span>
       </div>
     </div>
     <div class="row" style="margin-top:8px;align-items:flex-start;flex-wrap:wrap;gap:8px">
       <div style="display:flex;flex-direction:column;gap:2px">
         <label style="white-space:nowrap;font-size:13px;font-weight:500">Artifact storage${tip("Where the master node stores and commits completed work files.\n• Local — files saved under the server's data directory, organized as projects/<id>/tasks/<id>/filename.\n• GitHub — master node commits each uploaded file to your repo via the GitHub API. Workers never need git access.\n• Azure DevOps — master node publishes as a Universal Package to your ADO Artifacts feed.\nIn all cases: workers upload to this server; the master node pushes onwards on their behalf.")}</label>
         <span style="font-size:10px;color:var(--fg3)">where completed work files are stored</span>
       </div>
       <select id="pjstore" style="width:auto" onchange="renderProjStoreForm()">
         <option value="local">📁 Local (master node filesystem)</option>
         <option value="github">⎇ GitHub repository</option>
         <option value="ado">☁ Azure DevOps</option>
       </select>
     </div>
     <div id="pjstore-form" style="margin-top:8px"></div>
     <div class="row" style="margin-top:10px">
       <button class="primary" onclick="createProject()">Create project</button>
       <span style="font-size:12px;color:var(--fg3)">A planning task is auto-created and sent to the first available planner agent.</span>
     </div>
   </div>
   <div class="grid" id="projlist">
     ${P.map(p=>projCard(p)).join("")||'<div class="card"><div class="empty">no projects yet</div></div>'}
   </div>`);
}
async function pageProject(id){
  let p;
  try{p=await api("/api/projects/"+id)}catch(e){return shell("projects","Project not found",`<div class="card"><div class="empty">${esc(e.message)}</div><span class="backlink" onclick="go('#/projects')">← back to projects</span></div>`)}
  const items=p.task_items||[];
  const reqs=p.requests||[];
  const kindColors={feature:"#dbeafe",enhancement:"#d1fae5",bugfix:"#fee2e2"};
  const kindIcon={feature:"✨",enhancement:"🔧",bugfix:"🐛"};
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
         <dt>artifact storage</dt><dd>${storageTag(p.artifact_storage)}</dd>
       </dl>
       ${p.description?`<h2 style="margin-top:14px">Description</h2><div>${esc(p.description)}</div>`:""}
       <h2 style="margin-top:14px">Context</h2><pre>${esc(JSON.stringify(p.context||{},null,1))}</pre>
       <div style="margin-top:14px;display:flex;gap:8px;flex-wrap:wrap">
         <button class="sm" onclick="closeProject('${esc(p.id)}','done')">mark done</button>
         <button class="sm" onclick="closeProject('${esc(p.id)}','paused')">pause</button>
         <button class="sm danger" onclick="closeProject('${esc(p.id)}','cancelled')">cancel</button>
         ${ADMIN?`<button class="sm" onclick="toggleMigrateForm('${esc(p.id)}')">⎇ Migrate storage…</button>`:''}
       </div>
       ${ADMIN?`<div id="migrate-form-${esc(p.id)}" style="display:none;margin-top:14px;border-top:1px solid var(--bd);padding-top:12px">
         <h2>Migrate artifact storage ${tip("Copies all existing artifacts for this project to the new destination, then updates the project's storage config.\n\nCredentials are verified before any data is moved. If some artifacts no longer exist on disk (e.g. were manually deleted) they are skipped.\n\nThe local flat store (artifacts/<id> files) is always kept — migration adds an organised copy to the new destination.")}</h2>
         <div class="ev" style="margin-bottom:8px">
           Move this project's artifacts to a different storage backend. All existing files are copied to the new destination first, then the project's storage config is updated.
           ${p.artifact_storage&&p.artifact_storage.type!=='local'?`<br><b>Current: ${esc(p.artifact_storage.type)}</b> — entering new credentials will replace them.`:''}
         </div>
         <div class="row" style="align-items:center;gap:8px;margin-bottom:8px">
           <label style="font-size:13px;font-weight:500;white-space:nowrap">New destination:</label>
           <select id="mig-type-${esc(p.id)}" style="width:auto" onchange="renderMigrateFields('${esc(p.id)}')">
             <option value="local">📁 Local filesystem</option>
             <option value="github">⎇ GitHub repository</option>
             <option value="ado">☁ Azure DevOps</option>
           </select>
         </div>
         <div id="mig-fields-${esc(p.id)}"></div>
         <div class="row" style="margin-top:10px;gap:8px;align-items:center">
           <button class="primary sm" id="mig-btn-${esc(p.id)}" onclick="doMigrateStorage('${esc(p.id)}')">Copy &amp; migrate</button>
           <button class="sm" onclick="toggleMigrateForm('${esc(p.id)}')">Cancel</button>
           <span id="mig-status-${esc(p.id)}" class="ev" style="color:var(--fg3)"></span>
         </div>
       </div>`:''}
     </div>
     <div class="card">
       <h2>Submit a request${tip("Write what you need in plain language — no need to know task IDs or agent names. The server creates a queued task and notifies the first available planner agent to pick it up and break it into work.\n\nExamples:\n• 'Refactor the output report in the warehouse inventory module'\n• 'Fix the null pointer exception on CSV export'\n• 'Add a date-range filter to the inventory summary page'")}</h2>
       <div class="ev" style="margin-bottom:8px">
         Write your request in plain language. A planner agent will be notified and will decompose it into specific tasks.
         <b>Examples:</b> "Refactor the output report to include warehouse_id" · "Fix the null pointer on CSV export" · "Add date-range filter to the summary page".
       </div>
       <div class="row" style="align-items:start;flex-wrap:wrap;gap:8px">
         <div style="display:flex;flex-direction:column;gap:2px;flex:1;min-width:200px">
           <textarea id="req-text" class="grow" rows="3" placeholder="Describe what you need in plain language — be specific about what should change and why…" style="resize:vertical" title="Plain-language description of the work you need done. Include enough detail for a planner agent to understand the scope — e.g. mention the file, module, or feature affected."></textarea>
           <span style="font-size:10px;color:var(--fg3)">plain language · be specific about scope and expected outcome</span>
         </div>
         <div style="display:flex;flex-direction:column;gap:6px">
           <div style="display:flex;flex-direction:column;gap:2px">
             <select id="req-kind" style="width:auto" title="Categorises the request so agents can prioritise and route it correctly.">
               <option value="feature">✨ Feature — new capability</option>
               <option value="enhancement">🔧 Enhancement — improve existing</option>
               <option value="bugfix">🐛 Bug fix — fix broken behaviour</option>
             </select>
             <span style="font-size:10px;color:var(--fg3)">request type</span>
           </div>
           <button class="sm primary" onclick="submitRequest('${esc(p.id)}')">Submit request</button>
         </div>
       </div>
       ${reqs.length?`<h2 style="margin-top:16px">Request history (${reqs.length})</h2>
       <div style="display:flex;flex-direction:column;gap:6px;margin-top:4px">
         ${reqs.map(r=>`<div style="border:1px solid #e3e3e8;border-radius:6px;padding:8px 10px;background:${kindColors[r.kind]||'#f7f7f9'}">
           <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
             <span><b>${kindIcon[r.kind]||'📋'} ${esc(r.kind)}</b></span>
             <span class="ev">${ago(r.created_at)}</span>
           </div>
           <div>${esc(r.text)}</div>
           ${r.task_id?`<div class="ev mono" style="margin-top:4px">→ task <span class="clickable" onclick="go('#/task/${esc(r.task_id)}')">${esc(r.task_id)}</span></div>`:''}
         </div>`).join('')}
       </div>`:''}
     </div>
     <div class="card" style="grid-column:1/-1"><h2 id="ptaskcount">Task history (${items.length})</h2>
       <div class="ev" style="margin-bottom:8px">Add a specific technical task directly to this project.
         For high-level requests ("add CSV export", "fix login bug") use the <b>Submit a request</b> card above — it notifies a planner agent to decompose the work.
         Direct tasks are useful when you already know exactly what needs doing and want to assign it to a specific worker.</div>
       <div class="row" style="flex-wrap:wrap;gap:6px;align-items:center">
         <input id="pttitle" class="grow" placeholder="Task title — e.g. 'Update report schema to include warehouse_id column'" title="A clear, specific description of the work. Workers see this as their task headline — make it actionable." style="min-width:200px">
         <div style="display:flex;flex-direction:column;gap:2px">
           <select id="ptkind" style="width:auto" title="Kind of work: code=write/edit code, research=investigate and report, docs=write documentation, ops=deploy/configure, test=write or run tests, generic=anything else">${KINDS.map(k=>`<option>${k}</option>`).join("")}</select>
           <span style="font-size:10px;color:var(--fg3)">work kind</span>
         </div>
         <div style="display:flex;flex-direction:column;gap:2px">
           <input id="ptprio" type="number" min="0" max="5" value="3" style="width:56px" title="Priority 0–5. Lower number = higher urgency (0 is most urgent, 5 is lowest). Workers see all their queued tasks sorted by priority.">
           <span style="font-size:10px;color:var(--fg3)">priority 0–5</span>
         </div>
         <button class="sm primary" onclick="addTaskToProject('${esc(p.id)}')">Add task</button>
       </div>
       <table id="ptasktable"><thead><tr><th>title</th><th>kind</th><th>status</th><th>assignee</th><th>prio</th><th>updated</th></tr></thead>
       <tbody id="ptaskbody">${items.map(t=>`<tr class="clickable" onclick="go('#/task/${t.id}')"><td>${esc(t.title)}</td><td class="ev">${esc(t.kind)}</td><td>${statusPill(t.status)}</td><td class="mono">${esc(t.assigned_to||"—")}</td><td>${t.priority}</td><td>${ago(t.updated_at)}</td></tr>`).join("")||'<tr><td colspan=6 class="empty">no tasks</td></tr>'}</tbody>
       </table>
     </div>
   </div>`);
}
async function submitRequest(pid){
  const text=($("#req-text").value||"").trim();
  const kind=$("#req-kind").value;
  if(!text){flash("enter a request description",1);return;}
  try{
    const r=await api("/api/projects/"+pid+"/request",{method:"POST",admin:true,body:{text,kind}});
    flash("request submitted → task "+r.task_id);
    $("#req-text").value="";
    pageProject(pid).then(setApp);
  }catch(e){flash(e.message,1);}
}

function pageTasks(){
  const T=CACHE.tasks||[];
  return shell("tasks","Task board",`
   <div class="intro">All work in the swarm. Create a task here, then <b>assign it to a worker</b> (open the task → set assignee) —
    workers only pull tasks assigned to them, so an unassigned task just sits in <i>queued</i>.
    Click any row for detail, actions (start/cancel/requeue), and review.</div>
   <div class="card">
     <div class="ev" style="margin-bottom:8px">
       Create a task, then open it to <b>set the assignee</b> — workers only pull tasks that are assigned to them by agent ID.
       An unassigned task stays <i>queued</i> until someone claims it manually.
       ${tip("Task lifecycle: queued → claimed (worker pulled it) → in_progress (worker started) → done/failed → approved/rejected (reviewer).\n\nPriority 0 = most urgent, 5 = lowest. Workers see their tasks sorted by priority.")}
     </div>
     <div class="row" style="flex-wrap:wrap;gap:6px;align-items:center">
       <input id="tt" class="grow" placeholder="Task title — e.g. 'Write unit tests for InventoryReport.export()'" title="A specific, actionable description. This is what the assigned worker sees as their headline. Include the function, module, or file name when possible." style="min-width:180px">
       <div style="display:flex;flex-direction:column;gap:2px">
         <select id="tk" style="width:auto" title="Work kind — used by agents and planners to categorise and filter tasks:\n• code — write or modify source code\n• research — investigate, compare, or document findings\n• docs — write or update documentation\n• ops — deploy, configure, or run infrastructure commands\n• test — write or execute tests\n• generic — any other work">${kindOpts("code")}</select>
         <span style="font-size:10px;color:var(--fg3)">work kind</span>
       </div>
       <div style="display:flex;flex-direction:column;gap:2px">
         <input id="tp" type="number" min="0" max="5" value="3" style="width:64px" title="Priority 0–5. Lower = more urgent. 0 is highest urgency, 5 is lowest. Workers receive tasks in priority order.">
         <span style="font-size:10px;color:var(--fg3)">priority 0–5</span>
       </div>
       <button class="primary" onclick="mkTask()">Create task</button>
     </div>
     <div class="filters" style="margin-top:10px;display:flex;align-items:center;gap:10px">
       <label style="font-size:12px;color:var(--fg3)">Filter by status:</label>
       <select id="tf" onchange="renderTaskFilter()" style="width:auto" title="Show only tasks in a particular status. Useful for finding all failed tasks or all tasks awaiting review.">
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
     <div class="card"><h2>Actions</h2>
       <div class="ev" style="margin-bottom:8px">
         Manage this task's state.
         ${tip("Task states:\n• queued — waiting; not yet claimed by a worker\n• claimed — a worker pulled it but hasn't started yet\n• in_progress — worker is actively working it\n• done — worker submitted a result\n• failed — worker reported it could not complete the task\n• cancelled — stopped by an admin/orchestrator\n• approved — reviewer accepted the result\n• rejected — reviewer sent it back")}
       </div>
       <div class="row" style="flex-wrap:wrap;gap:6px">
         ${t.status==="queued"||t.status==="claimed"?`<button class="primary" onclick="taskAct('${t.id}','start')" title="Manually move this task to in_progress — useful if a worker is processing it outside the normal claim flow">Start ${tip("Marks the task as in_progress. Normally workers call this automatically when they claim work. Use manually when a worker has begun but didn't update the status.")}</button>`:""}
         ${["queued","claimed","in_progress"].includes(t.status)?`<button class="danger" onclick="taskAct('${t.id}','cancel')" title="Stop this task — the worker will no longer receive it">Cancel ${tip("Moves the task to cancelled. The task record and any uploaded artifacts remain for reference. If you want to try again later, use Requeue.")}</button>`:""}
         ${["failed","cancelled","rejected"].includes(t.status)?`<button onclick="taskAct('${t.id}','requeue')" title="Return this task to queued so it can be picked up again">Requeue ${tip("Puts the task back to queued so a worker can try it again. Useful after fixing the root cause of a failure or after a cancellation you want to reverse.")}</button>`:""}
         ${ADMIN?`<button class="danger" onclick="delTask('${t.id}')" title="Permanently delete this task and all its uploaded files (admin only, irreversible)">Delete ${tip("Permanently removes this task and every artifact it uploaded. Admin only and cannot be undone.\nUse Cancel or Requeue if you just want to stop or restart work — they keep the history.")}</button>`:""}
       </div>
       ${canReview?`<h2 style="margin-top:12px">Review ${tip("Review is available once a task reaches 'done' or 'failed'.\nOnly agents with the qa, reviewer, or orchestrator role can approve or reject.\nApprove accepts the work as complete. Reject sends it back — use the note to explain what needs to change so the worker can correct it.")}</h2>
         <div class="ev" style="margin-bottom:6px">Only <b>qa</b>, <b>reviewer</b>, or <b>orchestrator</b> agents can review. Approve to accept the result; Reject to send it back.</div>
         <div class="row" style="flex-wrap:wrap;gap:6px">
           <button class="primary" onclick="taskReview('${t.id}','approved')" title="Accept this task's result as satisfactory — marks it approved">Approve</button>
           <button class="danger" onclick="taskReview('${t.id}','rejected')" title="Send this task back — the worker should correct and resubmit">Reject</button>
           <input id="rnote" class="grow" placeholder="Review note — e.g. 'Missing edge case for null input' or 'Looks good, approved'" title="Optional note recorded alongside your review decision. If rejecting, explain clearly what needs to change so the worker knows what to fix.">
         </div>`:`<div class="ev" style="margin-top:8px">Review becomes available once the task reaches <b>done</b> or <b>failed</b> status.</div>`}
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
   <div class="intro">A running record of everything that happened — task state changes, agent check-ins, artifact uploads, chat messages, and more.
    Use the filters to trace a specific task's history, see what one agent has done, or find a particular event type.
    Expand any event row to see its full detail payload.
    ${tip("Events are immutable — they can't be edited or deleted. They are the authoritative record of what happened and when.\n\nCommon event types:\n• task.created / task.claimed / task.done / task.failed — task lifecycle\n• artifact.uploaded / artifact.pushed — file uploads\n• agent.checkin — heartbeat from a running agent\n• project.request — admin submitted a request\n• chat.message — admin chat message")}
   </div>
   <div class="card">
     <div class="filters" style="display:flex;flex-wrap:wrap;gap:10px;align-items:center">
       <div style="display:flex;flex-direction:column;gap:2px">
         <select id="ef-actor" onchange="reloadEvents()" style="width:auto" title="Filter to events produced by a specific agent or 'admin'. 'actor' is the agent ID or 'admin' for console actions."><option value="">all actors</option>${[...new Set(E.map(e=>e.actor).filter(Boolean))].map(a=>`<option>${esc(a)}</option>`).join("")}</select>
         <span style="font-size:10px;color:var(--fg3)">filter by actor (who)</span>
       </div>
       <div style="display:flex;flex-direction:column;gap:2px">
         <select id="ef-type" onchange="reloadEvents()" style="width:auto" title="Filter to a specific event type — e.g. 'task.done' to see all completed tasks, or 'artifact.uploaded' to see all file uploads."><option value="">all event types</option>${[...new Set(E.map(e=>e.type))].map(t=>`<option>${esc(t)}</option>`).join("")}</select>
         <span style="font-size:10px;color:var(--fg3)">filter by type (what)</span>
       </div>
       <div style="display:flex;flex-direction:column;gap:2px">
         <input id="ef-task" placeholder="task-…  (press Enter)" style="width:220px" title="Enter a full or partial task ID to see all events for that specific task. Press Enter to apply." onkeyup="if(event.key==='Enter')reloadEvents()">
         <span style="font-size:10px;color:var(--fg3)">filter by task ID · press Enter</span>
       </div>
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
  setApp(shell("artifacts","Artifacts",`
   <div class="intro">Files agents uploaded as task results — builds, reports, datasets, etc. Each is tied to the task that produced it. Click <b>download</b> to grab one.</div>
   <div class="card" id="artcard"><div class="empty">loading…</div></div>`));
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

function pageSettings(){
  setApp(shell("settings","Settings — Artifact Targets",`
   <div class="intro">
     Global artifact push targets. When a worker uploads a file, it is always saved locally on the master node first.
     Configure a target here to also push every uploaded artifact to a GitHub repo or Azure DevOps feed automatically.
     ${tip("These are server-wide targets applied to all uploads. Per-project storage (set when creating a project) takes precedence over these global targets for that project's tasks.")}
     <br><br>
     <b>How it works:</b> Worker uploads file → master node saves it → master node pushes a copy to each enabled target.
     Workers never need credentials or git access — the master node commits on their behalf.
   </div>
   <div class="card" style="margin-bottom:16px">
     <h2>Add artifact target ${tip("Give the target a meaningful name so you can tell them apart in the list below — e.g. 'Main GitHub Repo' or 'ADO QA Feed'.")}</h2>
     <div class="row" style="flex-wrap:wrap;gap:8px;align-items:center">
       <div style="display:flex;flex-direction:column;gap:2px;flex:1;min-width:180px">
         <input id="tgt-name" class="grow" placeholder="Target name — e.g. 'Main GitHub Repo' or 'ADO Artifacts Feed'" title="A human-readable label shown in the targets list. Use something descriptive so you know which repo or feed this is.">
         <span style="font-size:10px;color:var(--fg3)">display name for this target</span>
       </div>
       <div style="display:flex;flex-direction:column;gap:2px">
         <select id="tgt-type" style="width:auto" onchange="renderTargetForm()" title="The kind of destination:\n• GitHub — commits each artifact as a file in a GitHub repository\n• Azure DevOps — publishes each artifact to an ADO Universal Packages feed\n• Local — copies files to an additional local path (e.g. a network share)">
           <option value="github">⎇ GitHub repository</option>
           <option value="ado">☁ Azure DevOps feed</option>
           <option value="local">📁 Local (extra path)</option>
         </select>
         <span style="font-size:10px;color:var(--fg3)">destination type</span>
       </div>
     </div>
     <div id="tgt-form" style="margin-top:8px"></div>
     <div class="row" style="margin-top:8px;gap:10px;align-items:center">
       <button class="primary" onclick="createTarget()">Add target</button>
       <span style="font-size:11px;color:var(--fg3)">Secrets are stored server-side only — shown as *** after saving. To rotate a secret, enter the new value and save again.</span>
     </div>
   </div>
   <div class="card" id="tgt-list"><div class="empty">loading…</div></div>`));
  renderTargetForm();
  loadTargets();
  return null;
}
function renderTargetForm(){
  const t=$("#tgt-type");if(!t)return;
  const f=$("#tgt-form");if(!f)return;
  const v=t.value;
  if(v==="github")f.innerHTML=`
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="tgt-repo" class="grow" placeholder="owner/repo — e.g. acme/build-artifacts" title="The GitHub repository to push artifacts into. Format: owner/repo. The PAT below must have 'Contents: write' permission on this repo.">
      <span style="font-size:10px;color:var(--fg3)">GitHub repo — owner/repo format</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="tgt-branch" class="grow" placeholder="Branch — e.g. main  (leave blank for main)" title="The branch to commit artifacts into. Defaults to 'main' if left blank.">
      <span style="font-size:10px;color:var(--fg3)">branch to commit into (default: main)</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="tgt-path" class="grow" placeholder="Path prefix in repo — e.g. artifacts/  (default)" title="Optional folder path inside the repo where files will be committed. e.g. 'builds/output/' — include the trailing slash. Defaults to 'artifacts/'.">
      <span style="font-size:10px;color:var(--fg3)">folder path in repo (default: artifacts/)</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="tgt-token" class="grow" placeholder="GitHub PAT — ghp_…  (Contents: write scope required)" type="password" autocomplete="new-password" title="A GitHub Personal Access Token with at minimum 'Contents: write' scope on the target repo. Stored server-side only — never returned after saving. To rotate: enter the new token and re-save.">
      <span style="font-size:10px;color:var(--fg3)">PAT with 'Contents: write' scope — stored server-side, shown as *** after save</span></div></div>`;
  else if(v==="ado")f.innerHTML=`
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="tgt-org" class="grow" placeholder="ADO organisation — e.g. mycompany  (from dev.azure.com/ORG)" title="Your Azure DevOps organisation name — the segment after dev.azure.com/ in your ADO URL.">
      <span style="font-size:10px;color:var(--fg3)">from dev.azure.com/ORG/…</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="tgt-project" class="grow" placeholder="ADO project name — e.g. WarehouseApp" title="The name of the ADO project that contains the Artifacts feed.">
      <span style="font-size:10px;color:var(--fg3)">ADO project containing the feed</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="tgt-feed" class="grow" placeholder="Artifacts feed name — e.g. agent-builds" title="The name of the Universal Packages feed within the ADO project. Artifacts are published as packages named by task ID.">
      <span style="font-size:10px;color:var(--fg3)">Universal Packages feed name</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="tgt-pat" class="grow" placeholder="ADO PAT — Packaging: read &amp; write scope required" type="password" autocomplete="new-password" title="Azure DevOps Personal Access Token with 'Packaging (read &amp; write)' scope. Stored server-side only.">
      <span style="font-size:10px;color:var(--fg3)">PAT with Packaging scope — stored server-side</span></div></div>`;
  else f.innerHTML=`
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="tgt-localpath" class="grow" placeholder="Extra local path — e.g. /mnt/nas/artifacts  (optional)" title="An additional local directory to copy artifacts into after upload. Useful for mirroring to a mounted network share. Leave blank to skip.">
      <span style="font-size:10px;color:var(--fg3)">optional extra copy path — e.g. a network share mount</span></div></div>`;
}
async function loadTargets(){
  try{
    const d=await api("/api/admin/artifact-targets",{admin:true});
    const el=$("#tgt-list");if(!el)return;
    if(!d.items.length){el.innerHTML='<div class="empty">no artifact targets configured — add one above</div>';return}
    el.innerHTML=`<table>
      <tr>
        <th>name / id</th>
        <th>type</th>
        <th title="Non-secret config fields. Tokens and PATs are stored server-side and shown as ***.">config (secrets hidden)</th>
        <th title="Toggle whether this target receives artifact pushes. Disable to temporarily pause pushes without deleting the config.">enabled</th>
        <th></th>
      </tr>
      ${d.items.map(t=>`<tr>
        <td><b>${esc(t.name)}</b><div class="ev mono">${esc(t.id)}</div></td>
        <td>${esc(t.type)}</td>
        <td class="mono ev" style="font-size:11px;max-width:300px;word-break:break-all">${esc(JSON.stringify(t.config))}</td>
        <td><button class="sm ${t.enabled?'primary':''}" onclick="toggleTarget('${esc(t.id)}',${!t.enabled})" title="${t.enabled?'Click to disable — pauses artifact pushes to this target':'Click to enable — resumes artifact pushes to this target'}">${t.enabled?'✓ on':'off'}</button></td>
        <td><button class="sm danger" onclick="delTarget('${esc(t.id)}')" title="Permanently remove this target. Existing uploaded artifacts are not affected.">delete</button></td>
      </tr>`).join("")}
    </table>`;
  }catch(e){const el=$("#tgt-list");if(el)el.innerHTML=`<div class="empty">${esc(e.message)}</div>`}
}
async function createTarget(){
  const name=$("#tgt-name").value.trim();if(!name)return flash("name required",1);
  const type=$("#tgt-type").value;
  let config={};
  if(type==="github"){
    config={repo:$("#tgt-repo")?.value.trim(),branch:$("#tgt-branch")?.value.trim()||"main",
            path:$("#tgt-path")?.value.trim()||"artifacts/",token:$("#tgt-token")?.value.trim()};
  }else if(type==="ado"){
    config={org:$("#tgt-org")?.value.trim(),project:$("#tgt-project")?.value.trim(),
            feed:$("#tgt-feed")?.value.trim(),pat:$("#tgt-pat")?.value.trim()};
  }else{
    config={local_path:$("#tgt-localpath")?.value.trim()};
  }
  try{await api("/api/admin/artifact-targets",{method:"POST",admin:true,body:{name,type,config}});
    flash("target added");loadTargets()}catch(e){flash(e.message,1)}
}
async function toggleTarget(id,enabled){
  try{await api("/api/admin/artifact-targets/"+id,{method:"PATCH",admin:true,body:{enabled}});
    flash(enabled?"target enabled":"target disabled");loadTargets()}catch(e){flash(e.message,1)}
}
async function delTarget(id){
  if(!confirm("Delete artifact target "+id+"?"))return;
  try{await api("/api/admin/artifact-targets/"+id,{method:"DELETE",admin:true});flash("target deleted");loadTargets()}catch(e){flash(e.message,1)}
}

/* ---------------- admin chat page ---------------- */
let _chatES=null;   // active SSE connection for the chat page

function pageChat(){
  // Render the chat panel inside the standard shell (uses existing session)
  setApp(shell("chat","Admin Chat",`
   <div class="intro">
     Real-time message channel between admin operators and remote agent nodes.
     All messages are persisted on the master node and visible to everyone with console access.
     ${tip("Who can post:\n• Any admin token holder (that's you, if you're reading this)\n• Any registered agent whose key has been granted the 'admin cap'\n\nRemote agent nodes can send alerts here without needing the console — they POST to:\n  POST /api/admin/chat\n  Authorization: Bearer <agent-key>\n  {\"text\": \"Node 3 is degraded\", \"sender_name\": \"Node 3 Monitor\"}\n\nMessages survive server restarts — they are stored in the mesh.db SQLite database.")}
     <br><b>Keyboard shortcut:</b> <kbd>Enter</kbd> to send. <kbd>Ctrl+Enter</kbd> (or <kbd>⌘+Enter</kbd> on Mac) adds a new line.
   </div>
   <div style="display:grid;grid-template-rows:1fr auto;height:calc(100vh - 180px);
               max-width:860px;margin:0 auto;gap:0">
     <div id="chat-msgs" style="overflow-y:auto;display:flex;flex-direction:column;
          gap:8px;padding:12px 4px;background:var(--bg)"></div>
     <div style="border-top:1px solid var(--bd);padding:12px 0;
          display:flex;gap:8px;align-items:flex-end;background:var(--bg)">
       <textarea id="chat-inp" class="grow" rows="1" placeholder="Message all console users… (Enter to send, Ctrl+Enter for new line)"
         style="resize:none;min-height:40px;max-height:120px;font:inherit;font-size:14px"
         oninput="chatAutosize(this)" onkeydown="chatKey(event)"></textarea>
       <button class="sm primary" id="chat-send" onclick="chatSend()">Send</button>
       <div id="chat-dot" title="stream status"
            style="width:8px;height:8px;border-radius:50%;background:#cbd5e1;flex-shrink:0;margin-bottom:16px"></div>
     </div>
   </div>`));
  chatLoadHistory();
  chatConnect();
}

async function chatLoadHistory(){
  try{
    const d=await api("/api/admin/chat?limit=200",{admin:true});
    const box=$("#chat-msgs");
    if(!box)return;
    box.innerHTML="";
    (d.messages||[]).forEach(chatAppend);
    chatScroll();
  }catch(e){chatSys("Could not load history: "+e.message)}
}

function chatConnect(){
  if(_chatES){try{_chatES.close()}catch{} _chatES=null}
  const dot=$("#chat-dot");
  const tok=sessionStorage.getItem("mesh_adm")||localStorage.getItem("mesh_agent_key")||"";
  _chatES=new EventSource(BASE+"/api/admin/chat/stream?token="+encodeURIComponent(tok));
  _chatES.addEventListener("hello",()=>{if(dot)dot.style.background="#22c55e"});
  _chatES.addEventListener("message",e=>{
    try{chatAppend(JSON.parse(e.data));chatScroll()}catch{}
  });
  _chatES.onerror=()=>{
    if(dot)dot.style.background="#ef4444";
    try{_chatES.close()}catch{} _chatES=null;
    // Only reconnect if we're still on the chat page
    if((location.hash||"#/")==="#/chat")setTimeout(chatConnect,3000);
  };
}

function chatAppend(m){
  const box=$("#chat-msgs");
  if(!box)return;
  if(document.querySelector('[data-chat-id="'+m.id+'"]'))return; // dedupe
  const isMe=(m.sender==="admin");
  const d=document.createElement("div");
  d.setAttribute("data-chat-id",m.id);
  d.style.cssText="display:flex;flex-direction:column;gap:2px;max-width:70%;"
    +(isMe?"align-self:flex-end;align-items:flex-end":"align-self:flex-start;align-items:flex-start");
  const t=new Date(m.ts*1000).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"});
  d.innerHTML=`<div style="padding:8px 12px;border-radius:14px;font-size:14px;line-height:1.5;
    word-break:break-word;white-space:pre-wrap;
    background:${isMe?"var(--accent,#2563eb);color:#fff":"var(--card)"};
    border-bottom-${isMe?"right":"left"}-radius:4px;border:1px solid var(--bd)">${esc(m.text)}</div>
    <div style="font-size:11px;color:var(--fg3);padding:0 4px">${esc(m.sender_name)} · ${t}</div>`;
  box.appendChild(d);
}

function chatSys(t){
  const box=$("#chat-msgs");
  if(!box)return;
  const d=document.createElement("div");
  d.style.cssText="align-self:center;font-size:11px;color:var(--fg3);font-style:italic;padding:4px 0";
  d.textContent=t; box.appendChild(d);
}

function chatScroll(){const b=$("#chat-msgs");if(b)b.scrollTop=b.scrollHeight}

async function chatSend(){
  const inp=$("#chat-inp");
  if(!inp)return;
  const text=inp.value.trim();
  if(!text)return;
  inp.value=""; chatAutosize(inp);
  const btn=$("#chat-send"); if(btn)btn.disabled=true;
  try{
    // POST returns the saved message; append it immediately (SSE echo is deduped)
    const msg=await api("/api/admin/chat",{method:"POST",admin:true,body:{text}});
    if(msg&&msg.id)chatAppend(msg);
    chatScroll();
  }catch(e){chatSys("Send failed: "+e.message)}
  if(btn)btn.disabled=false;
  if(inp)inp.focus();
}

function chatKey(e){
  if(e.key==="Enter"&&!e.ctrlKey&&!e.metaKey&&!e.shiftKey){
    e.preventDefault();chatSend();
  }
  // Ctrl+Enter / Shift+Enter → insert newline (browser default handles it)
}
function chatAutosize(el){el.style.height="auto";el.style.height=Math.min(el.scrollHeight,120)+"px"}

/* ---------------- router ---------------- */
function route(refresh=true){
  if(!hasSession()){renderLock();return}
  const h=location.hash||"#/";
  // Chat page manages its own live stream — never let loadAll() or the SSE
  // change-event re-render wipe the panel while a conversation is active.
  if(h==="#/chat"){
    if(!$("#chat-msgs")){
      // First visit: build the shell and connect
      pageChat();
    }
    // Subsequent calls (e.g. from SSE change events): do nothing — the chat
    // panel is already live and updating via its own stream.
    return;
  }
  // Navigating away from chat: tear down its stream
  if(_chatES){try{_chatES.close()}catch{} _chatES=null}
  if(refresh)loadAll();
  let m;
  // Helper: set #app to a full shell string, OR if shell() updated in place return null
  function setPage(html){if(html!=null)$("#app").innerHTML=html;}
  function loadingShell(active){
    const html=shell(active,"","");
    if(html!=null)$("#app").innerHTML=html;
    const pg=document.getElementById("page");
    if(pg)pg.innerHTML='<div class="empty" style="padding:32px;text-align:center">Loading…</div>';
  }
  if(h==="#/"||h===""){setPage(pageDash())}
  else if(h==="#/projects"){setPage(pageProjects());renderProjStoreForm()}
  else if(m=h.match(/^#\/project\/([^/]+)$/)){
    loadingShell("projects");
    pageProject(decodeURIComponent(m[1])).then(html=>{
      const pg=document.getElementById("page");
      if(pg&&html!=null)pg.innerHTML=html;
      else if(html!=null)setPage(html);
    });
  }
  else if(h==="#/agents"){setPage(pageAgents())}
  else if(h==="#/tasks"){setPage(pageTasks())}
  else if(m=h.match(/^#\/task\/([^/]+)$/)){
    loadingShell("tasks");
    pageTask(decodeURIComponent(m[1])).then(html=>{
      const pg=document.getElementById("page");
      if(pg&&html!=null)pg.innerHTML=html;
      else if(html!=null)setPage(html);
    });
  }
  else if(h==="#/events"){setPage(pageEvents())}
  else if(h==="#/artifacts"){pageArtifacts()}
  else if(h==="#/settings"){pageSettings()}
  else{setPage(pageDash())}
  const who=$("#whoami");if(who)who.textContent=ADMIN?"admin":"agent";
}

/* ---------------- actions ---------------- */
/* ---------- key reveal modal (replaces prompt() calls) ---------- */
function showKeyModal(title, subtitle, key){
  const existing=document.getElementById("key-modal-backdrop");
  if(existing)existing.remove();
  const el=document.createElement("div");
  el.id="key-modal-backdrop";
  el.className="modal-backdrop";
  el.innerHTML=`<div class="modal" style="max-width:520px">
    <h3 style="margin-bottom:4px">${esc(title)}</h3>
    <div style="margin-bottom:12px;color:var(--bad);font-weight:600;font-size:13px">
      ⚠ Copy this now — it will not be shown again.
    </div>
    <p style="font-size:13px;color:var(--fg2);margin-bottom:10px">${esc(subtitle)}</p>
    <div style="display:flex;gap:6px;align-items:center">
      <input id="key-modal-val" type="text" value="${esc(key)}" readonly
        style="flex:1;font-family:monospace;font-size:13px;padding:8px 10px;
               border:1px solid var(--bd2);border-radius:6px;background:var(--surf2);color:var(--fg);cursor:text"
        onclick="this.select()">
      <button id="key-modal-copy" class="primary sm" onclick="copyKey()" style="white-space:nowrap">Copy</button>
    </div>
    <div id="key-modal-copied" style="font-size:12px;color:var(--ok,#16a34a);height:16px;margin-top:4px"></div>
    <div style="margin-top:16px;text-align:right">
      <button class="sm" onclick="document.getElementById('key-modal-backdrop').remove()">Close</button>
    </div>
  </div>`;
  document.body.appendChild(el);
  // Auto-select the key text so user can copy immediately
  setTimeout(()=>{const inp=document.getElementById("key-modal-val");if(inp){inp.focus();inp.select()}},50);
  // Close on backdrop click
  el.addEventListener("click",e=>{if(e.target===el)el.remove()});
}
function copyKey(){
  const inp=document.getElementById("key-modal-val");
  if(!inp)return;
  const val=inp.value;
  const msg=document.getElementById("key-modal-copied");
  const btn=document.getElementById("key-modal-copy");
  function onOk(){
    if(msg)msg.textContent="✓ Copied to clipboard";
    if(btn)btn.textContent="Copied ✓";
    setTimeout(()=>{if(msg)msg.textContent="";if(btn)btn.textContent="Copy"},2000);
  }
  function fallback(){
    // textarea trick works even on non-HTTPS origins
    const ta=document.createElement("textarea");
    ta.value=val;ta.style.position="fixed";ta.style.opacity="0";
    document.body.appendChild(ta);ta.focus();ta.select();
    try{const ok=document.execCommand("copy");if(ok){onOk();}else{if(msg)msg.textContent="Select all + Ctrl+C to copy";}}
    catch(e){if(msg)msg.textContent="Select all + Ctrl+C to copy";}
    document.body.removeChild(ta);
  }
  if(navigator.clipboard&&window.isSecureContext){
    navigator.clipboard.writeText(val).then(onOk).catch(fallback);
  } else {
    fallback();
  }
}

async function regAgent(){
  const name=$("#na").value.trim(),role=$("#nr").value;
  const caps=$("#ncap")&&$("#ncap").checked?["admin"]:[];
  if(!name)return flash("name required",1);
  try{const d=await api("/api/agents/register",{method:"POST",admin:true,body:{name,role,caps}});
    flash("registered "+d.agent.id+(caps.length?" (admin cap)":""));
    showKeyModal(
      "Agent registered — API Key",
      "This is the API key for agent \""+d.agent.name+"\" ("+d.agent.role+"). "+
      "Give it to the agent so it can authenticate. The server never returns it again after this dialog.",
      d.api_key);
    loadAll();}catch(e){flash(e.message,1)}
}
async function issueJoinKey(){
  try{const d=await api("/api/admin/join-key",{method:"POST",admin:true,body:{}});
    showKeyModal(
      "Join Key Issued",
      "Hand this key to any new agent box along with your server URL. "+
      "The box runs: ./install.sh <your-server-url> <key>\n"+
      "It will enroll as 'observer' — promote its role from the Agents table once it appears. "+
      "One key works for multiple agents; issuing a new key immediately invalidates this one.",
      d.join_key);
    flash("join key issued");}catch(e){flash(e.message,1)}
}
function renderProjStoreForm(){
  const t=$("#pjstore");const f=$("#pjstore-form");if(!t||!f)return;
  const v=t.value;
  if(v==="github")f.innerHTML=`
    <div class="row"><label class="field-lbl">GitHub repo${tip("owner/repo — e.g. acme/artifacts. The master node will commit artifacts here on behalf of all workers.")}</label>
      <input id="pjgh-repo" class="grow" placeholder="owner/repo e.g. myorg/project-artifacts"></div>
    <div class="row"><label class="field-lbl">Branch</label>
      <input id="pjgh-branch" class="grow" placeholder="main"></div>
    <div class="row"><label class="field-lbl">Path prefix</label>
      <input id="pjgh-path" class="grow" placeholder="artifacts/ (default)"></div>
    <div class="row"><label class="field-lbl">Personal access token${tip("GitHub PAT with 'repo' scope. Stored server-side only — never returned to browser after save.")}</label>
      <input id="pjgh-token" class="grow" type="password" placeholder="ghp_…" autocomplete="new-password"></div>`;
  else if(v==="ado")f.innerHTML=`
    <div class="row"><label class="field-lbl">Organization${tip("Your Azure DevOps org name — the part in dev.azure.com/ORG")}</label>
      <input id="pjado-org" class="grow" placeholder="myorg"></div>
    <div class="row"><label class="field-lbl">Project</label>
      <input id="pjado-proj" class="grow" placeholder="MyProject"></div>
    <div class="row"><label class="field-lbl">Artifacts feed</label>
      <input id="pjado-feed" class="grow" placeholder="my-feed"></div>
    <div class="row"><label class="field-lbl">Personal access token${tip("ADO PAT with 'Packaging (read & write)' scope. Stored server-side only.")}</label>
      <input id="pjado-pat" class="grow" type="password" placeholder="PAT…" autocomplete="new-password"></div>`;
  else f.innerHTML=`
    <div class="row"><label class="field-lbl">Local path (optional)${tip("Extra directory to mirror artifacts into — e.g. a mounted network share. Leave blank to use the server data directory.")}</label>
      <input id="pjlocal-path" class="grow" placeholder="e.g. /mnt/nas/projects (blank = data dir)"></div>`;
}
async function createProject(){
  const name=$("#pjname").value.trim();if(!name)return flash("name required",1);
  const description=($("#pjdesc")?.value||"").trim();
  let ctx={};const raw=$("#pjctx").value.trim();
  if(raw){try{ctx=JSON.parse(raw)}catch(e){return flash("context must be valid JSON",1)}}
  // Build artifact_storage object from the form
  const stype=($("#pjstore")?.value||"local");
  let storage={type:stype};
  if(stype==="github"){
    const repo=($("#pjgh-repo")?.value||"").trim();
    const token=($("#pjgh-token")?.value||"").trim();
    if(!repo)return flash("GitHub repo required",1);
    if(!token)return flash("GitHub token required",1);
    storage={type:"github",repo,token,
              branch:($("#pjgh-branch")?.value||"main").trim()||"main",
              path:($("#pjgh-path")?.value||"artifacts/").trim()||"artifacts/"};
  }else if(stype==="ado"){
    const org=($("#pjado-org")?.value||"").trim();
    const project=($("#pjado-proj")?.value||"").trim();
    const feed=($("#pjado-feed")?.value||"").trim();
    const pat=($("#pjado-pat")?.value||"").trim();
    if(!org||!project||!feed||!pat)return flash("All ADO fields required",1);
    storage={type:"ado",org,project,feed,pat};
  }else{
    const lp=($("#pjlocal-path")?.value||"").trim();
    storage={type:"local",...(lp?{local_path:lp}:{})};
  }
  try{await api("/api/projects",{method:"POST",body:{name,description,context:ctx,artifact_storage:storage}});
    flash("project created");loadAll();}catch(e){flash(e.message,1)}
}
async function closeProject(id,status){
  try{await api("/api/projects/"+id,{method:"PATCH",body:{status}});
    flash("project "+status);loadAll();}catch(e){flash(e.message,1)}
}

/* ---------- storage migration ---------- */
function toggleMigrateForm(pid){
  const f=$("#migrate-form-"+pid);if(!f)return;
  const open=f.style.display==="none";
  f.style.display=open?"block":"none";
  if(open)renderMigrateFields(pid);
}
function renderMigrateFields(pid){
  const sel=$("#mig-type-"+pid);
  const f=$("#mig-fields-"+pid);
  if(!sel||!f)return;
  const v=sel.value;
  if(v==="github")f.innerHTML=`
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="mig-repo-${pid}" class="grow" placeholder="owner/repo — e.g. acme/project-artifacts" title="The GitHub repository to push artifacts into. Must exist before migrating.">
      <span style="font-size:10px;color:var(--fg3)">GitHub repo in owner/repo format</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="mig-branch-${pid}" class="grow" placeholder="Branch — default: main">
      <span style="font-size:10px;color:var(--fg3)">branch to commit into</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="mig-path-${pid}" class="grow" placeholder="Path prefix — default: artifacts/">
      <span style="font-size:10px;color:var(--fg3)">folder path inside the repo (trailing slash)</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="mig-token-${pid}" class="grow" type="password" placeholder="GitHub PAT — ghp_… (Contents: write scope)" autocomplete="new-password">
      <span style="font-size:10px;color:var(--fg3)">PAT with Contents: write — stored server-side, never returned</span></div></div>`;
  else if(v==="ado")f.innerHTML=`
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="mig-org-${pid}" class="grow" placeholder="ADO organisation — from dev.azure.com/ORG">
      <span style="font-size:10px;color:var(--fg3)">Azure DevOps org name</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="mig-proj-${pid}" class="grow" placeholder="ADO project name">
      <span style="font-size:10px;color:var(--fg3)">project containing the feed</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="mig-feed-${pid}" class="grow" placeholder="Artifacts feed name">
      <span style="font-size:10px;color:var(--fg3)">Universal Packages feed</span></div></div>
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="mig-pat-${pid}" class="grow" type="password" placeholder="ADO PAT — Packaging: read &amp; write" autocomplete="new-password">
      <span style="font-size:10px;color:var(--fg3)">PAT with Packaging scope</span></div></div>`;
  else f.innerHTML=`
    <div class="row"><div style="display:flex;flex-direction:column;gap:2px;flex:1">
      <input id="mig-lpath-${pid}" class="grow" placeholder="Extra local path — e.g. /mnt/nas/projects  (blank = data dir)">
      <span style="font-size:10px;color:var(--fg3)">optional extra local directory</span></div></div>`;
}
async function doMigrateStorage(pid){
  const sel=$("#mig-type-"+pid);if(!sel)return;
  const v=sel.value;
  const btn=$("#mig-btn-"+pid);
  const status=$("#mig-status-"+pid);
  let target={type:v};
  if(v==="github"){
    target.repo=($("#mig-repo-"+pid)?.value||"").trim();
    target.branch=($("#mig-branch-"+pid)?.value||"").trim()||"main";
    target.path=($("#mig-path-"+pid)?.value||"").trim()||"artifacts/";
    target.token=($("#mig-token-"+pid)?.value||"").trim();
    if(!target.repo||!target.token)return flash("GitHub repo and token are required",1);
  }else if(v==="ado"){
    target.org=($("#mig-org-"+pid)?.value||"").trim();
    target.project=($("#mig-proj-"+pid)?.value||"").trim();
    target.feed=($("#mig-feed-"+pid)?.value||"").trim();
    target.pat=($("#mig-pat-"+pid)?.value||"").trim();
    if(!target.org||!target.project||!target.feed||!target.pat)
      return flash("All ADO fields are required",1);
  }else{
    target.local_path=($("#mig-lpath-"+pid)?.value||"").trim()||null;
  }
  if(btn)btn.disabled=true;
  if(status)status.textContent="Verifying credentials…";
  try{
    const r=await api("/api/projects/"+pid+"/migrate-storage",
                      {method:"POST",admin:true,body:{target}});
    const msgs=[];
    if(r.pushed)msgs.push(r.pushed+" artifact(s) copied");
    if(r.skipped)msgs.push(r.skipped+" skipped (not on disk)");
    if(r.errors&&r.errors.length)msgs.push(r.errors.length+" error(s)");
    flash("Migrated to "+r.storage_type+(msgs.length?" — "+msgs.join(", "):""));
    if(status)status.textContent="";
    // Reload the project page so the storage tag updates
    pageProject(pid).then(setApp);
  }catch(e){
    if(status)status.textContent="";
    flash(e.message,1);
  }finally{
    if(btn)btn.disabled=false;
  }
}
async function addTaskToProject(pid){
  const title=$("#pttitle").value.trim();if(!title)return flash("title required",1);
  const kind=$("#ptkind").value,priority=parseInt($("#ptprio").value||"3",10);
  try{await api("/api/tasks",{method:"POST",body:{title,kind,priority,project_id:pid,spec:{}}});
    flash("task added");loadAll();
    pageProject(pid).then(setApp);}catch(e){flash(e.message,1)}
}
async function issueKey(id){
  try{const d=await api("/api/admin/keys",{method:"POST",admin:true,body:{agent_id:id}});
    showKeyModal(
      "New API Key — Old Key Revoked",
      "A new key has been issued for agent "+id+". The previous key is immediately invalid. "+
      "Update the agent's configuration with this new key before restarting it.",
      d.api_key);
    loadAll()}catch(e){flash(e.message,1)}
}
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
    if(location.hash.startsWith("#/task/"))pageTask(id).then(setApp)}
  catch(e){flash(e.message,1)}
}
async function taskReview(id,verdict){
  const note=($("#rnote")?$("#rnote").value.trim():"");
  // Review must be done as a real reviewer agent (qa/reviewer/orchestrator).
  // Use the agent identity when present; otherwise the admin token is rejected
  // server-side with a clear message.
  const asAgent=!!localStorage.getItem("mesh_agent_key");
  try{await api(`/api/tasks/${id}/review`,{method:"POST",admin:!asAgent,body:{verdict,note}});flash(verdict+" ok");loadAll();
    pageTask(id).then(setApp)}
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
async function boot(){
  if(!hasSession()){renderLock();return}
  // Re-validate the stored token before rendering any internal pages.
  // This catches stale tokens from a previous session or a server restart.
  const admTok=sessionStorage.getItem("mesh_adm");
  const agentKey=localStorage.getItem("mesh_agent_key");
  if(admTok){
    try{
      const r=await fetch(BASE+"/api/admin/stats",{headers:{Authorization:"Bearer "+admTok}});
      if(!r.ok){
        // Token is no longer valid — clear it and show the lock screen
        ADMIN=null;sessionStorage.removeItem("mesh_adm");
        renderLock("Session expired or token invalid — please log in again.");
        return;
      }
    }catch(e){
      // Network error — still let through (server might be momentarily unreachable)
    }
  }else if(agentKey){
    try{
      const r=await fetch(BASE+"/api/agents/me",{headers:{Authorization:"Bearer "+agentKey}});
      if(r.status===403||r.status===401){
        localStorage.removeItem("mesh_agent_key");
        renderLock("Agent key is no longer valid — please log in again.");
        return;
      }
    }catch(e){/* network — let through */}
  }
  // Reset #app to sidebar flex layout (may have been set to centering by renderLock)
  $("#app").style.cssText="display:flex;min-height:100vh";
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
    ap.add_argument("--mode", default=os.environ.get("MESH_MODE", "master"),
                    choices=["master", "guest"],
                    help="'master' (default) runs the full admin console; "
                         "'guest' hides the admin console and exposes /node-chat "
                         "for agent operators on guest boxes")
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
    Handler.node_mode = args.mode   # "master" | "guest"

    # Extract logo base64 once at startup for reuse in node-chat page
    import re as _re
    _m = _re.search(r'data:image/png;base64,([A-Za-z0-9+/=]+)', UI_HTML)
    Handler._logo_b64 = _m.group(1) if _m else ""

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
