# agent-mesh — Operator's Manual

How to deploy, run, secure, and operate the agent org work endpoint.
The API contract lives in [`SCHEMA.md`](SCHEMA.md); this is the runbook.

---

## 1. What it is (30-second version)

`agent-mesh` is a single-file HTTP server that lets agents in a development
organization **check in, get work, report status, and upload results** through
a role-gated REST API. It is the *work channel* for the org; the omarchy-relay
A2A protocol stays the *social/emergency* channel.

- One file (`mesh_server.py`), Python ≥3.9 **standard library only** — no venv,
  no pip, no build step. That's what makes it portable.
- Durable state in SQLite (WAL) + an artifact store on disk. Survives restarts;
  offline workers still find their queue when they check back in.
- Every call authenticates with a per-agent API key. Keys are created, rotated,
  and revoked from a web console gated by an admin token.

```
                    ┌─────────────────────────────────────────────┐
   orchestrator ───▶│  POST /api/tasks        (dispatch work)     │
   planner      ───▶│  GET  /api/admin/*      (manage keys/roles) │
                    ├─────────────────────────────────────────────┤
   worker       ───▶│  GET  /api/work/pull    (claim a task)      │
   qa         ───▶│  POST /api/tasks/{id}/review                 │
                    │  POST /api/agents/checkin  (all roles)      │
                    │  POST /api/artifacts       (upload results) │
                    └─────────────────────────────────────────────┘
                            http://127.0.0.1:4850  (localhost by default)
```

---

## 2. Deploying

### Option A — this box (already done)

It runs as a systemd **user** service. Verify:

```bash
systemctl --user status agent-mesh
curl -s http://127.0.0.1:4850/api/health
# {"ok": true, "version": "0.1", "agents": N, "tasks_queued": N}
```

The unit file is `~/.config/systemd/user/agent-mesh.service`:

```ini
[Unit]
Description=agent-mesh endpoint (agent org work channel)
After=network.target

[Service]
Type=simple
Environment=MESH_ADMIN_TOKEN=$(cat %h/.local/state/agent-mesh/admin_token)
ExecStart=/usr/bin/python3 %h/Work/agent-mesh/mesh_server.py \
            --data %h/.local/state/agent-mesh --port 4850
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
```

Day-to-day:

```bash
journalctl --user -u agent-mesh -f        # live logs
systemctl --user restart agent-mesh       # after editing mesh_server.py
systemctl --user stop agent-mesh          # pause
```

### Option B — any other machine (the portable path)

Because it's stdlib-only, deployment is copy-and-run:

```bash
# 1. Copy the one file to the target box (scp, relay send, git, whatever)
scp mesh_server.py user@box:~/agent-mesh/

# 2. Run it (foreground, to see the first-run admin token)
python3 ~/agent-mesh/mesh_server.py --data ~/.local/state/agent-mesh --port 4850
```

First run prints **one line you must capture**:

```
[agent-mesh] generated admin token: adm_XXXXXXXXXXXX
[agent-mesh] (shown once — store it; the UI needs it)
```

That token unlocks the web console and all `/api/admin/*` endpoints. Store it
somewhere secret (env var, password manager). It is never shown again.

To make it persistent, wrap it in a systemd user unit (same shape as Option A)
or any supervisor. The unit template is in [`SCHEMA.md`](SCHEMA.md) §Portability.

### Configuration surface

Everything is flags or env vars — there is no config file to edit:

| Knob | Flag | Env | Default | Meaning |
|---|---|---|---|---|
| State dir | `--data` | — | `~/.local/state/agent-mesh` | SQLite DB + `artifacts/` |
| Port | `--port` | `MESH_PORT` | `4850` | Listen port |
| Bind | `--host` | `MESH_HOST` | `127.0.0.1` | `0.0.0.0` to expose on LAN |
| Admin token | — | `MESH_ADMIN_TOKEN` | *(generated)* | Preset the admin token |

If `MESH_ADMIN_TOKEN` is set, it overrides the generated one (and is persisted
to the DB meta so restarts stay consistent).

---

## 3. Security model

Two credential types, both sent as `Authorization: Bearer <token>`:

1. **Agent API keys** (`mesh_…`) — one per agent. Identify the agent *and*
   carry its role. Stored server-side as SHA-256 hashes only; plaintext is
   returned exactly once at register/rekey. An agent may also hold the
   **`admin` capability** (granted at register or via the console), which lets
   it use the full web console *as itself* — its role still gates what it can
   do (e.g. only qa/reviewer/orchestrator can approve).
2. **Admin token** (`adm_…`) — the human operator's key. Gates the console and
   `/api/admin/*`. It is a top-privilege *viewer*, but it deliberately cannot
   rubber-stamp a review — approving requires a real reviewer-role agent.

Rules that keep it safe:
- **Localhost by default.** Nothing is reachable off-box unless you set
  `MESH_HOST=0.0.0.0`. Don't do that without a network decision (tailscale /
  trusted LAN) and ideally TLS via a reverse proxy.
- **Role gating is server-side.** A worker key literally cannot dispatch tasks
  or review — the requests 403 before touching data. Review additionally
  requires a genuine qa/reviewer/orchestrator principal.
- **Key rotation is instant.** Reissuing a key revokes the old one immediately
  (verified in tests). Revoke = agent disabled until reissued.
- **No secrets in responses.** Agent lists never include keys; only metadata.

Threat notes (be honest about scope):
- This is coordination plumbing for a trusted internal org, not a public API.
  There is no rate limiting, no TLS of its own, no multi-tenant isolation.
- If you expose it beyond localhost, put it behind something that terminates
  TLS and adds authz (reverse proxy / tailscale serve). The app assumes the
  transport is already trustworthy.

---

## 4. Using it — the web console

Open **`http://127.0.0.1:4850/`**. It's a multi-page app with a top nav bar:
**Dashboard · Agents · Tasks · Events · Artifacts** (hash-routed, no build step).

1. **Unlock** — two ways:
   - **Admin token** (human operator): top-privilege viewer; can manage keys and
     roles, but *cannot* approve/review (that needs a reviewer-role agent).
   - **Agent key with the `admin` capability**: opens the full console *as that
     agent*, so its role still gates actions — a QA agent unlocked this way can
     approve/review from the UI. ("lock" button re-locks.)
2. **Dashboard** — stat tiles (queued / active / done / failed / agents by
   role), recent tasks, and a live event feed. Auto-refreshes every 5s.
3. **Agents** — register (name + role + optional *admin cap* → one-time key
   prompt), change role via the row dropdown, grant/revoke the **admin cap**
   (console access) per agent, rekey / revoke / delete.
4. **Tasks** — create task (title + kind + priority), filter by status, click
   any row for the **task detail page**: full spec, result, actions (start /
   cancel / requeue), inline **review** (approve/reject when done/failed — only
   visible/enabled for reviewer roles), artifact list with download links, and
   the task's event trail.
5. **Events** — full audit log with actor / type / task-id filters.
6. **Artifacts** — everything uploaded, with task links and downloads.

---

## 5. Using it — as an agent (API)

An agent holds one `mesh_…` key and talks plain HTTP. Two ways:

### 5a. Bundled client (recommended)

`MeshClient` ships at the bottom of `mesh_server.py` — import it, no install:

```python
import sys
sys.path.insert(0, "/home/sigmaxgfx/Work/agent-mesh")   # wherever it lives
from mesh_server import MeshClient

mc = MeshClient("http://127.0.0.1:4850", "mesh_YOUR_KEY")

mc.checkin(load=0.2)                       # heartbeat; -> {pending_tasks}
task = mc.pull()                           # claim next task, or None
if task:
    mc.start(task["id"])                   # claimed -> in_progress
    mc.progress(task["id"], pct=50, note="halfway")
    mc.report(task["id"], "ok", output={"commit": "abc"})
    mc.upload(task["id"], "/path/result.tar.gz")   # optional artifact
```

### 5b. Raw curl (no SDK)

```bash
KEY=mesh_YOUR_KEY
B=http://127.0.0.1:4850

curl -s $B/api/agents/checkin -X POST -H "Authorization: Bearer $KEY" \
     -H 'Content-Type: application/json' -d '{"load":0.2}'

curl -s $B/api/work/pull -H "Authorization: Bearer $KEY"

curl -s $B/api/tasks/task-XXXX/start -X POST -H "Authorization: Bearer $KEY"

curl -s $B/api/tasks/task-XXXX/result -X POST \
     -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
     -d '{"status":"ok","output":{"commit":"abc"}}'

# upload an artifact (multipart)
curl -s $B/api/artifacts -X POST -H "Authorization: Bearer $KEY" \
     -F "task_id=task-XXXX" -F "file=@/path/result.tar.gz"
```

### 5c. A realistic worker loop

A worker just polls. Sketch (add a sleep/backoff between pulls):

```python
while True:
    mc.checkin()
    task = mc.pull()
    if not task:
        time.sleep(30); continue
    try:
        mc.start(task["id"])
        # ... actually do the work, posting mc.progress(...) along the way ...
        mc.report(task["id"], "ok", output=result_dict)
        mc.upload(task["id"], artifact_path)      # if there's a file
    except Exception as e:
        mc.report(task["id"], "failed", error=str(e))
```

---

## 6. Roles & who can do what

| Role | dispatch | pull | results | review | cancel others |
|---|---|---|---|---|---|
| `orchestrator` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `planner` | ✓ | ✓ | ✓ | ✓ | – |
| `worker` | – | ✓ | ✓ | – | – |
| `qa` | – | ✓ | ✓ | ✓ | – |
| `reviewer` | – | – | – | ✓ | – |
| `observer` | – | – | – | – | – |

Task lifecycle:
`queued → claimed → in_progress → done | failed | cancelled`, then optionally
`→ approved | rejected` (by qa/reviewer/orchestrator). A `failed`/`cancelled`/
`rejected` task can be `requeue`d by an orchestrator/planner.

Typical org wiring:
- **Orchestrator** (e.g. the future VPS Hermes) creates tasks, watches the board.
- **Planners** break big work into tasks too.
- **Workers** (Haans, Harry, …) pull and execute.
- **QA / reviewer** approve or reject finished work.
- **Observer** read-only visibility (dashboards, humans).

---

## 7. Endpoints at a glance

Full request/response shapes: [`SCHEMA.md`](SCHEMA.md). Errors are
`{"detail": str}` with proper codes (401 bad/missing key, 403 role-forbidden,
404 unknown id, 422 validation, 500 internal). Lists return
`{"items":[...]}` newest-first with `?limit=` (default 50, max 200). Timestamps
are Unix epoch seconds (float).

| Area | Endpoints |
|---|---|
| Health | `GET /api/health` (no auth) |
| Agents | `POST /api/agents/register` (admin) · `GET /api/agents` · `GET/PATCH /api/agents/me` · `POST /api/agents/checkin` · `DELETE /api/agents/{id}` (admin) |
| Tasks | `POST /api/tasks` (orch/planner) · `GET /api/tasks` · `GET /api/tasks/{id}` · `GET /api/work/pull` · `POST /api/tasks/{id}/start\|progress\|result\|cancel\|review\|requeue` |
| Artifacts | `POST /api/artifacts` (multipart) · `GET /api/artifacts` · `GET /api/artifacts/{id}` |
| Events | `GET /api/events[?task_id=&actor=&type=]` |
| Admin | `GET/POST /api/admin/keys` · `DELETE /api/admin/keys/{id}` · `PATCH /api/admin/agents/{id}` · `GET /api/admin/stats` |
| Console | `GET /` (HTML) |

---

## 8. Operations & troubleshooting

**Health check**
```bash
curl -s http://127.0.0.1:4850/api/health
```

**"Connection refused"** — service down:
```bash
systemctl --user status agent-mesh
journalctl --user -u agent-mesh -n 50 --no-pager
systemctl --user restart agent-mesh
```

**"403 valid agent API key required"** — wrong/revoked key, or the agent was
disabled. Reissue from the console (rekey) and update the agent's config.

**"403 role 'worker' cannot create tasks"** — expected; that's the hierarchy
working. Use an orchestrator/planner key for dispatch.

**Lost the admin token** — it's in `~/.local/state/agent-mesh/admin_token` on
this box. On a fresh box where it wasn't saved, the only reset is wiping the
state dir (`rm -rf ~/.local/state/agent-mesh`) and restarting, which regenerates
a token (and loses agents/tasks — do this deliberately).

**Port already in use** — pick another: `MESH_PORT=4851 python3 mesh_server.py …`
(or edit the unit). Check the squatter: `ss -ltnp | grep 4850`.

**Backing up** — copy the whole state dir; it's self-contained:
```bash
tar czf agent-mesh-backup.tgz -C ~/.local/state agent-mesh
```

**Resetting to empty** — stop the service, remove the state dir, start it. A
fresh admin token is printed.

---

## 9. Roadmap / non-goals (v0.1)

Deliberately out of scope for now (add when needed):
- WebSocket/SSE push (polling is fine at this scale).
- Multi-tenant isolation (one org per instance).
- Built-in TLS (terminate upstream if exposing).
- LLM logic — this is pure coordination plumbing.

Natural next steps once the VPS orchestrator lands: run the server on the VPS,
expose it over tailscale, register each box's agent as a worker, and point the
relay at it for announcements ("new orchestrator online at <url>").
