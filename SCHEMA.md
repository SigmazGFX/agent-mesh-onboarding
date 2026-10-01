# agent-mesh — API-based agent org endpoint

Version: 0.1 (2026-10-01)
Status: contract-first — this file is the source of truth; code conforms to it.

## What it is

A single portable Python module (`mesh_server.py`) that exposes an HTTP API so
agents in a development organization can **check in, get work, report status,
and upload results**. It replaces ad-hoc relay chatter as the *work* channel:
the relay stays for human/social traffic and emergency pings; all task flow
goes through this endpoint.

Design goals:
- **Portable**: one file + stdlib only (Python ≥3.9). Drop into any agent box,
  point it at a data dir, done. No venv, no pip.
- **Role hierarchy**: every registered agent has a role. Roles gate what it
  may do (dispatch vs. pull vs. review). Hierarchy is defined by config, not
  hardcoded.
- **API-key protected**: every call authenticates with a per-agent key. Keys
  are managed (create/revoke/list) through a web admin interface, which is
  itself protected by an admin token.
- **Durable**: SQLite (WAL) for agents, tasks, events, artifacts. Survives
  restarts. Offline workers still get their queue when they check back in.

Base URL: `http://127.0.0.1:4850` (override with env `MESH_PORT`). Binds
`127.0.0.1` only by default; set `MESH_HOST=0.0.0.0` to expose on LAN
(only after you've decided the network path).

## Roles & permissions

| Role | May dispatch | May pull work | May submit results | May review/approve | May cancel others' tasks |
|---|---|---|---|---|---|
| `orchestrator` | yes | yes | yes | yes | yes |
| `planner` | yes | yes | yes | yes | no |
| `worker` | no | yes | yes | no | no |
| `qa` | no | yes | yes | yes | no |
| `reviewer` | no | no | no | yes | no |
| `observer` | no | no | no | no | no |

Rules enforced server-side:
- Only `orchestrator`/`planner` may create tasks via `POST /api/tasks`.
- `GET /api/work/pull` returns tasks assigned to the caller's agent id (or
  unassigned tasks if the caller's role allows claiming). Workers claim by
  pulling; the task becomes `in_progress` under them.
- Only the assignee or a higher role may update/cancel a task.
- `qa`/`reviewer` may transition a task to `approved`/`rejected` via
  `POST /api/tasks/{id}/review`.
- An agent cannot promote its own role (admin action only).

## Auth model

Two kinds of credentials:
1. **Agent API keys** — one per agent. Sent as `Authorization: Bearer <key>`
   on every `/api/*` call. The key identifies the agent AND carries its role.
2. **Admin token** — protects the web UI and key-management endpoints
   (`/api/admin/*`). Set via env `MESH_ADMIN_TOKEN` or generated on first run
   (printed once to stdout + stored hashed in the DB). Never returned again.

Key storage: SHA-256 hash only. Plaintext shown exactly once at creation.

## Data model

### Agent
```json
{
  "id": "haans-grueber-d23a5adf",
  "name": "Haans",
  "role": "worker",
  "caps": ["task.worker", "file.transfer"],
  "created_at": 1759315200.0,
  "last_seen": 1759318800.0,
  "status": "online"
}
```
`status`: `online` (seen within heartbeat window), `offline`, `disabled`.

### Task
```json
{
  "id": "task-uuid",
  "title": "Add latency chart to dashboard",
  "kind": "code",
  "spec": {"repo": "~/Work/box-pulse", "branch": "main", "instructions": "..."},
  "priority": 3,
  "status": "queued",
  "created_by": "vps-orch-01",
  "assigned_to": null,
  "deadline": null,
  "retry": {"max": 3, "backoff_s": 10},
  "result": null,
  "artifacts": [],
  "created_at": 1759315200.0,
  "updated_at": 1759315200.0
}
```
Task status lifecycle:
`queued → claimed → in_progress → done | failed | cancelled`
then optionally `→ approved | rejected` (by qa/reviewer).

### Event (audit log)
Every state change appends an event: `{ts, actor, type, task_id, detail}`.
Queryable via `GET /api/events`.

### Artifact
Uploaded files/results: `{id, task_id, name, size, sha256, uploaded_by, ts,
path}`. Served back via `GET /api/artifacts/{id}`.

## Endpoints

All request/response bodies are JSON. Timestamps are Unix epoch seconds
(float). List endpoints return `{"items": [...]}` newest-first and accept
`?limit=` (default 50, max 200). Errors are `{"detail": str}` with proper
HTTP status (401 bad/missing key, 403 role-forbidden, 404 unknown id,
422 validation, 500 internal).

### Health
| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/api/health` | none | `{"ok": true, "version": "0.1", "agents": N, "tasks_queued": N}` |

### Agents (self-service)
| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api/agents/join` | **none** | Open self-service join. A new box registers itself as `observer` and gets a one-time key. Body: `{name, caps?}`. Admin then assigns a real role. This is how remote agents check in without an admin token. |
| POST | `/api/agents/register` | admin | Create agent with a chosen role + return plaintext key ONCE. Body: `{name, role, caps?}` |
| GET | `/api/agents` | agent | List agents (id, name, role, status, last_seen). No keys. |
| GET | `/api/agents/me` | agent | Caller's own record. |
| POST | `/api/agents/checkin` | agent | Heartbeat. Updates `last_seen`, sets `online`. Body optional: `{load?, queue_depth?}`. Returns current time + pending task count for caller. |
| PATCH | `/api/agents/me` | agent | Update own `caps`/`name`. Role changes require admin. |
| DELETE | `/api/agents/{id}` | admin | Disable/remove agent. Revokes its key. |

### Work queue
| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api/tasks` | orchestrator/planner | Create task. Body: `{title, kind, spec, priority?, deadline?, assigned_to?, retry?}`. Returns created task. |
| GET | `/api/tasks` | agent | List tasks. Filters: `?status=`, `?assigned_to=`, `?created_by=`. |
| GET | `/api/tasks/{id}` | agent | One task (must be related: creator, assignee, or higher role). |
| GET | `/api/work/pull` | worker+ | Claim next eligible task for caller. Sets `assigned_to=caller`, status `claimed`. Returns task or `{"task": null}` if none. |
| POST | `/api/tasks/{id}/start` | assignee | `claimed → in_progress`. |
| POST | `/api/tasks/{id}/progress` | assignee | Body: `{pct?, note?}`. Appends progress event. |
| POST | `/api/tasks/{id}/result` | assignee | Body: `{status: ok\|failed\|partial, output?, error?}`. Sets `done`/`failed`. |
| POST | `/api/tasks/{id}/cancel` | creator/higher | Cancel queued/in-progress task. |
| POST | `/api/tasks/{id}/review` | qa/reviewer/orchestrator | Body: `{verdict: approved\|rejected, note?}`. |
| POST | `/api/tasks/{id}/requeue` | orchestrator/planner | Put a failed/cancelled task back to `queued`. |

### Artifacts
| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api/artifacts` | agent (assignee of task) | Upload result file. Multipart form: `file` + fields `task_id`, `name?`. Returns artifact record. |
| GET | `/api/artifacts` | agent | List artifacts (filters: `?task_id=`). |
| GET | `/api/artifacts/{id}` | agent | Download. `Content-Disposition: attachment`. |

### Events
| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/api/events` | agent | Audit log. Filters: `?task_id=`, `?actor=`, `?type=`. Newest first. |

### Admin (web UI + key management)
Protected by `Authorization: Bearer <ADMIN_TOKEN>`.
| Method | Path | Description |
|---|---|---|
| GET | `/api/admin/keys` | List agents with key metadata (no plaintext). |
| POST | `/api/admin/keys` | Issue a NEW key for an existing agent (old key revoked). Body: `{agent_id}`. Returns plaintext once. |
| DELETE | `/api/admin/keys/{agent_id}` | Revoke agent's key (agent disabled until re-issued). |
| PATCH | `/api/admin/agents/{id}` | Change role/status. Body: `{role?, status?}`. |
| GET | `/api/admin/stats` | Org-wide counters: agents by role, tasks by status, throughput. |

### Web UI
| Route | Description |
|---|---|
| GET `/` | Admin dashboard (HTML). Prompts for admin token (stored in localStorage). Shows agents, live task board, events feed, key management. Auto-refreshes. |
| GET `/static/...` | Inline-served CSS/JS (no build step). |

The web UI is the ONLY place keys are created/revoked visually. It calls the
`/api/admin/*` endpoints with the admin token.

## Portability / deployment

Single file. Run:
```bash
python3 mesh_server.py --data ~/.local/state/agent-mesh --port 4850
```
- `--data` : directory for SQLite DB + artifact store (default
  `~/.local/state/agent-mesh`).
- `--port` / `MESH_PORT` : listen port (default 4850).
- `--host` / `MESH_HOST` : bind address (default `127.0.0.1`).
- `MESH_ADMIN_TOKEN` : preset admin token (else generated on first run).

To add to any agent: copy `mesh_server.py` to the box, run it (optionally as
a systemd user service), then register the agent via the web UI and hand the
plaintext key to that agent's config. The agent talks to it with plain HTTP
Bearer calls — no SDK required, but a tiny client helper is included
(`MeshClient` class at bottom of the file) for convenience.

systemd unit template (user service):
```ini
[Unit]
Description=agent-mesh endpoint
After=network.target

[Service]
ExecStart=/usr/bin/python3 %h/Work/agent-mesh/mesh_server.py --data %h/.local/state/agent-mesh --port 4850
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
```

## Relationship to A2A-over-relay

This endpoint is the **work channel**; the relay A2A protocol remains the
**social/emergency channel**. Mapping:
- A2A `task.dispatch` ≈ `POST /api/tasks` + `GET /api/work/pull`
- A2A `task.progress` ≈ `POST /api/tasks/{id}/progress`
- A2A `task.result` ≈ `POST /api/tasks/{id}/result` (+ artifact upload)
- A2A `hello`/`heartbeat` ≈ `POST /api/agents/checkin`
- A2A `note` (freeform) → stays on the relay, NOT here.

An orchestrator on a VPS runs this server; workers on each box poll
`/api/work/pull`. Relay is used to announce "new orchestrator online at
<url>" and for anything conversational.

## Non-goals (v0.1)
- No WebSocket/SSE push (polling is fine at this scale; add later if needed).
- No multi-tenant isolation (single org per instance).
- No TLS termination (run behind a reverse proxy / tailscale if exposing).
- No built-in LLM logic — this is pure coordination plumbing.
