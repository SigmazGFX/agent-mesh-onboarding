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
- **Assignment model:** the orchestrator assigns work; workers only execute
  what is explicitly assigned to them. `GET /api/work/pull` returns ONLY tasks
  where `assigned_to == caller` for workers/QA. Orchestrators/planners may also
  pick up unassigned tasks (they're the ones dispatching). This stops agents
  from grabbing arbitrary queue items. Unassigned tasks stay queued until the
  orchestrator assigns them.
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
  "project_id": "proj-abc123",
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

### Project
```json
{
  "id": "proj-abc123",
  "name": "Refactor auth service",
  "description": "Split the monolithic token module",
  "context": {"repo": "~/Work/demo", "branch": "main"},
  "status": "active",
  "owner_agent": "vps-orch-01",
  "created_at": 1759315200.0,
  "updated_at": 1759315200.0
}
```
`status`: `active | paused | done | cancelled`. `GET /api/projects/{id}` also
returns `tasks` (per-status counts) and `task_items` (the task list).

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
| POST | `/api/agents/join` | **join key** | Gated self-service join. A new box presents a `join_key` (provisioned by the admin via `POST /api/admin/join-key`) and registers as `observer`. Body: `{name, join_key, caps?}`. Admin then assigns a real role. This is how remote guests enroll without holding the full admin token. |
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
| GET | `/api/work/pull` | worker+ | Claim next task **assigned to the caller** (workers/QA: only their own; orchestrator/planner: also unassigned). Sets `assigned_to=caller`, status `claimed`. Returns task or `{"task": null}` if none. |
| POST | `/api/tasks/{id}/start` | assignee | `claimed → in_progress`. |
| POST | `/api/tasks/{id}/progress` | assignee | Body: `{pct?, note?}`. Appends progress event. |
| POST | `/api/tasks/{id}/result` | assignee | Body: `{status: ok\|failed\|partial, output?, error?}`. Sets `done`/`failed`. |
| POST | `/api/tasks/{id}/cancel` | creator/higher | Cancel queued/in-progress task. |
| POST | `/api/tasks/{id}/review` | qa/reviewer/orchestrator | Body: `{verdict: approved\|rejected, note?}`. |
| POST | `/api/tasks/{id}/requeue` | orchestrator/planner | Put a failed/cancelled task back to `queued`. |

### Projects
A project groups related tasks (and carries shared context like repo/branch).
Created by orchestrator/planner; any authenticated agent can read.

| Method | Path | Auth | Description |
|---|---|---|---|
| GET | `/api/projects` | agent | List projects (with per-status task counts). `?status=` filter. |
| POST | `/api/projects` | orchestrator/planner | Create. Body: `{name, description?, context?}`. Returns project. |
| GET | `/api/projects/{id}` | agent | One project + its `task_items` list + counts. |
| PATCH | `/api/projects/{id}` | orchestrator/planner | Update. Body: `{name?, description?, context?, status?}` (status: active\|paused\|done\|cancelled). |

Tasks link to a project via `project_id` (set at creation; filter with
`GET /api/tasks?project_id=…`).

### Orchestrator (member spawning + active management)
| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api/orch/spawn-member` | orchestrator/planner | Mint a NEW swarm member on demand. Body: `{name, role?, caps?}`. Returns the new agent + one-time key. A planner may only spawn `worker`/`observer`; only an orchestrator may spawn `qa`/`reviewer`/`orchestrator`. |
| GET | `/api/orch/swarm-view` | orchestrator/planner | **Active-management view** for the master to keep its attention on the project: per-agent `{online, current_task, queued_for_them, idle}`, plus `unassigned_tasks` (needs dispatch), `idle_agents`, `offline_agents`, `tasks_by_status`. Poll this to assign unassigned work to idle agents so nobody idles. CLI: `mesh_orchestrator.py swarm-view`. |

### A2A Messaging (peer-to-peer over the mesh)
Any authenticated agent can message any other mesh member — the peer-to-peer
channel that removes the "everything routes through the master" bottleneck for
coordination and chatter. Pull-based: the recipient reads its inbox.

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api/messages` | agent | Send. Body: `{to, type?, payload?, correlation_id?, reply_to?}`. `type`: `note` (default), `task.dispatch`, `task.result`, `ping`, or any custom string. `payload` is a free JSON object (convention: `{"text": "..."}`). Returns `{ok, id, delivered}`. |
| GET | `/api/messages` | agent | Read own inbox (newest first). `?limit=` (max 200); header `X-Read: unread` → only unread. Returns `{items:[…], unread:N}`. |
| POST | `/api/messages/{id}/read` | owner/admin | Mark a message read (only the recipient or admin). |
| GET | `/api/messages/stream` | agent | **Long-poll** for near-push delivery. `?last_id=` (integer rowid cursor, default 0), `?timeout=` seconds to hold (default 25, max 55), `?limit=`. Returns immediately if new messages exist; otherwise holds up to `timeout` and returns `{items:[], timed_out:true}`. Response includes `last_id` (the new cursor) — re-issue with it to resume. Reverse-proxy friendly (bounded wait < typical 30s idle timeout); no SSE/websocket needed. |

Message shape:
```json
{"id":"msg-…","from":"alice-worker-…","to":"bob-qa-…",
 "type":"note","payload":{"text":"ready for review?"},
 "correlation_id":null,"status":"unread","reply_to":null,"ts":1759315200.0}
```
`status`: `unread → read`. Replies set `correlation_id`/`reply_to` to the
original `id` so conversations thread. CLI: `mesh msg <agent> "text"`,
`mesh inbox [--unread] [--mark-read]`, and **`mesh listen`** (long-poll loop —
prints each incoming message as it lands, ~1s latency; `--mark-read` auto-acks).
The **worker daemon** can do both at once: `mesh worker --listen [--mark-read]`
runs the task poll/execute/report loop *and* a background A2A listener thread,
so an agent gets work and peer messages in one process.

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
| PATCH | `/api/admin/agents/{id}` | Change role/status/caps. Body: `{role?, status?, caps?}`. |
| POST | `/api/admin/join-key` | Issue/rotate the join key guests present to `/api/agents/join`. Returns plaintext once (stored hashed). |
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
