# agent-mesh

**A portable work channel for a swarm of agents.** One small Python server that
lets agents **check in, get assigned work, report status, and upload results**,
with a role hierarchy (orchestrator / planner / worker / qa / reviewer /
observer) enforced server-side. A human governs it from a web console; an
orchestrator (human or agent) dispatches work; workers execute it.

**Clone it, run one command, and you have a swarm endpoint.** No venv, no pip,
no build step — pure Python ≥3.9 standard library. Any agent platform can join:
Hermes, Claude Code, Codex, OpenCode, a custom LLM loop, or a plain script. It
talks plain HTTP + JSON with Bearer auth — there is no SDK to adopt.

```
   YOU ─▶ master node  ──▶  mesh_server.py  (one file, stdlib only)
                              │  • web console  (govern roles, keys, tasks)
                              │  • REST API     (checkin / pull / result / upload)
                              │  • durable state (SQLite WAL + artifacts)
                              └──▶ guest nodes (workers / qa / …) poll & report
```

---

## The 60-second tour

| You are… | Do this | Read next |
|---|---|---|
| **Setting up a swarm** (master) | `./install.sh master` | [ADMIN.md](ADMIN.md) |
| **Joining an existing swarm** (guest) | `./install.sh guest` | [AGENT-INTEGRATION.md](AGENT-INTEGRATION.md) |
| **An agent** being onboarded | get your `mesh_…` key, run the worker loop | [AGENT-INTEGRATION.md](AGENT-INTEGRATION.md) |
| **Dispatching work** (orchestrator) | create project → add tasks → assign → track | [ORCHESTRATOR.md](ORCHESTRATOR.md) |
| **Integrating a non-Python runtime** | raw-HTTP examples below + SCHEMA | [SCHEMA.md](SCHEMA.md) |
| **Deploying behind a proxy** (shared domain) | `--base-path /agent-mesh` | [REMOTE-DEPLOY.md](REMOTE-DEPLOY.md) |

Everything else in this repo is detail. Start with the row that matches you.

---

## Install

Pointing at this repo and running the installer is the whole setup. It asks
whether this box is a **master** (runs the orchestrator) or a **guest** (enrolls
in an existing swarm):

```bash
git clone https://github.com/SigmazGFX/agent-mesh.git
cd agent-mesh
./install.sh                 # interactive: pick master | guest
# or non-interactive:
./install.sh master          # stand up the orchestrator HERE
./install.sh guest           # enroll THIS box into an existing swarm
```

### Master mode (the first box)
Runs the endpoint as a systemd **user** service (or a background process where
systemd isn't available — containers/macOS), stores the admin token, prints the
console URL + admin token, and issues a **join key** to hand out to guests.

```
Console     : http://127.0.0.1:4850/   (unlock with the admin token)
Admin token : adm_XXXXXXXXXXXX
Join key for guests (hand this out):
------------------------------------
join_YYYYYYYYYY
------------------------------------
```

**Capture both secrets now.** The admin token isn't shown again by the server
(it *is* saved to `~/.local/state/agent-mesh/admin_token`, mode 600); the join
key can be re-issued anytime (which invalidates the old one).

### Guest mode (joining a swarm)
Asks for the swarm's **base URL**, a **join key**, and a **friendly name** for
the agent (defaults to the hostname — pick something unique, since names must be
unique in the swarm). Then:
1. Enrolls via `POST /api/agents/join` (presents the join key; lands as
   `observer`).
2. Stores `{base_url, api_key, agent_id}` in `~/.config/agent-mesh/config.json`.
3. Checks in so the master sees the new agent.
4. Installs a `mesh` CLI on PATH (`~/.local/bin/mesh`).

Then the **swarm admin assigns a real role** in the console (Agents page) —
until then the agent can only observe. Day-to-day from the box:

```bash
mesh status        # who am I, what role
mesh peers         # who's in the swarm + who's online
mesh worker        # run the poll/execute/report loop (daemon)
mesh checkin       # one-off heartbeat
mesh pull          # claim next task (needs worker+ role)
mesh msg <agent> "text"      # message another agent (A2A over the mesh)
mesh inbox --unread          # read your peer messages
mesh listen                    # long-poll: print each incoming message live
```

---

## How a swarm actually works

The mental model in one paragraph: **the orchestrator assigns work; workers only
do what's assigned to them.** This is deliberate — it stops the classic failure
where every agent grabs the whole queue and does nothing.

1. **Orchestrator** creates a **project**, breaks it into **tasks**, and
   **assigns** each task to a specific capable member (`assigned_to`).
2. **Workers** poll `GET /api/work/pull`, which returns *only* tasks assigned to
   them. They `start` → do the real work → `progress` → `result` (+ optional
   artifact upload).
3. **QA / reviewer** approve or reject finished work.
4. The orchestrator **tracks** the swarm (`swarm-view`) and keeps it busy:
   assigns unassigned work to idle agents, reassigns stale tasks, requeues
   failures. On a Hermes master this runs as an **autonomous watchdog** (see
   [ORCHESTRATOR.md](ORCHESTRATOR.md)).

Task lifecycle:
`queued → claimed → in_progress → done | failed | cancelled → approved | rejected`

### Roles & what they may do

| Role | dispatch | pull | results | review | cancel others |
|---|---|---|---|---|---|
| orchestrator | ✓ | ✓ | ✓ | ✓ | ✓ |
| planner | ✓ | ✓ | ✓ | ✓ | – |
| worker | – | ✓ | ✓ | – | – |
| qa | – | ✓ | ✓ | ✓ | – |
| reviewer | – | – | – | ✓ | – |
| observer | – | – | – | – | – |

Enforced **server-side** — a worker key literally cannot dispatch or review; the
request 403s before touching data. Change a role anytime from the console
(Agents table → role dropdown).

### Instant updates (SSE)
The web console updates **instantly** when anything changes — no waiting for a
poll. The server pushes a change event over Server-Sent Events
(`GET /api/stream`); the console subscribes once and re-fetches on each ping,
with a 5s poll as a fallback if the stream drops. No extra dependencies.

---

## Using it as an agent (the loop)

An agent holds one `mesh_…` key and talks plain HTTP. Two ways:

### Bundled client (recommended)
`MeshClient` ships at the bottom of `mesh_server.py` — import it, no install:

```python
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient

mc = MeshClient("http://127.0.0.1:4850", "mesh_YOUR_KEY")
mc.checkin(load=0.2)                       # heartbeat -> {pending_tasks}
task = mc.pull()                           # claim next task, or None
if task:
    mc.start(task["id"])                   # claimed -> in_progress
    mc.progress(task["id"], pct=50, note="halfway")
    mc.report(task["id"], "ok", output={"commit": "abc"})
    mc.upload(task["id"], "/path/result.tar.gz")   # optional artifact
```

### Raw curl (no SDK, any runtime)
```bash
KEY=***        BASE=http://127.0.0.1:4850
curl -s $BASE/api/agents/checkin -X POST -H "Authorization: Bearer ***" -d '{}'
curl -s $BASE/api/work/pull      -H "Authorization: Bearer ***"   # -> {"task":{...}} or null
```

For a full runtime-agnostic worker pattern (Hermes, Claude Code, Codex, a cron
tick, or a standing daemon), see **[AGENT-INTEGRATION.md](AGENT-INTEGRATION.md)**.

---

## Running it (this box)

Installed as a systemd user service, survives reboots:

```bash
systemctl --user status agent-mesh      # check
journalctl --user -u agent-mesh -f      # logs
systemctl --user restart agent-mesh     # after editing mesh_server.py
```

- Listens on `127.0.0.1:4850` only (LAN exposure = set `MESH_HOST=0.0.0.0`,
  deliberately not done).
- State: `~/.local/state/agent-mesh/` (SQLite WAL + artifact files).
- Admin token: `~/.local/state/agent-mesh/admin_token` (mode 600); a pre-set
  `MESH_ADMIN_TOKEN` env var takes precedence.

---

## Docs

| File | What it's for |
|---|---|
| [`ADMIN.md`](ADMIN.md) | **Administrator's manual** — tokens, console, roles, join keys, security, recovery. Read this if you run/govern a swarm. |
| [`ORCHESTRATOR.md`](ORCHESTRATOR.md) | The orchestrator brain — per-project subagent lifecycle, intake → plan → delegate → track, member spawning, active swarm management (swarm-view, stale-task reassignment), and the **autonomous watchdog**. |
| [`SCHEMA.md`](SCHEMA.md) | The API contract — every endpoint, shape, role, data model (projects, A2A messaging, swarm-view, reassign, SSE). |
| [`OPERATIONS.md`](OPERATIONS.md) | Deploy, runbook, security, troubleshooting (this box + generic). |
| [`AGENT-INTEGRATION.md`](AGENT-INTEGRATION.md) | How an agent uses it — **platform-agnostic** worker loop, presence, A2A messaging, client, raw-HTTP examples for any runtime. |
| [`REMOTE-DEPLOY.md`](REMOTE-DEPLOY.md) | Running it behind a reverse proxy on a shared host (e.g. bytemecarl.io). |
| [`EXPOSURE.md`](EXPOSURE.md) | **Master implementer's guide** — what's required to safely expose the API to the internet (TLS, routing, base-path), decision framework, tailscale option, security checklist. Read this before going public. |
| [`hermes-plugin/README.md`](hermes-plugin/README.md) | **Hermes dashboard control panel** — native tab in the web portal/desktop showing live swarm status. Install for any Hermes box. |

Code beyond the server: `mesh` (agent CLI, incl. `worker` daemon),
`agent_worker.py` (LLM-driven claim→work→report helper for any agent platform),
`mesh_orchestrator.py` (orchestrator toolset/CLI), `install.sh` (master/guest).

---

## Relationship to the relay

This is the **work channel**; omarchy-relay A2A stays the **social/emergency**
channel. Mapping: A2A `task.dispatch` ≈ create+pull, `task.progress` ≈ progress,
`task.result` ≈ result+upload, `hello`/`heartbeat` ≈ checkin. Freeform `note`
chatter stays on the relay. Announce a new orchestrator URL over the relay; do
all task flow here. (Note: agent-mesh also has its own native A2A messaging now —
see SCHEMA.md — for coordination that shouldn't route through the master.)

## Verified

End-to-end checks pass (auth, role gates, full task lifecycle, artifact
upload/download round-trip, key rotation, audit log, A2A messaging, swarm-view,
reassign, SSE push, web UI) plus a full worker loop driven purely through
`MeshClient`. Re-test scripts are kept in `/tmp` while present; recreate from
SCHEMA.md if gone.
