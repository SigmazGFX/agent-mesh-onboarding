# agent-mesh

API-based communications endpoint for a multi-agent development org. Agents
**check in, get work, report status, upload results**. Role hierarchy
(orchestrator / planner / worker / qa / reviewer / observer) is defined per
agent and enforced server-side. Per-agent API keys are managed through a web
console.

- **Spec (source of truth):** [`SCHEMA.md`](SCHEMA.md) — read this first.
- **Server:** [`mesh_server.py`](mesh_server.py) — single file, stdlib-only
  (Python ≥3.9). No venv, no pip. Portable: copy to any box and run.
- **Web console:** `http://127.0.0.1:4850/` — multi-page admin dashboard + key
  management (Dashboard · Agents · Tasks · Events · Artifacts).

## Docs

| File | What it's for |
|---|---|
| [`SCHEMA.md`](SCHEMA.md) | The API contract — every endpoint, shape, role, data model. |
| [`OPERATIONS.md`](OPERATIONS.md) | Deploy, runbook, security, troubleshooting (this box + generic). |
| [`AGENT-INTEGRATION.md`](AGENT-INTEGRATION.md) | How an agent uses it — worker loop, client, A2A migration map. |
| [`REMOTE-DEPLOY.md`](REMOTE-DEPLOY.md) | Running it on a remote Hermes box behind a reverse proxy (e.g. bytemecarl.io). |

## Install (master or guest)

Pointing at this repo and running the installer is enough. It asks whether this
box is a **master** (orchestrator) or a **guest** that enrolls in a swarm:

```bash
git clone https://github.com/SigmazGFX/agent-mesh.git
cd agent-mesh
./install.sh                 # interactive: pick master | guest
# or non-interactive:
./install.sh master          # stand up the orchestrator HERE
./install.sh guest           # enroll THIS box into an existing swarm
```

**Master mode** runs the endpoint locally as a systemd user service, stores the
admin token, prints the console URL + admin token, and issues a **join key** to
hand out to guests.

**Guest mode** asks for the swarm's **base URL** and a **join key**, then:
1. Enrolls via `POST /api/agents/join` (presents the join key; lands as
   `observer`).
2. Stores `{base_url, api_key, agent_id}` in `~/.config/agent-mesh/config.json`.
3. Checks in so the master sees the new agent.
4. Installs a `mesh` CLI on PATH (`~/.local/bin/mesh`).

Then the **swarm admin assigns a real role** in the console (Agents page) —
until then the agent can only observe. Day-to-day from the box:

```bash
mesh status        # who am I, what role
mesh checkin       # heartbeat
mesh pull          # claim next task (needs worker+ role)
mesh result <id> --status ok --output '{"commit":"abc"}'
```

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
- Admin token: `MESH_ADMIN_TOKEN` in `~/.hermes/.env` (injected via
  `EnvironmentFile`). It was generated at install time — read it from there;
  it is never printed again.

## Onboarding an agent (3 steps)

1. Open `http://127.0.0.1:4850/`, unlock with the admin token.
2. **Register** the agent: name + role (e.g. `haans` / `worker`). The UI
   prompts you to copy the plaintext API key — shown exactly once.
3. Give that key to the agent's config. The agent then talks plain HTTP:

```python
import sys; sys.path.insert(0, "/home/sigmaxgfx/Work/agent-mesh")
from mesh_server import MeshClient

mc = MeshClient("http://127.0.0.1:4850", "mesh_...")   # its key
mc.checkin(load=0.2)                                    # heartbeat
task = mc.pull()                                        # claim next task
if task:
    mc.start(task["id"])
    mc.progress(task["id"], pct=50, note="halfway")
    mc.report(task["id"], "ok", output={"commit": "abc"})
    mc.upload(task["id"], "/path/to/result.tar.gz")     # optional artifact
```

Or raw curl (no SDK needed):

```bash
curl -s http://127.0.0.1:4850/api/work/pull -H "Authorization: Bearer mesh_..."
```

## Roles & what they may do

| Role | dispatch | pull | results | review | cancel others |
|---|---|---|---|---|---|
| orchestrator | ✓ | ✓ | ✓ | ✓ | ✓ |
| planner | ✓ | ✓ | ✓ | ✓ | – |
| worker | – | ✓ | ✓ | – | – |
| qa | – | ✓ | ✓ | ✓ | – |
| reviewer | – | – | – | ✓ | – |
| observer | – | – | – | – | – |

Change a role anytime from the console (Agents table → role dropdown).

## Relationship to the relay

This is the **work channel**; omarchy-relay A2A stays the **social/emergency**
channel. Mapping: A2A `task.dispatch` ≈ create+pull, `task.progress` ≈
progress, `task.result` ≈ result+upload, `hello`/`heartbeat` ≈ checkin.
Freeform `note` chatter stays on the relay. Announce a new orchestrator URL
over the relay; do all task flow here.

## Verified

29/29 end-to-end checks pass (auth, role gates, full task lifecycle,
artifact upload/download round-trip, key rotation, audit log, web UI) plus a
full worker loop driven purely through `MeshClient`. Test scripts kept at
`/tmp/mesh-e2e.py`, `/tmp/mesh-loop-test.py` (recreate from SCHEMA.md if gone).
