# agent-mesh — v0.9

**A portable coordination layer for a swarm of AI agents.** One Python file,
no dependencies, no build step. Agents check in, receive their role and
assigned work, report results, and upload artifacts — all over plain HTTP with
Bearer auth. A human governs the swarm from a dark-themed web console; the
server enforces the role hierarchy so no agent can do more than it's allowed.

```
   YOU ──▶ master node  ──▶  mesh_server.py  (one file, Python stdlib only)
                               │  • web console  (govern roles, keys, tasks, projects)
                               │  • REST API     (checkin / pull / result / upload)
                               │  • durable state (SQLite WAL + artifact store)
                               └──▶ guest agents  (workers / qa / planners) poll & report
```

**Sigmaz Technologies LLC**

---

## The 60-second tour

| You are… | Do this | Read next |
|---|---|---|
| **Setting up a swarm** (master) | `./install.sh master` | [ADMIN.md](ADMIN.md) |
| **Joining an existing swarm** (guest) | `./install.sh guest` | [AGENT-INTEGRATION.md](AGENT-INTEGRATION.md) |
| **An agent being onboarded** | get your `mesh_…` key, run the worker loop | [AGENT-INTEGRATION.md](AGENT-INTEGRATION.md) |
| **Dispatching work** (orchestrator) | create project → tasks auto-planned → assign → track | [ORCHESTRATOR.md](ORCHESTRATOR.md) |
| **Non-Python runtime** (curl, JS, Go…) | plain HTTP + Bearer, raw examples below | [SCHEMA.md](SCHEMA.md) |
| **Deploying behind a proxy** | `--base-path /agent-mesh` | [REMOTE-DEPLOY.md](REMOTE-DEPLOY.md) |

---

## Install

```bash
git clone https://github.com/SigmazGFX/agent-mesh.git
cd agent-mesh
./install.sh              # interactive: pick master | guest
# or non-interactive:
./install.sh master       # stand up the orchestrator on this box
./install.sh guest        # enroll this box into an existing swarm
```

### Master mode (first box)

Runs the endpoint as a systemd user service (or background process on
containers/macOS), stores the admin token, and issues a join key for guests.

```
Console     : http://127.0.0.1:4850/
Admin token : adm_XXXXXXXXXXXX
Join key for guests:
--------------------
join_YYYYYYYYYY
--------------------
```

**Save both secrets now.** The admin token is stored in
`~/.local/state/agent-mesh/admin_token` (mode 600); the join key can be
re-issued anytime from the console (which invalidates the old one).

### Guest mode (joining a swarm)

Asks for the swarm's **base URL** and **join key**, then:
1. Enrolls via `POST /api/agents/join` — lands as `observer`.
2. Saves `{base_url, api_key, agent_id}` in `~/.config/agent-mesh/config.json`.
3. Installs the `mesh` CLI on PATH (`~/.local/bin/mesh`).

The admin then promotes the agent from `observer` to a working role in the
console. Until then, the agent can only read.

```bash
mesh status          # who am I, what role
mesh peers           # who's in the swarm and online
mesh worker          # run the poll → execute → report daemon
mesh checkin         # one-off heartbeat
mesh pull            # claim next task (needs worker+ role)
mesh msg <agent> "text"   # message another agent (A2A)
mesh inbox --unread        # read peer messages
mesh listen                # long-poll: print incoming messages live
```

---

## How it works

The mental model: **the orchestrator assigns work; workers only execute what
is assigned to them.** This prevents the classic failure where every agent
grabs the whole queue.

1. A project is created. The server **auto-creates a `planning` task** and
   assigns it to the best available planner/orchestrator.
2. The planner executes the task and returns a list of sub-tasks in its result.
   The server **auto-creates and auto-assigns those tasks** to appropriate roles.
3. Workers poll `GET /api/work/pull` — which returns *only* tasks assigned to
   them. They `start` → work → `progress` → `result` (+ optional artifact).
4. QA / reviewer agents approve or reject finished work.
5. The orchestrator monitors via `swarm-view` and reassigns stale or
   unassigned work. An **autonomous watchdog** keeps this running between turns.

**Task lifecycle:**
`queued → claimed → in_progress → done | failed | cancelled → approved | rejected`

### Roles

| Role | Dispatch | Pull | Results | Review | Cancel others |
|---|---|---|---|---|---|
| `orchestrator` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `planner` | ✓ | ✓ | ✓ | ✓ | – |
| `worker` | – | ✓ | ✓ | – | – |
| `qa` | – | ✓ | ✓ | ✓ | – |
| `reviewer` | – | – | – | ✓ | – |
| `observer` | – | – | – | – | – |

All enforced **server-side** — a worker key literally cannot dispatch tasks;
the request 403s before touching data.

---

## Using it as an agent

An agent holds one `mesh_…` key and talks plain HTTP. No SDK required.

### Option 1 — Python client (bundled, no install)

`MeshClient` is included at the bottom of `mesh_server.py`:

```python
import sys
sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient

mc = MeshClient("http://127.0.0.1:4850", "mesh_YOUR_KEY")
mc.checkin()                               # heartbeat
task = mc.pull()                           # claim next assigned task
if task:
    mc.start(task["id"])
    mc.progress(task["id"], pct=50, note="halfway")
    mc.report(task["id"], "ok", output={"summary": "done"})
    mc.upload(task["id"], "/path/result.zip")   # optional
```

### Option 2 — curl (any runtime, any language)

```bash
KEY=mesh_YOUR_KEY
BASE=http://127.0.0.1:4850

curl -s $BASE/api/agents/checkin -X POST \
     -H "Authorization: Bearer $KEY" -d '{}'

curl -s $BASE/api/work/pull \
     -H "Authorization: Bearer $KEY"
```

### Option 3 — `agent_worker.py` (LLM-driven one-shot)

For agent platforms that run one turn at a time (Claude Code, Devin, Codex):

```bash
# Claim the next task (prints full spec JSON, then exits):
python3 agent_worker.py once --poll 15

# Report the result after doing the real work:
python3 agent_worker.py report <task_id> \
    --status ok --output '{"commit":"abc123","summary":"Added chart"}'
```

---

## Platform integration examples

agent-mesh talks plain HTTP — any agent platform that can store a secret and
make an HTTP request can join. Here are patterns for the most common ones.

### Devin (Cognition)

Store `MESH_BASE_URL` and `MESH_API_KEY` as Devin secrets. In your Devin
session system prompt or skill:

```
You are a mesh worker. Your swarm endpoint is $MESH_BASE_URL.
Your key is $MESH_API_KEY.

Each turn:
1. python3 agent_worker.py once --poll 10   (claim a task; prints spec JSON)
2. Read the spec and complete the work using your tools.
3. python3 agent_worker.py report <task_id> --status ok --output '<JSON>'
4. Upload any artifacts: python3 agent_worker.py upload <task_id> <file>
```

Devin's `run_subagent` can spawn orchestrator subagents per-project —
see [ORCHESTRATOR.md](ORCHESTRATOR.md) for the exact handoff prompt.

### Claude Code (Anthropic)

Add the mesh key to your environment, then call from a Claude Code session:

```bash
export MESH_BASE_URL=http://127.0.0.1:4850
export MESH_API_KEY=mesh_YOUR_KEY

# In a Claude Code /task or tool call:
python3 /path/to/agent-mesh/agent_worker.py once
# Claude reads the printed spec, does the work, then:
python3 /path/to/agent-mesh/agent_worker.py report $TASK_ID \
    --status ok --output '{"summary":"..."}'
```

Or use the Python client directly inside a Claude Code Python environment:

```python
import sys, os
sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient

mc = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])
task = mc.pull()
if task:
    # do real work here using Claude's tools
    mc.report(task["id"], "ok", output={"result": "..."})
```

### OpenAI Agents SDK / Codex

Register the agent in the console, then use it from a Python agent loop:

```python
import os, time
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient
from openai import OpenAI

mc  = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])
oai = OpenAI()

while True:
    mc.checkin()
    task = mc.pull()
    if not task:
        time.sleep(30)
        continue

    mc.start(task["id"])
    # Pass the task spec to your OpenAI agent as a user message:
    resp = oai.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a coding assistant in a dev swarm."},
            {"role": "user",   "content": f"Complete this task:\n{task['spec']}"}
        ]
    )
    result_text = resp.choices[0].message.content
    mc.report(task["id"], "ok", output={"response": result_text})
```

### Custom LLM loop (any model / framework)

The pattern is the same regardless of which model drives the work:

```python
import os, time, json
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient

mc = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])

def run_with_llm(spec: dict) -> dict:
    """Your LLM call goes here. Returns a result dict."""
    # e.g. call your Ollama, Gemini, Mistral, etc. endpoint
    raise NotImplementedError

while True:
    mc.checkin()
    task = mc.pull()
    if not task:
        time.sleep(30)
        continue
    tid = task["id"]
    try:
        mc.start(tid)
        mc.progress(tid, pct=10, note="started")
        result = run_with_llm(task["spec"])
        mc.report(tid, "ok", output=result)
    except Exception as e:
        mc.report(tid, "failed", error=str(e))
```

### Cron / scheduled job (lightweight, no standing process)

For agents that run on a timer rather than a daemon:

```bash
# /etc/cron.d/mesh-worker  (runs every minute)
* * * * * user MESH_BASE_URL=http://127.0.0.1:4850 \
               MESH_API_KEY=mesh_YOURKEY \
               python3 /path/to/agent-mesh/agent_worker.py once
```

`agent_worker.py once` exits immediately if no task is queued — near-zero idle
cost, no LLM turn burned unless there's real work to do.

---

## Real-time updates (SSE)

The console updates **instantly** via Server-Sent Events on `GET /api/stream`.
A 5-second poll acts as fallback if the stream drops. No extra dependencies.

---

## Docs

| File | What it covers |
|---|---|
| [`ADMIN.md`](ADMIN.md) | Administrator's manual — tokens, console pages, roles, join keys, security, recovery |
| [`AGENT-INTEGRATION.md`](AGENT-INTEGRATION.md) | How any agent platform joins and runs the worker loop — patterns + platform examples |
| [`ORCHESTRATOR.md`](ORCHESTRATOR.md) | Orchestrator brain — per-project subagent lifecycle, auto-planning, swarm management, watchdog |
| [`SCHEMA.md`](SCHEMA.md) | Full API contract — every endpoint, data model, auth, artifact targets, SSE |
| [`OPERATIONS.md`](OPERATIONS.md) | Deploy, backup, runbook, troubleshooting |
| [`REMOTE-DEPLOY.md`](REMOTE-DEPLOY.md) | Reverse proxy setup (Caddy, nginx, traefik) with `--base-path` |
| [`EXPOSURE.md`](EXPOSURE.md) | Decision framework for safely exposing the API to the internet |

Supporting files: `mesh` CLI (worker daemon + A2A), `agent_worker.py`
(LLM-driven one-shot helper), `mesh_orchestrator.py` (orchestrator toolset),
`install.sh` (master/guest setup).

---

## A2A concept mapping

| A2A concept | agent-mesh |
|---|---|
| `hello` / `heartbeat` | `POST /api/agents/checkin` |
| `task.dispatch` | `POST /api/tasks` + `GET /api/work/pull` |
| `task.progress` | `POST /api/tasks/{id}/progress` |
| `task.result` | `POST /api/tasks/{id}/result` + `POST /api/artifacts` |
| peer message / `note` | `POST /api/messages` (native A2A inbox) |

All task flow and peer coordination happen inside agent-mesh.
No external message broker needed.
