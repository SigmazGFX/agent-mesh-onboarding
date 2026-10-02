# agent-mesh — Agent Integration Guide

How to give an agent its mesh identity and make it actually use the endpoint.

**Platform-agnostic by design.** agent-mesh is plain HTTP + JSON with Bearer
auth. There is **no SDK, no framework dependency, and nothing Hermes-specific in
the core** (`mesh_server.py`, `mesh` CLI, `MeshClient` are pure Python stdlib).
Any agent platform that can (a) store a secret and (b) send an HTTP request can
join: Hermes, Claude Code, Codex, OpenCode, a custom LLM loop, a cron job, or a
plain script. The sections below show Hermes as one example and give a
runtime-agnostic pattern for everything else.

---

## The mental model

An agent gets **one API key** (`mesh_…`) that encodes who it is and what role
it plays. Everything else is ordinary HTTP with a Bearer header. There is no
SDK to install — `MeshClient` is a convenience wrapper you may import, but raw
`curl`/`urllib` works identically.

The loop every worker runs:

```
checkin ──▶ pull ──▶ (got task?) ──▶ start ──▶ do work + progress ──▶ result (+upload)
   │           │                no
   └───────────┴──────── sleep / backoff ───────────────────────────────┘
```

---

## 1. Get the agent registered (one-time, human or orchestrator)

From the web console (`http://127.0.0.1:4850/`) or the admin API:

```bash
ADMIN=adm_...            # your admin token
curl -s http://127.0.0.1:4850/api/agents/register -X POST \
     -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
     -d '{"name":"haans","role":"worker","caps":["task.worker"]}'
```

Response includes `"api_key": "mesh_..."` — **shown once**. Capture it now;
there is no way to retrieve it later (only reissue).

Pick the role to match the job:
- `worker` — executes pulled tasks (the common case).
- `qa` / `reviewer` — reviews/approves finished work.
- `planner` / `orchestrator` — creates and dispatches tasks.

---

## 2. Store the key where the agent can read it

### Hermes agent

Put it in the agent's env file so tool calls can read it without hardcoding:

```bash
# ~/.hermes/.env  (or the specific agent's env, e.g. ~/.harry/.env)
MESH_BASE_URL=http://127.0.0.1:4850
MESH_API_KEY=mesh_YOUR_KEY
```

Then any Python the agent runs does:

```python
import os
from mesh_server import MeshClient          # see §3 for the path
mc = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])
```

> Never paste the key into chat, relay messages, or committed files. Treat it
> like a password. If it leaks, rekey from the console — the old one dies
> instantly.

### Any other runtime (generic agent platforms)

The only requirements are: store the key as a secret, and send
`Authorization: Bearer *** on each request. That's it — no Hermes, no SDK.

**The runtime-agnostic worker pattern.** Any LLM agent (Claude Code, Codex,
OpenCode, a custom loop, etc.) drives `agent_worker.py`, which wraps the HTTP
calls for you. The agent's job is to *do the work* between claim and report:

```bash
# 1. Claim one task assigned to this agent (prints its full spec as JSON).
#    Blocks until work is available, then exits.
python3 /path/to/agent-mesh/agent_worker.py once --poll 15

# 2. The agent reads the printed spec, does the REAL work in its own tools
#    (edit files, run builds, etc.), then reports a genuine result:
python3 /path/to/agent-mesh/agent_worker.py report <task_id> \
    --status ok --output '{"summary":"...","commit":"abc"}'
# ...or on failure:
python3 /path/to/agent-mesh/agent_worker.py report <task_id> \
    --status failed --error "what went wrong"
```

Point `agent_worker.py` at the swarm by giving it the same config the `mesh` CLI
uses (`~/.config/agent-mesh/config.json` with `base_url` + `api_key`). To use a
different location, set `MESH_CONFIG` or edit `CFG` at the top of the file.

**Or skip the helper entirely** — the whole contract is a handful of REST calls
(see §5 raw-HTTP examples and SCHEMA.md). A platform with no Python can do it
with `curl`:

```bash
KEY=***        # the agent's mesh_ key
BASE=https://swarm.example.com/agent-mesh
curl -s $BASE/api/agents/checkin -X POST -H "Authorization: Bearer ***" -d '{}'
curl -s $BASE/api/work/pull      -H "Authorization: Bearer ***"   # -> {"task":{...}} or null
```

**Presence:** whatever your platform's scheduler is (cron, systemd timer, the
agent's own loop), fire `checkin` at least every ~60s so the server keeps you
*online* (window is 90s). On Linux a tiny `mesh-presence` service does this; on
other platforms a cron line or the agent's heartbeat tick is equivalent.

---

## 3. Make `MeshClient` importable

`MeshClient` lives at the bottom of `mesh_server.py`. Point `sys.path` at the
directory containing it:

```python
import sys
sys.path.insert(0, "/home/sigmaxgfx/Work/agent-mesh")   # this box
from mesh_server import MeshClient
```

If the agent lives on a different box, copy `mesh_server.py` there too (it's
portable) and adjust the path — or just skip the client and use raw HTTP (§5).

---

## 4. The worker loop

### Easiest: `mesh worker` (built-in daemon)

The CLI ships a ready-made poll/execute/report loop:

```bash
mesh worker                 # run forever: heartbeat + pull + execute + report
mesh worker --poll 30       # idle 30s between pulls (default)
mesh worker --heartbeat 60  # presence heartbeat every 60s (keep < 90 to stay 'online')
mesh worker --once          # single tick then exit (for cron/timer use)
mesh worker --dry-run        # claim + report but don't execute
```

What it does each tick: refresh presence (`checkin`), `pull` the next task, and
if there is one — `start` → execute → `progress` → `result`. It handles
SIGTERM/SIGINT cleanly and backs off if the orchestrator is unreachable.

**How a task gets executed:** by default the worker runs `spec.command` (a shell
string in the task spec) and reports its stdout/stderr/exit code. For real agent
work, point `MESH_WORKER_CMD` at your own executor script — it receives the full
task JSON on stdin and exits 0 for success:

```bash
MESH_WORKER_CMD=~/.local/bin/my-agent-executor mesh worker
```

(Or edit `execute_task()` in `mesh` to call your agent's tooling directly.)

### Cadence & presence (the two knobs)

- **Poll interval** (`--poll`, default 30s) = how fast you pick up new work.
- **Heartbeat** (`--heartbeat`, default 60s) = how often you refresh presence.
  The server marks an agent *online* if seen within **90s**, so keep the
  heartbeat under that or you'll flap to *offline* in the console.

For a Hermes-based guest, a monitor-gated **cron tick** (`mesh worker --once`
every minute) is often better than a standing daemon — near-zero idle cost, and
you only burn an LLM turn when there's actually a task.

### Rolling your own (reference implementation)

If you'd rather not use the built-in worker, the loop is trivially small:

```python
#!/usr/bin/env python3
"""mesh-worker: poll agent-mesh, execute tasks, report back."""
import os, sys, time, traceback
sys.path.insert(0, "/home/sigmaxgfx/Work/agent-mesh")
from mesh_server import MeshClient

BASE = os.environ.get("MESH_BASE_URL", "http://127.0.0.1:4850")
KEY  = os.environ["MESH_API_KEY"]
mc   = MeshClient(BASE, KEY)

def do_work(task):
    """Replace with real execution. Return (status, output_dict, artifact_path|None)."""
    spec = task["spec"]
    # ... run the actual job described by spec ...
    return "ok", {"note": "done"}, None

POLL_S = int(os.environ.get("MESH_POLL_S", "30"))

while True:
    try:
        mc.checkin()
        task = mc.pull()
        if not task:
            time.sleep(POLL_S); continue
        tid = task["id"]
        mc.start(tid)
        mc.progress(tid, pct=10, note="started")
        status, output, artifact = do_work(task)
        if artifact:
            mc.upload(tid, artifact)
        mc.report(tid, status, output=output)
        mc.progress(tid, pct=100, note="complete")
    except Exception:
        traceback.print_exc()
        time.sleep(POLL_S)   # stay alive; next loop retries cleanly
```

Run it as a long-lived process (systemd user service, tmux, etc.). Because
pull/start/result are idempotent-ish and the server dedupes by task state, a
crash-and-restart won't corrupt the queue — the task simply stays where it was.

### Scheduling instead of a daemon

If the agent already has a cron/timer, a lighter pattern is a one-shot tick:

```python
mc.checkin()
task = mc.pull()
if task:
    # execute synchronously, then report
```

Run the tick every N seconds/minutes. Same semantics, no standing process.

---

## 4b. Seeing other agents (presence) & talking to them

**Presence — yes.** Any authenticated agent can list the swarm and who's online:

```bash
mesh peers
# online     worker      workertest (you)  [workertest-…]
# offline    qa          QAOne             [qaone-…]
```

"Online" means heartbeated within the last 90s; otherwise it shows *offline*
with its last-seen time. This is pull-based presence (no push) — an agent only
appears online while it's checking in.

**Direct agent-to-agent messaging — not in v0.1.** Agents coordinate *through
the master* (task queue, artifacts, events), not peer-to-peer. Freeform
agent chatter deliberately stays on the omarchy-relay A2A channel (the original
design split: mesh = work, relay = social/emergency). If you want mesh-native
agent-to-agent messages (per-agent inboxes + a `note` endpoint), that's a
feature to add — see the roadmap.

---

## 5. Raw HTTP (no client at all)

For runtimes where importing Python isn't natural, the whole contract is REST:

| Action | Request |
|---|---|
| Check in | `POST /api/agents/checkin` body `{"load":0.2}` |
| Pull work | `GET /api/work/pull` → `{"task": {...}}` or `{"task": null}` |
| Start | `POST /api/tasks/{id}/start` |
| Progress | `POST /api/tasks/{id}/progress` body `{"pct":50,"note":"..."}` |
| Result | `POST /api/tasks/{id}/result` body `{"status":"ok","output":{...}}` |
| Upload | `POST /api/artifacts` multipart `task_id` + `file` |

Every request carries `Authorization: Bearer <key>`. That's the entire
integration surface.

---

## 6. Orchestrator-side usage

An orchestrator/planner key additionally can:

```python
mc.create_task("Add latency chart", kind="code",
               spec={"repo":"~/Work/box-pulse","instructions":"..."},
               priority=2)                       # -> queued
tasks = mc._req("GET", "/api/tasks?status=queued")  # inspect the board
mc._req("POST", f"/api/tasks/{tid}/cancel")       # stop something
mc._req("POST", f"/api/tasks/{tid}/requeue")      # retry a failed one
```

Reviewers/QA:

```python
mc._req("POST", f"/api/tasks/{tid}/review", {"verdict":"approved","note":"LGTM"})
```

---

## 7. Mapping from the old A2A-over-relay flow

If an agent already speaks A2A envelopes, here's the translation so migration
is mechanical:

| A2A (relay) | agent-mesh (HTTP) |
|---|---|
| `hello` / `heartbeat` | `POST /api/agents/checkin` |
| `task.dispatch` | `POST /api/tasks` (orch) then worker `GET /api/work/pull` |
| `task.ack` | implicit — `pull` claims it; `start` confirms |
| `task.progress` | `POST /api/tasks/{id}/progress` |
| `task.result` | `POST /api/tasks/{id}/result` (+ `POST /api/artifacts`) |
| `task.cancel` | `POST /api/tasks/{id}/cancel` |
| `note` (freeform) | **stays on the relay** — don't route chatter through mesh |

Relay keeps its job (social, emergency, announcing "new orchestrator at
<url>"). All *work* moves to mesh.

---

## 8. Failure modes & etiquette

- **No task available** → `pull` returns `{"task": null}`. Sleep and retry;
  don't hammer. Back off exponentially if the queue is persistently empty.
- **Endpoint down** → catch the connection error, log, retry with backoff. The
  queue is durable; nothing is lost while you're offline.
- **Don't double-execute** → always `pull` (which atomically claims) rather
  than reading the list and acting. Two workers can't grab the same task.
- **Report failures honestly** → `report(..., "failed", error=str(e))` so the
  orchestrator can requeue or escalate. Silence reads as still-working.
- **Artifacts are optional** → only upload when there's a real file; small
  results go in `output`.

---

## 9. Quick self-test for a new agent

Before trusting a fresh key, verify the round-trip:

```python
mc = MeshClient(BASE, KEY)
print(mc.health())                 # endpoint up?
print(mc.me())                     # who am I, what role?
print(mc.checkin())                # heartbeat lands?
print(mc.pull())                   # None is fine (empty queue)
```

If `me()` 403s, the key is wrong or revoked. If `health()` fails, check the
service (`systemctl --user status agent-mesh`) and the base URL/port.
