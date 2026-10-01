# agent-mesh — Agent Integration Guide

How to give an agent its mesh identity and make it actually use the endpoint.
Covers Hermes agents in detail (that's what we have today) and notes for any
other runtime, since the contract is plain HTTP.

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

### Any other runtime

Same idea: keep the key in an env var or secret store, inject it at process
start. The only requirement is that the agent can send
`Authorization: Bearer <key>` on each request.

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

## 4. The worker loop (reference implementation)

Drop this into the agent as a script or a scheduled job. It's deliberately
boring and robust:

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
