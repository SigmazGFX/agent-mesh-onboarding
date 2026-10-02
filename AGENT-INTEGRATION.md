# agent-mesh — Agent Integration Guide

How to give an agent its mesh identity and have it join, receive work, and
report results. **Platform-agnostic by design.** agent-mesh is plain HTTP +
JSON with Bearer auth — no SDK, no framework, no platform-specific dependency.
Any agent that can store a secret and make an HTTP request can participate:
Devin, Claude Code, OpenAI Agents, Codex, a custom LLM loop, a cron job, or
a plain script.

---

## Mental model

An agent gets **one API key** (`mesh_…`) that identifies who it is and what
role it plays. Everything else is ordinary HTTP with a `Bearer` header.

The loop every worker runs:

```
checkin ──▶ pull ──▶ (got task?) ──▶ start ──▶ work + progress ──▶ result [+ upload]
   │           │                no
   └───────────┴──────── sleep / backoff ──────────────────────────────────┘
```

**What the agent receives per turn (on every checkin):**
- Its own role and agent ID
- Its current assignment — task ID, title, kind, project context, and the full
  `spec` (the structured payload describing exactly what to do)
- Count of unread peer messages

So each turn is self-contained. The agent doesn't need memory between polls.

---

## Step 1 — Register the agent (one-time, done by a human admin)

From the web console or API:

```bash
ADMIN=adm_...
curl -s http://127.0.0.1:4850/api/agents/register -X POST \
     -H "Authorization: Bearer $ADMIN" \
     -H 'Content-Type: application/json' \
     -d '{"name":"my-worker","role":"worker","caps":["task.worker"]}'
```

Response includes `"api_key": "mesh_..."` — **shown once**. Capture it. You
can only reissue, never retrieve, a lost key.

**Or** let a guest box self-enroll with a join key:

```bash
./install.sh guest
# → enter base URL + join key → lands as observer → admin promotes the role
```

**Pick the right role:**
- `worker` — executes pulled tasks (the common case)
- `qa` — pulls tasks, submits results, and can approve/reject work
- `reviewer` — approve/reject only; no task execution
- `planner` — creates and dispatches tasks; also executes
- `orchestrator` — full control: dispatch, execute, review, cancel

---

## Step 2 — Store the key where the agent can read it

**Never paste a mesh key into a chat, a commit, or a log.** Treat it as a
password. If leaked, rekey from the console — the old key is immediately dead.

```bash
# Recommended: env vars or a secrets manager
export MESH_BASE_URL=http://127.0.0.1:4850
export MESH_API_KEY=mesh_YOUR_KEY
```

```python
import os
from mesh_server import MeshClient
mc = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])
```

---

## Step 3 — Choose how the agent runs the loop

### A. `mesh worker` — built-in daemon (easiest)

```bash
mesh worker               # run forever: heartbeat + pull + execute + report
mesh worker --poll 30     # idle 30s between pulls (default)
mesh worker --once        # single tick then exit (good for cron/timer)
mesh worker --listen      # also start an A2A inbox listener in same process
```

By default the worker runs `spec.command` (a shell string in the task spec).
To plug in a real LLM executor, set `MESH_WORKER_CMD`:

```bash
MESH_WORKER_CMD=~/.local/bin/my-llm-executor mesh worker
# executor receives the full task JSON on stdin, exits 0 for success
```

### B. `agent_worker.py` — one-shot helper (for turn-based platforms)

```bash
# Claim a task and print its spec JSON, then exit:
python3 agent_worker.py once --poll 15

# After doing the real work, report back:
python3 agent_worker.py report <task_id> \
    --status ok --output '{"commit":"abc","summary":"Added the chart"}'

# On failure:
python3 agent_worker.py report <task_id> \
    --status failed --error "Build failed: missing dependency"

# Upload an artifact file:
python3 agent_worker.py upload <task_id> ./result.tar.gz
```

### C. Python client — embed in your own loop

```python
import sys, os, time
sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient

mc = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])

def do_work(task) -> tuple[str, dict, str | None]:
    """Your actual execution. Returns (status, output, artifact_path|None)."""
    spec = task["spec"]
    # ... run the job described by spec ...
    return "ok", {"summary": "done"}, None

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
        status, output, artifact = do_work(task)
        if artifact:
            mc.upload(tid, artifact)
        mc.report(tid, status, output=output)
    except Exception as e:
        mc.report(tid, "failed", error=str(e))
    time.sleep(5)
```

### D. Raw HTTP (any language, no Python)

```bash
KEY=mesh_YOUR_KEY
BASE=http://127.0.0.1:4850

# Heartbeat
curl -s $BASE/api/agents/checkin -X POST \
     -H "Authorization: Bearer $KEY" -d '{}'

# Claim next task assigned to me
curl -s $BASE/api/work/pull \
     -H "Authorization: Bearer $KEY"
# Returns: {"task": {...}} or {"task": null}

# Mark claimed task as started
curl -s $BASE/api/tasks/task-XXXX/start -X POST \
     -H "Authorization: Bearer $KEY"

# Report progress
curl -s $BASE/api/tasks/task-XXXX/progress -X POST \
     -H "Authorization: Bearer $KEY" \
     -H 'Content-Type: application/json' \
     -d '{"pct":50,"note":"halfway"}'

# Submit result
curl -s $BASE/api/tasks/task-XXXX/result -X POST \
     -H "Authorization: Bearer $KEY" \
     -H 'Content-Type: application/json' \
     -d '{"status":"ok","output":{"commit":"abc123"}}'

# Upload an artifact file (multipart)
curl -s $BASE/api/artifacts -X POST \
     -H "Authorization: Bearer $KEY" \
     -F "task_id=task-XXXX" \
     -F "file=@/path/to/result.zip"
```

---

## Platform examples

### Devin (Cognition)

Store `MESH_BASE_URL` and `MESH_API_KEY` as Devin secrets. Add to the session
system prompt or a `.devin/skills/mesh-worker/SKILL.md`:

```
You are a worker in an agent-mesh swarm.

On each turn:
1. Run: python3 /path/to/agent-mesh/agent_worker.py once --poll 10
   (This claims your next assigned task and prints its full spec as JSON.
    If no task is available it waits up to 10 seconds then exits quietly.)
2. Read the printed JSON carefully. Complete the work described by "spec"
   using your tools (edit files, run tests, call APIs, etc.)
3. Report the result:
   python3 /path/to/agent-mesh/agent_worker.py report <task_id> \
       --status ok --output '{"summary":"...","files_changed":["..."]}'
4. If you produced an output file, upload it:
   python3 /path/to/agent-mesh/agent_worker.py upload <task_id> <file>
5. If the task failed, report honestly:
   python3 /path/to/agent-mesh/agent_worker.py report <task_id> \
       --status failed --error "what went wrong"

Never claim you completed work you did not complete. The orchestrator can see
task status and will requeue failed work.
```

To spawn an orchestrator subagent from Devin, use `run_subagent` — see the
full handoff prompt in [ORCHESTRATOR.md](ORCHESTRATOR.md).

### Claude Code (Anthropic)

Set env vars in your shell before opening Claude Code, or add them to
`.env` in the project root (never commit real keys):

```bash
export MESH_BASE_URL=http://127.0.0.1:4850
export MESH_API_KEY=mesh_YOUR_KEY
```

In a Claude Code `/task` or shell invocation:

```bash
# Claim the next task:
python3 /path/to/agent-mesh/agent_worker.py once
# Claude reads the spec and does the work, then:
python3 /path/to/agent-mesh/agent_worker.py report $TASK_ID \
    --status ok --output '{"summary":"..."}'
```

Or integrate directly from a Python tool call:

```python
import sys, os
sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient

mc = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])
print(mc.me())        # verify identity
task = mc.pull()      # claim next task
if task:
    print(task["spec"])   # Claude reads and acts on this
    # ... do work ...
    mc.report(task["id"], "ok", output={"summary": "completed"})
```

For orchestrator use from Claude Code (spawning a planning subagent via
`/task`), see [ORCHESTRATOR.md](ORCHESTRATOR.md).

### OpenAI Agents SDK

```python
import os, time
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient
from agents import Agent, Runner   # openai-agents SDK

mc  = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])

coding_agent = Agent(
    name="MeshWorker",
    instructions="You are a software development assistant in a dev swarm. "
                 "You receive task specs and produce working code changes.",
    model="gpt-4o",
)

while True:
    mc.checkin()
    task = mc.pull()
    if not task:
        time.sleep(30)
        continue
    tid = task["id"]
    mc.start(tid)
    try:
        result = Runner.run_sync(
            coding_agent,
            f"Complete this task:\n\n{task['title']}\n\nSpec:\n{task['spec']}"
        )
        mc.report(tid, "ok", output={"response": result.final_output})
    except Exception as e:
        mc.report(tid, "failed", error=str(e))
```

### OpenAI Chat Completions (raw)

```python
import os, time, json
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient
from openai import OpenAI

mc  = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])
oai = OpenAI()

SYSTEM = (
    "You are a software development agent in a dev swarm. "
    "When given a task spec, complete it and reply with a JSON object "
    '{"summary": "...", "files_changed": [...], "notes": "..."}.'
)

while True:
    mc.checkin()
    task = mc.pull()
    if not task:
        time.sleep(30)
        continue
    tid = task["id"]
    mc.start(tid)
    try:
        resp = oai.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM},
                {"role": "user",   "content": json.dumps(task["spec"])}
            ]
        )
        content = resp.choices[0].message.content
        output  = json.loads(content) if content.startswith("{") else {"response": content}
        mc.report(tid, "ok", output=output)
    except Exception as e:
        mc.report(tid, "failed", error=str(e))
```

### Google Gemini

```python
import os, time, json
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient
import google.generativeai as genai

genai.configure(api_key=os.environ["GEMINI_API_KEY"])
model = genai.GenerativeModel("gemini-1.5-pro")
mc    = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])

while True:
    mc.checkin()
    task = mc.pull()
    if not task:
        time.sleep(30)
        continue
    tid = task["id"]
    mc.start(tid)
    try:
        prompt = f"Complete this task:\n{task['title']}\n\nSpec:\n{json.dumps(task['spec'], indent=2)}"
        resp   = model.generate_content(prompt)
        mc.report(tid, "ok", output={"response": resp.text})
    except Exception as e:
        mc.report(tid, "failed", error=str(e))
```

### Cron / scheduled job (lowest overhead)

For agents that need to be on-demand rather than always running. Near-zero
idle cost — an LLM turn is only spent when there's real work queued.

```bash
# /etc/cron.d/mesh-worker  — runs every minute
* * * * * youruser \
    MESH_BASE_URL=http://127.0.0.1:4850 \
    MESH_API_KEY=mesh_YOURKEY \
    python3 /path/to/agent-mesh/agent_worker.py once
```

Or as a systemd timer (`OnCalendar=minutely`) for tighter control over the
service environment.

### Custom LLM loop (any model/framework)

```python
import os, time
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient

mc = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_API_KEY"])

def call_my_llm(spec: dict) -> dict:
    """Replace with Ollama, Mistral, Cohere, Anthropic, etc."""
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
        mc.progress(tid, pct=10, note="working")
        result = call_my_llm(task["spec"])
        mc.report(tid, "ok", output=result)
    except Exception as e:
        mc.report(tid, "failed", error=str(e))
```

---

## Making `MeshClient` importable

`MeshClient` lives at the bottom of `mesh_server.py`. Point `sys.path` at the
directory containing it:

```python
import sys
sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient
```

If the agent runs on a different box, copy `mesh_server.py` there or skip the
client entirely and use raw HTTP — it's the same calls either way.

---

## Presence: staying online

The server marks an agent **online** if it heartbeated within the last 90s.
To stay online while working, fire `checkin` at least every 60s.

- **Daemon mode** (`mesh worker`): handles this automatically.
- **One-shot mode** (`agent_worker.py once`): fire `checkin` separately if the
  job runs longer than 90s (or use `mesh worker --once` which does a checkin
  before pulling).
- **Cron/timer**: a one-minute schedule naturally keeps presence alive as long
  as `checkin` is the first call each tick.

Offline agents still receive queued work — they just show as `offline` in the
console until their next heartbeat.

---

## Agent-to-agent messaging (A2A)

Agents have a native peer inbox. No external channel needed.

```bash
mesh msg <agent-name> "ready for review?"   # send a note
mesh inbox --unread                          # read your inbox
mesh listen                                  # long-poll: print messages as they land
mesh worker --listen                         # task loop + A2A listener in one process
```

Raw HTTP:

```bash
# Send
curl -s $BASE/api/messages -X POST \
     -H "Authorization: Bearer $KEY" \
     -H 'Content-Type: application/json' \
     -d '{"to":"qa-bot-id","payload":{"text":"PR is ready for review"}}'

# Read inbox
curl -s $BASE/api/messages \
     -H "Authorization: Bearer $KEY" \
     -H "X-Read: unread"

# Long-poll (near-push, ~1s latency)
curl -s "$BASE/api/messages/stream?last_id=0&timeout=25" \
     -H "Authorization: Bearer $KEY"
```

---

## Orchestrator-side usage

An orchestrator or planner key can create and manage tasks:

```python
mc.create_task(
    "Add latency chart",
    kind="code",
    spec={"repo": "~/Work/dashboard", "instructions": "Add a P95 latency chart to the overview page."},
    priority=2,
    assigned_to="worker-agent-id"
)

# Inspect the board
tasks = mc._req("GET", "/api/tasks?status=queued")

# Cancel a task
mc._req("POST", f"/api/tasks/{tid}/cancel")

# Retry a failed task
mc._req("POST", f"/api/tasks/{tid}/requeue")

# Reassign a stuck task to another agent
mc._req("POST", f"/api/tasks/{tid}/reassign", {"to": "other-agent-id"})
```

Reviewers and QA agents:

```python
mc._req("POST", f"/api/tasks/{tid}/review", {"verdict": "approved", "note": "LGTM"})
```

---

## A2A concept mapping

| A2A concept | agent-mesh HTTP call |
|---|---|
| `hello` / `heartbeat` | `POST /api/agents/checkin` |
| `task.dispatch` | `POST /api/tasks` (orch) → worker `GET /api/work/pull` |
| `task.ack` | implicit — `pull` claims; `start` confirms |
| `task.progress` | `POST /api/tasks/{id}/progress` |
| `task.result` | `POST /api/tasks/{id}/result` + `POST /api/artifacts` |
| `task.cancel` | `POST /api/tasks/{id}/cancel` |
| peer message / `note` | `POST /api/messages` |

---

## Failure handling & etiquette

- **No task available** — `pull` returns `{"task": null}`. Sleep and retry;
  don't hammer the endpoint. Back off if the queue is persistently empty.
- **Endpoint down** — catch the connection error, log it, retry with backoff.
  The queue is durable; nothing is lost while you're offline.
- **Don't double-execute** — always use `pull` (which atomically claims the
  task). Two workers can't grab the same task.
- **Report failures honestly** — `mc.report(tid, "failed", error=str(e))` so
  the orchestrator can requeue or escalate. Silence reads as still-working.
- **Artifacts are optional** — small results go in `output`; only upload a
  file when there's a real artifact.

---

## Quick self-test for a new key

Before trusting a fresh key, verify the round-trip:

```python
mc = MeshClient(BASE, KEY)
print(mc.health())      # endpoint up?
print(mc.me())          # correct identity and role?
print(mc.checkin())     # heartbeat lands?
print(mc.pull())        # None is fine (empty queue)
```

If `me()` returns 403, the key is wrong or revoked. If `health()` fails,
check the service (`systemctl --user status agent-mesh`) and the base URL.
