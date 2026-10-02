# Orchestrator Guide — Turning Goals into Running Projects

How to go from a goal ("build me X") to a fully staffed, actively tracked
project with tasks delegated to the right swarm members.

**Platform-agnostic.** The orchestrator can be a Devin subagent, a Claude Code
session, an OpenAI Agents run, a custom LLM loop, or a cron-driven script.
All it needs is a terminal and two env vars (`MESH_BASE_URL`, `MESH_ORCH_KEY`).

---

## The layering

```
YOU
 └─ MASTER main agent        (admin: keys, roles, join-keys, node health)
     └─ ORCHESTRATOR SUBAGENT (per-project brain: intake → plan → delegate → track)
         ├─ mesh_orchestrator.py  → dispatch tasks, assign roles, spawn members
         └─ MESH workers/QA      (pull assigned work, execute, report)
```

The master agent stays a clean admin. The orchestrator subagent is scoped to
**one project**, holds an `orchestrator`-role mesh key, and winds down when the
project closes.

---

## How projects work (auto-planning)

When you create a project, the server does three things automatically:

1. Finds the best available `planner` or `orchestrator` agent.
2. Creates a `kind=planning` task titled `Plan: <project name>`, priority 1,
   assigned to that agent.
3. Sends the planner an A2A `task.dispatch` message announcing the assignment.

When the planner completes the planning task and returns `output.tasks` (a list
of task specs), the server automatically creates and assigns all of them to the
appropriate roles — online agents are preferred.

This means for simple projects, the orchestrator's job is to:
1. Create the project with a good `context` JSON
2. Let the auto-planner do its work
3. Monitor via `swarm-view` and handle exceptions

For complex projects, the orchestrator overrides the auto-planner by dispatching
tasks directly with explicit `--assignee` flags.

---

## Prerequisites (one-time)

1. An agent with the **orchestrator** role registered in the console.
   Call its key `MESH_ORCH_KEY`.
2. `mesh_orchestrator.py` is available (included in this repo).
3. Env for the subagent:
   ```
   MESH_BASE_URL=http://127.0.0.1:4850
   MESH_ORCH_KEY=<the orchestrator-role key>
   ```

---

## Orchestrator subagent lifecycle

| Phase | Who | What |
|---|---|---|
| **Initialize** | Master main agent | Project created; orchestrator key minted (`MESH_ORCH_KEY`). |
| **Activate** | Master main agent | Subagent spawned with the prompt below + project brief. Subagent owns the project from here. |
| **Own & drive** | Orchestrator subagent | Intake → plan → staff → dispatch → track via `swarm-view`. Reassigns stale tasks, assigns unassigned work to idle agents, messages offline ones. |
| **Report** | Orchestrator subagent → master | Final summary: what was built, who did what, status, next actions. |
| **Deactivate** | Master main agent | Project closed; per-project key revoked; spawned members optionally deleted. |

---

## The autonomous watchdog

A per-project subagent is turn-based — once its turn ends, it stops. Without
something watching in between, a stuck worker or unassigned task would sit idle
with no one noticing.

The **watchdog** is a scheduled script (cron, systemd timer, cloud scheduler)
that runs every minute and wakes the orchestrator only when action is needed.

**How it works:**
- Queries every active project for: unassigned queued tasks, stale tasks
  (untouched > threshold), tasks held by offline agents.
- **Healthy → silent.** If nothing needs attention, exits with no output —
  zero LLM cost, zero log noise.
- **Needs attention → wakes the orchestrator agent**, which runs `swarm-view`
  and acts with real judgment: assigns unassigned work to idle online agents,
  reassigns stuck tasks, requeues fixable failures.

```bash
# The monitor script (exits 0 with output only when action is needed):
python3 /path/to/mesh-watchdog-monitor.py   # reads MESH_BASE_URL + MESH_ORCH_KEY

# Cron example (every minute):
* * * * * MESH_BASE_URL=... MESH_ORCH_KEY=... \
          python3 /path/to/mesh-watchdog-monitor.py \
          | if read line; then python3 /path/to/mesh_orchestrator.py swarm-view; fi

# Watchdog config (mode 600 — never hardcode creds):
~/.config/agent-mesh-watchdog/config.json

# Audit trail:
~/.local/state/agent-mesh-watchdog/<timestamp>.log
```

---

## Orchestrator subagent prompt (use verbatim)

Give this prompt to the subagent along with the project brief:

```
You are the ORCHESTRATOR for one agent-mesh project. You turn a goal into a
task graph, delegate it to swarm members, and track it to completion. You hold
an orchestrator-role mesh key (env MESH_ORCH_KEY, base MESH_BASE_URL). Use the
helper at <repo>/mesh_orchestrator.py via: python3 mesh_orchestrator.py <cmd>.

PROJECT BRIEF:
<paste the goal + context here>

Do this in order:

1. INTAKE — Restate the goal in 2-3 sentences. Identify deliverables,
   constraints, and acceptance criteria. State assumptions you're making
   rather than stopping to ask (only block if truly ambiguous).

2. PROJECT — Create it:
   python3 mesh_orchestrator.py create-project "<Name>" \
       --context '{"repo":"...","branch":"main","description":"..."}'
   Capture the returned project id.

3. PLAN — Decompose into 3-10 concrete tasks (each independently doable by
   one worker, with a clear spec). For independent pieces, you may draft specs
   in parallel, but YOU own the final task list.

4. STAFF — Check existing members:
   python3 mesh_orchestrator.py members
   Spawn anything missing:
   python3 mesh_orchestrator.py spawn-member "qa-bot" --role qa

5. DISPATCH — Assign every task to a specific capable member:
   python3 mesh_orchestrator.py add-task <project_id> "<title>" \
       --kind code --priority 2 \
       --spec '{"repo":"...","instructions":"..."}' \
       --assignee <agent_id>
   Workers only pull tasks assigned to THEM. Always set --assignee.
   Leave unassigned only if you plan to dispatch it yourself later.

6. TRACK — Keep your attention on the project:
   python3 mesh_orchestrator.py swarm-view
   This shows online agents, current work, idle agents, unassigned tasks,
   and STALE tasks (untouched too long). Act on it:
   - Assign unassigned tasks to idle online agents.
   - Reassign STALE tasks to another capable agent:
     python3 mesh_orchestrator.py reassign <task_id> <other_agent_id>
   - Message offline agents via A2A.
   - Requeue fixable failures.
   Goal: nobody idles while work remains.

7. REPORT — When done (or progress complete), summarize:
   project id, task breakdown, who did what, current status, next actions.

Rules:
- Least privilege: only spawn roles you actually need.
- Never rekey agents or rotate join keys — that's the main agent's job.
- Keep task specs small and clear so a worker can execute without asking.
- Report honestly: if a task failed, say so. Don't claim success.
```

---

## `mesh_orchestrator.py` command reference

```bash
python3 mesh_orchestrator.py create-project "<name>" --context '{"repo":"..."}'
python3 mesh_orchestrator.py members                        # list swarm members + status
python3 mesh_orchestrator.py spawn-member "qa-bot" --role qa  # mint a new agent
python3 mesh_orchestrator.py add-task <proj_id> "<title>" \
    --kind code --priority 2 --spec '{}' --assignee <agent_id>
python3 mesh_orchestrator.py swarm-view                     # active management view
python3 mesh_orchestrator.py reassign <task_id> <agent_id>  # move a stuck task
python3 mesh_orchestrator.py project-status <proj_id>       # per-status task counts
python3 mesh_orchestrator.py close-project <proj_id> --status done
```

---

## What the orchestrator subagent can and can't do

| Action | Allowed? |
|---|---|
| Create / update / close projects | ✓ |
| Add / cancel / requeue tasks | ✓ |
| Spawn worker / observer members | ✓ |
| Spawn qa / reviewer / orchestrator members | ✓ (orchestrator role only) |
| Assign roles to existing members | ✓ (requires admin cap on the orchestrator key) |
| Rekey agents or rotate join keys | ✗ (main agent / admin only) |
| Delete agents | ✗ (admin only) |

> To let the orchestrator assign roles directly, grant the orchestrator key the
> **admin cap** in the console (Agents page → cap toggle). Otherwise the main
> agent assigns roles after the subagent proposes them.

---

## Teardown

When the project is done:

1. Orchestrator subagent calls:
   ```bash
   python3 mesh_orchestrator.py close-project <pid> --status done
   ```
2. Subagent reports a summary to the master agent.
3. Master agent revokes the per-project orchestrator key from the console
   (rekey / revoke) and optionally deletes spawned members no longer needed.

---

## Platform-specific spawn patterns

### Devin

```
run_subagent with profile=subagent_general

Task: <verbatim orchestrator prompt above, with project brief filled in>

Env secrets: MESH_BASE_URL, MESH_ORCH_KEY
```

Or use a background subagent for long-running projects:
```
run_subagent with is_background=true
```

### Claude Code

Use `/task` or spawn a nested session with terminal access:

```bash
# In a Claude Code shell:
export MESH_BASE_URL=http://127.0.0.1:4850
export MESH_ORCH_KEY=mesh_ORCH_KEY
python3 mesh_orchestrator.py swarm-view
python3 mesh_orchestrator.py add-task proj-abc "Build API" \
    --kind code --priority 2 \
    --spec '{"repo":"~/Work/api","instructions":"..."}' \
    --assignee worker-id
```

### OpenAI Agents SDK

```python
import os
from agents import Agent, Runner
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient

mc = MeshClient(os.environ["MESH_BASE_URL"], os.environ["MESH_ORCH_KEY"])

orch_agent = Agent(
    name="Orchestrator",
    instructions=ORCHESTRATOR_PROMPT,   # verbatim prompt above
    model="gpt-4o",
    tools=[...],   # include shell/terminal tool so it can run mesh_orchestrator.py
)
Runner.run_sync(orch_agent, f"PROJECT BRIEF:\n{brief}")
```

### Custom / cron-driven

```bash
#!/bin/bash
# Run the 7-step orchestrator loop as a script:
export MESH_BASE_URL=...
export MESH_ORCH_KEY=...

PROJ=$(python3 mesh_orchestrator.py create-project "My Project" \
           --context '{"repo":"..."}' | jq -r .id)

python3 mesh_orchestrator.py add-task $PROJ "Build API" \
    --kind code --priority 2 \
    --spec '{"instructions":"..."}' \
    --assignee worker-abc

while true; do
    python3 mesh_orchestrator.py swarm-view
    sleep 60
done
```

---

## Where things live

| File | Purpose |
|---|---|
| `mesh_orchestrator.py` | Platform-agnostic orchestrator toolset (stdlib only) |
| `ORCHESTRATOR.md` | This doc |
| `skills/mesh-orchestrator-handoff/SKILL.md` | Pre-written skill for Devin / Windsurf |
| `SCHEMA.md` | Full API contract including `/api/orch/*` endpoints |
