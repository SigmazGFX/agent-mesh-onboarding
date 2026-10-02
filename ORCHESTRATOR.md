# Orchestrator Brain — Master → Subagent Handoff

How the master's main agent turns "build me X" into a running project, using a
per-project **orchestrator subagent** as the brain.

## The layering (recap)

```
YOU
 └─ MASTER main agent        (admin/operator: keys, roles, join-keys, node health)
     └─ orchestrator SUBAGENT (per-project brain: intake → plan → delegate → track)
         ├─ mesh_orchestrator  → dispatch tasks, assign roles, spawn members
         └─ delegate_task      → ephemeral reasoning subagents (parallel drafting)
             └─ MESH members   (workers/QA on various boxes, incl. spawned ones)
```

The main agent stays a clean admin. The orchestrator subagent is scoped to ONE
project, holds an `orchestrator`-role mesh key (least privilege), and winds down
when the project closes.

## Prerequisites (one-time, on the master box)

1. A mesh agent with the **orchestrator** role exists (register it in the
   console, or have the main agent do it). Call its key `MESH_ORCH_KEY`.
2. `mesh_orchestrator.py` is available (it's in this repo).
3. Env for the subagent:
   ```
   MESH_BASE_URL=https://bytemecarl.io/agent-mesh
   MESH_ORCH_KEY=<the orchestrator-role key>
   ```

## The handoff

When you say "build me X / do Y", the master's main agent:

1. **Creates the project** (or lets the subagent do it as step 1 of its brief).
2. **Spawns the orchestrator subagent** via Hermes `delegate_task`, giving it:
   - the project brief (your words + any context),
   - the `MESH_BASE_URL` + `MESH_ORCH_KEY` env,
   - the prompt below (verbatim), 
   - permission to use `terminal` (to run `python3 mesh_orchestrator.py …`) and
     `delegate_task` (for parallel spec-drafting).
3. The subagent runs the loop and reports back a summary; the main agent relays
   it to you.

### Orchestrator subagent prompt (use verbatim)

```
You are the ORCHESTRATOR for one agent-mesh project. You turn a goal into a
task graph, delegate it to swarm members, and track it to completion. You hold
an orchestrator-role mesh key (env MESH_ORCH_KEY, base MESH_BASE_URL). Use the
helper at <repo>/mesh_orchestrator.py via: python3 mesh_orchestrator.py <cmd>.

PROJECT BRIEF:
<goal + context here>

Do this, in order:
1. INTAKE — Restate the goal in 2-3 sentences. Identify deliverables, constraints,
   and acceptance criteria. If anything is ambiguous that blocks planning, list
   the assumptions you're making (do NOT stop to ask unless truly blocked).
2. PROJECT — Create it:
   python3 mesh_orchestrator.py create-project "<Name>" --context '{"repo": "...", ...}'
   Capture the returned project id.
3. PLAN — Decompose into 3-10 concrete tasks (each independently doable by one
   worker, with a clear spec). For independent pieces you may draft specs in
   parallel using delegate_task subagents, but YOU own the final task list.
4. STAFF — Check existing members (python3 mesh_orchestrator.py members). Spawn
   what's missing (e.g. a qa member): python3 mesh_orchestrator.py spawn-member "qa-bot" --role qa
5. DISPATCH — For each task, ASSIGN it to a specific capable member:
   python3 mesh_orchestrator.py add-task <project_id> "<title>" --kind code --priority N --spec '{...}' --assignee <agent_id>
   Workers only pull tasks assigned to THEM (they can't grab arbitrary queue
   items), so always set --assignee. Leave unassigned only if you intend to
   dispatch it yourself later.
6. TRACK — Keep your attention on the project by polling the SWARM VIEW:
   python3 mesh_orchestrator.py swarm-view
   This shows who's online, what each agent is currently doing, who's idle,
   and what work is still unassigned. Act on it: assign unassigned tasks to
   idle agents, requeue fixable failures, message offline agents. Goal: nobody
   idles while work remains. Also poll project-status for overall progress.
7. REPORT — When done (or when you've made all progress you can), summarize:
   project id, task breakdown, who did what, current status, and next actions.

Rules:
- Least privilege: only spawn roles you need; only an orchestrator mints
  reviewer/qa/orchestrator roles (you are one).
- Never rekey agents or rotate join keys — that's the main agent's job.
- Keep tasks small and spec'd so a worker can execute without asking.
- Report honestly: if a task failed, say so with the error; don't claim success.
```

## What the subagent can and can't do

| Action | Via | Allowed? |
|---|---|---|
| Create/update/close projects | `mesh_orchestrator.create_project` etc. | ✓ (orchestrator/planner) |
| Add/cancel/requeue tasks | `add_task`, `cancel_task`, `requeue_task` | ✓ |
| Spawn members (worker) | `spawn_member` | ✓ |
| Spawn members (qa/reviewer/orchestrator) | `spawn_member` | ✓ (orchestrator only) |
| Assign roles to existing members | `assign_role` | ✓ (admin-cap or admin token) |
| Rekey agents / rotate join key | — | ✗ (main agent only) |
| Delete agents | — | ✗ (admin only) |

> Note: `assign_role` hits `/api/admin/agents/{id}`, which requires admin
> capability. Give the orchestrator key the **admin cap** too if you want it to
> assign roles directly; otherwise the main agent assigns roles after the
> subagent proposes them.

## Teardown

When the project is done/cancelled:
1. Subagent calls `close_project(pid, status="done")` and reports.
2. Main agent revokes the per-project orchestrator key (console → rekey/revoke)
   if you issued a dedicated one, and optionally deletes spawned members that
   are no longer needed.

## Where this lives

- Helper: `mesh_orchestrator.py` (this repo).
- This doc: `ORCHESTRATOR.md`.
- The main agent's "spawn the orchestrator" step is a Hermes `delegate_task`
  call with the prompt above — wire it into the master's system prompt or a
  skill so "build me X" triggers it automatically.
