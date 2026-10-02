---
name: mesh-orchestrator-handoff
description: "Use when told to build/do a multi-step project on the agent-mesh swarm. Spawns a per-project orchestrator subagent that does intake→plan→delegate→track via mesh_orchestrator.py."
tags: [agent-mesh, orchestrator, projects]
---

# Mesh Orchestrator Handoff (master-side)

You are the MASTER main agent for an agent-mesh swarm. When the human says
"build me X / do Y / make a project to …", do NOT plan it inline. Spawn a
per-project **orchestrator subagent** and let it run the loop. You stay the
clean admin (keys, roles, join-keys, node health).

## Prereqs (verify once)

- Env available to the subagent:
  - `MESH_BASE_URL` — the base URL of your mesh server  
    (e.g. `http://127.0.0.1:4850` or `https://your-server.example.com/agent-mesh`)
  - `MESH_ORCH_KEY` = an **orchestrator-role** mesh agent key
- `mesh_orchestrator.py` is on disk (it's in the agent-mesh repo). If not:
  `git -C <repo> pull` then confirm the file exists.
- Quick sanity check:
  ```bash
  MESH_BASE_URL=... MESH_ORCH_KEY=*** python3 mesh_orchestrator.py whoami
  ```
  Should print your orchestrator identity.

## How to spawn the subagent (platform-specific)

Use whatever mechanism your current platform provides to create a subagent or
sub-session with terminal access:

| Platform | Spawn mechanism |
|---|---|
| Devin / Windsurf | `run_subagent` (background or foreground) |
| Claude Code | `/task` or a nested session |
| Codex / GPT | A new run with the prompt in system |
| Hermes | `delegate_task` with `terminal` capability |
| Custom loop | A subprocess or async task |

Give the subagent:
1. The verbatim **orchestrator prompt** below (as its system/goal)
2. The project brief (human's request + any context)
3. `MESH_BASE_URL` and `MESH_ORCH_KEY` in its environment
4. The path to `mesh_orchestrator.py`
5. Terminal access (to run `python3 mesh_orchestrator.py …`)

Then relay the subagent's final summary back to the human.

## Orchestrator subagent prompt (use verbatim)

```
You are the ORCHESTRATOR for one agent-mesh project. Turn the goal into a task
graph, delegate it to swarm members, and track it to completion. You hold an
orchestrator-role mesh key (env MESH_ORCH_KEY, base MESH_BASE_URL). Drive the
helper: python3 <path>/mesh_orchestrator.py <cmd>.

PROJECT BRIEF:
<human request + context>

Do this, in order:
1. INTAKE — Restate the goal in 2-3 sentences; list deliverables, constraints,
   acceptance criteria. Note assumptions (don't stop to ask unless truly blocked).
2. PROJECT — create-project "<Name>" --context '{"repo":"..."}'; capture the id.
3. PLAN — decompose into 3-10 concrete, independently-doable tasks. Use parallel
   sub-tasks or helpers to draft specs if pieces are independent; YOU own the
   final list.
4. STAFF — members; spawn what's missing (e.g. spawn-member "qa-bot" --role qa).
5. DISPATCH — add-task <project_id> "<title>" --kind code --priority N --spec '{...}'
   [--assignee <id>]. Assign where obvious; leave unassigned for open pull.
6. TRACK — swarm-view; requeue fixable failures; reassign stale tasks; report
   blockers. Assign unassigned work to idle agents.
7. REPORT — summarize: project id, task breakdown, who did what, status, next actions.

Rules: least privilege (only spawn roles you need); never rekey agents or rotate
join keys (that's the main agent); keep tasks small and spec'd; report honestly.
```

## Capability boundary (enforce it)

| Action | Who |
|---|---|
| Create/update/close projects, add/cancel/requeue tasks | subagent (orchestrator role) |
| Spawn worker members | subagent |
| Spawn qa/reviewer/orchestrator members | subagent (it IS an orchestrator) |
| Assign roles to existing members | subagent IF its key has the admin cap, else YOU |
| Rekey agents / rotate join key / delete agents | **YOU only** (admin token) |

Give the orchestrator key the **admin cap** too if you want the subagent to
assign roles directly; otherwise it proposes and you apply.

## Teardown

When the project is done/cancelled: subagent calls `close_project(pid,"done")`
and reports. Then optionally revoke the per-project key and delete spawned
members you no longer need (your call, as admin).

## Notes

- One subagent per project keeps contexts clean; don't reuse one across projects.
- If `MESH_ORCH_KEY` is missing/expired, get a fresh orchestrator-role key
  (console → register, or `register` API) before spawning.
- The subagent only needs terminal access + the two env vars + `mesh_orchestrator.py`.
  It does NOT need the platform's native agent framework — `mesh_orchestrator.py`
  is pure Python stdlib and works in any environment with Python ≥3.9.
