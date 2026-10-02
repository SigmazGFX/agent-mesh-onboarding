#!/usr/bin/env python3
"""mesh_orchestrator — tools for the orchestrator agent (any platform).

The orchestrator brain (an agent holding an 'orchestrator'-role mesh key, on
any platform — Claude Code, Codex, a custom LLM loop, a cron job, or a
Hermes subagent) uses this module to do project intake, decompose into tasks,
assign roles, spawn members, and track status — all thin wrappers over the
agent-mesh HTTP API. No SDK or framework dependency; pure Python stdlib.

Setup (on the master box or whichever box runs the orchestrator):
    export MESH_BASE_URL=http://127.0.0.1:4850   # or https://your-server.example.com/agent-mesh
    export MESH_ORCH_KEY=***                      # an orchestrator-role agent key

As a library:
    from mesh_orchestrator import Orchestrator
    o = Orchestrator()                      # reads env
    pid = o.create_project("Refactor auth", context={"repo": "~/Work/x"})
    o.add_task(pid, "Split token service", kind="code", priority=2)
    o.spawn_member("qa-bot", role="qa")
    print(o.project_status(pid))

As a CLI (for quick manual use / testing):
    python3 mesh_orchestrator.py list-projects
    python3 mesh_orchestrator.py create-project "Name" [--context JSON]
    python3 mesh_orchestrator.py add-task <project_id> "Title" [--kind code] [--priority 2] [--assignee ID]
    python3 mesh_orchestrator.py spawn-member "name" [--role worker]
    python3 mesh_orchestrator.py assign-role <agent_id> <role>
    python3 mesh_orchestrator.py project-status <project_id>
    python3 mesh_orchestrator.py members
"""
import json
import os
import sys
import urllib.request
import urllib.error


class MeshError(RuntimeError):
    pass


class Orchestrator:
    def __init__(self, base_url=None, api_key=None):
        self.base = (base_url or os.environ.get("MESH_BASE_URL",
                                                "http://127.0.0.1:4850")).rstrip("/")
        self.key = api_key or os.environ.get("MESH_ORCH_KEY", "")
        if not self.key:
            raise MeshError("no orchestrator key (set MESH_ORCH_KEY)")

    # -- low-level
    def _call(self, method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self.key)
        if data:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            try:
                d = json.loads(e.read().decode())
            except Exception:
                d = {"detail": str(e)}
            raise MeshError(f"{method} {path} -> HTTP {e.code}: {d.get('detail', d)}")
        except Exception as e:
            raise MeshError(f"{method} {path} -> {e}")

    # -- identity
    def whoami(self):
        return self._call("GET", "/api/agents/me")

    # -- projects
    def create_project(self, name, description="", context=None, owner_agent=None):
        body = {"name": name, "description": description,
                "context": context or {}}
        if owner_agent:
            body["owner_agent"] = owner_agent
        return self._call("POST", "/api/projects", body)

    def list_projects(self, status=None):
        q = f"?status={status}" if status else ""
        return self._call("GET", "/api/projects" + q)["items"]

    def get_project(self, pid):
        return self._call("GET", f"/api/projects/{pid}")

    def update_project(self, pid, **fields):
        return self._call("PATCH", f"/api/projects/{pid}", fields)

    def close_project(self, pid, status="done"):
        return self.update_project(pid, status=status)

    # -- tasks
    def add_task(self, project_id, title, kind="generic", priority=3,
                 spec=None, assigned_to=None, deadline=None):
        body = {"title": title, "kind": kind, "priority": priority,
                "spec": spec or {}, "project_id": project_id}
        if assigned_to:
            body["assigned_to"] = assigned_to
        if deadline:
            body["deadline"] = deadline
        return self._call("POST", "/api/tasks", body)

    def list_tasks(self, project_id=None, status=None, assigned_to=None):
        q = []
        if project_id:
            q.append(f"project_id={project_id}")
        if status:
            q.append(f"status={status}")
        if assigned_to:
            q.append(f"assigned_to={assigned_to}")
        qs = ("?" + "&".join(q)) if q else ""
        return self._call("GET", "/api/tasks" + qs)["items"]

    def cancel_task(self, tid):
        return self._call("POST", f"/api/tasks/{tid}/cancel", {})

    def requeue_task(self, tid):
        return self._call("POST", f"/api/tasks/{tid}/requeue", {})

    # -- members / roles
    def spawn_member(self, name, role="worker", caps=None):
        body = {"name": name, "role": role}
        if caps:
            body["caps"] = caps
        return self._call("POST", "/api/orch/spawn-member", body)

    def assign_role(self, agent_id, role):
        return self._call("PATCH", f"/api/admin/agents/{agent_id}",
                          {"role": role})

    def members(self):
        return self._call("GET", "/api/agents")["items"]

    # -- active swarm management
    def swarm_view(self):
        """Who's online, what each agent is doing, what's unassigned, who's idle.
        The orchestrator polls this to keep its attention on the project and
        keep agents busy (assign unassigned work to idle agents)."""
        return self._call("GET", "/api/orch/swarm-view")

    def reassign(self, task_id, to_agent_id):
        """Move a stuck/stale task to another agent (resets it to queued under
        the new owner). Use when an assigned worker doesn't pick up work in a
        reasonable time."""
        return self._call("POST", f"/api/tasks/{task_id}/reassign",
                          {"to": to_agent_id})

    # -- reporting
    def project_status(self, pid):
        p = self.get_project(pid)
        counts = p.get("tasks", {})
        total = sum(counts.values())
        done = counts.get("done", 0) + counts.get("approved", 0)
        failed = counts.get("failed", 0) + counts.get("rejected", 0)
        active = (counts.get("in_progress", 0) + counts.get("claimed", 0)
                  + counts.get("queued", 0))
        return {
            "id": p["id"], "name": p["name"], "status": p["status"],
            "tasks_total": total, "tasks_done": done, "tasks_failed": failed,
            "tasks_active": active, "by_status": counts,
        }


# ------------------------------------------------------------------ CLI
def _cli():
    import argparse
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("whoami")
    sub.add_parser("list-projects")
    p = sub.add_parser("create-project"); p.add_argument("name")
    p.add_argument("--description", default=""); p.add_argument("--context", default="{}")
    p = sub.add_parser("get-project"); p.add_argument("project_id")
    p = sub.add_parser("add-task"); p.add_argument("project_id"); p.add_argument("title")
    p.add_argument("--kind", default="generic"); p.add_argument("--priority", type=int, default=3)
    p.add_argument("--spec", default="{}"); p.add_argument("--assignee")
    p = sub.add_parser("list-tasks"); p.add_argument("--project"); p.add_argument("--status")
    p = sub.add_parser("spawn-member"); p.add_argument("name"); p.add_argument("--role", default="worker")
    p = sub.add_parser("assign-role"); p.add_argument("agent_id"); p.add_argument("role")
    sub.add_parser("members")
    p = sub.add_parser("project-status"); p.add_argument("project_id")
    sub.add_parser("swarm-view", help="who's online/working/idle + unassigned work")
    p = sub.add_parser("reassign", help="move a stuck task to another agent")
    p.add_argument("task_id"); p.add_argument("to_agent_id")
    args = ap.parse_args()

    o = Orchestrator()
    out = None
    if args.cmd == "whoami":
        out = o.whoami()
    elif args.cmd == "list-projects":
        out = o.list_projects()
    elif args.cmd == "create-project":
        out = o.create_project(args.name, args.description, json.loads(args.context))
    elif args.cmd == "get-project":
        out = o.get_project(args.project_id)
    elif args.cmd == "add-task":
        out = o.add_task(args.project_id, args.title, args.kind, args.priority,
                         json.loads(args.spec), args.assignee)
    elif args.cmd == "list-tasks":
        out = o.list_tasks(args.project, args.status)
    elif args.cmd == "spawn-member":
        out = o.spawn_member(args.name, args.role)
    elif args.cmd == "assign-role":
        out = o.assign_role(args.agent_id, args.role)
    elif args.cmd == "members":
        out = o.members()
    elif args.cmd == "project-status":
        out = o.project_status(args.project_id)
    elif args.cmd == "swarm-view":
        out = o.swarm_view()
    elif args.cmd == "reassign":
        out = o.reassign(args.task_id, args.to_agent_id)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    _cli()
