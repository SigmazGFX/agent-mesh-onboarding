#!/usr/bin/env python3
"""agent_worker.py — a REAL agent worker for agent-mesh.

Unlike `mesh worker` (a dumb shell-executor that marks spec-less tasks "done /
no-op"), this worker is designed to be driven by an LLM agent: it claims ONE
task, prints its full spec as JSON, and waits for you (the agent) to do the
actual work and report a real result. No faking, no auto-close.

Two modes:
  --once      claim one task, print spec JSON, exit. You do the work, then run
              `report` with the real outcome. (Recommended for LLM-driven use.)
  --loop      keep claiming+printing tasks until Ctrl-C (you handle each).

Report your real result afterwards:
  agent_worker.py report <task_id> --status ok --output '{"summary":"...","commit":"abc"}'
  agent_worker.py report <task_id> --status failed --error "what went wrong"

It reads the same config as the `mesh` CLI (~/.config/agent-mesh/config.json).
"""
import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error

CFG = os.path.expanduser("~/.config/agent-mesh/config.json")


def load_cfg():
    if not os.path.exists(CFG):
        sys.exit("no config at %s — run ./install.sh guest first" % CFG)
    with open(CFG) as f:
        return json.load(f)


def call(cfg, method, path, body=None, _exit=True):
    url = cfg["base_url"].rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", "Bearer " + cfg["api_key"])
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            d = json.loads(e.read().decode())
        except Exception:
            d = {"detail": str(e)}
        msg = "%s %s -> HTTP %s: %s" % (method, path, e.code, d.get("detail", d))
        if _exit:
            sys.exit(msg)
        raise RuntimeError(msg)
    except Exception as e:
        if _exit:
            sys.exit("%s %s -> %s" % (method, path, e))
        raise


def cmd_once(cfg, poll):
    """Claim one task; print its full spec as JSON for the agent to act on."""
    while True:
        call(cfg, "POST", "/api/agents/checkin", {}, _exit=False)
        d = call(cfg, "GET", "/api/work/pull", _exit=False)
        task = d.get("task")
        if task:
            # Emit a clean machine-readable brief the agent can read.
            print(json.dumps({
                "action": "TASK_CLAIMED",
                "task_id": task["id"],
                "title": task["title"],
                "kind": task.get("kind"),
                "priority": task.get("priority"),
                "spec": task.get("spec"),
                "project_id": task.get("project_id"),
                "instructions": (
                    "Do the REAL work described in 'spec'. Then report:\n"
                    "  agent_worker.py report %s --status ok --output '<json>'\n"
                    "or on failure:\n"
                    "  agent_worker.py report %s --status failed --error 'why'"
                    % (task["id"], task["id"])),
            }, indent=2))
            return
        time.sleep(poll)


def cmd_loop(cfg, poll):
    print("[agent_worker] loop mode — will claim+print each task; Ctrl-C to stop",
          flush=True)
    while True:
        try:
            cmd_once(cfg, poll)
        except KeyboardInterrupt:
            print("\n[agent_worker] stopped")
            return


def cmd_report(cfg, task_id, status, output, error):
    body = {"status": status}
    if output:
        body["output"] = json.loads(output)
    if error:
        body["error"] = error
    d = call(cfg, "POST", f"/api/tasks/{task_id}/result", body)
    print(json.dumps(d, indent=2))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")

    p = sub.add_parser("once", help="claim one task, print its spec, exit")
    p.add_argument("--poll", type=float, default=15,
                   help="seconds to wait when nothing is queued")

    p = sub.add_parser("loop", help="keep claiming+printing tasks")
    p.add_argument("--poll", type=float, default=15)

    p = sub.add_parser("report", help="report a real result for a claimed task")
    p.add_argument("task_id")
    p.add_argument("--status", required=True, choices=["ok", "failed", "partial"])
    p.add_argument("--output", help="JSON object of real results")
    p.add_argument("--error", help="what went wrong (for failed)")

    args = ap.parse_args()
    cfg = load_cfg()
    if args.cmd == "once":
        cmd_once(cfg, args.poll)
    elif args.cmd == "loop":
        cmd_loop(cfg, args.poll)
    elif args.cmd == "report":
        cmd_report(cfg, args.task_id, args.status, args.output, args.error)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
