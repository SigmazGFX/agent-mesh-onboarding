# agent-mesh — Hermes Dashboard Adapter (optional)

> **This is a Hermes-specific adapter.** It is not required for any other
> agent platform. The agent-mesh web console (`http://<host>:4850/`) works on
> its own for all platforms. Install this only if you run a Hermes dashboard
> and want a native tab for it.

A native tab in the Hermes web portal / desktop app that shows your local
agent-mesh swarm: live status, agents, projects, tasks, and events. Built as a
standard Hermes **dashboard plugin** (same pattern as Kanban), so it appears as
a first-class tab — no separate browser window.

## What it shows

| Sub-tab | Data |
|---|---|
| **Overview** | Queued/active/done/failed counts, per-agent online/idle status + current task, and a "needs attention" list (unassigned + stale tasks). |
| **Agents** | Every registered agent: role, status, last-seen, key prefix. |
| **Projects** | Each project with task counts by status. |
| **Tasks** | Filterable task table (status filter), assignee, priority, updated. |
| **Events** | The audit log (newest first). |

The panel points at the **local mesh** (`127.0.0.1:4850`) by default. To point
it at a remote swarm, set `MESH_BASE_URL` on the dashboard backend's environment.
The admin token is read server-side from
`~/.local/state/agent-mesh/admin_token` (or `MESH_ADMIN_TOKEN`) and is **never
exposed to the browser**.

## Install (on a Hermes box)

```bash
# 1. Copy the plugin into the user plugins dir
mkdir -p ~/.hermes/plugins/agent-mesh
cp -r hermes-plugin/dashboard ~/.hermes/plugins/agent-mesh/dashboard

# 2. Enable it (required before its Python runs — security gate)
hermes config set plugins.enabled '["agent-mesh"]'   # or add to existing list

# 3. Restart the dashboard (or the desktop app) so it discovers the new tab
#    e.g. systemctl --user restart <your dashboard service>, or relaunch the app
```

After restart, an **agent-mesh** tab appears in the Hermes dashboard (positioned
after Skills). Click it to see the swarm.

## How it works

- **Frontend** (`dashboard/dist/index.js`): a plain IIFE, no build step. Uses
  `window.__HERMES_PLUGIN_SDK__` (React + shadcn primitives) and registers via
  `window.__HERMES_PLUGINS__.register("agent-mesh", Component)`. Polls every 5s.
- **Backend** (`dashboard/plugin_api.py`): a FastAPI router mounted at
  `/api/plugins/agent-mesh/` by the dashboard host. Read-only proxy to the mesh;
  attaches the admin token server-side.
- **Manifest** (`dashboard/manifest.json`): declares the tab path, entry, icon.

## Files

```
hermes-plugin/dashboard/
├── manifest.json      # tab + entry + api declaration
├── plugin_api.py      # FastAPI router (backend)
└── dist/index.js      # React IIFE (frontend, no build step)
```

## Notes

- Read-only: the panel surfaces status; mutations (assign roles, create tasks,
  reassign) are done via the `mesh` CLI / `mesh_orchestrator.py` or the mesh's
  own console. Adding write actions is a natural extension.
- The dashboard must be (re)started after installing/enabling for the tab to
  appear — discovery happens at server boot.
