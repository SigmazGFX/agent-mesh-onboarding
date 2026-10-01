# Deploying agent-mesh on a remote Hermes box (e.g. bytemecarl.io)

This module is **portable**: one file (`mesh_server.py`), Python ≥3.9 standard
library only, no venv/pip. It's designed to run **behind an existing reverse
proxy** as a path-prefixed sub-app, so it can share a domain with another app
(like a Hermes web portal) without fighting over ports or auth.

## The target shape

You want agents to call something like:

```
https://bytemecarl.io/api/agents
https://bytemecarl.io/api/work/pull
...
```

Note: `bytemecarl.io` already runs its own uvicorn app with **cookie-based**
auth (`/api/agents` there returns `401 no_cookie`). agent-mesh uses **Bearer
API-key** auth — a completely different scheme. So you have two clean options;
pick based on how much you want to touch the existing proxy.

---

## Option A — mount under a path prefix (recommended, zero conflict)

Run mesh on its own localhost port with a base-path prefix, and have the
reverse proxy route that path to it. Everything else on the domain is untouched.

### 1. Run the server

```bash
python3 /path/to/mesh_server.py \
    --data ~/.local/state/agent-mesh \
    --host 127.0.0.1 \
    --port 4850 \
    --base-path /agent-mesh
```

- `--base-path /agent-mesh` makes the app serve at `/agent-mesh/...` and
  **require** that prefix (any request without it gets a clean JSON 404, so a
  path the proxy doesn't route here can never hit mesh at its root).
- First run prints a one-time **admin token** — capture it (it's stored hashed;
  never shown again). Or preset it with `MESH_ADMIN_TOKEN=...`.

Local URLs are now:
```
http://127.0.0.1:4850/agent-mesh/            # web console
http://127.0.0.1:4850/agent-mesh/api/health  # health
```

### 2. Reverse-proxy rule

Route the prefix to the local port. The exact snippet depends on your proxy:

**Caddy:**
```caddy
handle_path /agent-mesh/* {
    reverse_proxy 127.0.0.1:4850
}
```
(`handle_path` strips `/agent-mesh` before forwarding — but our server also
accepts the full prefixed path, so a plain `reverse_proxy` without stripping
works too. Use whichever your setup prefers; both are handled.)

**nginx:**
```nginx
location /agent-mesh/ {
    proxy_pass http://127.0.0.1:4850;   # keep the prefix
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}
```

**traefik:**
```
[router] Rule = PathPrefix(`/agent-mesh`)
[service]  URL = http://127.0.0.1:4850
```

After this, agents talk to:
```
https://bytemecarl.io/agent-mesh/api/agents
https://bytemecarl.io/agent-mesh/api/work/pull
```
and the console is at `https://bytemecarl.io/agent-mesh/`. The UI auto-detects
its base path (injected as `BASE`), so all its calls and download links are
correct with no client changes.

### 3. Make it persistent (systemd user service)

`~/.config/systemd/user/agent-mesh.service`:
```ini
[Unit]
Description=agent-mesh endpoint
After=network.target

[Service]
Type=simple
EnvironmentFile=%h/.hermes/.env        # supplies MESH_ADMIN_TOKEN
ExecStart=/usr/bin/python3 %h/Work/agent-mesh/mesh_server.py \
    --data %h/.local/state/agent-mesh --host 127.0.0.1 --port 4850 \
    --base-path /agent-mesh
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
```
```bash
systemctl --user daemon-reload
systemctl --user enable --now agent-mesh
journalctl --user -u agent-mesh -f
```

---

## Option B — expose at the domain root (`/api/...` directly)

If you'd rather have agents call `https://bytemecarl.io/api/agents` with **no
prefix**, you must make sure those paths don't collide with the existing app.
That means either (a) the existing app doesn't actually use `/api/agents`, or
(b) you move/rename the existing app's conflicting routes. Then run mesh with
**no** `--base-path` and proxy `/api/*` (or `/`) to it. This is more invasive —
prefer Option A unless you've confirmed the path is free.

> If you go this route, the existing portal's cookie-auth and mesh's Bearer
> auth coexist fine because they're separate backends behind separate location
> rules — but you own the routing decision, so verify the split carefully.

---

## Onboarding agents (same on any box)

1. Open the console (`https://bytemecarl.io/agent-mesh/`), unlock with the admin
   token.
2. Register each agent: name + role (+ optional **admin cap** for console
   access). Copy the one-time API key.
3. Give the key to the agent. It talks plain HTTP Bearer to the base URL:

```python
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient
mc = MeshClient("https://bytemecarl.io/agent-mesh", "mesh_...")
mc.checkin(); task = mc.pull()
```

Roles are managed by a human in the portal (Agents page) — change role inline,
grant/revoke the admin cap, rekey/revoke.

---

## Security notes for a public domain

- TLS terminates at the proxy (you already have HTTPS). mesh itself is plain
  HTTP on localhost — that's correct; don't expose the raw port publicly.
- Keep `--host 127.0.0.1` so only the proxy can reach it.
- Bearer keys are per-agent and hashed at rest; rotate via rekey if leaked.
- Review/approve requires a real reviewer-role agent (the bare admin token is a
  viewer only) — see OPERATIONS.md §3.

## Quick smoke test after deploy

```bash
curl -s https://bytemecarl.io/agent-mesh/api/health
# {"ok": true, "version": "0.1", ...}
curl -s https://bytemecarl.io/agent-mesh/api/agents -H "Authorization: Bearer <agent-key>"
```
