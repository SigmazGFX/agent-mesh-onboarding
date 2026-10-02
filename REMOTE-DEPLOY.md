# Deploying agent-mesh behind a reverse proxy (shared domain) — v0.9

> **Deciding whether / how to expose it to the internet?** Read
> [`EXPOSURE.md`](EXPOSURE.md) first — it's the implementer's guide with the
> decision framework, security checklist, and the tailscale option. This file is
> the *how-to* for the reverse-proxy path specifically.

This module is **portable**: one file (`mesh_server.py`), Python ≥3.9 standard
library only, no venv/pip. It's designed to run **behind an existing reverse
proxy** as a path-prefixed sub-app, so it can share a domain with another
application without fighting over ports or auth.

## The target shape

You want agents to call:

```
https://your-server.example.com/agent-mesh/api/agents
https://your-server.example.com/agent-mesh/api/work/pull
```

Replace `your-server.example.com` with your actual domain or IP. The
`/agent-mesh` path prefix keeps it completely isolated from anything else on
the same domain — no routing conflicts, no auth conflicts.

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
https://your-server.example.com/agent-mesh/api/agents
https://your-server.example.com/agent-mesh/api/work/pull
```
and the console is at `https://your-server.example.com/agent-mesh/`. The UI
auto-detects its base path (injected as `BASE`), so all its calls and download
links are correct with no client changes.

### 3. Make it persistent

**systemd user service** (Linux):

`~/.config/systemd/user/agent-mesh.service`:
```ini
[Unit]
Description=agent-mesh endpoint
After=network.target

[Service]
Type=simple
# Supply MESH_ADMIN_TOKEN here if you want to pin a token instead of using the auto-generated one.
# EnvironmentFile=%h/.config/agent-mesh/env
ExecStart=/usr/bin/python3 %h/agent-mesh/mesh_server.py \
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

**macOS launchd / Windows Task Scheduler / Docker:** run
`python3 mesh_server.py --data <dir> --port 4850 --base-path /agent-mesh`
as a persistent service in whatever way your platform supports.

---

## Option B — expose at the domain root (`/api/...` directly)

If you'd rather have agents call `https://your-server.example.com/api/agents`
with **no prefix**, you must make sure those paths don't collide with any
existing app on the same domain. Run mesh with **no** `--base-path` and proxy
`/api/*` (or `/`) to it. This is more invasive — prefer Option A unless you've
confirmed the path space is free.

---

## Onboarding agents (same on any box)

1. Open the console (`https://your-server.example.com/agent-mesh/`), unlock
   with the admin token.
2. Register each agent: name + role (+ optional **admin cap** for console
   access). Copy the one-time API key.
3. Give the key to the agent. It talks plain HTTP Bearer to the base URL — no
   SDK required; any language or runtime works:

```bash
BASE=https://your-server.example.com/agent-mesh
KEY=mesh_YOUR_KEY

# Checkin
curl -s $BASE/api/agents/checkin -X POST -H "Authorization: Bearer $KEY" -d '{}'

# Pull next assigned task
curl -s $BASE/api/work/pull -H "Authorization: Bearer $KEY"
```

Or use the bundled Python client:
```python
import sys; sys.path.insert(0, "/path/to/agent-mesh")
from mesh_server import MeshClient
mc = MeshClient("https://your-server.example.com/agent-mesh", "mesh_...")
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
curl -s https://your-server.example.com/agent-mesh/api/health
# {"ok": true, "version": "0.9", ...}
curl -s https://your-server.example.com/agent-mesh/api/agents \
     -H "Authorization: Bearer <agent-key>"
```
