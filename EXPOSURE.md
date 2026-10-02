# Exposing agent-mesh to the internet — Master Implementer's Guide

This is for the person who runs the **master** node and wants agents on *other
boxes* (or humans from other networks) to reach the swarm over the internet.
It explains **what is required**, **what the installer does and does not do for
you**, and gives an exact checklist + config. If you're only running the swarm
on one trusted machine or over a private network (tailscale/LAN), you can skip
most of this — see [§6](#6-when-you-dont-need-internet-exposure).

> Short version: **agent-mesh has no TLS and no public exposure of its own.**
> You put it behind a reverse proxy that terminates TLS and routes one path to
> it. The server stays bound to `127.0.0.1`. That's the whole model.

---

## 1. What "expose to the internet" actually means here

Three separate things must be true, and the installer handles **none** of them
for you:

| Requirement | Who does it | Why |
|---|---|---|
| **TLS** (HTTPS) | Your reverse proxy / CDN | mesh is plain HTTP; browsers and agents need a trusted cert. mesh does not terminate TLS. |
| **Routing** (public path → local port) | Your reverse proxy | Agents call `https://yourdomain/agent-mesh/...`; the proxy forwards to `127.0.0.1:4850`. |
| **Base-path awareness** | The mesh server (`--base-path`) | So the app knows it's mounted at `/agent-mesh` and builds correct URLs. |

The installer (`./install.sh master`) deliberately keeps the server **local and
safe by default**: it binds `127.0.0.1:4850`, generates the admin token, issues
a join key, and starts the service. It does **not** open any port, set up a
proxy, or add TLS. Exposure is an explicit, separate step you perform.

```
internet ──▶ yourdomain.com (reverse proxy: TLS + routing)
                │  PathPrefix(/agent-mesh)
                ▼
        127.0.0.1:4850  ◀── mesh_server.py --base-path /agent-mesh
                (localhost only — nothing else can reach it)
```

Guests then enroll against the **public URL**:
`./install.sh guest` → base url `https://yourdomain.com/agent-mesh` → join key.

---

## 2. Decision framework (read this first)

Before you expose anything, answer these. Most internal swarms should stop at
option A or B and never touch option C.

**Q1 — Do the agents need to reach the master from outside this machine?**
- No (same box / LAN / tailscale) → **don't expose publicly.** Use §6.
- Yes → continue.

**Q2 — Is there already a trusted domain + reverse proxy you control?**
- Yes → **Option A: mount under a path prefix** (§3). This is the recommended,
  zero-conflict way.
- No, but you have a bare VPS → stand up a proxy (Caddy/nginx/traefik) with a
  cert, then Option A.
- You'd rather not run a proxy at all → **Option B: tailscale serve** (§5) —
  exposes to your tailnet only, no public internet, no proxy config.

**Q3 — Will untrusted parties ever hit this endpoint?**
- If yes, you **must** add throttling/WAF in front (mesh has no rate limiting)
  and treat the admin token as crown jewels. See §7. For a trusted org, keep it
  off the open internet entirely.

---

## 3. Option A — reverse proxy with a path prefix (recommended)

Mount mesh under `/agent-mesh` so it shares a domain with whatever else you run
without fighting over ports or auth schemes.

### Step 1 — Run the server with a base path

Stop the installed service, then start it with `--base-path`:

```bash
systemctl --user stop agent-mesh

python3 /path/to/mesh_server.py \
    --data ~/.local/state/agent-mesh \
    --host 127.0.0.1 \
    --port 4850 \
    --base-path /agent-mesh
```

What `--base-path /agent-mesh` does:
- Serves everything under `/agent-mesh/...`.
- **Requires** that prefix — any request without it gets a clean JSON 404, so a
  path the proxy doesn't route here can never hit mesh at its root.
- The web UI auto-detects its base path (injected as `BASE`), so all its calls
  and download links are correct with no client changes.

Local URLs become:
```
http://127.0.0.1:4850/agent-mesh/            # console
http://127.0.0.1:4850/agent-mesh/api/health  # health
```

Make it persistent by editing the systemd unit's `ExecStart` to include
`--base-path /agent-mesh`, then `systemctl --user daemon-reload && systemctl
--user restart agent-mesh`. (The unit template in [REMOTE-DEPLOY.md](REMOTE-DEPLOY.md)
shows the full file.)

### Step 2 — Add the proxy rule

Route the prefix to the local port. Pick your proxy:

**Caddy** (auto-TLS):
```caddy
handle_path /agent-mesh/* {
    reverse_proxy 127.0.0.1:4850
}
```
(`handle_path` strips `/agent-mesh` before forwarding; mesh also accepts the
full prefixed path, so a plain `reverse_proxy` without stripping works too.)

**nginx** (with certbot/cert-manager for TLS):
```nginx
location /agent-mesh/ {
    proxy_pass http://127.0.0.1:4850;   # keep the prefix
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    # SSE: don't buffer the live-update stream
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 300s;
}
```

**traefik:**
```
[router] Rule = PathPrefix(`/agent-mesh`)
[service]  URL = http://127.0.0.1:4850
```

> **SSE note:** the console's instant updates use Server-Sent Events
> (`GET /api/stream`). Make sure your proxy **doesn't buffer** that response
> (the server sends `X-Accel-Buffering: no` and `Cache-Control: no-cache`;
> nginx additionally needs `proxy_buffering off` as shown above) and allows a
> long idle read timeout, or the live feed will stall.

### Step 3 — Verify

```bash
curl -s https://yourdomain.com/agent-mesh/api/health
# {"ok": true, "version": "0.7", "agents": N, "tasks_queued": N}

curl -s https://yourdomain.com/agent-mesh/api/agents \
     -H "Authorization: Bearer ***"
```

Open `https://yourdomain.com/agent-mesh/` in a browser, unlock with the admin
token, and confirm the dashboard loads and updates live.

### Step 4 — Onboard guests against the public URL

Hand each new box:
```bash
git clone https://github.com/SigmazGFX/agent-mesh.git && cd agent-mesh
./install.sh guest
#   base url : https://yourdomain.com/agent-mesh
#   join key : join_XXXXXXX   (from the master console → Agents → issue/rotate)
```
Then assign the guest a real role in the console.

---

## 4. Option B — expose at the domain root (only if the path is free)

If you want agents to call `https://yourdomain.com/api/agents` with **no
prefix**, you must guarantee those paths don't collide with anything else on
that domain. That means the existing app doesn't use `/api/agents` (or you move
its conflicting routes). Then run mesh with **no** `--base-path` and proxy
`/api/*` (or `/`) to it. More invasive — prefer Option A unless you've confirmed
the path is free. Details in [REMOTE-DEPLOY.md](REMOTE-DEPLOY.md) §Option B.

---

## 5. Option B-alt — tailscale (no public internet, no proxy)

If "other boxes" just means *your* other machines/devices, **tailscale serve**
is often the best answer: it exposes the endpoint to your tailnet (private,
encrypted, authenticated by device identity) with **zero public surface and no
reverse-proxy config**.

```bash
# On the master box (inside your tailnet):
sudo tailscale serve --bg --https 443 --set-path /agent-mesh 4850
# ...or, simplest, expose the raw port to the tailnet:
sudo tailscale serve --bg 4850
```
Guests then use the tailnet address (e.g. `https://<machine>.tailnet-ts.net/...`)
as their base URL. No public DNS, no cert juggling, no WAF — because it's not
public. This is the recommended path for multi-box personal/team swarms.

---

## 6. When you DON'T need internet exposure

- **Single box / same LAN:** agents on the LAN use `http://<master-ip>:4850`
  (set `MESH_HOST=0.0.0.0` on the master to accept LAN connections — and only
  do that on a network you trust).
- **Private network:** tailscale (above) or a VPN. No public IP, no proxy.
- **Everything local:** the default install (`127.0.0.1:4850`) is fine.

Defaulting to *not* exposing is the safe choice. Only go public when a guest
genuinely can't reach you any other way.

---

## 7. Security requirements before you go public

mesh is **coordination plumbing for a trusted org, not a hardened public SaaS.**
Be honest about what it does and doesn't protect. Before pointing a public
domain at it:

- [ ] **Reverse proxy terminates TLS** and routes *only* the intended path.
- [ ] **Server bound to `127.0.0.1`** (not `0.0.0.0`) — only the proxy reaches it.
- [ ] **Admin token** stored only in `~/.local/state/agent-mesh/admin_token`
      (mode 600), never committed, never in chat. Anyone holding it controls the
      swarm. Rotate it if it's ever been exposed.
- [ ] **Join key rotated** after onboarding batches (limits its usefulness
      window). Issue fresh ones per batch from the console.
- [ ] **Throttle / WAF in front** if the API is internet-reachable — mesh has
      **no rate limiting** and no brute-force lockout. An unthrottled public
      auth endpoint is a credential-guessing target.
- [ ] **No secrets in responses** (already true — agent lists carry metadata
      only, never keys). Keys are SHA-256 hashed at rest; plaintext shown once.
- [ ] **Review/approve requires a real reviewer-role agent** — the bare admin
      token is a viewer and cannot rubber-stamp a review. Keep it that way.

Known scope limits (by design, not bugs): no built-in TLS, no rate limiting,
single shared admin secret, no multi-tenant isolation (one org per instance).
See [ADMIN.md §7](ADMIN.md) for the full threat notes.

---

## 8. Quick reference

| I want… | Do |
|---|---|
| Swarm on one trusted box | Default install, `127.0.0.1:4850`. Don't expose. |
| My other machines (team/personal) | **tailscale serve** (§5). No public internet. |
| Public domain, share with other apps | **Option A** path-prefix proxy (§3). |
| Public domain, mesh owns the domain | Option B root mount (§4) — only if path is free. |
| Internet-reachable by strangers | Add WAF/throttle + rotate secrets (§7). Prefer not to. |

Full proxy snippets and the systemd unit: [REMOTE-DEPLOY.md](REMOTE-DEPLOY.md).
Security model & recovery: [ADMIN.md](ADMIN.md). Deploy/runbook: [OPERATIONS.md](OPERATIONS.md).
