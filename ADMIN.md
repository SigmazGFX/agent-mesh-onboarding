# agent-mesh — Administrator's Manual

The complete reference for the human who runs and governs an agent-mesh swarm:
credentials, the console, roles, join keys, security, recovery, and day-to-day
operations. If you're an *agent* being onboarded, see [`AGENT-INTEGRATION.md`](AGENT-INTEGRATION.md);
if you're wiring up the server, see [`OPERATIONS.md`](OPERATIONS.md) and
[`REMOTE-DEPLOY.md`](REMOTE-DEPLOY.md). The machine-readable API contract is
[`SCHEMA.md`](SCHEMA.md).

---

## 1. The mental model

One **master** node runs the endpoint (the orchestrator). Any number of
**guest** nodes enroll into it. Everything is governed by three kinds of
secret, all sent as `Authorization: Bearer <token>`:

| Secret | Prefix | Who holds it | What it does | Can approve/review? |
|---|---|---|---|---|
| **Admin token** | `adm_…` | The human admin | Full console + key/join-key management; top-privilege *viewer* | **No** |
| **Agent API key** | `mesh_…` | Each agent | Identifies the agent *and* carries its role | Only if role is qa/reviewer/orchestrator |
| **Join key** | `join_…` | Admin → handed to new guests | Lets a new box *enroll* (lands as `observer`) | No (it's only for joining) |

Key principles:
- **Roles are assigned by a human**, in the console. Agents never self-promote.
- **Joining is gated** by the join key — there is no open door on a public domain.
- **Approving work requires a real reviewer-role agent.** The bare admin token
  deliberately cannot rubber-stamp a review.
- **Secrets are shown once.** Plaintext appears exactly at creation; only a hash
  is stored afterward.

```
        ┌─────────────────────────── MASTER node ───────────────────────────┐
        │  mesh_server.py  (systemd user service)                            │
        │   • serves the web console at /                                    │
        │   • serves the API at /api/...                                     │
        │   • holds: agents, tasks, events, artifacts (SQLite)               │
        │   • secrets: admin token (env), join key (db meta, hashed)         │
        └──────────────▲──────────────────────────────▲──────────────────────┘
                       │ Bearer adm_… (human)          │ Bearer mesh_… (agents)
              ┌────────┴───────┐             ┌─────────┴─────────┐
              │  HUMAN ADMIN   │             │  GUEST agents     │
              │  (browser)     │             │  (worker/qa/…)    │
              └────────────────┘             └───────────────────┘
```

---

## 2. Becoming the master (one-time)

On the box that will be the orchestrator:

```bash
git clone https://github.com/SigmazGFX/agent-mesh.git
cd agent-mesh
./install.sh master
```

What happens:
1. Generates (or reuses) the **admin token** and stores it in
   `~/.local/state/agent-mesh/admin_token` (mode 600). A pre-set
   `MESH_ADMIN_TOKEN` env var takes precedence.
2. Installs/updates the `agent-mesh` service and starts it — a systemd **user**
   service where available, otherwise a background process (containers/macOS).
3. Health-checks the endpoint.
4. **Issues a join key** and prints it.

You'll see:
```
Console     : http://127.0.0.1:4850/   (unlock with the admin token)
Admin token : adm_XXXXXXXXXXXX
Join key for guests (hand this out):
------------------------------------
join_YYYYYYYYYY
------------------------------------
```

**Capture both secrets now.** The admin token is not shown again by the server;
the join key can be re-issued anytime (which invalidates the old one).

> For a public/remote master (e.g. behind a reverse proxy on a shared domain),
> see [`EXPOSURE.md`](EXPOSURE.md) for the decision framework + security
> checklist, and [`REMOTE-DEPLOY.md`](REMOTE-DEPLOY.md) for the proxy config —
> run with `--base-path /agent-mesh` and add one proxy rule.

---

## 3. Getting & using the admin token

### Where it lives
- **Printed once** by `./install.sh master`.
- **Stored** on the master box in `~/.local/state/agent-mesh/admin_token`
  (mode 600). A pre-set `MESH_ADMIN_TOKEN` env var takes precedence.

### Reading it later
```bash
cat ~/.local/state/agent-mesh/admin_token
```

### Using it
Open the console URL in a browser → lock screen → paste the token → **Unlock**.
The token is kept in the browser's `sessionStorage` for that tab (survives
navigation, clears on tab close or **lock**). There is no account/password
system — the token *is* the admin credential.

### Handing it to another admin
Out-of-band, like any root secret (encrypted channel, password manager, in
person). Anyone holding it has full administrative control of the swarm.

### Rotation & recovery
- **Rotate:** issue a new token, replace the contents of
  `~/.local/state/agent-mesh/admin_token`, then restart the service
  (`systemctl --user restart agent-mesh` or restart your process manager). Old
  token stops working. *(An in-console "rotate admin token" action is a planned
  convenience — see §10 Roadmap.)*
- **Lost it:** read it from `~/.local/state/agent-mesh/admin_token`. If that's
  gone too, the only reset
  is wiping state (`rm -rf ~/.local/state/agent-mesh`) and restarting — a fresh
  token is printed, but **all agents/tasks/events are lost**. Last resort only.

---

## 4. The console, page by page

Unlock at the console URL. Top nav: **Dashboard · Projects · Agents · Tasks ·
Events · Artifacts**. Updates **instantly** via Server-Sent Events (a 5s poll
remains as a fallback if the stream drops). A **lock** button signs out.

### Dashboard
Stat tiles (queued / active / done / failed / agents-by-role), recent tasks, and
a live event feed. Your at-a-glance health view. Click a task to open it.

### Agents  ← where administration happens
Two cards:

**Join key (for new agents)**
- Explains that new boxes present this key to `/api/agents/join` and land as
  `observer`.
- **"Issue / rotate join key"** — generates a fresh `join_…` key (old one
  invalidated) and prompts you to copy it. This is what you hand to each new
  guest.

**Register an agent directly**
- Name + role (+ optional *admin cap*) → **Register** → copy the one-time key.
  Use this when you're creating an agent yourself rather than letting a box
  self-enroll.
- The table lists every agent with:
  - **role** dropdown — change it live (this is how you promote an `observer`
    to `worker`/`qa`/etc.).
  - **status** pill (online/offline/disabled) + last-seen.
  - **key** prefix (metadata only, never plaintext).
  - **console** column — grant/revoke the **admin cap** (lets that agent use the
    full console *as itself*; its role still gates actions).
  - **rekey** — new key, old revoked instantly.
  - **revoke** — disable the agent until reissued.
  - **delete** — remove the agent and its key.

### Tasks
- **Create task** (title + kind + priority 0–5, lower = more urgent).
- Filter by status; click any row for the **task detail page**:
  - full spec, result, deadline, assignee, timestamps
  - **Actions**: start / cancel / requeue (role-gated)
  - **Review**: Approve / Reject — visible and enabled only for reviewer roles
    (qa/reviewer/orchestrator). The bare admin token gets a clear error here.
  - **Artifacts**: list with download links
  - **Task events**: the audit trail for that task

### Events
Full audit log with actor / type / task-id filters. Every state change is
recorded (register, join, checkin, dispatch, progress, result, review, key
rotation, etc.).

### Artifacts
Everything uploaded, with task links and download buttons.

---

## 5. Roles & permissions (the governance core)

| Role | dispatch | pull | results | review | cancel others |
|---|---|---|---|---|---|
| `orchestrator` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `planner` | ✓ | ✓ | ✓ | ✓ | – |
| `worker` | – | ✓ | ✓ | – | – |
| `qa` | – | ✓ | ✓ | ✓ | – |
| `reviewer` | – | – | – | ✓ | – |
| `observer` | – | – | – | – | – |

Enforced **server-side** — a worker key literally cannot dispatch or review;
the request 403s before touching data. Task lifecycle:
`queued → claimed → in_progress → done|failed|cancelled → approved|rejected`.

Typical org wiring:
- **Orchestrator** — the master brain; creates/watchs/cancels tasks.
- **Planners** — break big work into tasks.
- **Workers** — pull and execute.
- **QA / reviewer** — approve or reject finished work.
- **Observer** — read-only visibility (dashboards, humans, newly-joined boxes).

**Assigning a role** is a human act: console → Agents → role dropdown. New
guests arrive as `observer`; you promote them when ready.

### The "admin cap" (separate from role)
Granting an agent the **admin cap** lets it use the full console *as itself*.
This is how you give, say, your QA agent the ability to **approve from the UI**:
give it role `qa` (or `reviewer`/`orchestrator`) **and** the admin cap, then
unlock the console with *its* key. Its role still gates what it can do — a
worker with the admin cap still can't approve.

---

## 6. Onboarding a new agent (the loop you asked for)

1. **Master side:** you have the **join key** (from install, or re-issue in the
   console).
2. **Guest side:** on the new box,
   ```bash
   git clone https://github.com/SigmazGFX/agent-mesh.git && cd agent-mesh
   ./install.sh guest
   # → enter the master's base URL + the join key
   ```
   It enrolls as `observer`, checks in, installs the `mesh` CLI, and prints the
   agent's one-time API key.
3. **Admin side:** open the console → Agents → find the new agent → **set its
   role**. Until then it can only observe.

That's the whole flow: *master accepts the check-in, admin assigns the role.*

---

## 7. Security model & threat notes

**What's protected**
- All `/api/*` require a valid Bearer credential (agent key, admin-cap agent, or
  admin token). `/api/health` is the only open route.
- Joining requires the join key (no open registration).
- Roles gate every privileged action server-side.
- Review requires a genuine reviewer principal (admin token can't fake it).
- Keys stored as SHA-256 hashes; plaintext shown once.
- Key rotation is instant (old key dead immediately).

**Be honest about scope** — this is coordination plumbing for a *trusted
internal org*, not a hardened public SaaS:
- **No TLS of its own.** Terminate TLS at the reverse proxy (you already have
  HTTPS on the domain). Keep the raw port bound to `127.0.0.1`.
- **No rate limiting.** Don't expose the API to untrusted internet without a
  throttling front.
- **Single shared admin secret.** Anyone with the admin token controls the
  swarm. Protect it accordingly.
- **No multi-tenant isolation.** One org per instance.

**Exposure checklist** (before pointing a public domain at it):
- [ ] Reverse proxy terminates TLS and routes only the intended path.
- [ ] Server bound to `127.0.0.1` (not `0.0.0.0`) unless you mean it.
- [ ] Admin token stored only in `~/.local/state/agent-mesh/admin_token` (mode 600), never committed.
- [ ] Join key rotated after onboarding batches (limit its usefulness window).
- [ ] Consider a throttle/WAF in front if the API is internet-reachable.

---

## 8. Day-to-day operations

```bash
# health
curl -s http://127.0.0.1:4850/api/health

# service
systemctl --user status agent-mesh
journalctl --user -u agent-mesh -f            # live logs
systemctl --user restart agent-mesh           # after code changes

# backup (self-contained state dir)
tar czf agent-mesh-backup.tgz -C ~/.local/state agent-mesh

# reset to empty (LAST RESORT — loses agents/tasks)
systemctl --user stop agent-mesh
rm -rf ~/.local/state/agent-mesh
systemctl --user start agent-mesh             # fresh admin token printed
```

**Troubleshooting**
- *Connection refused* → service down: check `systemctl --user status agent-mesh`
  + journal, then restart.
- *403 "valid agent API key or admin token required"* → wrong/revoked key, or
  agent disabled. Rekey from the console.
- *403 "role 'worker' cannot …"* → expected; that's the hierarchy working.
- *403 "review requires a qa/reviewer/orchestrator agent key"* → you tried to
  approve with the bare admin token. Use a reviewer-role agent.
- *Port busy* → pick another (`MESH_PORT=…`) or free it (`ss -ltnp | grep 4850`).

---

## 9. Secrets reference (cheat sheet)

| Secret | Format | Created by | Stored as | Shown | Rotate via |
|---|---|---|---|---|---|
| Admin token | `adm_…` | `install.sh master` | `~/.local/state/agent-mesh/admin_token` (mode 600) + db meta | once at install | edit file + restart |
| Agent key | `mesh_…` | register / join / rekey | SHA-256 hash | once, at creation | console **rekey** |
| Join key | `join_…` | console / `POST /api/admin/join-key` | SHA-256 hash (db meta) | once, at issuance | console **issue/rotate** |

Config locations:
- Master state: `~/.local/state/agent-mesh/` (SQLite WAL + `artifacts/`)
- Guest config: `~/.config/agent-mesh/config.json` (`{base_url, api_key, agent_id}`)
- Guest CLI: `~/.local/bin/mesh`

---

## 10. Roadmap / known gaps

- **In-console admin-token rotation** (authenticated by current token) so you
  never have to edit files to rotate.
- **Named-admin login** (username + password) as an alternative to the single
  shared bearer secret, if you want distinct admin identities.
- Rate limiting / basic brute-force lockout on auth endpoints (if exposed).
- Multi-tenant isolation (one org per instance today).

Already shipped (not open items): SSE instant console updates (`GET /api/stream`),
A2A peer messaging, projects, swarm-view + reassign, `--base-path` proxy
mounting, platform-agnostic portability, and the autonomous orchestrator
watchdog (see ORCHESTRATOR.md).
