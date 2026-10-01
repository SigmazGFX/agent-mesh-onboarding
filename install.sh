#!/usr/bin/env bash
#
# agent-mesh installer — make this box an agent on the org mesh.
#
# Usage:  ./install.sh [ORCHESTRATOR_BASE_URL]
#   e.g.   ./install.sh https://bytemecarl.io/agent-mesh
#          ./install.sh http://127.0.0.1:4850
#
# What it does:
#   1. Asks for (or takes) the base URL of the MASTER orchestrator.
#   2. Registers THIS box as an agent with a generated name + a temporary
#      'observer' role, and prints the one-time API key.
#   3. Stores the key + base URL in ~/.config/agent-mesh/config.json.
#   4. Checks in so the orchestrator sees the new agent.
#   5. Tells you to have the ADMIN assign a real role in the console
#      (Agents page). Until then the agent can only observe.
#
# Stdlib-only; needs python3 (>=3.9) and curl. No root required.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER="$REPO_DIR/mesh_server.py"
CFG_DIR="${HOME}/.config/agent-mesh"
CFG="$CFG_DIR/config.json"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2; exit 1
fi
[ -f "$SERVER" ] || { echo "error: $SERVER not found (run from the repo)" >&2; exit 1; }

# --- 1. orchestrator base URL ---------------------------------------------
BASE="${1:-}"
if [ -z "$BASE" ]; then
  printf 'Base address of the master orchestrator (e.g. https://host/agent-mesh): '
  read -r BASE
fi
[ -n "$BASE" ] || { echo "error: no orchestrator address given" >&2; exit 1; }
# normalize: strip trailing slash
BASE="${BASE%/}"

echo
echo "Orchestrator : $BASE"

# health check
HEALTH="$(curl -fsS --max-time 10 "$BASE/api/health" 2>/dev/null || true)"
if [ -z "$HEALTH" ]; then
  echo "warning: could not reach $BASE/api/health — continuing, but check the URL." >&2
else
  echo "Health       : $HEALTH"
fi

# --- 2. register this box --------------------------------------------------
HOSTN="$(hostname 2>/dev/null || echo agent)"
AGENT_NAME="${MESH_AGENT_NAME:-$HOSTN}"

read -r KEY AGENT_ID < <(python3 - "$BASE" "$AGENT_NAME" <<'PY'
import json, sys, urllib.request, uuid
base, name = sys.argv[1], sys.argv[2]
# Self-service join: registers as 'observer'; admin assigns a real role after.
body = {"name": name, "caps": []}
req = urllib.request.Request(base + "/api/agents/join",
    data=json.dumps(body).encode(), method="POST",
    headers={"Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=15) as r:
        d = json.loads(r.read().decode())
except Exception as e:
    sys.stderr.write(f"\nerror: join failed: {e}\n"
                     f"Is the orchestrator reachable and accepting joins?\n")
    sys.exit(1)
print(d["api_key"], d["agent"]["id"])
PY
)
[ -n "$KEY" ] || { echo "error: no key returned" >&2; exit 1; }

# --- 3. store config -------------------------------------------------------
mkdir -p "$CFG_DIR"
python3 - "$CFG" "$BASE" "$KEY" "$AGENT_ID" <<'PY'
import json, sys, os
cfg_path, base, key, aid = sys.argv[1:5]
data = {"base_url": base, "api_key": key, "agent_id": aid}
with open(cfg_path, "w") as f:
    json.dump(data, f, indent=2)
os.chmod(cfg_path, 0o600)
PY
chmod 600 "$CFG"

# --- 4. check in -----------------------------------------------------------
CHECKIN="$(curl -fsS --max-time 10 -X POST "$BASE/api/agents/checkin" \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{}' 2>/dev/null || true)"

# --- 5. install the mesh CLI onto PATH ------------------------------------
CLI_LINK="${HOME}/.local/bin/mesh"
mkdir -p "${HOME}/.local/bin"
ln -sf "$REPO_DIR/mesh" "$CLI_LINK"
chmod +x "$REPO_DIR/mesh"
PATH_NOTE=""
case ":$PATH:" in
  *":${HOME}/.local/bin:"*) ;;
  *) PATH_NOTE=" (add ${HOME}/.local/bin to your PATH if 'mesh' isn't found)" ;;
esac

cat <<EOF

──────────────────────────────────────────────────────────────
  This box is now registered on the mesh.

  Agent id   : $AGENT_ID
  Name       : $AGENT_NAME
  Role       : observer   (temporary — see below)
  API key    : $KEY
  Config     : $CFG
  CLI        : $CLI_LINK$PATH_NOTE

  Check-in   : ${CHECKIN:-ok}
──────────────────────────────────────────────────────────────

NEXT STEP — have the ADMIN assign a real role:
  Open the orchestrator console ($BASE/) → Agents → find
  "$AGENT_NAME" → set its role (worker / qa / reviewer / planner /
  orchestrator). Until then it can only observe.

Save that API key somewhere safe — it is shown only once.

Try it now:
  mesh status
  mesh checkin
  mesh pull          # once a worker+ role is assigned
EOF
