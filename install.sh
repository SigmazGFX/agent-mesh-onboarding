#!/usr/bin/env bash
#
# agent-mesh installer — set this box up as either a MASTER (orchestrator)
# node or a GUEST that enrolls into an existing swarm.
#
#   ./install.sh                 # interactive: asks for master|guest
#   ./install.sh master          # stand up the orchestrator on this box
#   ./install.sh guest           # enroll this box into a remote swarm
#
# MASTER mode: runs the endpoint locally (systemd user service), prints the
#   admin token + console URL, and issues a join key you hand to guests.
# GUEST mode:  asks for the swarm's base URL + a join key, enrolls this box
#   as an 'observer', stores config, checks in, installs the mesh CLI. The
#   swarm admin then assigns a real role.
#
# Stdlib-only; needs python3 (>=3.9) and curl. No root required.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER="$REPO_DIR/mesh_server.py"
STATE="${HOME}/.local/state/agent-mesh"
CFG_DIR="${HOME}/.config/agent-mesh"
CFG="$CFG_DIR/config.json"
UNIT="${HOME}/.config/systemd/user/agent-mesh.service"
PORT="${MESH_PORT:-4850}"
HOST="${MESH_HOST:-127.0.0.1}"

command -v python3 >/dev/null 2>&1 || { echo "error: python3 is required" >&2; exit 1; }
[ -f "$SERVER" ] || { echo "error: $SERVER not found (run from the repo)" >&2; exit 1; }

# --- pick mode -------------------------------------------------------------
MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "Set this box up as:"
  echo "  1) master  — run the orchestrator endpoint HERE (other agents join it)"
  echo "  2) guest   — enroll THIS box into an existing swarm"
  printf 'choice [master/guest]: '
  read -r MODE
fi
case "$MODE" in
  master|m) MODE=master ;;
  guest|g)  MODE=guest ;;
  *) echo "error: mode must be 'master' or 'guest' (got '$MODE')" >&2; exit 1 ;;
esac
echo
echo "Mode: $MODE"
echo

# ===========================================================================
# MASTER — stand up the orchestrator on this box
# ===========================================================================
if [ "$MODE" = "master" ]; then
  # SAFETY GUARD — refuse to stand up a NEW master over an EXISTING one.
  # Running install.sh master twice (or with a different MESH_PORT/MESH_HOST)
  # would otherwise generate a fresh admin token, overwrite the systemd unit,
  # and restart the service against a state dir that no longer matches — i.e.
  # silently orphan the running swarm's data. Detect that and stop, unless the
  # operator explicitly passes --force (re-run / intentional re-master).
  FORCE=0
  for arg in "$@"; do [ "$arg" = "--force" ] && FORCE=1; done
  if [ "$FORCE" -eq 0 ]; then
    TOKF="$STATE/admin_token"   # same path used below; needed for the check
    EXISTING_TOKEN=0; [ -f "$TOKF" ] && EXISTING_TOKEN=1
    EXISTING_DB=0;   [ -f "$STATE/mesh.db" ] && EXISTING_DB=1
    UNIT_EXISTS=0;   [ -f "$UNIT" ] && UNIT_EXISTS=1
    PORT_BUSY=0
    if command -v ss >/dev/null 2>&1; then
      ss -ltn 2>/dev/null | grep -q ":${PORT}\b" && PORT_BUSY=1
    fi
    if [ "$EXISTING_TOKEN" -eq 1 ] || [ "$EXISTING_DB" -eq 1 ] \
       || [ "$UNIT_EXISTS" -eq 1 ] || [ "$PORT_BUSY" -eq 1 ]; then
      {
        echo "error: an agent-mesh master already exists on this box."
        echo "  state dir : $STATE"
        [ "$EXISTING_TOKEN" -eq 1 ] && echo "  - admin token present ($TOKF)"
        [ "$EXISTING_DB" -eq 1 ]     && echo "  - database present ($STATE/mesh.db)"
        [ "$UNIT_EXISTS" -eq 1 ]     && echo "  - systemd unit present ($UNIT)"
        [ "$PORT_BUSY" -eq 1 ]       && echo "  - port $PORT is in use"
        echo
        echo "Re-running 'master' here would generate a new admin token, overwrite"
        echo "the service unit, and restart against a state dir that may not match"
        echo "the running swarm — orphaning its data."
        echo
        echo "If you meant to RE-INSTALL the existing master (same box), just:"
        echo "  systemctl --user restart agent-mesh"
        echo "If you intentionally want to stand up a FRESH master here (wiping"
        echo "the existing one), run again with --force:"
        echo "  ./install.sh master --force"
      } >&2
      exit 1
    fi
  fi

  mkdir -p "$STATE" "$HOME/.config/systemd/user"

  # 1. generate + store the admin token (idempotent: reuse if present).
  #    Portable: stored in ~/.local/state/agent-mesh/admin_token (mode 600),
  #    NOT tied to any agent platform. A pre-set MESH_ADMIN_TOKEN env var wins.
  ADMIN_TOKEN="${MESH_ADMIN_TOKEN:-}"
  TOKF="$STATE/admin_token"
  if [ -z "$ADMIN_TOKEN" ] && [ -f "$TOKF" ]; then
    ADMIN_TOKEN="$(cat "$TOKF")"
    echo "Reusing existing admin token from $TOKF"
  fi
  if [ -z "$ADMIN_TOKEN" ]; then
    ADMIN_TOKEN="adm_$(python3 -c 'import secrets;print(secrets.token_urlsafe(24))')"
    printf '%s' "$ADMIN_TOKEN" > "$TOKF"
    chmod 600 "$TOKF" 2>/dev/null || true
    echo "Generated admin token, stored in $TOKF"
  fi

  # 2. write the systemd unit (point ExecStart at the actual repo path).
  #    Token is read from $TOKF via a wrapper so this works on any platform
  #    with systemd; if systemd is absent we fall back to a foreground hint.
  cat > "$UNIT" <<EOF
[Unit]
Description=agent-mesh endpoint (master/orchestrator)
After=network.target

[Service]
Type=simple
Environment=MESH_ADMIN_TOKEN=\$(cat ${TOKF})
ExecStart=/usr/bin/python3 ${SERVER} --data ${STATE} --host ${HOST} --port ${PORT}
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
EOF

  # Install the service only if systemd user sessions are actually available.
  if command -v systemctl >/dev/null 2>&1 && systemctl --user list-unit-files >/dev/null 2>&1; then
    # If an agent-mesh service is ALREADY installed, don't clobber it — just
    # report and reuse what's running (idempotent re-run / already-master box).
    if systemctl --user list-unit-files 2>/dev/null | grep -q '^agent-mesh\.service'; then
      echo "Existing 'agent-mesh' user service detected — leaving it as-is."
      systemctl --user daemon-reload || true
      systemctl --user restart agent-mesh || true
      sleep 1.5
    else
      systemctl --user daemon-reload
      systemctl --user enable --now agent-mesh
      sleep 1.5
    fi
    SERVICE_MODE="systemd user service (systemctl --user status agent-mesh)"
  else
    # No systemd (containers, macOS, minimal boxes): run in background + document.
    nohup /usr/bin/python3 "${SERVER}" --data "${STATE}" --host "${HOST}" --port "${PORT}" \
      >> "${STATE}/server.log" 2>&1 &
    echo $! > "${STATE}/server.pid"
    sleep 1.5
    SERVICE_MODE="background process (pid $(cat "${STATE}/server.pid") 2>/dev/null; log: ${STATE}/server.log)"
  fi

  # 3. health check
  HEALTH="$(curl -fsS --max-time 10 "http://${HOST}:${PORT}/api/health" 2>/dev/null || true)"
  if [ -z "$HEALTH" ]; then
    echo "warning: endpoint not responding at http://${HOST}:${PORT} yet." >&2
    journalctl --user -u agent-mesh -n 15 --no-pager || true
  else
    echo "Endpoint healthy: $HEALTH"
  fi

  # 4. issue a join key for guests
  JOINKEY="$(curl -fsS --max-time 10 -X POST "http://${HOST}:${PORT}/api/admin/join-key" \
    -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' -d '{}' \
    2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin)["join_key"])' 2>/dev/null || true)"

  # 5. set up a DAILY state backup (so a wipe/corruption is recoverable).
  #    Uses the user crontab if available; otherwise just points at the script.
  BACKUP="$REPO_DIR/mesh-backup.sh"
  chmod +x "$BACKUP" 2>/dev/null || true
  BACKUP_NOTE="manual: run '$BACKUP' to snapshot state"
  if command -v crontab >/dev/null 2>&1; then
    CRON_LINE="0 3 * * * $BACKUP >> ${STATE}/backup.log 2>&1"
    if ! crontab -l 2>/dev/null | grep -qF "mesh-backup.sh"; then
      ( crontab -l 2>/dev/null; echo "$CRON_LINE" ) | crontab - 2>/dev/null \
        && BACKUP_NOTE="daily at 03:00 via crontab (keep last 7; log: ${STATE}/backup.log)"
    fi
  fi

  cat <<EOF

──────────────────────────────────────────────────────────────
  MASTER orchestrator is running on this box.

  Console     : http://${HOST}:${PORT}/   (unlock with the admin token)
  Admin token : $ADMIN_TOKEN
               (stored in ${TOKF})
  Service     : $SERVICE_MODE
  Backup      : $BACKUP_NOTE

  Join key for guests (hand this out):
  ------------------------------------
  $JOINKEY
  ------------------------------------

  A guest enrolls with:
    ./install.sh guest
    → base url : http://<this-box>:${PORT}   (or your public/proxied URL)
    → join key : $JOINKEY

  After a guest joins it appears as 'observer' in the console → assign a role.

  Re-running './install.sh master' here is BLOCKED (it would orphan this
  swarm's data). To re-apply config: systemctl --user restart agent-mesh.
  To deliberately stand up a fresh master: ./install.sh master --force
──────────────────────────────────────────────────────────────
EOF
  exit 0
fi

# ===========================================================================
# GUEST — enroll this box into an existing swarm
# ===========================================================================

# 1. base URL of the swarm's master
BASE="${MESH_BASE_URL:-}"
if [ -z "$BASE" ]; then
  printf 'Base address of the swarm master (e.g. https://host/agent-mesh): '
  read -r BASE
fi
[ -n "$BASE" ] || { echo "error: no swarm base address given" >&2; exit 1; }
BASE="${BASE%/}"

# 2. join key (provisioned by the swarm admin)
JOINKEY="${MESH_JOIN_KEY:-}"
if [ -z "$JOINKEY" ]; then
  printf 'Join key (ask the swarm admin; shown once in their console): '
  read -r JOINKEY
fi
[ -n "$JOINKEY" ] || { echo "error: a join key is required to join" >&2; exit 1; }

echo
echo "Swarm master : $BASE"
HEALTH="$(curl -fsS --max-time 10 "$BASE/api/health" 2>/dev/null || true)"
if [ -z "$HEALTH" ]; then
  echo "warning: could not reach $BASE/api/health — check the URL." >&2
else
  echo "Health       : $HEALTH"
fi

# 3. enroll
HOSTN="$(hostname 2>/dev/null || echo agent)"
AGENT_NAME="${MESH_AGENT_NAME:-$HOSTN}"

read -r KEY AGENT_ID < <(python3 - "$BASE" "$AGENT_NAME" "$JOINKEY" <<'PY'
import json, sys, urllib.request
base, name, join_key = sys.argv[1], sys.argv[2], sys.argv[3]
body = {"name": name, "join_key": join_key, "caps": []}
req = urllib.request.Request(base + "/api/agents/join",
    data=json.dumps(body).encode(), method="POST",
    headers={"Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=15) as r:
        d = json.loads(r.read().decode())
except Exception as e:
    sys.stderr.write(f"\nerror: join failed: {e}\n"
                     f"Check the swarm URL and that the join key is valid.\n")
    sys.exit(1)
print(d["api_key"], d["agent"]["id"])
PY
)
[ -n "$KEY" ] || { echo "error: no key returned" >&2; exit 1; }

# 4. store config
mkdir -p "$CFG_DIR"
python3 - "$CFG" "$BASE" "$KEY" "$AGENT_ID" <<'PY'
import json, sys, os
cfg_path, base, key, aid = sys.argv[1:5]
with open(cfg_path, "w") as f:
    json.dump({"base_url": base, "api_key": key, "agent_id": aid}, f, indent=2)
os.chmod(cfg_path, 0o600)
PY
chmod 600 "$CFG"

# 5. check in
CHECKIN="$(curl -fsS --max-time 10 -X POST "$BASE/api/agents/checkin" \
  -H "Authorization: Bearer ***" -H 'Content-Type: application/json' \
  -d '{}' 2>/dev/null || true)"

# 6. install the mesh CLI
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
  This box is now enrolled in the swarm as a GUEST.

  Swarm      : $BASE
  Agent id   : $AGENT_ID
  Name       : $AGENT_NAME
  Role       : observer   (temporary — see below)
  API key    : $KEY
  Config     : $CFG
  CLI        : $CLI_LINK$PATH_NOTE

  Check-in   : ${CHECKIN:-ok}
──────────────────────────────────────────────────────────────

NEXT STEP — have the SWARM ADMIN assign a real role:
  Open the master console ($BASE/) → Agents → find "$AGENT_NAME"
  → set its role (worker / qa / reviewer / planner / orchestrator).
  Until then it can only observe.

Save that API key somewhere safe — it is shown only once.

Try it now:
  mesh status
  mesh checkin
  mesh pull          # once a worker+ role is assigned
EOF
