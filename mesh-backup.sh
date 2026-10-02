#!/usr/bin/env bash
#
# agent-mesh state backup — snapshot the master's state dir so a wipe or
# corruption is recoverable (see OPERATIONS.md §Backup).
#
#   mesh-backup.sh            # back up now, keep the last 7 snapshots
#   MESH_KEEP=14 mesh-backup.sh   # keep 14 instead of 7
#
# Output goes to ~/.local/state/agent-mesh-backups/ as timestamped tarballs.
# Safe to run from cron (e.g. daily). Idempotent; never touches the live DB
# (tar reads it; SQLite WAL is consistent enough for a cold-ish snapshot — for
# a fully consistent copy you could also stop the service first, but at this
# scale a running snapshot is fine and far less disruptive).

set -euo pipefail

STATE="${MESH_STATE:-${HOME}/.local/state/agent-mesh}"
BACKUP_DIR="${MESH_BACKUP_DIR:-${HOME}/.local/state/agent-mesh-backups}"
KEEP="${MESH_KEEP:-7}"

if [ ! -d "$STATE" ]; then
  echo "mesh-backup: no state dir at $STATE (nothing to back up)" >&2
  exit 0
fi
# Nothing worth backing up if there's no database yet.
if [ ! -f "$STATE/mesh.db" ]; then
  echo "mesh-backup: no mesh.db in $STATE (nothing to back up)" >&2
  exit 0
fi

mkdir -p "$BACKUP_DIR"
TS="$(date +%Y%m%d-%H%M%S)"
DEST="$BACKUP_DIR/agent-mesh-$TS.tgz"

# Tar the whole state dir (DB + WAL + SHM + admin_token + artifacts/).
tar czf "$DEST" -C "$(dirname "$STATE")" "$(basename "$STATE")"

SIZE="$(du -h "$DEST" | cut -f1)"
echo "mesh-backup: wrote $DEST ($SIZE)"

# Prune old snapshots, keeping the newest $KEEP.
ls -1t "$BACKUP_DIR"/agent-mesh-*.tgz 2>/dev/null | tail -n +"$((KEEP + 1))" | while read -r old; do
  rm -f "$old"
done
COUNT="$(ls -1 "$BACKUP_DIR"/agent-mesh-*.tgz 2>/dev/null | wc -l | tr -d ' ')"
echo "mesh-backup: $COUNT snapshot(s) retained (keep=$KEEP)"
