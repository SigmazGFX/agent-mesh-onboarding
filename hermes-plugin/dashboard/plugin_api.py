"""agent-mesh dashboard plugin — backend API routes, mounted at /api/plugins/agent-mesh/.

Thin proxy to the local agent-mesh endpoint (127.0.0.1:4850 by default). The
dashboard host already authenticates the browser session; this router adds the
mesh admin token server-side so the frontend never sees it. Read-only status
surfaces: health, agents, projects, tasks, swarm-view, events.

Config: read from env MESH_BASE_URL / MESH_ADMIN_TOKEN, falling back to the
portable install locations (~/.local/state/agent-mesh/admin_token). Override the
endpoint with MESH_BASE_URL if the panel should point at a remote swarm.
"""

from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

# --- Mesh endpoint resolution ---------------------------------------------

def _admin_token() -> str:
    tok = os.environ.get("MESH_ADMIN_TOKEN", "").strip()
    if tok:
        return tok
    # Portable install location (see install.sh).
    for cand in (
        Path.home() / ".local" / "state" / "agent-mesh" / "admin_token",
        Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
        / "state" / "agent-mesh" / "admin_token",
    ):
        try:
            if cand.is_file():
                t = cand.read_text(encoding="utf-8").strip()
                if t:
                    return t
        except OSError:
            continue
    return ""


def _base_url() -> str:
    return os.environ.get("MESH_BASE_URL", "http://127.0.0.1:4850").rstrip("/")


def _mesh_get(path: str, *, admin: bool = True, timeout: float = 10.0) -> Dict[str, Any]:
    """GET a mesh endpoint, attaching the admin token when requested."""
    url = _base_url() + path
    req = urllib.request.Request(url, method="GET")
    if admin:
        tok = _admin_token()
        if not tok:
            raise HTTPException(503, "agent-mesh admin token not configured on this box")
        req.add_header("Authorization", "Bearer " + tok)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        try:
            detail = json.loads(body).get("detail", body)
        except Exception:
            detail = body
        raise HTTPException(e.code, f"mesh {e.code}: {detail}")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"cannot reach agent-mesh at {_base_url()}: {e}")


# --- Routes ----------------------------------------------------------------

@router.get("/health")
def health() -> Dict[str, Any]:
    return _mesh_get("/api/health", admin=False)


@router.get("/stats")
def stats() -> Dict[str, Any]:
    return _mesh_get("/api/admin/stats")


@router.get("/agents")
def agents() -> Dict[str, Any]:
    return _mesh_get("/api/agents")


@router.get("/projects")
def projects(status: Optional[str] = Query(None)) -> Dict[str, Any]:
    q = f"?status={status}" if status else ""
    return _mesh_get("/api/projects" + q)


@router.get("/projects/{project_id}")
def project(project_id: str) -> Dict[str, Any]:
    return _mesh_get(f"/api/projects/{project_id}")


@router.get("/tasks")
def tasks(status: Optional[str] = Query(None),
          assigned_to: Optional[str] = Query(None),
          project_id: Optional[str] = Query(None),
          limit: int = Query(100, le=500)) -> Dict[str, Any]:
    parts = []
    if status:
        parts.append(f"status={status}")
    if assigned_to:
        parts.append(f"assigned_to={assigned_to}")
    if project_id:
        parts.append(f"project_id={project_id}")
    parts.append(f"limit={limit}")
    return _mesh_get("/api/tasks?" + "&".join(parts))


@router.get("/swarm-view")
def swarm_view() -> Dict[str, Any]:
    return _mesh_get("/api/orch/swarm-view")


@router.get("/events")
def events(limit: int = Query(60, le=200), task_id: Optional[str] = Query(None)) -> Dict[str, Any]:
    parts = [f"limit={limit}"]
    if task_id:
        parts.append(f"task_id={task_id}")
    return _mesh_get("/api/events?" + "&".join(parts))
