/**
 * agent-mesh — Hermes Dashboard Plugin (frontend)
 *
 * Native control panel for the local agent-mesh swarm. Plain IIFE, no build
 * step. Uses window.__HERMES_PLUGIN_SDK__ for React + shadcn primitives and
 * calls the plugin backend at /api/plugins/agent-mesh/. Sub-tabs: Overview,
 * Agents, Projects, Tasks, Events. Polls every 5s for live-ish data.
 */
(function () {
  "use strict";
  const SDK = window.__HERMES_PLUGIN_SDK__;
  if (!SDK) return;
  const REG = window.__HERMES_PLUGINS__;
  if (!REG) return;

  const { React } = SDK;
  const h = React.createElement;
  const { useState, useEffect, useCallback } = SDK.hooks;
  const { fetchJSON, cn, timeAgo } = SDK;

  const API = "/api/plugins/agent-mesh";

  // ---- tiny helpers -------------------------------------------------------
  function ago(ts) {
    if (!ts) return "—";
    try { return timeAgo(Math.round(ts * 1000)); } catch (e) { return String(ts); }
  }
  const ROLE_COLORS = {
    orchestrator: "bg-purple-500/20 text-purple-300",
    planner: "bg-blue-500/20 text-blue-300",
    worker: "bg-emerald-500/20 text-emerald-300",
    qa: "bg-amber-500/20 text-amber-300",
    reviewer: "bg-pink-500/20 text-pink-300",
    observer: "bg-slate-500/20 text-slate-300",
  };
  const STATUS_COLORS = {
    queued: "bg-slate-500/20 text-slate-300",
    claimed: "bg-blue-500/20 text-blue-300",
    in_progress: "bg-amber-500/20 text-amber-300",
    done: "bg-emerald-500/20 text-emerald-300",
    approved: "bg-emerald-600/30 text-emerald-200",
    failed: "bg-red-500/20 text-red-300",
    cancelled: "bg-slate-600/30 text-slate-400",
    rejected: "bg-red-500/20 text-red-300",
  };
  function Pill({ kind, value }) {
    const cls = (kind === "role" ? ROLE_COLORS : STATUS_COLORS)[value] || "bg-slate-500/20 text-slate-300";
    return h("span", { className: cn("px-2 py-0.5 rounded-full text-xs font-medium", cls) }, value);
  }
  function Card({ title, children, right }) {
    return h("div", { className: "rounded-lg border border-white/10 bg-white/[0.02] p-4" },
      h("div", { className: "flex items-center justify-between mb-3" },
        h("h3", { className: "text-sm font-semibold text-white/90" }, title),
        right || null),
      children);
  }
  function Stat({ label, value }) {
    return h("div", { className: "rounded-lg border border-white/10 bg-white/[0.02] p-3 text-center" },
      h("div", { className: "text-2xl font-bold text-white" }, String(value)),
      h("div", { className: "text-xs text-white/50 mt-1" }, label));
  }
  function Empty({ msg }) {
    return h("div", { className: "text-sm text-white/40 py-6 text-center" }, msg || "Nothing here yet.");
  }
  function usePoll(fetcher, ms) {
    const [data, setData] = useState(null);
    const [err, setErr] = useState(null);
    const [loading, setLoading] = useState(true);
    const load = useCallback(async () => {
      try { const d = await fetcher(); setData(d); setErr(null); }
      catch (e) { setErr(String(e.message || e)); }
      finally { setLoading(false); }
    }, []);
    useEffect(() => { load(); const t = setInterval(load, ms); return () => clearInterval(t); }, []);
    return { data, err, loading, reload: load };
  }

  // ---- sub-tab: Overview --------------------------------------------------
  function Overview() {
    const health = usePoll(() => fetchJSON(API + "/health"), 5000);
    const stats = usePoll(() => fetchJSON(API + "/stats"), 5000);
    const sv = usePoll(() => fetchJSON(API + "/swarm-view"), 5000);
    const S = (stats.data && stats.data.tasks_by_status) || {};
    const active = (S.in_progress || 0) + (S.claimed || 0);
    const done = (S.done || 0) + (S.approved || 0);
    const agents = (sv.data && sv.data.agents) || [];
    const idle = (sv.data && sv.data.idle_agents) || [];
    const offline = (sv.data && sv.data.offline_agents) || [];
    const unassigned = (sv.data && sv.data.unassigned_tasks) || [];
    const stale = (sv.data && sv.data.stale_tasks) || [];
    return h("div", { className: "space-y-4" },
      h("div", { className: "grid grid-cols-2 md:grid-cols-5 gap-3" },
        h(Stat, { label: "queued", value: S.queued || 0 }),
        h(Stat, { label: "active", value: active }),
        h(Stat, { label: "done/approved", value: done }),
        h(Stat, { label: "failed", value: S.failed || 0 }),
        h(Stat, { label: "agents", value: (stats.data && stats.data.agents_total) || 0 })),
      h(Card, { title: "Swarm" },
        h("div", { className: "space-y-2" },
          agents.map((a) => h("div", { key: a.id, className: "flex items-center gap-3 text-sm" },
            h("span", { className: cn("w-2 h-2 rounded-full", a.online ? "bg-emerald-400" : "bg-slate-600") }),
            h("span", { className: "font-medium text-white/90 w-32 truncate" }, a.name),
            h(Pill, { kind: "role", value: a.role }),
            a.current_task
              ? h("span", { className: "text-white/50 truncate flex-1" }, "→ " + a.current_task.title)
              : h("span", { className: cn("text-xs flex-1", a.idle ? "text-amber-400/70" : "text-white/30") },
                  a.idle ? "idle" : (a.online ? "" : "offline")),
            h("span", { className: "text-xs text-white/40" }, ago(a.last_seen_s_ago ? -a.last_seen_s_ago : undefined)))))),
      (unassigned.length || stale.length) ? h(Card, { title: "Needs attention" },
        h("div", { className: "space-y-1 text-sm" },
          unassigned.map((t) => h("div", { key: t.id, className: "text-amber-300" }, "⚠ unassigned: " + t.title)),
          stale.map((t) => h("div", { key: t.id, className: "text-red-300" }, "⏱ stale (" + t.status + "): " + t.title))))
        : null,
      (health.err || stats.err) ? h("div", { className: "text-sm text-red-400" }, "mesh unreachable: " + (health.err || stats.err)) : null);
  }

  // ---- sub-tab: Agents ----------------------------------------------------
  function Agents() {
    const { data, err } = usePoll(() => fetchJSON(API + "/agents"), 5000);
    const items = (data && data.items) || [];
    return h(Card, { title: "Agents & keys (" + items.length + ")" },
      err ? h("div", { className: "text-sm text-red-400" }, err)
        : items.length ? h("div", { className: "overflow-x-auto" },
          h("table", { className: "w-full text-sm" },
            h("thead", null, h("tr", { className: "text-left text-white/40 border-b border-white/10" },
              ["name", "role", "status", "seen", "key"].map((c) => h("th", { key: c, className: "py-2 pr-4" }, c)))),
            h("tbody", null, items.map((a) => h("tr", { key: a.id, className: "border-b border-white/5" },
              h("td", { className: "py-2 pr-4" },
                h("div", { className: "font-medium text-white/90" }, a.name),
                h("div", { className: "text-xs text-white/40 font-mono" }, a.id)),
              h("td", { className: "pr-4" }, h(Pill, { kind: "role", value: a.role })),
              h("td", { className: "pr-4" }, h(Pill, { kind: "status", value: a.status })),
              h("td", { className: "pr-4 text-white/50" }, ago(a.last_seen)),
              h("td", { className: "font-mono text-xs text-white/50" }, a.key_prefix || "—"))))))
        : h(Empty, { msg: "no agents registered" }));
  }

  // ---- sub-tab: Projects --------------------------------------------------
  function Projects() {
    const { data } = usePoll(() => fetchJSON(API + "/projects"), 5000);
    const items = (data && data.items) || [];
    return h("div", { className: "grid grid-cols-1 md:grid-cols-2 gap-4" },
      items.length ? items.map((p) => {
        const t = p.tasks || {};
        const total = Object.values(t).reduce((a, b) => a + b, 0);
        return h(Card, { key: p.id, title: p.name,
          right: h(Pill, { kind: "status", value: p.status }) },
          h("div", { className: "text-xs text-white/40 font-mono mb-3" }, p.id),
          p.description ? h("p", { className: "text-sm text-white/70 mb-3" }, p.description) : null,
          h("div", { className: "grid grid-cols-4 gap-2" },
            h(Stat, { label: "tasks", value: total }),
            h(Stat, { label: "active", value: (t.in_progress || 0) + (t.claimed || 0) }),
            h(Stat, { label: "done", value: (t.done || 0) + (t.approved || 0) }),
            h(Stat, { label: "failed", value: (t.failed || 0) + (t.rejected || 0) })));
      }) : h(Empty, { msg: "no projects yet" }));
  }

  // ---- sub-tab: Tasks -----------------------------------------------------
  function Tasks() {
    const [filter, setFilter] = useState("");
    const { data } = usePoll(() => fetchJSON(API + "/tasks?limit=200" + (filter ? "&status=" + filter : "")), 5000);
    const items = (data && data.items) || [];
    return h(Card, { title: "Tasks (" + items.length + ")",
      right: h("select", { value: filter, onChange: (e) => setFilter(e.target.value),
        className: "bg-black/30 border border-white/10 rounded px-2 py-1 text-xs text-white/80" },
        ["", "queued", "claimed", "in_progress", "done", "approved", "failed", "cancelled", "rejected"]
          .map((s) => h("option", { key: s, value: s }, s || "all statuses"))) },
      items.length ? h("div", { className: "overflow-x-auto max-h-[60vh] overflow-y-auto" },
        h("table", { className: "w-full text-sm" },
          h("thead", null, h("tr", { className: "text-left text-white/40 border-b border-white/10 sticky top-0 bg-[#0b0f0c]" },
            ["title", "kind", "status", "assignee", "prio", "updated"].map((c) => h("th", { key: c, className: "py-2 pr-4" }, c)))),
          h("tbody", null, items.map((t) => h("tr", { key: t.id, className: "border-b border-white/5" },
            h("td", { className: "py-2 pr-4" },
              h("div", { className: "font-medium text-white/90" }, t.title),
              h("div", { className: "text-xs text-white/40 font-mono" }, t.id)),
            h("td", { className: "pr-4 text-white/60" }, t.kind),
            h("td", { className: "pr-4" }, h(Pill, { kind: "status", value: t.status })),
            h("td", { className: "pr-4 text-white/50 font-mono text-xs" }, (t.assigned_to || "—").split("-").slice(-2).join("-")),
            h("td", { className: "pr-4 text-white/60" }, t.priority),
            h("td", { className: "text-white/40 text-xs" }, ago(t.updated_at)))))))
        : h(Empty, { msg: "no tasks" }));
  }

  // ---- sub-tab: Events ----------------------------------------------------
  function Events() {
    const { data } = usePoll(() => fetchJSON(API + "/events?limit=60"), 5000);
    const items = (data && data.items) || [];
    return h(Card, { title: "Event audit log (" + items.length + ")" },
      items.length ? h("div", { className: "space-y-1 max-h-[60vh] overflow-y-auto" },
        items.map((e, i) => h("div", { key: i, className: "text-sm text-white/70" },
          h("span", { className: "text-white/40 mr-2" }, ago(e.ts)),
          h("b", { className: "text-white/90" }, e.actor || "?"),
          h("span", { className: "mx-1 text-white/50" }, "·"),
          h("span", { className: "font-mono text-xs" }, e.type),
          e.task_id ? h("span", { className: "ml-2 font-mono text-xs text-white/40" }, e.task_id) : null)))
        : h(Empty, { msg: "no events" }));
  }

  // ---- main tab component -------------------------------------------------
  const TABS = [
    ["overview", "Overview", Overview],
    ["agents", "Agents", Agents],
    ["projects", "Projects", Projects],
    ["tasks", "Tasks", Tasks],
    ["events", "Events", Events],
  ];
  function AgentMeshPanel() {
    const [tab, setTab] = useState("overview");
    const Active = (TABS.find((t) => t[0] === tab) || TABS[0])[2];
    return h("div", { className: "p-4 space-y-4" },
      h("div", { className: "flex items-center gap-2 flex-wrap" },
        h("span", { className: "text-lg" }, "🐐"),
        h("h2", { className: "text-base font-semibold text-white" }, "agent-mesh"),
        h("span", { className: "text-xs text-white/40" }, "local swarm control panel"),
        h("div", { className: "ml-auto flex gap-1" },
          TABS.map(([id, label]) => h("button", { key: id, onClick: () => setTab(id),
            className: cn("px-3 py-1.5 rounded-md text-sm transition-colors",
              tab === id ? "bg-white/10 text-white" : "text-white/50 hover:text-white/80 hover:bg-white/5") }, label)))),
      h(Active, {}));
  }

  REG.register("agent-mesh", AgentMeshPanel);
})();
