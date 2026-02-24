#!/usr/bin/env node
/**
 * Read-only system visualizer server.
 *
 * Serves:
 * - GET /api/graph?hours=24
 * - static UI from agent-holo-viz/client/
 * - WebSocket stream on /ws/holo
 */
const fs = require('fs');
const http = require('http');
const path = require('path');
const { URL } = require('url');
let WebSocketServer = null;
try {
  ({ WebSocketServer } = require('ws'));
} catch {
  WebSocketServer = null;
}

const REPO_ROOT = process.env.OPENCLAW_WORKSPACE
  ? path.resolve(process.env.OPENCLAW_WORKSPACE)
  : path.resolve(__dirname, '..', '..');
const RUNS_DIR = path.join(REPO_ROOT, 'state', 'autonomy', 'runs');
const SPINE_RUNS_DIR = path.join(REPO_ROOT, 'state', 'spine', 'runs');
const STATIC_DIR = path.join(__dirname, '..', 'client');

const DEFAULT_HOST = '127.0.0.1';
const DEFAULT_PORT = 8787;
const DEFAULT_HOURS = 24;
const MAX_EVENTS = 6000;
const MAX_PROPOSALS = 80;
const WS_PATH = '/ws/holo';
const WS_TICK_MS = 1300;
const WS_WATCH_DEBOUNCE_MS = 220;
const WS_PING_MS = 15000;
const MODULE_SCAN_CACHE_TTL_MS = 7000;
const MAX_LAYER_MODULES = 24;
const MAX_MODULE_SUBMODULES = 20;

const LAYER_ROOTS = [
  { key: 'adaptive', label: 'Adaptive', rel: 'adaptive' },
  { key: 'systems', label: 'Systems', rel: 'systems' },
  { key: 'memory', label: 'Memory', rel: 'memory' },
  { key: 'habits', label: 'Habits', rel: 'habits' },
  { key: 'lib', label: 'Library', rel: 'lib' },
  { key: 'config', label: 'Config', rel: 'config' },
  { key: 'state', label: 'State', rel: 'state' }
];

let MODULE_SCAN_CACHE = {
  ts: 0,
  payload: null
};

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml'
};

function nowIso() {
  return new Date().toISOString();
}

function parseArgs(argv) {
  const out = { host: DEFAULT_HOST, port: DEFAULT_PORT, hours: DEFAULT_HOURS };
  for (let i = 0; i < argv.length; i += 1) {
    const tok = String(argv[i] || '');
    if (tok === '--host' && argv[i + 1]) {
      out.host = String(argv[i + 1]);
      i += 1;
      continue;
    }
    if (tok === '--port' && argv[i + 1]) {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) out.port = Math.round(v);
      i += 1;
      continue;
    }
    if (tok === '--hours' && argv[i + 1]) {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) out.hours = Math.round(v);
      i += 1;
      continue;
    }
  }
  return out;
}

function clampNumber(v, lo, hi, fallback = lo) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  if (n < lo) return lo;
  if (n > hi) return hi;
  return n;
}

function safeJsonParse(line) {
  try {
    return JSON.parse(line);
  } catch {
    return null;
  }
}

function parseTsMs(ts) {
  const ms = Date.parse(String(ts || ''));
  return Number.isFinite(ms) ? ms : null;
}

function listJsonlFilesDesc(absDir) {
  if (!fs.existsSync(absDir)) return [];
  return fs.readdirSync(absDir)
    .filter((f) => /^\d{4}-\d{2}-\d{2}\.jsonl$/.test(f))
    .sort()
    .reverse();
}

function listRunFilesDesc() {
  return listJsonlFilesDesc(RUNS_DIR);
}

function listSpineRunFilesDesc() {
  return listJsonlFilesDesc(SPINE_RUNS_DIR);
}

function loadRecentTelemetry(hours = DEFAULT_HOURS, maxEvents = MAX_EVENTS) {
  const h = clampNumber(hours, 1, 24 * 30, DEFAULT_HOURS);
  const cap = clampNumber(maxEvents, 100, 20000, MAX_EVENTS);
  const cutoffMs = Date.now() - (h * 60 * 60 * 1000);
  const runs = [];
  const audits = [];

  const files = listRunFilesDesc();
  for (const file of files) {
    const fp = path.join(RUNS_DIR, file);
    if (!fs.existsSync(fp)) continue;
    const lines = String(fs.readFileSync(fp, 'utf8') || '').split('\n');
    for (let i = lines.length - 1; i >= 0; i -= 1) {
      const line = String(lines[i] || '').trim();
      if (!line) continue;
      const evt = safeJsonParse(line);
      if (!evt || typeof evt !== 'object') continue;
      const ms = parseTsMs(evt.ts);
      if (ms == null || ms < cutoffMs) continue;
      if (evt.type === 'autonomy_run') runs.push(evt);
      else if (evt.type === 'autonomy_candidate_audit') audits.push(evt);
      if (runs.length + audits.length >= cap) {
        return { runs, audits, window_hours: h };
      }
    }
  }
  return { runs, audits, window_hours: h };
}

function loadRecentSpineEvents(hours = DEFAULT_HOURS, maxEvents = 600) {
  const h = clampNumber(hours, 1, 24 * 30, DEFAULT_HOURS);
  const cap = clampNumber(maxEvents, 20, 6000, 600);
  const cutoffMs = Date.now() - (h * 60 * 60 * 1000);
  const events = [];
  const files = listSpineRunFilesDesc();
  for (const file of files) {
    const fp = path.join(SPINE_RUNS_DIR, file);
    if (!fs.existsSync(fp)) continue;
    const lines = String(fs.readFileSync(fp, 'utf8') || '').split('\n');
    for (let i = lines.length - 1; i >= 0; i -= 1) {
      const line = String(lines[i] || '').trim();
      if (!line) continue;
      const evt = safeJsonParse(line);
      if (!evt || typeof evt !== 'object') continue;
      const ms = parseTsMs(evt.ts);
      if (ms == null || ms < cutoffMs) continue;
      events.push({
        ts: String(evt.ts || ''),
        type: String(evt.type || 'unknown'),
        mode: String(evt.mode || ''),
        status: String(evt.status || ''),
        outcome: String(evt.outcome || ''),
        reason: String(evt.reason || ''),
        objective_id: String(evt.objective_id || '')
      });
      if (events.length >= cap) return events;
    }
  }
  return events;
}

function loadDirectiveSummary() {
  try {
    const { loadActiveDirectives } = require(path.join(REPO_ROOT, 'lib', 'directive_resolver'));
    const rows = loadActiveDirectives();
    if (!Array.isArray(rows)) return [];
    return rows.map((row) => {
      const data = row && row.data && typeof row.data === 'object' ? row.data : {};
      const meta = data.metadata && typeof data.metadata === 'object' ? data.metadata : {};
      const title = String(meta.title || data.title || row.id || '').trim();
      return {
        id: String(row.id || '').trim(),
        tier: Number(row.tier || meta.tier || 99),
        title: title || String(row.id || '').trim()
      };
    }).filter((d) => d.id);
  } catch {
    return [];
  }
}

function loadStrategySummary() {
  try {
    const { loadActiveStrategy } = require(path.join(REPO_ROOT, 'lib', 'strategy_resolver'));
    const s = loadActiveStrategy();
    const campaignsRaw = Array.isArray(s && s.campaigns) ? s.campaigns : [];
    const campaigns = campaignsRaw.map((c) => {
      const phases = Array.isArray(c && c.phases) ? c.phases : [];
      const phaseTypes = [];
      for (const ph of phases) {
        const pt = Array.isArray(ph && ph.proposal_types) ? ph.proposal_types : [];
        for (const t of pt) {
          const v = String(t || '').trim().toLowerCase();
          if (v) phaseTypes.push(v);
        }
      }
      return {
        id: String(c && c.id || '').trim(),
        name: String(c && c.name || c && c.id || '').trim(),
        status: String(c && c.status || 'active').trim().toLowerCase(),
        proposal_types: Array.from(new Set(phaseTypes))
      };
    }).filter((c) => c.id);
    return {
      id: String(s && s.id || '').trim() || 'default_general',
      name: String(s && s.name || s && s.id || '').trim() || 'default_general',
      mode: String(s && s.execution_policy && s.execution_policy.mode || '').trim().toLowerCase() || 'unknown',
      campaigns
    };
  } catch {
    return {
      id: 'default_general',
      name: 'default_general',
      mode: 'unknown',
      campaigns: []
    };
  }
}

function objectiveIdFromRun(evt) {
  if (!evt || typeof evt !== 'object') return '';
  const pulse = evt.directive_pulse && typeof evt.directive_pulse === 'object'
    ? evt.directive_pulse
    : {};
  const binding = evt.objective_binding && typeof evt.objective_binding === 'object'
    ? evt.objective_binding
    : {};
  const raw = String(
    evt.objective_id
    || pulse.objective_id
    || binding.objective_id
    || ''
  ).trim();
  return raw;
}

function proposalTypeFromRun(evt) {
  const explicit = String(evt && evt.proposal_type || '').trim().toLowerCase();
  if (explicit) return explicit;
  const cap = String(evt && evt.capability_key || '').trim().toLowerCase();
  const m = cap.match(/^proposal:([a-z0-9:_-]+)$/);
  if (m && m[1]) return String(m[1]).replace(/_opportunity$/, '');
  return 'unknown';
}

function outcomeLabel(evt) {
  const result = String(evt && evt.result || '').trim();
  if (result === 'executed') {
    const o = String(evt && evt.outcome || 'unknown').trim().toLowerCase() || 'unknown';
    return `executed:${o}`;
  }
  return result || 'unknown';
}

function isPolicyHoldResult(result) {
  const r = String(result || '').trim().toLowerCase();
  if (!r) return false;
  return r.startsWith('no_candidates_policy_')
    || r === 'stop_init_gate_budget_autopause'
    || r === 'stop_init_gate_readiness'
    || r === 'stop_init_gate_readiness_blocked'
    || r === 'stop_init_gate_criteria_quality_insufficient'
    || r === 'score_only_fallback_route_block'
    || r === 'score_only_fallback_low_execution_confidence';
}

function buildCampaignTypeIndex(campaigns) {
  const map = {};
  for (const c of campaigns || []) {
    const cid = String(c && c.id || '').trim();
    if (!cid) continue;
    const types = Array.isArray(c && c.proposal_types) ? c.proposal_types : [];
    for (const t of types) {
      const k = String(t || '').trim().toLowerCase();
      if (!k) continue;
      if (!map[k]) map[k] = new Set();
      map[k].add(cid);
    }
  }
  return map;
}

function isRenderableEntryName(name) {
  const n = String(name || '').trim();
  if (!n) return false;
  if (n.startsWith('.')) return false;
  if (n === 'node_modules') return false;
  if (n === '__pycache__') return false;
  return true;
}

function safeReadDirWithTypes(absDir) {
  try {
    if (!fs.existsSync(absDir)) return [];
    return fs.readdirSync(absDir, { withFileTypes: true });
  } catch {
    return [];
  }
}

function direntSort(a, b) {
  const ad = a.isDirectory() ? 0 : 1;
  const bd = b.isDirectory() ? 0 : 1;
  if (ad !== bd) return ad - bd;
  return String(a.name || '').localeCompare(String(b.name || ''));
}

function shouldIncludeSubEntry(ent) {
  if (!ent) return false;
  if (!isRenderableEntryName(ent.name)) return false;
  if (ent.isDirectory()) return true;
  const ext = path.extname(String(ent.name || '')).toLowerCase();
  return ext === '.js' || ext === '.ts' || ext === '.json' || ext === '.md';
}

function tokenize(text) {
  const raw = String(text || '').toLowerCase();
  return raw
    .split(/[^a-z0-9]+/g)
    .map((s) => s.trim())
    .filter((s) => s.length >= 3 && s !== 'json' && s !== 'node' && s !== 'test');
}

function scanLayerModelRaw() {
  const layers = [];
  const aliasToId = {};

  for (const layerDef of LAYER_ROOTS) {
    const layerAbs = path.join(REPO_ROOT, layerDef.rel);
    if (!fs.existsSync(layerAbs)) continue;
    const layerId = `layer:${layerDef.key}`;
    const layerNode = {
      id: layerId,
      key: layerDef.key,
      name: layerDef.label,
      rel: layerDef.rel,
      activity: 0,
      modules: []
    };
    aliasToId[layerDef.key] = layerId;
    aliasToId[layerDef.rel.toLowerCase()] = layerId;

    const entries = safeReadDirWithTypes(layerAbs)
      .filter((ent) => isRenderableEntryName(ent.name))
      .sort(direntSort)
      .slice(0, MAX_LAYER_MODULES);

    for (const ent of entries) {
      const modName = String(ent.name || '').trim();
      const modId = `module:${layerDef.key}/${modName}`;
      const modRel = `${layerDef.rel}/${modName}`;
      const moduleNode = {
        id: modId,
        parent_id: layerId,
        key: modName.toLowerCase(),
        name: modName,
        rel: modRel,
        type: ent.isDirectory() ? 'dir' : 'file',
        activity: 0,
        submodules: []
      };
      aliasToId[`${layerDef.key}/${modName}`.toLowerCase()] = modId;
      aliasToId[modName.toLowerCase()] = aliasToId[modName.toLowerCase()] || modId;

      if (ent.isDirectory()) {
        const modAbs = path.join(layerAbs, modName);
        const subEntries = safeReadDirWithTypes(modAbs)
          .filter((sub) => shouldIncludeSubEntry(sub))
          .sort(direntSort)
          .slice(0, MAX_MODULE_SUBMODULES);

        for (const sub of subEntries) {
          const subName = String(sub.name || '').trim();
          const subId = `submodule:${layerDef.key}/${modName}/${subName}`;
          const subNode = {
            id: subId,
            parent_id: modId,
            key: subName.toLowerCase(),
            name: subName,
            rel: `${modRel}/${subName}`,
            type: sub.isDirectory() ? 'dir' : 'file',
            activity: 0
          };
          moduleNode.submodules.push(subNode);
          aliasToId[`${layerDef.key}/${modName}/${subName}`.toLowerCase()] = subId;
        }
      }

      layerNode.modules.push(moduleNode);
    }

    layers.push(layerNode);
  }

  return {
    layers,
    alias_to_id: aliasToId
  };
}

function cloneJson(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function loadLayerModelCached() {
  const nowMs = Date.now();
  if (
    MODULE_SCAN_CACHE.payload
    && (nowMs - Number(MODULE_SCAN_CACHE.ts || 0)) < MODULE_SCAN_CACHE_TTL_MS
  ) {
    return cloneJson(MODULE_SCAN_CACHE.payload);
  }
  const payload = scanLayerModelRaw();
  MODULE_SCAN_CACHE = {
    ts: nowMs,
    payload
  };
  return cloneJson(payload);
}

function edgeKey(from, to, label) {
  return `${String(from)}|${String(to)}|${String(label || '')}`;
}

function addNode(map, node) {
  const id = String(node && node.id || '').trim();
  if (!id) return;
  if (!map[id]) {
    map[id] = {
      id,
      label: String(node.label || id),
      type: String(node.type || 'unknown'),
      weight: Number(node.weight || 0),
      meta: node.meta && typeof node.meta === 'object' ? node.meta : {}
    };
    return;
  }
  map[id].weight = Number(map[id].weight || 0) + Number(node.weight || 0);
}

function addEdge(map, from, to, label, count = 1) {
  if (!from || !to) return;
  const k = edgeKey(from, to, label);
  if (!map[k]) {
    map[k] = {
      id: k,
      from,
      to,
      label: String(label || ''),
      count: Number(count || 1)
    };
    return;
  }
  map[k].count += Number(count || 1);
}

function topCounts(rows, limit = 10) {
  return Object.entries(rows || {})
    .map(([k, v]) => [k, Number(v || 0)])
    .sort((a, b) => b[1] - a[1] || String(a[0]).localeCompare(String(b[0])))
    .slice(0, limit);
}

function buildSummary(runs, audits, windowHours) {
  const resultCounts = {};
  const capabilityCounts = {};
  const proposalTypeCounts = {};
  const gateCounts = {};
  let executed = 0;
  let shipped = 0;
  let noChange = 0;
  let reverted = 0;
  let confidenceFallback = 0;
  let routeBlocked = 0;
  let policyHolds = 0;

  for (const evt of runs) {
    const result = String(evt && evt.result || 'unknown').trim() || 'unknown';
    resultCounts[result] = Number(resultCounts[result] || 0) + 1;
    if (evt && evt.policy_hold === true) policyHolds += 1;
    if (result === 'score_only_fallback_low_execution_confidence') confidenceFallback += 1;
    if (result === 'score_only_fallback_route_block' || result === 'init_gate_blocked_route') routeBlocked += 1;
    if (result === 'executed') {
      executed += 1;
      const outcome = String(evt && evt.outcome || '').trim().toLowerCase();
      if (outcome === 'shipped') shipped += 1;
      else if (outcome === 'no_change') noChange += 1;
      else if (outcome === 'reverted') reverted += 1;
    }
    const cap = String(evt && evt.capability_key || '').trim().toLowerCase();
    if (cap) capabilityCounts[cap] = Number(capabilityCounts[cap] || 0) + 1;
    const pType = proposalTypeFromRun(evt);
    if (pType) proposalTypeCounts[pType] = Number(proposalTypeCounts[pType] || 0) + 1;
  }

  for (const audit of audits) {
    const rej = audit && audit.rejected_by_gate && typeof audit.rejected_by_gate === 'object'
      ? audit.rejected_by_gate
      : {};
    for (const [gate, count] of Object.entries(rej)) {
      gateCounts[String(gate)] = Number(gateCounts[String(gate)] || 0) + Number(count || 0);
    }
  }

  const totalRuns = runs.length;
  return {
    generated_at: nowIso(),
    window_hours: windowHours,
    run_events: totalRuns,
    candidate_audits: audits.length,
    executed,
    shipped,
    no_change: noChange,
    reverted,
    policy_holds: policyHolds,
    confidence_fallback: confidenceFallback,
    route_blocked: routeBlocked,
    top_results: topCounts(resultCounts, 12),
    top_capabilities: topCounts(capabilityCounts, 10),
    top_proposal_types: topCounts(proposalTypeCounts, 10),
    top_rejected_gates: topCounts(gateCounts, 12)
  };
}

function buildGraph(runs, directives, strategy) {
  const nodeMap = {};
  const edgeMap = {};
  const latestByProposal = {};
  const objectiveSet = new Set();
  const strategyId = String(strategy && strategy.id || '').trim() || 'default_general';
  const campaignTypeIndex = buildCampaignTypeIndex(strategy && strategy.campaigns || []);

  for (const d of directives || []) {
    addNode(nodeMap, {
      id: `directive:${d.id}`,
      label: `${d.id}`,
      type: 'directive',
      weight: 1,
      meta: { tier: Number(d.tier || 99), title: String(d.title || d.id) }
    });
    objectiveSet.add(String(d.id));
  }

  addNode(nodeMap, {
    id: `strategy:${strategyId}`,
    label: strategyId,
    type: 'strategy',
    weight: 1,
    meta: { mode: String(strategy && strategy.mode || 'unknown') }
  });

  for (const c of strategy && strategy.campaigns || []) {
    const cid = String(c && c.id || '').trim();
    if (!cid) continue;
    addNode(nodeMap, {
      id: `campaign:${cid}`,
      label: cid,
      type: 'campaign',
      weight: 1,
      meta: { status: String(c.status || 'active') }
    });
    addEdge(edgeMap, `strategy:${strategyId}`, `campaign:${cid}`, 'contains', 1);
  }

  for (const evt of runs) {
    const pid = String(evt && evt.proposal_id || '').trim();
    if (!pid) continue;
    const ts = parseTsMs(evt.ts) || 0;
    if (!latestByProposal[pid] || ts > (latestByProposal[pid].ts || 0)) {
      latestByProposal[pid] = { evt, ts };
    }
  }

  const proposalIds = Object.entries(latestByProposal)
    .sort((a, b) => Number(b[1].ts || 0) - Number(a[1].ts || 0))
    .slice(0, MAX_PROPOSALS)
    .map(([pid]) => pid);
  const proposalIdSet = new Set(proposalIds);

  for (const pid of proposalIds) {
    const row = latestByProposal[pid];
    const evt = row && row.evt ? row.evt : {};
    const pType = proposalTypeFromRun(evt);
    addNode(nodeMap, {
      id: `proposal:${pid}`,
      label: pType === 'unknown' ? pid : `${pType}:${pid.slice(0, 8)}`,
      type: 'proposal',
      weight: 1,
      meta: {
        proposal_id: pid,
        proposal_type: pType,
        risk: String(evt && evt.risk || 'unknown')
      }
    });
    addEdge(edgeMap, `strategy:${strategyId}`, `proposal:${pid}`, 'selects', 1);

    const objectiveId = objectiveIdFromRun(evt);
    if (objectiveId) {
      objectiveSet.add(objectiveId);
      addNode(nodeMap, {
        id: `directive:${objectiveId}`,
        label: objectiveId,
        type: 'directive',
        weight: 1,
        meta: { tier: objectiveId.startsWith('T1_') ? 1 : null }
      });
      addEdge(edgeMap, `directive:${objectiveId}`, `proposal:${pid}`, 'targets', 1);
    }

    const campaignIds = campaignTypeIndex[pType] ? Array.from(campaignTypeIndex[pType]) : [];
    for (const cid of campaignIds) {
      addEdge(edgeMap, `campaign:${cid}`, `proposal:${pid}`, 'contains_type', 1);
    }
  }

  for (const evt of runs) {
    const pid = String(evt && evt.proposal_id || '').trim();
    if (!pid || !proposalIdSet.has(pid)) continue;
    const outLabel = outcomeLabel(evt);
    const outId = `outcome:${outLabel}`;
    addNode(nodeMap, {
      id: outId,
      label: outLabel,
      type: 'outcome',
      weight: 1,
      meta: {}
    });
    addEdge(edgeMap, `proposal:${pid}`, outId, 'produces', 1);
  }

  return {
    nodes: Object.values(nodeMap),
    edges: Object.values(edgeMap)
  };
}

function flattenLayerNodes(layers) {
  const rows = [];
  const byId = {};
  for (const layer of layers || []) {
    rows.push(layer);
    byId[layer.id] = layer;
    for (const mod of layer.modules || []) {
      rows.push(mod);
      byId[mod.id] = mod;
      for (const sub of mod.submodules || []) {
        rows.push(sub);
        byId[sub.id] = sub;
      }
    }
  }
  return { rows, by_id: byId };
}

function eventSignalText(evt) {
  return [
    evt && evt.capability_key,
    proposalTypeFromRun(evt),
    evt && evt.result,
    evt && evt.outcome,
    evt && evt.hold_scope,
    evt && evt.hold_reason,
    objectiveIdFromRun(evt),
    evt && evt.reason
  ].map((v) => String(v || '').toLowerCase()).join(' ');
}

function assignLayerActivity(layers, runs, summary, aliasToId) {
  const rows = flattenLayerNodes(layers);
  const targets = rows.rows.map((node) => ({
    node,
    tokens: Array.from(new Set([
      ...tokenize(node.name),
      ...tokenize(node.rel || ''),
      ...tokenize(node.key || '')
    ]))
  }));
  const raw = {};
  const bump = (id, weight) => {
    const key = String(id || '').trim();
    if (!key || !rows.by_id[key]) return;
    raw[key] = Number(raw[key] || 0) + Number(weight || 0);
  };
  const bumpAlias = (alias, weight) => {
    const id = aliasToId[String(alias || '').toLowerCase()];
    if (!id) return;
    bump(id, weight);
  };

  const eventRows = Array.isArray(runs) ? runs.slice(0, 2400) : [];
  for (const evt of eventRows) {
    const text = eventSignalText(evt);
    let w = String(evt && evt.result || '') === 'executed' ? 1.45 : 1;
    if (isPolicyHoldResult(evt && evt.result)) w += 0.25;
    for (const t of targets) {
      if (!t.tokens.length) continue;
      let matched = false;
      for (const token of t.tokens) {
        if (text.includes(token)) {
          matched = true;
          break;
        }
      }
      if (matched) bump(t.node.id, w);
    }

    const cap = String(evt && evt.capability_key || '').toLowerCase();
    const result = String(evt && evt.result || '').toLowerCase();
    const outcome = String(evt && evt.outcome || '').toLowerCase();

    if (cap.startsWith('proposal:')) {
      bumpAlias('adaptive', 1.6);
      bumpAlias('systems', 1.3);
      bumpAlias('systems/autonomy', 1.2);
    }
    if (cap.startsWith('actuation:')) {
      bumpAlias('systems/actuation', 1.8);
      bumpAlias('systems', 1.2);
    }
    if (result.includes('route') || cap.includes('route')) {
      bumpAlias('systems/routing', 1.5);
      bumpAlias('systems', 0.9);
    }
    if (result.includes('memory') || cap.includes('memory')) {
      bumpAlias('memory', 1.6);
      bumpAlias('systems/memory', 1.3);
    }
    if (result.includes('policy') || result.includes('hold') || isPolicyHoldResult(result)) {
      bumpAlias('state/autonomy', 1.8);
      bumpAlias('systems/security', 1.5);
      bumpAlias('systems/autonomy', 1.2);
    }
    if (result.includes('spawn') || cap.includes('spawn')) {
      bumpAlias('systems/spawn', 1.7);
      bumpAlias('state/spawn', 1.2);
    }
    if (result === 'executed' && outcome === 'shipped') {
      bumpAlias('systems', 1.5);
      bumpAlias('systems/autonomy', 1.1);
      bumpAlias('habits', 0.9);
    }
  }

  bumpAlias('systems', Math.max(0.5, Number(summary && summary.run_events || 0) * 0.012));
  bumpAlias('state', Math.max(0.2, Number(summary && summary.policy_holds || 0) * 0.04));
  bumpAlias('adaptive', Math.max(0.2, Number(summary && summary.executed || 0) * 0.025));

  let maxRaw = 0;
  for (const value of Object.values(raw)) {
    maxRaw = Math.max(maxRaw, Number(value || 0));
  }
  if (maxRaw <= 0) maxRaw = 1;

  for (const layer of layers || []) {
    for (const mod of layer.modules || []) {
      for (const sub of mod.submodules || []) {
        const r = Number(raw[sub.id] || 0);
        sub.activity = Number((0.06 + (0.94 * (r / maxRaw))).toFixed(4));
      }
      const subAvg = (mod.submodules || []).length
        ? (mod.submodules.reduce((acc, s) => acc + Number(s.activity || 0), 0) / mod.submodules.length)
        : 0;
      const selfRaw = Number(raw[mod.id] || 0);
      const selfNorm = selfRaw / maxRaw;
      mod.activity = Number((Math.max(0.07, Math.min(1, (selfNorm * 0.7) + (subAvg * 0.3)))).toFixed(4));
    }
    const layerAvg = (layer.modules || []).length
      ? (layer.modules.reduce((acc, m) => acc + Number(m.activity || 0), 0) / layer.modules.length)
      : 0.07;
    const layerRaw = Number(raw[layer.id] || 0) / maxRaw;
    layer.activity = Number((Math.max(0.07, Math.min(1, (layerRaw * 0.6) + (layerAvg * 0.4)))).toFixed(4));
  }

  return layers;
}

function buildHoloLinks(layers, summary, runs, aliasToId) {
  const edgeMap = {};
  const add = (from, to, count, kind) => {
    const f = String(from || '').trim();
    const t = String(to || '').trim();
    if (!f || !t || f === t) return;
    const key = `${f}|${t}|${String(kind || 'flow')}`;
    if (!edgeMap[key]) {
      edgeMap[key] = { from: f, to: t, count: 0, kind: String(kind || 'flow') };
    }
    edgeMap[key].count += Math.max(0.2, Number(count || 0));
  };
  const byAlias = (alias) => aliasToId[String(alias || '').toLowerCase()] || null;

  for (const layer of layers || []) {
    for (const mod of layer.modules || []) {
      add(layer.id, mod.id, Math.max(0.5, Number(mod.activity || 0) * 3), 'hierarchy');
      for (const sub of mod.submodules || []) {
        add(mod.id, sub.id, Math.max(0.3, Number(sub.activity || 0) * 2), 'hierarchy');
      }
    }
  }

  const staticPairs = [
    ['adaptive', 'systems', Math.max(1, Number(summary && summary.executed || 0) * 0.25)],
    ['systems', 'state', Math.max(1, Number(summary && summary.policy_holds || 0) * 0.35)],
    ['systems', 'memory', Math.max(1, Number(summary && summary.executed || 0) * 0.2)],
    ['memory', 'systems', Math.max(0.7, Number(summary && summary.reverted || 0) * 0.5)],
    ['habits', 'systems', Math.max(0.7, Number(summary && summary.run_events || 0) * 0.08)],
    ['config', 'systems', 1],
    ['lib', 'systems', 1]
  ];
  for (const [a, b, c] of staticPairs) {
    const from = byAlias(a);
    const to = byAlias(b);
    if (from && to) add(from, to, c, 'route');
  }

  const adaptiveLayer = byAlias('adaptive') || byAlias('systems');
  const systemsLayer = byAlias('systems') || byAlias('adaptive');
  const stateLayer = byAlias('state') || byAlias('systems');
  const runCount = Number(summary && summary.run_events || 0);
  const shippedCount = Number(summary && summary.shipped || 0);
  const noChangeCount = Number(summary && summary.no_change || 0);
  const revertedCount = Number(summary && summary.reverted || 0);
  const policyHoldCount = Number(summary && summary.policy_holds || 0);

  if (adaptiveLayer) add('io:input:sensory', adaptiveLayer, Math.max(0.8, runCount * 0.3), 'ingress');
  if (systemsLayer) add('io:input:directive', systemsLayer, Math.max(0.7, policyHoldCount * 0.35), 'ingress');
  if (stateLayer) add('io:input:directive', stateLayer, Math.max(0.5, policyHoldCount * 0.25), 'ingress');
  if (systemsLayer) add(systemsLayer, 'io:output:shipped', Math.max(0.5, shippedCount * 0.42), 'egress');
  if (systemsLayer) add(systemsLayer, 'io:output:no_change', Math.max(0.3, noChangeCount * 0.35), 'egress');
  if (systemsLayer) add(systemsLayer, 'io:output:reverted', Math.max(0.2, revertedCount * 0.45), 'egress');

  const runRows = Array.isArray(runs) ? runs.slice(0, 1200) : [];
  for (const evt of runRows) {
    const cap = String(evt && evt.capability_key || '').toLowerCase();
    const result = String(evt && evt.result || '').toLowerCase();
    const outcome = String(evt && evt.outcome || '').toLowerCase();
    let from = byAlias('systems');
    if (cap.startsWith('proposal:')) from = byAlias('adaptive') || from;
    if (cap.startsWith('actuation:')) from = byAlias('systems/actuation') || byAlias('systems') || from;
    let to = byAlias('systems');
    if (isPolicyHoldResult(result) || result.includes('hold')) to = byAlias('state') || to;
    if (result === 'executed' && outcome === 'shipped') to = 'io:output:shipped';
    if (result === 'executed' && outcome === 'no_change') to = 'io:output:no_change';
    if (result === 'executed' && outcome === 'reverted') to = 'io:output:reverted';
    if (result.includes('memory')) to = byAlias('memory') || to;
    add(from, to, 1, 'flow');
  }

  const links = Object.values(edgeMap);
  let maxCount = 0;
  for (const link of links) maxCount = Math.max(maxCount, Number(link.count || 0));
  if (maxCount <= 0) maxCount = 1;
  for (const link of links) {
    link.activity = Number((0.08 + (0.92 * (Number(link.count || 0) / maxCount))).toFixed(4));
  }
  return links;
}

function buildHoloModel(runs, summary) {
  const scanned = loadLayerModelCached();
  const layers = assignLayerActivity(scanned.layers || [], runs || [], summary || {}, scanned.alias_to_id || {});
  const io = {
    inputs: [
      {
        id: 'io:input:sensory',
        name: 'Sensory Input',
        activity: Number((0.2 + (Math.min(1, Number(summary && summary.run_events || 0) / 1200) * 0.8)).toFixed(4)),
        count: Number(summary && summary.run_events || 0)
      },
      {
        id: 'io:input:directive',
        name: 'Directive Input',
        activity: Number((0.15 + (Math.min(1, Number(summary && summary.policy_holds || 0) / 80) * 0.85)).toFixed(4)),
        count: Number(summary && summary.policy_holds || 0)
      }
    ],
    outputs: [
      {
        id: 'io:output:shipped',
        name: 'Shipped Output',
        activity: Number((0.2 + (Math.min(1, Number(summary && summary.shipped || 0) / 50) * 0.8)).toFixed(4)),
        count: Number(summary && summary.shipped || 0)
      },
      {
        id: 'io:output:no_change',
        name: 'No-Change Output',
        activity: Number((0.12 + (Math.min(1, Number(summary && summary.no_change || 0) / 100) * 0.88)).toFixed(4)),
        count: Number(summary && summary.no_change || 0)
      },
      {
        id: 'io:output:reverted',
        name: 'Reverted Output',
        activity: Number((0.1 + (Math.min(1, Number(summary && summary.reverted || 0) / 40) * 0.9)).toFixed(4)),
        count: Number(summary && summary.reverted || 0)
      }
    ]
  };

  const links = buildHoloLinks(layers, summary, runs, scanned.alias_to_id || {});
  const executed = Number(summary && summary.executed || 0);
  const shipped = Number(summary && summary.shipped || 0);
  const noChange = Number(summary && summary.no_change || 0);
  const yieldRate = executed > 0 ? shipped / executed : 0;
  const driftRate = executed > 0 ? noChange / executed : 0;

  return {
    generated_at: nowIso(),
    layers,
    links,
    io,
    metrics: {
      run_events: Number(summary && summary.run_events || 0),
      executed,
      shipped,
      no_change: noChange,
      reverted: Number(summary && summary.reverted || 0),
      policy_holds: Number(summary && summary.policy_holds || 0),
      yield_rate: Number(yieldRate.toFixed(4)),
      drift_proxy: Number(driftRate.toFixed(4))
    }
  };
}

function buildPayload(hours) {
  const telemetry = loadRecentTelemetry(hours, MAX_EVENTS);
  const directives = loadDirectiveSummary();
  const strategy = loadStrategySummary();
  const summary = buildSummary(telemetry.runs, telemetry.audits, telemetry.window_hours);
  const graph = buildGraph(telemetry.runs, directives, strategy);
  const holo = buildHoloModel(telemetry.runs, summary);
  return {
    ok: true,
    generated_at: nowIso(),
    summary,
    graph,
    holo
  };
}

function buildSpinePulse(hours = DEFAULT_HOURS) {
  const events = loadRecentSpineEvents(Math.min(Math.max(1, Number(hours || DEFAULT_HOURS)), 24), 220);
  const typeCounts = {};
  for (const evt of events) {
    const type = String(evt && evt.type || 'unknown');
    typeCounts[type] = Number(typeCounts[type] || 0) + 1;
  }
  return {
    generated_at: nowIso(),
    event_count: events.length,
    top_types: topCounts(typeCounts, 8),
    latest: events.slice(0, 18)
  };
}

function safeParseMessage(raw) {
  try {
    return JSON.parse(String(raw || ''));
  } catch {
    return null;
  }
}

function wsSendJson(socket, payload) {
  if (!socket || socket.readyState !== 1) return;
  try {
    socket.send(JSON.stringify(payload));
  } catch {
    // ignore closed sockets
  }
}

function createHoloSnapshot(hours, reason) {
  const h = clampNumber(hours, 1, 24 * 30, DEFAULT_HOURS);
  const payload = buildPayload(h);
  return {
    type: 'holo_snapshot',
    reason: String(reason || 'tick'),
    generated_at: payload.generated_at,
    summary: payload.summary,
    holo: payload.holo,
    spine_pulse: buildSpinePulse(h)
  };
}

function createWsHub(server, defaultHours) {
  if (!WebSocketServer) {
    return {
      enabled: false,
      clientCount: () => 0,
      broadcast: () => {},
      close: () => {}
    };
  }

  const wss = new WebSocketServer({ server, path: WS_PATH });
  const clients = new Set();
  const watchers = [];
  const pingTimer = setInterval(() => {
    for (const ws of clients) {
      if (ws.readyState !== 1) continue;
      try {
        ws.ping();
      } catch {
        // ignore ping errors
      }
    }
  }, WS_PING_MS);
  if (typeof pingTimer.unref === 'function') pingTimer.unref();

  const hoursFor = (ws) => clampNumber(ws && ws.sub_hours, 1, 24 * 30, defaultHours);

  const sendSnapshot = (ws, reason) => {
    const snapshot = createHoloSnapshot(hoursFor(ws), reason);
    wsSendJson(ws, snapshot);
  };

  const broadcast = (reason = 'tick') => {
    const openClients = Array.from(clients).filter((ws) => ws.readyState === 1);
    if (!openClients.length) return;
    const snapshotByHours = {};
    for (const ws of openClients) {
      const h = hoursFor(ws);
      if (!snapshotByHours[h]) {
        snapshotByHours[h] = createHoloSnapshot(h, reason);
      }
      wsSendJson(ws, snapshotByHours[h]);
    }
  };

  let debounceTimer = null;
  const scheduleBroadcast = (reason = 'event') => {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      debounceTimer = null;
      broadcast(reason);
    }, WS_WATCH_DEBOUNCE_MS);
    if (typeof debounceTimer.unref === 'function') debounceTimer.unref();
  };

  const watchTarget = (absDir) => {
    if (!fs.existsSync(absDir)) return;
    try {
      const watcher = fs.watch(absDir, { persistent: false }, () => {
        scheduleBroadcast('fswatch');
      });
      watchers.push(watcher);
    } catch {
      // ignore watcher failures
    }
  };
  watchTarget(RUNS_DIR);
  watchTarget(SPINE_RUNS_DIR);

  wss.on('connection', (ws, req) => {
    let subHours = defaultHours;
    try {
      const parsed = new URL(String(req && req.url || WS_PATH), `http://${DEFAULT_HOST}:${DEFAULT_PORT}`);
      const qHours = Number(parsed.searchParams.get('hours'));
      if (Number.isFinite(qHours)) subHours = clampNumber(qHours, 1, 24 * 30, defaultHours);
    } catch {
      // ignore malformed URL
    }
    ws.sub_hours = subHours;
    clients.add(ws);
    sendSnapshot(ws, 'connect');

    ws.on('message', (raw) => {
      const msg = safeParseMessage(raw);
      if (!msg || typeof msg !== 'object') return;
      const type = String(msg.type || '').trim().toLowerCase();
      if (type === 'subscribe' || type === 'set_hours') {
        const h = Number(msg.hours);
        if (Number.isFinite(h)) ws.sub_hours = clampNumber(h, 1, 24 * 30, defaultHours);
        sendSnapshot(ws, 'subscribe');
      } else if (type === 'refresh') {
        sendSnapshot(ws, 'refresh');
      }
    });

    ws.on('close', () => {
      clients.delete(ws);
    });
    ws.on('error', () => {
      clients.delete(ws);
    });
  });

  return {
    enabled: true,
    clientCount: () => Array.from(clients).filter((ws) => ws.readyState === 1).length,
    broadcast,
    close: () => {
      if (debounceTimer) clearTimeout(debounceTimer);
      clearInterval(pingTimer);
      for (const w of watchers) {
        try { w.close(); } catch {}
      }
      for (const ws of clients) {
        try { ws.close(); } catch {}
      }
      try { wss.close(); } catch {}
      clients.clear();
    }
  };
}

function sendJson(res, code, payload) {
  const body = JSON.stringify(payload, null, 2) + '\n';
  res.writeHead(code, {
    'Content-Type': MIME['.json'],
    'Cache-Control': 'no-store',
    'Content-Length': Buffer.byteLength(body)
  });
  res.end(body);
}

function sendText(res, code, text) {
  const body = String(text || '');
  res.writeHead(code, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Cache-Control': 'no-store',
    'Content-Length': Buffer.byteLength(body)
  });
  res.end(body);
}

function serveStatic(reqPath, res) {
  const rel = reqPath === '/' ? '/index.html' : reqPath;
  const normalized = path.normalize(rel).replace(/^(\.\.(\/|\\|$))+/, '');
  const fp = path.join(STATIC_DIR, normalized);
  if (!fp.startsWith(STATIC_DIR)) {
    sendText(res, 403, 'forbidden\n');
    return true;
  }
  if (!fs.existsSync(fp) || !fs.statSync(fp).isFile()) return false;
  const ext = path.extname(fp).toLowerCase();
  const mime = MIME[ext] || 'application/octet-stream';
  const data = fs.readFileSync(fp);
  res.writeHead(200, {
    'Content-Type': mime,
    'Cache-Control': ext === '.html' ? 'no-store' : 'public, max-age=60',
    'Content-Length': data.length
  });
  res.end(data);
  return true;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const host = args.host || DEFAULT_HOST;
  const port = Number(args.port || DEFAULT_PORT);
  const defaultHours = clampNumber(args.hours, 1, 24 * 30, DEFAULT_HOURS);

  const server = http.createServer((req, res) => {
    const parsed = new URL(req.url || '/', `http://${req.headers.host || `${host}:${port}`}`);
    const pathname = parsed.pathname || '/';
    if (req.method !== 'GET') {
      sendText(res, 405, 'method_not_allowed\n');
      return;
    }
    if (pathname === '/api/graph') {
      const qHours = Number(parsed.searchParams.get('hours'));
      const hours = Number.isFinite(qHours) ? qHours : defaultHours;
      try {
        sendJson(res, 200, buildPayload(hours));
      } catch (err) {
        sendJson(res, 500, {
          ok: false,
          error: String(err && err.message || err || 'graph_build_failed'),
          ts: nowIso()
        });
      }
      return;
    }
    if (pathname === '/api/holo') {
      const qHours = Number(parsed.searchParams.get('hours'));
      const hours = Number.isFinite(qHours) ? qHours : defaultHours;
      try {
        const payload = buildPayload(hours);
        sendJson(res, 200, {
          ok: true,
          generated_at: payload.generated_at,
          summary: payload.summary,
          holo: payload.holo
        });
      } catch (err) {
        sendJson(res, 500, {
          ok: false,
          error: String(err && err.message || err || 'holo_build_failed'),
          ts: nowIso()
        });
      }
      return;
    }
    if (pathname === '/api/healthz') {
      sendJson(res, 200, { ok: true, ts: nowIso() });
      return;
    }
    if (!serveStatic(pathname, res)) {
      sendText(res, 404, 'not_found\n');
    }
  });
  const wsHub = createWsHub(server, defaultHours);
  const tickTimer = setInterval(() => {
    if (wsHub.clientCount() > 0) wsHub.broadcast('tick');
  }, WS_TICK_MS);
  if (typeof tickTimer.unref === 'function') tickTimer.unref();

  const shutdown = () => {
    clearInterval(tickTimer);
    wsHub.close();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  server.listen(port, host, () => {
    process.stdout.write(JSON.stringify({
      ok: true,
      type: 'system_visualizer_server',
      host,
      port,
      url: `http://${host}:${port}`,
      default_hours: defaultHours,
      static_dir: STATIC_DIR,
      ws_path: WS_PATH,
      ws_enabled: wsHub.enabled,
      ts: nowIso()
    }) + '\n');
  });
}

module.exports = {
  main
};

if (require.main === module) {
  main();
}
