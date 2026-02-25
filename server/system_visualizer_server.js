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
const { spawnSync } = require('child_process');
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
const FRACTAL_DIR = path.join(REPO_ROOT, 'state', 'autonomy', 'fractal');
const CONTINUUM_DIR = path.join(REPO_ROOT, 'state', 'autonomy', 'continuum');
const CONTINUUM_RUNS_DIR = path.join(CONTINUUM_DIR, 'runs');
const CONTINUUM_EVENTS_DIR = path.join(CONTINUUM_DIR, 'events');
const CONTINUUM_LATEST_PATH = path.join(CONTINUUM_DIR, 'latest.json');
const CONTINUUM_HISTORY_PATH = path.join(CONTINUUM_DIR, 'history.jsonl');
const FRACTAL_ORGANISM_DIR = path.join(FRACTAL_DIR, 'organism_cycle');
const FRACTAL_INTROSPECTION_DIR = path.join(FRACTAL_DIR, 'introspection');
const FRACTAL_PHEROMONE_DIR = path.join(FRACTAL_DIR, 'pheromones');
const FRACTAL_EPIGENETIC_PATH = path.join(FRACTAL_DIR, 'epigenetic_tags.json');
const FRACTAL_ARCHETYPE_PATH = path.join(FRACTAL_DIR, 'archetype_pool.json');
const GENOME_JOURNAL_PATH = path.join(REPO_ROOT, 'state', 'autonomy', 'genome', 'mutation_journal.jsonl');
const BLACK_BOX_CHAIN_PATH = path.join(REPO_ROOT, 'state', 'security', 'black_box_ledger', 'chain.jsonl');
const INTEGRITY_POLICY_PATH = path.join(REPO_ROOT, 'config', 'security_integrity_policy.json');
const INTEGRITY_LOG_PATH = path.join(REPO_ROOT, 'state', 'security', 'integrity_violations.jsonl');
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
const MAX_SUBMODULE_SUBFOLDERS = 14;
const MAX_INTEGRITY_FILES = 12;
const MAX_INTEGRITY_EVENTS = 8;
const GIT_CMD_TIMEOUT_MS = 1800;
const CHANGE_STATE_CACHE_TTL_MS = 1200;
const ACTIVE_WRITE_WINDOW_MS = 14000;
const JUST_PUSHED_WINDOW_MS = 2600;
const MAX_CHANGE_FILES = 10;
const EVOLUTION_CACHE_TTL_MS = 30000;
const FRACTAL_CACHE_TTL_MS = 3000;
const CONTINUUM_CACHE_TTL_MS = 3000;
const CODEBASE_SIZE_MAX_FILES = 2400;
const CODEBASE_SIZE_MAX_DEPTH = 10;
const CODE_PREVIEW_MAX_BYTES = 180 * 1024;
const CODEBASE_SIZE_EXTS = new Set([
  '.js', '.ts', '.jsx', '.tsx', '.mjs', '.cjs',
  '.json', '.md', '.yaml', '.yml',
  '.py', '.go', '.rs', '.java', '.kt', '.swift',
  '.rb', '.php', '.sh', '.zsh', '.bash',
  '.html', '.css', '.scss', '.sass',
  '.sql', '.toml', '.ini'
]);
const CODEBASE_SKIP_DIRS = new Set([
  'node_modules',
  '.git',
  '.next',
  'dist',
  'build',
  'out',
  'coverage',
  '.turbo'
]);

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

let CHANGE_STATE_CACHE = {
  ts: 0,
  payload: null
};

let EVOLUTION_CACHE = {
  ts: 0,
  payload: null
};

let FRACTAL_CACHE = {
  ts: 0,
  payload: null
};

let CONTINUUM_CACHE = {
  ts: 0,
  payload: null
};

let PUSH_TRANSITION_STATE = {
  last_ahead_count: null,
  just_pushed_until_ms: 0,
  last_push_ts: ''
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

function safeJsonRead(filePath, fallback = null) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const parsed = JSON.parse(String(fs.readFileSync(filePath, 'utf8') || ''));
    return parsed == null ? fallback : parsed;
  } catch {
    return fallback;
  }
}

function readJsonlRows(filePath) {
  if (!fs.existsSync(filePath)) return [];
  const rows = [];
  const lines = String(fs.readFileSync(filePath, 'utf8') || '').split('\n');
  for (const line of lines) {
    const row = safeJsonParse(String(line || '').trim());
    if (row && typeof row === 'object') rows.push(row);
  }
  return rows;
}

function parseTsMs(ts) {
  const ms = Date.parse(String(ts || ''));
  return Number.isFinite(ms) ? ms : null;
}

function normalizeRelPath(raw) {
  return String(raw || '')
    .trim()
    .replace(/\\/g, '/')
    .replace(/^\.\//, '')
    .replace(/\/{2,}/g, '/');
}

function runCmd(cmd, args, timeoutMs = GIT_CMD_TIMEOUT_MS) {
  const result = spawnSync(String(cmd || ''), Array.isArray(args) ? args : [], {
    cwd: REPO_ROOT,
    encoding: 'utf8',
    timeout: Math.max(200, Number(timeoutMs || GIT_CMD_TIMEOUT_MS))
  });
  return {
    ok: result.status === 0,
    code: result.status == null ? 1 : result.status,
    stdout: String(result.stdout || ''),
    stderr: String(result.stderr || ''),
    error: result.error ? String(result.error && result.error.message ? result.error.message : result.error) : ''
  };
}

function parsePathLines(raw, limit = 5000) {
  const lines = String(raw || '').split('\n');
  const out = [];
  for (const line of lines) {
    if (out.length >= limit) break;
    const rel = normalizeRelPath(line);
    if (!rel) continue;
    out.push(rel);
  }
  return Array.from(new Set(out));
}

function parseGitStatusPorcelain(raw) {
  const staged = new Set();
  const dirty = new Set();
  const lines = String(raw || '').split('\n');
  for (const line of lines) {
    const row = String(line || '');
    if (!row.trim() || row.length < 3) continue;
    const x = row[0];
    const y = row[1];
    let rel = normalizeRelPath(row.slice(3));
    if (!rel) continue;
    if (rel.includes(' -> ')) {
      const parts = rel.split(' -> ').map((p) => normalizeRelPath(p)).filter(Boolean);
      rel = parts.length ? parts[parts.length - 1] : rel;
    }
    const stagedFlag = x && x !== ' ' && x !== '?';
    const dirtyFlag = y && y !== ' ';
    const untrackedFlag = x === '?' && y === '?';
    if (stagedFlag) staged.add(rel);
    if (dirtyFlag || untrackedFlag) dirty.add(rel);
  }
  return {
    staged: Array.from(staged),
    dirty: Array.from(dirty)
  };
}

function gitAheadInfo() {
  const upstreamRes = runCmd('git', ['rev-parse', '--abbrev-ref', '--symbolic-full-name', '@{upstream}']);
  if (!upstreamRes.ok) {
    return {
      has_upstream: false,
      upstream: '',
      ahead_count: 0
    };
  }
  const upstream = String(upstreamRes.stdout || '').trim();
  if (!upstream) {
    return {
      has_upstream: false,
      upstream: '',
      ahead_count: 0
    };
  }
  const aheadRes = runCmd('git', ['rev-list', '--count', `${upstream}..HEAD`]);
  const ahead = aheadRes.ok ? Number(String(aheadRes.stdout || '').trim()) : 0;
  return {
    has_upstream: true,
    upstream,
    ahead_count: Number.isFinite(ahead) && ahead > 0 ? Math.round(ahead) : 0
  };
}

function gitLastCommitInfo() {
  const res = runCmd('git', ['log', '-1', '--name-only', '--pretty=format:%ct']);
  if (!res.ok) {
    return {
      epoch_s: null,
      ts: '',
      files: []
    };
  }
  const lines = String(res.stdout || '')
    .split('\n')
    .map((row) => String(row || '').trim())
    .filter(Boolean);
  if (!lines.length) {
    return {
      epoch_s: null,
      ts: '',
      files: []
    };
  }
  const epoch = Number(lines[0]);
  const epochS = Number.isFinite(epoch) && epoch > 0 ? Math.round(epoch) : null;
  return {
    epoch_s: epochS,
    ts: epochS ? new Date(epochS * 1000).toISOString() : '',
    files: Array.from(new Set(lines.slice(1).map((row) => normalizeRelPath(row)).filter(Boolean)))
  };
}

function collectRecentWriteFiles(rows, windowMs = ACTIVE_WRITE_WINDOW_MS) {
  const files = Array.isArray(rows) ? rows : [];
  const nowMs = Date.now();
  const out = [];
  for (const relRaw of files) {
    const rel = normalizeRelPath(relRaw);
    if (!rel) continue;
    const abs = path.join(REPO_ROOT, rel);
    try {
      const stat = fs.statSync(abs);
      const mtimeMs = Number(stat.mtimeMs || 0);
      if (!Number.isFinite(mtimeMs) || mtimeMs <= 0) continue;
      if ((nowMs - mtimeMs) <= Math.max(1000, Number(windowMs || ACTIVE_WRITE_WINDOW_MS))) {
        out.push(rel);
      }
    } catch {
      // ignore missing files (deleted/renamed)
    }
  }
  return Array.from(new Set(out));
}

function relMatchesPath(relPath, filePath) {
  const rel = normalizeRelPath(relPath).toLowerCase();
  const file = normalizeRelPath(filePath).toLowerCase();
  if (!rel || !file) return false;
  if (file === rel) return true;
  if (file.startsWith(`${rel}/`)) return true;
  if (rel.startsWith(`${file}/`)) return true;
  return false;
}

function collectMatchingFiles(files, relPath, limit = MAX_CHANGE_FILES) {
  const src = Array.isArray(files) ? files : [];
  const out = [];
  for (const file of src) {
    if (out.length >= limit) break;
    if (!relMatchesPath(relPath, file)) continue;
    out.push(normalizeRelPath(file));
  }
  return out;
}

function buildNodeChangeState(relPath, sets, options = {}) {
  const dirtyFiles = collectMatchingFiles(sets.dirty, relPath, MAX_CHANGE_FILES);
  const stagedFiles = collectMatchingFiles(sets.staged, relPath, MAX_CHANGE_FILES);
  const activeWriteFiles = collectMatchingFiles(sets.active_write, relPath, MAX_CHANGE_FILES);
  const pendingPushFiles = collectMatchingFiles(sets.pending_push, relPath, MAX_CHANGE_FILES);
  const justPushedFiles = collectMatchingFiles(sets.just_pushed, relPath, MAX_CHANGE_FILES);
  const files = Array.from(new Set([
    ...activeWriteFiles,
    ...dirtyFiles,
    ...stagedFiles,
    ...pendingPushFiles,
    ...justPushedFiles
  ])).slice(0, MAX_CHANGE_FILES);
  const activeWrite = activeWriteFiles.length > 0;
  const dirty = dirtyFiles.length > 0;
  const staged = stagedFiles.length > 0;
  const pendingPush = pendingPushFiles.length > 0;
  const justPushed = justPushedFiles.length > 0 && !pendingPush;
  const changed = activeWrite || dirty || staged || pendingPush || justPushed;
  return {
    active_write: activeWrite,
    dirty,
    staged,
    pending_push: pendingPush,
    just_pushed: justPushed,
    changed,
    file_count: files.length,
    dirty_file_count: dirtyFiles.length,
    staged_file_count: stagedFiles.length,
    pending_push_file_count: pendingPushFiles.length,
    active_write_file_count: activeWriteFiles.length,
    top_files: files,
    last_push_ts: String(options.last_push_ts || '')
  };
}

function listJsonlFilesDesc(absDir) {
  if (!fs.existsSync(absDir)) return [];
  return fs.readdirSync(absDir)
    .filter((f) => /^\d{4}-\d{2}-\d{2}\.jsonl$/.test(f))
    .sort()
    .reverse();
}

function listJsonFilesDesc(absDir) {
  if (!fs.existsSync(absDir)) return [];
  return fs.readdirSync(absDir)
    .filter((f) => /^\d{4}-\d{2}-\d{2}(?:\.\d+)?\.json$/.test(f))
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
        objective_id: String(evt.objective_id || ''),
        violation_counts: evt && typeof evt.violation_counts === 'object'
          ? evt.violation_counts
          : {}
      });
      if (events.length >= cap) return events;
    }
  }
  return events;
}

function latestDatedJson(absDir) {
  const files = listJsonFilesDesc(absDir);
  if (!files.length) return null;
  const file = files[0];
  const rel = path.relative(REPO_ROOT, path.join(absDir, file)).replace(/\\/g, '/');
  const payload = safeJsonRead(path.join(absDir, file), null);
  if (!payload || typeof payload !== 'object') return null;
  return {
    file,
    rel,
    payload
  };
}

function loadFractalSnapshot() {
  const nowMs = Date.now();
  if (
    FRACTAL_CACHE.payload
    && (nowMs - Number(FRACTAL_CACHE.ts || 0)) < FRACTAL_CACHE_TTL_MS
  ) {
    return cloneJson(FRACTAL_CACHE.payload);
  }

  const organism = latestDatedJson(FRACTAL_ORGANISM_DIR);
  const introspection = latestDatedJson(FRACTAL_INTROSPECTION_DIR);
  const pheromones = latestDatedJson(FRACTAL_PHEROMONE_DIR);
  const epigenetic = safeJsonRead(FRACTAL_EPIGENETIC_PATH, null);
  const archetypes = safeJsonRead(FRACTAL_ARCHETYPE_PATH, null);
  const genomeRows = readJsonlRows(GENOME_JOURNAL_PATH);
  const genomeLast = genomeRows.length ? genomeRows[genomeRows.length - 1] : null;
  const blackBoxRows = readJsonlRows(BLACK_BOX_CHAIN_PATH);
  const blackBoxLast = blackBoxRows.length ? blackBoxRows[blackBoxRows.length - 1] : null;

  const organismPayload = organism && organism.payload && typeof organism.payload === 'object'
    ? organism.payload
    : {};
  const introspectionPayload = introspection && introspection.payload && typeof introspection.payload === 'object'
    ? introspection.payload
    : {};
  const pheromonePayload = pheromones && pheromones.payload && typeof pheromones.payload === 'object'
    ? pheromones.payload
    : {};
  const epigeneticTags = epigenetic && epigenetic.tags && typeof epigenetic.tags === 'object'
    ? Object.keys(epigenetic.tags).length
    : 0;
  const archetypeCount = archetypes && Array.isArray(archetypes.archetypes)
    ? archetypes.archetypes.length
    : 0;
  const symbiosisPlans = Array.isArray(organismPayload.symbiosis_plans)
    ? organismPayload.symbiosis_plans.length
    : 0;
  const predatorCandidates = organismPayload.predator_prey && Array.isArray(organismPayload.predator_prey.candidates)
    ? organismPayload.predator_prey.candidates.length
    : 0;
  const harmonyScore = Number(organismPayload.resonance && organismPayload.resonance.score);
  const restructureCandidates = Array.isArray(introspectionPayload.restructure_candidates)
    ? introspectionPayload.restructure_candidates.length
    : 0;
  const pheromoneCount = Array.isArray(pheromonePayload.packets)
    ? pheromonePayload.packets.length
    : 0;

  const payload = {
    generated_at: nowIso(),
    organism: {
      date: organism ? String(organism.file || '').slice(0, 10) : '',
      file: organism ? organism.rel : '',
      harmony_score: Number.isFinite(harmonyScore) ? harmonyScore : 0,
      symbiosis_plans: symbiosisPlans,
      predator_candidates: predatorCandidates
    },
    introspection: {
      date: introspection ? String(introspection.file || '').slice(0, 10) : '',
      file: introspection ? introspection.rel : '',
      restructure_candidates: restructureCandidates
    },
    pheromones: {
      date: pheromones ? String(pheromones.file || '').slice(0, 10) : '',
      file: pheromones ? pheromones.rel : '',
      packets: pheromoneCount
    },
    epigenetic: {
      file: fs.existsSync(FRACTAL_EPIGENETIC_PATH) ? path.relative(REPO_ROOT, FRACTAL_EPIGENETIC_PATH).replace(/\\/g, '/') : '',
      tags: epigeneticTags
    },
    archetypes: {
      file: fs.existsSync(FRACTAL_ARCHETYPE_PATH) ? path.relative(REPO_ROOT, FRACTAL_ARCHETYPE_PATH).replace(/\\/g, '/') : '',
      count: archetypeCount
    },
    genome: {
      rows: genomeRows.length,
      last_hash: genomeLast ? String(genomeLast.hash || '') : '',
      last_date: genomeLast ? String(genomeLast.date || '') : '',
      last_plan_id: genomeLast ? String(genomeLast.plan_id || '') : ''
    },
    black_box: {
      rows: blackBoxRows.length,
      last_hash: blackBoxLast ? String(blackBoxLast.hash || '') : '',
      last_date: blackBoxLast ? String(blackBoxLast.date || '') : '',
      last_mode: blackBoxLast ? String(blackBoxLast.mode || '') : ''
    }
  };

  FRACTAL_CACHE = {
    ts: nowMs,
    payload
  };
  return cloneJson(payload);
}

function listJsonlFilesDesc(absDir) {
  if (!fs.existsSync(absDir)) return [];
  const ents = fs.readdirSync(absDir, { withFileTypes: true });
  const rows = [];
  for (const ent of ents) {
    if (!ent || !ent.isFile()) continue;
    const name = String(ent.name || '');
    if (!name.endsWith('.jsonl')) continue;
    rows.push(name);
  }
  rows.sort((a, b) => b.localeCompare(a));
  return rows;
}

function loadContinuumSnapshot() {
  const nowMs = Date.now();
  if (
    CONTINUUM_CACHE.payload
    && (nowMs - Number(CONTINUUM_CACHE.ts || 0)) < CONTINUUM_CACHE_TTL_MS
  ) {
    return cloneJson(CONTINUUM_CACHE.payload);
  }

  const latest = safeJsonRead(CONTINUUM_LATEST_PATH, null);
  const latestPayload = latest && typeof latest === 'object' ? latest : {};
  const latestTs = String(latestPayload.ts || '');
  const latestDate = String(latestPayload.date || '');
  const latestTrit = latestPayload.trit && typeof latestPayload.trit === 'object'
    ? latestPayload.trit
    : {};
  const actions = Array.isArray(latestPayload.actions) ? latestPayload.actions : [];
  const actionById = {};
  for (const row of actions) {
    const id = String(row && row.id || '').trim();
    if (!id) continue;
    actionById[id] = row;
  }

  const cutoffMs = nowMs - (24 * 60 * 60 * 1000);
  const eventFiles = listJsonlFilesDesc(CONTINUUM_EVENTS_DIR).slice(0, 3);
  const byStage = {
    dream_consolidation: 0,
    anticipation: 0,
    self_improvement: 0,
    creative_incubation: 0,
    security_vigilance: 0,
    consolidation: 0
  };
  let events24h = 0;
  for (const file of eventFiles) {
    const rows = readJsonlRows(path.join(CONTINUUM_EVENTS_DIR, file));
    for (let i = rows.length - 1; i >= 0; i -= 1) {
      const row = rows[i];
      const tsMs = parseTsMs(row && row.ts);
      if (tsMs != null && tsMs < cutoffMs) break;
      const stage = String(row && row.stage || '').trim();
      if (!stage) continue;
      if (Object.prototype.hasOwnProperty.call(byStage, stage)) {
        byStage[stage] = Number(byStage[stage] || 0) + 1;
      }
      events24h += 1;
    }
  }

  const historyRows = readJsonlRows(CONTINUUM_HISTORY_PATH);
  let queueRows24h = 0;
  for (let i = historyRows.length - 1; i >= 0; i -= 1) {
    const row = historyRows[i];
    const tsMs = parseTsMs(row && row.ts);
    if (tsMs != null && tsMs < cutoffMs) break;
    queueRows24h += Number(row && row.training_queue_rows || 0);
  }
  const pulseAgeSec = latestTs
    ? Number(Math.max(0, (nowMs - Date.parse(latestTs)) / 1000).toFixed(2))
    : null;

  const anticipationAction = actionById.anticipation && typeof actionById.anticipation === 'object'
    ? actionById.anticipation
    : {};
  const securityAction = actionById.security_vigilance && typeof actionById.security_vigilance === 'object'
    ? actionById.security_vigilance
    : {};
  const selfImproveAction = actionById.self_improvement && typeof actionById.self_improvement === 'object'
    ? actionById.self_improvement
    : {};
  const lastSkipReasons = Array.isArray(latestPayload.skip_reasons) ? latestPayload.skip_reasons.slice(0, 6) : [];

  const payload = {
    generated_at: nowIso(),
    available: !!latest && typeof latest === 'object',
    file: fs.existsSync(CONTINUUM_LATEST_PATH) ? path.relative(REPO_ROOT, CONTINUUM_LATEST_PATH).replace(/\\/g, '/') : '',
    state_dir: fs.existsSync(CONTINUUM_DIR) ? path.relative(REPO_ROOT, CONTINUUM_DIR).replace(/\\/g, '/') : '',
    last_pulse_ts: latestTs || '',
    last_date: latestDate || '',
    pulse_age_sec: Number.isFinite(Number(pulseAgeSec)) ? pulseAgeSec : null,
    last_profile: String(latestPayload.profile || ''),
    last_trit: Number(latestTrit.value || 0),
    last_trit_label: String(latestTrit.label || ''),
    last_skipped: latestPayload.skipped === true,
    last_skip_reasons: lastSkipReasons,
    tasks_executed_last: Number(latestPayload.tasks_executed || 0),
    events_24h_total: events24h,
    events_24h_by_stage: byStage,
    training_queue_rows_24h: queueRows24h,
    anticipation_drafts_last: Number(anticipationAction.metrics && anticipationAction.metrics.drafts || 0),
    anticipation_candidates_last: Number(anticipationAction.metrics && anticipationAction.metrics.candidates || 0),
    red_team_cases_last: Number(securityAction.metrics && securityAction.metrics.executed_cases || 0),
    red_team_critical_last: Number(securityAction.metrics && securityAction.metrics.critical_fail_cases || 0),
    observer_mood_last: String(selfImproveAction.metrics && selfImproveAction.metrics.mood || ''),
    history_rows: historyRows.length
  };

  CONTINUUM_CACHE = {
    ts: nowMs,
    payload
  };
  return cloneJson(payload);
}

function safeCountMap(raw) {
  if (!raw || typeof raw !== 'object') return {};
  const out = {};
  for (const [k, v] of Object.entries(raw)) {
    const key = String(k || '').trim();
    const count = Number(v || 0);
    if (!key || !Number.isFinite(count) || count <= 0) continue;
    out[key] = Math.round(count);
  }
  return out;
}

function sumCountMap(raw) {
  let total = 0;
  for (const count of Object.values(safeCountMap(raw))) {
    total += Number(count || 0);
  }
  return total;
}

function compactIntegrityViolations(violations, limit = MAX_INTEGRITY_FILES) {
  const rows = Array.isArray(violations) ? violations : [];
  const out = [];
  for (const row of rows) {
    if (out.length >= limit) break;
    const type = String(row && row.type || 'unknown').trim() || 'unknown';
    const file = String(row && row.file || '').trim();
    const detail = String(row && row.detail || '').trim();
    out.push({
      type,
      file,
      detail: detail ? detail.slice(0, 160) : ''
    });
  }
  return out;
}

function loadRecentIntegrityEvents(hours = DEFAULT_HOURS, maxEvents = MAX_INTEGRITY_EVENTS) {
  if (!fs.existsSync(INTEGRITY_LOG_PATH)) return [];
  const h = clampNumber(hours, 1, 24 * 30, DEFAULT_HOURS);
  const cap = clampNumber(maxEvents, 1, 64, MAX_INTEGRITY_EVENTS);
  const cutoffMs = Date.now() - (h * 60 * 60 * 1000);
  const events = [];
  const lines = String(fs.readFileSync(INTEGRITY_LOG_PATH, 'utf8') || '').split('\n');
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const line = String(lines[i] || '').trim();
    if (!line) continue;
    const evt = safeJsonParse(line);
    if (!evt || typeof evt !== 'object') continue;
    const type = String(evt.type || '').trim();
    if (type !== 'integrity_violation_block' && type !== 'integrity_reseal_apply') continue;
    const ms = parseTsMs(evt.ts);
    if (ms != null && ms < cutoffMs) break;
    events.push({
      ts: String(evt.ts || ''),
      type,
      policy_version: String(evt.policy_version || ''),
      policy_path: String(evt.policy_path || ''),
      violation_counts: safeCountMap(evt.violation_counts),
      violations: compactIntegrityViolations(evt.violations, Math.min(6, MAX_INTEGRITY_FILES)),
      verify_ok_after: evt.verify_ok_after === true
    });
    if (events.length >= cap) break;
  }
  return events;
}

function loadIntegrityStatus(hours = DEFAULT_HOURS) {
  const recentEvents = loadRecentIntegrityEvents(hours, MAX_INTEGRITY_EVENTS);
  const latestViolation = recentEvents.find((evt) => evt && evt.type === 'integrity_violation_block') || null;
  const latestReseal = recentEvents.find((evt) => evt && evt.type === 'integrity_reseal_apply' && evt.verify_ok_after === true) || null;
  const fallback = {
    available: false,
    ok: null,
    active_alert: false,
    severity: latestViolation ? 'warning' : 'ok',
    policy_path: latestViolation ? String(latestViolation.policy_path || '') : '',
    policy_version: latestViolation ? String(latestViolation.policy_version || '') : '',
    checked_present_files: null,
    expected_files: null,
    violation_total: latestViolation ? sumCountMap(latestViolation.violation_counts) : 0,
    violation_counts: latestViolation ? safeCountMap(latestViolation.violation_counts) : {},
    top_files: latestViolation
      ? compactIntegrityViolations(latestViolation.violations, MAX_INTEGRITY_FILES).map((row) => String(row.file || '')).filter(Boolean)
      : [],
    violations: latestViolation ? compactIntegrityViolations(latestViolation.violations, MAX_INTEGRITY_FILES) : [],
    last_violation_ts: latestViolation ? String(latestViolation.ts || '') : '',
    last_reseal_ts: latestReseal ? String(latestReseal.ts || '') : '',
    recent_events: recentEvents
  };
  try {
    const { verifyIntegrity } = require(path.join(REPO_ROOT, 'lib', 'security_integrity'));
    const policyPath = String(process.env.SPINE_INTEGRITY_POLICY || INTEGRITY_POLICY_PATH).trim() || INTEGRITY_POLICY_PATH;
    const verify = verifyIntegrity(policyPath);
    const violationCounts = safeCountMap(verify && verify.violation_counts);
    const violations = compactIntegrityViolations(verify && verify.violations, MAX_INTEGRITY_FILES);
    const topFiles = violations
      .map((row) => String(row.file || '').trim())
      .filter(Boolean)
      .slice(0, MAX_INTEGRITY_FILES);
    const activeAlert = !(verify && verify.ok === true);
    return {
      available: true,
      ok: verify && verify.ok === true,
      active_alert: activeAlert,
      severity: activeAlert ? 'critical' : (latestViolation ? 'recent' : 'ok'),
      policy_path: String(verify && verify.policy_path || policyPath),
      policy_version: String(verify && verify.policy_version || ''),
      checked_present_files: Number(verify && verify.checked_present_files || 0),
      expected_files: Number(verify && verify.expected_files || 0),
      violation_total: sumCountMap(violationCounts),
      violation_counts: violationCounts,
      top_files: topFiles,
      violations,
      last_violation_ts: latestViolation ? String(latestViolation.ts || '') : '',
      last_reseal_ts: latestReseal ? String(latestReseal.ts || '') : '',
      recent_events: recentEvents
    };
  } catch {
    const hasLatestViolation = !!latestViolation;
    const latestViolationMs = latestViolation ? Number(parseTsMs(latestViolation.ts)) : null;
    const latestResealMs = latestReseal ? Number(parseTsMs(latestReseal.ts)) : null;
    const resealedAfterViolation = Number.isFinite(latestViolationMs)
      && Number.isFinite(latestResealMs)
      && latestResealMs >= latestViolationMs;
    return {
      ...fallback,
      active_alert: hasLatestViolation && !resealedAfterViolation,
      severity: hasLatestViolation
        ? (resealedAfterViolation ? 'recent' : 'critical')
        : 'ok'
    };
  }
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

function proposalDependenciesFromRun(evt) {
  const dep = evt && evt.proposal_dependencies && typeof evt.proposal_dependencies === 'object'
    ? evt.proposal_dependencies
    : null;
  if (!dep) return null;
  const parentObjectiveId = String(dep.parent_objective_id || '').trim();
  const childObjectiveIds = Array.isArray(dep.child_objective_ids)
    ? dep.child_objective_ids.map((v) => String(v || '').trim()).filter(Boolean).slice(0, 24)
    : [];
  const chain = Array.isArray(dep.chain)
    ? dep.chain.map((v) => String(v || '').trim()).filter(Boolean).slice(0, 24)
    : [];
  const edges = Array.isArray(dep.edges)
    ? dep.edges.map((row) => ({
      from: String(row && row.from || '').trim(),
      to: String(row && row.to || '').trim(),
      relation: String(row && row.relation || 'depends_on').trim()
    })).filter((row) => row.from && row.to)
    : [];
  return {
    parent_objective_id: parentObjectiveId || null,
    child_objective_ids: childObjectiveIds,
    chain,
    edges
  };
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

function eventPacketTokens(evt) {
  const row = evt && typeof evt === 'object' ? evt : {};
  const usage = row.token_usage && typeof row.token_usage === 'object'
    ? row.token_usage
    : {};
  const routeSummary = row.route_summary && typeof row.route_summary === 'object'
    ? row.route_summary
    : {};
  const routeBudget = routeSummary.route_budget && typeof routeSummary.route_budget === 'object'
    ? routeSummary.route_budget
    : {};
  const candidates = [
    usage.effective_tokens,
    usage.actual_total_tokens,
    usage.estimated_tokens,
    routeBudget.request_tokens_est
  ];
  for (const cand of candidates) {
    const n = Number(cand);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return 0;
}

function flowBlockReason(evt) {
  const row = evt && typeof evt === 'object' ? evt : {};
  const routeSummary = row.route_summary && typeof row.route_summary === 'object'
    ? row.route_summary
    : {};
  const routeReason = String(row.route_block_reason || '').trim().toLowerCase();
  if (routeReason) return routeReason;
  const holdReason = String(row.hold_reason || '').trim().toLowerCase();
  if (holdReason) return holdReason;
  const budgetReason = String(routeSummary.budget_block_reason || '').trim().toLowerCase();
  if (budgetReason) return budgetReason;
  const budgetEnforcement = routeSummary.budget_enforcement && typeof routeSummary.budget_enforcement === 'object'
    ? routeSummary.budget_enforcement
    : {};
  const enforcementReason = String(budgetEnforcement.reason || '').trim().toLowerCase();
  if (enforcementReason) return enforcementReason;
  const result = String(row.result || '').trim().toLowerCase();
  if (result) return result;
  return '';
}

function isFlowBlockedEvent(evt) {
  const row = evt && typeof evt === 'object' ? evt : {};
  const result = String(row.result || '').trim().toLowerCase();
  if (result.includes('blocked')) return true;
  if (result.includes('route_block')) return true;
  if (result.startsWith('stop_')) return true;
  if (isPolicyHoldResult(result)) return true;
  if (row.policy_hold === true) return true;
  const routeSummary = row.route_summary && typeof row.route_summary === 'object'
    ? row.route_summary
    : {};
  if (routeSummary.budget_blocked === true) return true;
  if (routeSummary.executable === false) return true;
  if (String(routeSummary.gate_decision || '').trim().toLowerCase() === 'manual') return true;
  if (String(row.route_block_reason || '').trim()) return true;
  return false;
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

function listRenderableChildDirs(absDir, limit = MAX_SUBMODULE_SUBFOLDERS) {
  const entries = safeReadDirWithTypes(absDir)
    .filter((ent) => ent && ent.isDirectory && ent.isDirectory())
    .filter((ent) => isRenderableEntryName(ent && ent.name))
    .sort(direntSort);
  const names = [];
  for (const ent of entries) {
    if (names.length >= Math.max(1, Number(limit || MAX_SUBMODULE_SUBFOLDERS))) break;
    const name = String(ent && ent.name || '').trim();
    if (!name) continue;
    names.push(name);
  }
  return names;
}

function tokenize(text) {
  const raw = String(text || '').toLowerCase();
  return raw
    .split(/[^a-z0-9]+/g)
    .map((s) => s.trim())
    .filter((s) => s.length >= 3 && s !== 'json' && s !== 'node' && s !== 'test');
}

function shouldIncludeCodebaseFile(fileName) {
  const name = String(fileName || '').trim();
  if (!name || name.startsWith('.')) return false;
  const ext = path.extname(name).toLowerCase();
  return CODEBASE_SIZE_EXTS.has(ext);
}

function codebaseSizeForPath(absPath, opts = {}) {
  const maxFiles = Math.max(50, Number(opts.max_files || CODEBASE_SIZE_MAX_FILES));
  const maxDepth = Math.max(1, Number(opts.max_depth || CODEBASE_SIZE_MAX_DEPTH));
  const result = {
    bytes: 0,
    files: 0,
    truncated: false
  };

  if (!absPath || !fs.existsSync(absPath)) return result;

  const stack = [{ abs: absPath, depth: 0 }];
  while (stack.length > 0) {
    const cur = stack.pop();
    if (!cur || result.files >= maxFiles) {
      result.truncated = true;
      break;
    }
    let stat = null;
    try {
      stat = fs.statSync(cur.abs);
    } catch {
      stat = null;
    }
    if (!stat) continue;

    if (stat.isFile()) {
      const base = path.basename(cur.abs);
      if (!shouldIncludeCodebaseFile(base)) continue;
      result.bytes += Math.max(0, Number(stat.size || 0));
      result.files += 1;
      continue;
    }
    if (!stat.isDirectory()) continue;
    if (cur.depth >= maxDepth) {
      result.truncated = true;
      continue;
    }

    const entries = safeReadDirWithTypes(cur.abs);
    for (const ent of entries) {
      const name = String(ent && ent.name || '').trim();
      if (!name || name.startsWith('.')) continue;
      if (ent.isDirectory()) {
        if (CODEBASE_SKIP_DIRS.has(name.toLowerCase())) continue;
      } else if (!ent.isFile()) {
        continue;
      }
      stack.push({
        abs: path.join(cur.abs, name),
        depth: cur.depth + 1
      });
    }
  }
  return result;
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
        submodules: [],
        codebase_size_bytes: 0,
        codebase_file_count: 0,
        codebase_truncated: false
      };
      aliasToId[`${layerDef.key}/${modName}`.toLowerCase()] = modId;
      aliasToId[modName.toLowerCase()] = aliasToId[modName.toLowerCase()] || modId;

      if (ent.isDirectory()) {
        const modAbs = path.join(layerAbs, modName);
        const modSize = codebaseSizeForPath(modAbs);
        moduleNode.codebase_size_bytes = Math.max(0, Number(modSize.bytes || 0));
        moduleNode.codebase_file_count = Math.max(0, Number(modSize.files || 0));
        moduleNode.codebase_truncated = modSize.truncated === true;
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
            activity: 0,
            codebase_size_bytes: 0,
            codebase_file_count: 0,
            subfolders: []
          };
          const subAbs = path.join(modAbs, subName);
          if (sub.isDirectory()) {
            const childDirNames = listRenderableChildDirs(subAbs, MAX_SUBMODULE_SUBFOLDERS);
            subNode.subfolders = childDirNames.map((childName) => ({
              name: childName,
              rel: `${modRel}/${subName}/${childName}`
            }));
          }
          if (sub.isFile()) {
            try {
              const stat = fs.statSync(subAbs);
              if (stat && stat.isFile() && shouldIncludeCodebaseFile(subName)) {
                subNode.codebase_size_bytes = Math.max(0, Number(stat.size || 0));
                subNode.codebase_file_count = 1;
              }
            } catch {
              // ignore stat errors for sidecar sizing metadata
            }
          }
          moduleNode.submodules.push(subNode);
          aliasToId[`${layerDef.key}/${modName}/${subName}`.toLowerCase()] = subId;
        }
      } else {
        const modAbs = path.join(layerAbs, modName);
        try {
          const stat = fs.statSync(modAbs);
          if (stat && stat.isFile() && shouldIncludeCodebaseFile(modName)) {
            moduleNode.codebase_size_bytes = Math.max(0, Number(stat.size || 0));
            moduleNode.codebase_file_count = 1;
          }
        } catch {
          // ignore stat errors for sidecar sizing metadata
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

function loadChangeStateSnapshot(layers) {
  const nowMs = Date.now();
  if (
    CHANGE_STATE_CACHE.payload
    && (nowMs - Number(CHANGE_STATE_CACHE.ts || 0)) < CHANGE_STATE_CACHE_TTL_MS
  ) {
    return cloneJson(CHANGE_STATE_CACHE.payload);
  }
  const statusRes = runCmd('git', ['status', '--porcelain=v1', '-uall']);
  const parsedStatus = statusRes.ok
    ? parseGitStatusPorcelain(statusRes.stdout)
    : { staged: [], dirty: [] };
  const aheadInfo = gitAheadInfo();
  const pendingPushFiles = (aheadInfo.has_upstream && aheadInfo.upstream && aheadInfo.ahead_count > 0)
    ? parsePathLines(runCmd('git', ['diff', '--name-only', `${aheadInfo.upstream}..HEAD`]).stdout)
    : [];
  const lastCommit = gitLastCommitInfo();

  if (
    Number.isFinite(Number(PUSH_TRANSITION_STATE.last_ahead_count))
    && Number(PUSH_TRANSITION_STATE.last_ahead_count) > 0
    && aheadInfo.ahead_count === 0
  ) {
    PUSH_TRANSITION_STATE.just_pushed_until_ms = nowMs + JUST_PUSHED_WINDOW_MS;
    PUSH_TRANSITION_STATE.last_push_ts = nowIso();
  }
  if (!Number.isFinite(Number(PUSH_TRANSITION_STATE.last_ahead_count))) {
    PUSH_TRANSITION_STATE.last_ahead_count = aheadInfo.ahead_count;
  } else {
    PUSH_TRANSITION_STATE.last_ahead_count = aheadInfo.ahead_count;
  }
  const justPushedActive = nowMs <= Number(PUSH_TRANSITION_STATE.just_pushed_until_ms || 0);
  const justPushedFiles = justPushedActive ? lastCommit.files : [];

  const fileUniverse = Array.from(new Set([
    ...parsedStatus.staged,
    ...parsedStatus.dirty,
    ...pendingPushFiles,
    ...justPushedFiles
  ]));
  const recentWriteFiles = collectRecentWriteFiles(fileUniverse, ACTIVE_WRITE_WINDOW_MS);
  const sets = {
    staged: parsedStatus.staged,
    dirty: parsedStatus.dirty,
    pending_push: pendingPushFiles,
    active_write: recentWriteFiles,
    just_pushed: justPushedFiles
  };

  const moduleByRel = {};
  const submoduleByRel = {};
  let activeModules = 0;
  let activeSubmodules = 0;
  for (const layer of layers || []) {
    for (const mod of layer.modules || []) {
      const modRel = normalizeRelPath(mod && mod.rel || '');
      if (!modRel) continue;
      const modState = buildNodeChangeState(modRel, sets, {
        last_push_ts: PUSH_TRANSITION_STATE.last_push_ts
      });
      moduleByRel[modRel] = modState;
      if (modState.changed) activeModules += 1;
      for (const sub of mod.submodules || []) {
        const subRel = normalizeRelPath(sub && sub.rel || '');
        if (!subRel) continue;
        const subState = buildNodeChangeState(subRel, sets, {
          last_push_ts: PUSH_TRANSITION_STATE.last_push_ts
        });
        submoduleByRel[subRel] = subState;
        if (subState.changed) activeSubmodules += 1;
      }
    }
  }
  const summary = {
    dirty_files_total: Number(parsedStatus.dirty.length || 0),
    staged_files_total: Number(parsedStatus.staged.length || 0),
    pending_push_files_total: Number(pendingPushFiles.length || 0),
    active_write_files_total: Number(recentWriteFiles.length || 0),
    ahead_count: Number(aheadInfo.ahead_count || 0),
    pending_push: Number(aheadInfo.ahead_count || 0) > 0,
    just_pushed: justPushedActive,
    active_modules: activeModules,
    active_submodules: activeSubmodules,
    top_files: fileUniverse.slice(0, MAX_CHANGE_FILES),
    has_upstream: aheadInfo.has_upstream === true,
    upstream: String(aheadInfo.upstream || ''),
    last_push_ts: String(PUSH_TRANSITION_STATE.last_push_ts || ''),
    last_commit_ts: String(lastCommit.ts || '')
  };
  const payload = {
    generated_at: nowIso(),
    summary,
    module_by_rel: moduleByRel,
    submodule_by_rel: submoduleByRel
  };
  CHANGE_STATE_CACHE = {
    ts: nowMs,
    payload
  };
  return cloneJson(payload);
}

function applyChangeStateToLayers(layers, changeSnapshot) {
  const moduleByRel = changeSnapshot && changeSnapshot.module_by_rel && typeof changeSnapshot.module_by_rel === 'object'
    ? changeSnapshot.module_by_rel
    : {};
  const submoduleByRel = changeSnapshot && changeSnapshot.submodule_by_rel && typeof changeSnapshot.submodule_by_rel === 'object'
    ? changeSnapshot.submodule_by_rel
    : {};
  for (const layer of layers || []) {
    for (const mod of layer.modules || []) {
      const modRel = normalizeRelPath(mod && mod.rel || '');
      mod.change_state = modRel && moduleByRel[modRel]
        ? moduleByRel[modRel]
        : {
            active_write: false,
            dirty: false,
            staged: false,
            pending_push: false,
            just_pushed: false,
            changed: false,
            file_count: 0,
            dirty_file_count: 0,
            staged_file_count: 0,
            pending_push_file_count: 0,
            active_write_file_count: 0,
            top_files: [],
            last_push_ts: ''
          };
      for (const sub of mod.submodules || []) {
        const subRel = normalizeRelPath(sub && sub.rel || '');
        sub.change_state = subRel && submoduleByRel[subRel]
          ? submoduleByRel[subRel]
          : {
              active_write: false,
              dirty: false,
              staged: false,
              pending_push: false,
              just_pushed: false,
              changed: false,
              file_count: 0,
              dirty_file_count: 0,
              staged_file_count: 0,
              pending_push_file_count: 0,
              active_write_file_count: 0,
              top_files: [],
              last_push_ts: ''
            };
      }
    }
  }
  return layers;
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

function parsePositiveInt(raw, fallback = 0) {
  const n = Number(String(raw || '').trim());
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.round(n);
}

function gitCountSince(sinceExpr) {
  const since = String(sinceExpr || '').trim();
  if (!since) return 0;
  const res = runCmd('git', ['rev-list', '--count', `--since=${since}`, 'HEAD'], 2400);
  if (!res.ok) return 0;
  return parsePositiveInt(res.stdout, 0);
}

function gitChurnSince(sinceExpr) {
  const since = String(sinceExpr || '').trim();
  if (!since) return { added: 0, deleted: 0 };
  const res = runCmd('git', ['log', `--since=${since}`, '--numstat', '--pretty=tformat:'], 2800);
  if (!res.ok) return { added: 0, deleted: 0 };
  let added = 0;
  let deleted = 0;
  const lines = String(res.stdout || '').split('\n');
  for (const line of lines) {
    const row = String(line || '').trim();
    if (!row) continue;
    const m = row.match(/^(\d+|-)\s+(\d+|-)\s+/);
    if (!m) continue;
    const a = Number(m[1]);
    const d = Number(m[2]);
    if (Number.isFinite(a) && a > 0) added += Math.round(a);
    if (Number.isFinite(d) && d > 0) deleted += Math.round(d);
  }
  return { added, deleted };
}

function gitPathVersion(relPath) {
  const rel = normalizeRelPath(relPath);
  if (!rel) {
    return { path: relPath || '', commit: '', ts: '', age_days: null };
  }
  const res = runCmd('git', ['log', '-1', '--format=%h|%ct', '--', rel], 2200);
  if (!res.ok) {
    return { path: rel, commit: '', ts: '', age_days: null };
  }
  const [commitRaw, epochRaw] = String(res.stdout || '').trim().split('|');
  const commit = String(commitRaw || '').trim();
  const epoch = Number(epochRaw || 0);
  const ts = Number.isFinite(epoch) && epoch > 0 ? new Date(epoch * 1000).toISOString() : '';
  const ageDays = Number.isFinite(epoch) && epoch > 0
    ? Number((((Date.now() / 1000) - epoch) / 86400).toFixed(2))
    : null;
  return {
    path: rel,
    commit,
    ts,
    age_days: ageDays
  };
}

function loadEvolutionSnapshot() {
  const nowMs = Date.now();
  if (
    EVOLUTION_CACHE.payload
    && (nowMs - Number(EVOLUTION_CACHE.ts || 0)) < EVOLUTION_CACHE_TTL_MS
  ) {
    return cloneJson(EVOLUTION_CACHE.payload);
  }
  const commits7d = gitCountSince('7 days ago');
  const commits30d = gitCountSince('30 days ago');
  const commits90d = gitCountSince('90 days ago');
  const churn30d = gitChurnSince('30 days ago');
  const totalChurn30d = Number(churn30d.added || 0) + Number(churn30d.deleted || 0);
  const velocity30d = Number((commits30d / 30).toFixed(3));
  const churnPerCommit = commits30d > 0 ? totalChurn30d / commits30d : totalChurn30d;
  const stabilityScore = Number(clampNumber(1 - (churnPerCommit / 1800), 0, 1, 0.5).toFixed(3));
  const trajectory = commits30d <= 0
    ? 'flat'
    : (
      commits7d >= Math.max(1, Math.round((commits30d / 30) * 7 * 1.15))
        ? 'accelerating'
        : (
          commits7d <= Math.max(0, Math.round((commits30d / 30) * 7 * 0.7))
            ? 'cooling'
            : 'steady'
        )
    );
  const components = {
    spine: gitPathVersion('systems/spine/spine.ts'),
    autonomy: gitPathVersion('systems/autonomy/autonomy_controller.ts'),
    skills: gitPathVersion('skills')
  };
  const payload = {
    generated_at: nowIso(),
    commits_7d: commits7d,
    commits_30d: commits30d,
    commits_90d: commits90d,
    lines_added_30d: Number(churn30d.added || 0),
    lines_deleted_30d: Number(churn30d.deleted || 0),
    churn_30d: totalChurn30d,
    commit_velocity_30d: velocity30d,
    stability_score: stabilityScore,
    trajectory,
    components
  };
  EVOLUTION_CACHE = {
    ts: nowMs,
    payload
  };
  return cloneJson(payload);
}

function directiveTier(id, directivesById) {
  const objectiveId = String(id || '').trim();
  if (!objectiveId) return null;
  const byId = directivesById && typeof directivesById === 'object' ? directivesById : {};
  const fromCatalog = byId[objectiveId] && Number.isFinite(Number(byId[objectiveId].tier))
    ? Number(byId[objectiveId].tier)
    : null;
  if (fromCatalog != null) return fromCatalog;
  if (/^T1[_:]/i.test(objectiveId)) return 1;
  if (/^T2[_:]/i.test(objectiveId)) return 2;
  return null;
}

function objectiveAlignmentScore(evt, directivesById) {
  const row = evt && typeof evt === 'object' ? evt : {};
  const result = String(row.result || '').trim().toLowerCase();
  const binding = row.objective_binding && typeof row.objective_binding === 'object'
    ? row.objective_binding
    : {};
  const pulse = row.directive_pulse && typeof row.directive_pulse === 'object'
    ? row.directive_pulse
    : {};
  const objectiveId = objectiveIdFromRun(row);
  const tier = directiveTier(objectiveId, directivesById);
  const pulseScore = Number(pulse.objective_allocation_score || 0);
  const bindingPass = binding.pass !== false && String(binding.objective_id || objectiveId || '').trim() !== '';
  let score = 0.42;
  if (bindingPass) score += 0.22;
  else if (String(result).startsWith('stop_init_gate_objective_binding')) score -= 0.24;
  if (tier === 1) score += 0.18;
  else if (tier === 2) score += 0.12;
  if (Number.isFinite(pulseScore) && pulseScore > 0) {
    score += clampNumber((pulseScore / 100) * 0.16, 0, 0.16, 0);
  }
  if (row.policy_hold === true || isPolicyHoldResult(result)) score -= 0.1;
  if (result.includes('reverted')) score -= 0.08;
  const bounded = Number(clampNumber(score, 0, 1, 0.5).toFixed(4));
  const band = bounded >= 0.66 ? 'green' : (bounded <= 0.4 ? 'red' : 'gray');
  return {
    score: bounded,
    band,
    objective_id: objectiveId || null,
    tier
  };
}

function buildConstitutionSnapshot(runs, directives, strategy) {
  const rows = Array.isArray(runs) ? runs : [];
  const directivesRows = Array.isArray(directives) ? directives : [];
  const directivesById = {};
  let tier1Count = 0;
  let tier2Count = 0;
  for (const d of directivesRows) {
    const id = String(d && d.id || '').trim();
    if (!id) continue;
    const tier = Number(d && d.tier || 99);
    directivesById[id] = {
      id,
      tier
    };
    if (tier === 1) tier1Count += 1;
    else if (tier === 2) tier2Count += 1;
  }
  const latestByProposal = {};
  for (const evt of rows) {
    const pid = String(evt && evt.proposal_id || '').trim();
    if (!pid) continue;
    const ts = parseTsMs(evt && evt.ts) || 0;
    if (!latestByProposal[pid] || ts > Number(latestByProposal[pid].ts || 0)) {
      latestByProposal[pid] = { evt, ts };
    }
  }
  const proposalRows = Object.entries(latestByProposal)
    .sort((a, b) => Number(b[1].ts || 0) - Number(a[1].ts || 0))
    .slice(0, 80)
    .map(([proposalId, row]) => {
      const evt = row && row.evt ? row.evt : {};
      const alignment = objectiveAlignmentScore(evt, directivesById);
      return {
        proposal_id: proposalId,
        proposal_type: proposalTypeFromRun(evt),
        objective_id: alignment.objective_id,
        objective_tier: alignment.tier,
        alignment_score: alignment.score,
        alignment_band: alignment.band,
        result: String(evt && evt.result || ''),
        outcome: String(evt && evt.outcome || ''),
        ts: String(evt && evt.ts || '')
      };
    });
  const bandCounts = { green: 0, gray: 0, red: 0 };
  let scoreTotal = 0;
  for (const row of proposalRows) {
    const band = String(row && row.alignment_band || 'gray');
    if (Object.prototype.hasOwnProperty.call(bandCounts, band)) bandCounts[band] += 1;
    scoreTotal += Number(row && row.alignment_score || 0);
  }
  const sampleSize = proposalRows.length;
  const avgScore = sampleSize > 0 ? Number((scoreTotal / sampleSize).toFixed(4)) : 0;
  const overallBand = avgScore >= 0.66 ? 'green' : (avgScore <= 0.4 ? 'red' : 'gray');
  return {
    generated_at: nowIso(),
    strategy_id: String(strategy && strategy.id || '').trim() || 'default_general',
    directives_total: directivesRows.length,
    tier1_total: tier1Count,
    tier2_total: tier2Count,
    proposals_sampled: sampleSize,
    alignment_score: avgScore,
    alignment_band: overallBand,
    alignment_bands: bandCounts,
    top_proposals: proposalRows.slice(0, 20)
  };
}

function buildSummary(runs, audits, windowHours, integrityStatus = null) {
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
  const integrity = integrityStatus && typeof integrityStatus === 'object' ? integrityStatus : {};
  const integrityOk = integrity.ok === true;
  const integrityAlert = integrity.active_alert === true;
  const integrityCounts = safeCountMap(integrity.violation_counts);
  const integrityTopFiles = Array.isArray(integrity.top_files)
    ? integrity.top_files.map((v) => String(v || '').trim()).filter(Boolean).slice(0, MAX_INTEGRITY_FILES)
    : [];
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
    integrity_ok: integrityOk,
    integrity_active_alert: integrityAlert,
    integrity_severity: String(integrity.severity || (integrityAlert ? 'critical' : 'ok')),
    integrity_violation_total: Number(integrity.violation_total || sumCountMap(integrityCounts)),
    integrity_violation_counts: integrityCounts,
    integrity_checked_present_files: Number(integrity.checked_present_files || 0),
    integrity_expected_files: Number(integrity.expected_files || 0),
    integrity_policy_path: String(integrity.policy_path || ''),
    integrity_policy_version: String(integrity.policy_version || ''),
    integrity_top_files: integrityTopFiles,
    integrity_last_violation_ts: String(integrity.last_violation_ts || ''),
    integrity_last_reseal_ts: String(integrity.last_reseal_ts || ''),
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
  const directivesById = {};

  for (const d of directives || []) {
    const did = String(d && d.id || '').trim();
    if (!did) continue;
    const dtier = Number(d && d.tier || 99);
    directivesById[did] = { tier: dtier };
    addNode(nodeMap, {
      id: `directive:${did}`,
      label: `${did}`,
      type: 'directive',
      weight: 1,
      meta: { tier: dtier, title: String(d.title || did) }
    });
    objectiveSet.add(did);
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
    const alignment = objectiveAlignmentScore(evt, directivesById);
    const deps = proposalDependenciesFromRun(evt);
    const depChildren = deps && Array.isArray(deps.child_objective_ids) ? deps.child_objective_ids.length : 0;
    addNode(nodeMap, {
      id: `proposal:${pid}`,
      label: pType === 'unknown' ? pid : `${pType}:${pid.slice(0, 8)}`,
      type: 'proposal',
      weight: 1,
      meta: {
        proposal_id: pid,
        proposal_type: pType,
        risk: String(evt && evt.risk || 'unknown'),
        alignment_score: Number(alignment.score || 0),
        alignment_band: String(alignment.band || 'gray'),
        objective_id: alignment.objective_id || null,
        objective_tier: alignment.tier,
        dependency_children: depChildren
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

    if (deps) {
      const parentId = String(deps.parent_objective_id || '').trim();
      if (parentId) {
        objectiveSet.add(parentId);
        addNode(nodeMap, {
          id: `directive:${parentId}`,
          label: parentId,
          type: 'directive',
          weight: 1,
          meta: { tier: parentId.startsWith('T1_') ? 1 : null }
        });
        addEdge(edgeMap, `proposal:${pid}`, `directive:${parentId}`, 'decomposes_parent', 1);
      }
      for (const childId of deps.child_objective_ids || []) {
        objectiveSet.add(childId);
        addNode(nodeMap, {
          id: `directive:${childId}`,
          label: childId,
          type: 'directive',
          weight: 1,
          meta: { tier: childId.startsWith('T1_') ? 1 : null }
        });
        addEdge(edgeMap, `proposal:${pid}`, `directive:${childId}`, 'decomposes_to', 1);
        if (parentId) {
          addEdge(edgeMap, `directive:${parentId}`, `directive:${childId}`, 'depends_on', 1);
        }
      }
      for (const edge of deps.edges || []) {
        const from = String(edge.from || '').trim();
        const to = String(edge.to || '').trim();
        if (!from || !to) continue;
        addNode(nodeMap, {
          id: `directive:${from}`,
          label: from,
          type: 'directive',
          weight: 1,
          meta: { tier: from.startsWith('T1_') ? 1 : null }
        });
        addNode(nodeMap, {
          id: `directive:${to}`,
          label: to,
          type: 'directive',
          weight: 1,
          meta: { tier: to.startsWith('T1_') ? 1 : null }
        });
        addEdge(edgeMap, `directive:${from}`, `directive:${to}`, String(edge.relation || 'depends_on'), 1);
      }
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
  const continuum = summary && summary.continuum && typeof summary.continuum === 'object'
    ? summary.continuum
    : {};
  const continuumEvents = Number(continuum.events_24h_total || 0);
  if (continuumEvents > 0) {
    bumpAlias('systems/autonomy', Math.max(0.4, continuumEvents * 0.02));
    bumpAlias('systems/workflow', Math.max(0.3, Number(continuum.anticipation_candidates_last || 0) * 0.07));
    bumpAlias('memory', Math.max(0.2, Number(continuum.events_24h_by_stage && continuum.events_24h_by_stage.dream_consolidation || 0) * 0.06));
  }

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
  const add = (from, to, count, kind, meta = null) => {
    const f = String(from || '').trim();
    const t = String(to || '').trim();
    if (!f || !t || f === t) return;
    const key = `${f}|${t}|${String(kind || 'flow')}`;
    if (!edgeMap[key]) {
      edgeMap[key] = {
        from: f,
        to: t,
        count: 0,
        kind: String(kind || 'flow'),
        packet_tokens_total: 0,
        packet_samples: 0,
        blocked_count: 0,
        event_count: 0,
        block_reason: ''
      };
    }
    const edge = edgeMap[key];
    edge.count += Math.max(0.2, Number(count || 0));
    if (meta && typeof meta === 'object') {
      const packetTokens = Number(meta.packet_tokens || 0);
      if (Number.isFinite(packetTokens) && packetTokens > 0) {
        edge.packet_tokens_total += packetTokens;
        edge.packet_samples += 1;
      }
      const events = Number(meta.event_count || 0);
      if (Number.isFinite(events) && events > 0) {
        edge.event_count += events;
      }
      if (meta.blocked === true) {
        edge.blocked_count += 1;
      }
      const blockReason = String(meta.block_reason || '').trim().toLowerCase();
      if (blockReason && !edge.block_reason) {
        edge.block_reason = blockReason;
      }
    }
  };
  const byAlias = (alias) => aliasToId[String(alias || '').toLowerCase()] || null;

  for (const layer of layers || []) {
    for (const mod of layer.modules || []) {
      add(
        layer.id,
        mod.id,
        Math.max(0.5, Number(mod.activity || 0) * 3),
        'hierarchy',
        {
          packet_tokens: Math.max(180, Number(mod.activity || 0) * 1200),
          event_count: 1
        }
      );
      for (const sub of mod.submodules || []) {
        add(
          mod.id,
          sub.id,
          Math.max(0.3, Number(sub.activity || 0) * 2),
          'hierarchy',
          {
            packet_tokens: Math.max(90, Number(sub.activity || 0) * 700),
            event_count: 1
          }
        );
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
    if (from && to) {
      add(from, to, c, 'route', {
        packet_tokens: Math.max(220, Number(c || 0) * 180),
        event_count: 1
      });
    }
  }

  const adaptiveLayer = byAlias('adaptive') || byAlias('systems');
  const systemsLayer = byAlias('systems') || byAlias('adaptive');
  const stateLayer = byAlias('state') || byAlias('systems');
  const runCount = Number(summary && summary.run_events || 0);
  const shippedCount = Number(summary && summary.shipped || 0);
  const noChangeCount = Number(summary && summary.no_change || 0);
  const revertedCount = Number(summary && summary.reverted || 0);
  const policyHoldCount = Number(summary && summary.policy_holds || 0);

  if (adaptiveLayer) {
    add('io:input:sensory', adaptiveLayer, Math.max(0.8, runCount * 0.3), 'ingress', {
      packet_tokens: Math.max(260, runCount * 40),
      event_count: Math.max(1, runCount)
    });
  }
  if (systemsLayer) {
    add('io:input:directive', systemsLayer, Math.max(0.7, policyHoldCount * 0.35), 'ingress', {
      packet_tokens: Math.max(220, policyHoldCount * 65),
      event_count: Math.max(1, policyHoldCount)
    });
  }
  if (stateLayer) {
    add('io:input:directive', stateLayer, Math.max(0.5, policyHoldCount * 0.25), 'ingress', {
      packet_tokens: Math.max(180, policyHoldCount * 52),
      event_count: Math.max(1, policyHoldCount)
    });
  }
  if (systemsLayer) {
    add(systemsLayer, 'io:output:shipped', Math.max(0.5, shippedCount * 0.42), 'egress', {
      packet_tokens: Math.max(200, shippedCount * 70),
      event_count: Math.max(1, shippedCount)
    });
    add(systemsLayer, 'io:output:no_change', Math.max(0.3, noChangeCount * 0.35), 'egress', {
      packet_tokens: Math.max(180, noChangeCount * 66),
      event_count: Math.max(1, noChangeCount)
    });
    add(systemsLayer, 'io:output:reverted', Math.max(0.2, revertedCount * 0.45), 'egress', {
      packet_tokens: Math.max(180, revertedCount * 68),
      event_count: Math.max(1, revertedCount)
    });
  }

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
    add(from, to, 1, 'flow', {
      packet_tokens: eventPacketTokens(evt),
      event_count: 1,
      blocked: isFlowBlockedEvent(evt),
      block_reason: flowBlockReason(evt)
    });
  }

  const links = Object.values(edgeMap);
  let maxCount = 0;
  for (const link of links) maxCount = Math.max(maxCount, Number(link.count || 0));
  if (maxCount <= 0) maxCount = 1;
  let minPacketTokens = Infinity;
  let maxPacketTokens = 0;
  for (const link of links) {
    const samples = Math.max(0, Number(link.packet_samples || 0));
    const avg = samples > 0
      ? Number(link.packet_tokens_total || 0) / samples
      : Math.max(90, Number(link.count || 0) * 140);
    link.packet_size_tokens = Number.isFinite(avg) && avg > 0 ? avg : 90;
    if (link.packet_size_tokens > 0) {
      minPacketTokens = Math.min(minPacketTokens, link.packet_size_tokens);
      maxPacketTokens = Math.max(maxPacketTokens, link.packet_size_tokens);
    }
    const eventCount = Math.max(0, Number(link.event_count || 0));
    const blockedCount = Math.max(0, Number(link.blocked_count || 0));
    link.blocked_ratio = eventCount > 0
      ? Number((blockedCount / eventCount).toFixed(4))
      : 0;
    link.flow_blocked = blockedCount > 0;
  }
  if (!Number.isFinite(minPacketTokens) || minPacketTokens <= 0) minPacketTokens = 90;
  if (!Number.isFinite(maxPacketTokens) || maxPacketTokens <= 0) maxPacketTokens = minPacketTokens;
  const minPacketLog = Math.log1p(minPacketTokens);
  const maxPacketLog = Math.log1p(maxPacketTokens);
  const denomPacket = Math.max(0.000001, maxPacketLog - minPacketLog);
  for (const link of links) {
    link.activity = Number((0.08 + (0.92 * (Number(link.count || 0) / maxCount))).toFixed(4));
    const packetLog = Math.log1p(Math.max(0, Number(link.packet_size_tokens || 0)));
    link.packet_size_norm = Number(clampNumber((packetLog - minPacketLog) / denomPacket, 0, 1, 0).toFixed(4));
  }
  return links;
}

function buildHoloModel(runs, summary) {
  const scanned = loadLayerModelCached();
  const layers = assignLayerActivity(scanned.layers || [], runs || [], summary || {}, scanned.alias_to_id || {});
  const changeSnapshot = loadChangeStateSnapshot(layers);
  applyChangeStateToLayers(layers, changeSnapshot);
  const changeSummary = changeSnapshot && changeSnapshot.summary && typeof changeSnapshot.summary === 'object'
    ? changeSnapshot.summary
    : {};
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
  const integrityAlert = summary && summary.integrity_active_alert === true ? 1 : 0;
  const integrityViolationTotal = Number(summary && summary.integrity_violation_total || 0);
  const pendingPush = changeSummary.pending_push === true ? 1 : 0;
  const justPushed = changeSummary.just_pushed === true ? 1 : 0;
  const constitution = summary && summary.constitution && typeof summary.constitution === 'object'
    ? summary.constitution
    : {};
  const evolution = summary && summary.evolution && typeof summary.evolution === 'object'
    ? summary.evolution
    : {};
  const fractal = summary && summary.fractal && typeof summary.fractal === 'object'
    ? summary.fractal
    : {};
  const continuum = summary && summary.continuum && typeof summary.continuum === 'object'
    ? summary.continuum
    : {};

  return {
    generated_at: nowIso(),
    layers,
    links,
    io,
    change: {
      dirty_files_total: Number(changeSummary.dirty_files_total || 0),
      staged_files_total: Number(changeSummary.staged_files_total || 0),
      pending_push_files_total: Number(changeSummary.pending_push_files_total || 0),
      active_write_files_total: Number(changeSummary.active_write_files_total || 0),
      active_modules: Number(changeSummary.active_modules || 0),
      active_submodules: Number(changeSummary.active_submodules || 0),
      ahead_count: Number(changeSummary.ahead_count || 0),
      pending_push: changeSummary.pending_push === true,
      just_pushed: changeSummary.just_pushed === true,
      top_files: Array.isArray(changeSummary.top_files) ? changeSummary.top_files.slice(0, MAX_CHANGE_FILES) : [],
      has_upstream: changeSummary.has_upstream === true,
      upstream: String(changeSummary.upstream || ''),
      last_push_ts: String(changeSummary.last_push_ts || ''),
      last_commit_ts: String(changeSummary.last_commit_ts || '')
    },
    metrics: {
      run_events: Number(summary && summary.run_events || 0),
      executed,
      shipped,
      no_change: noChange,
      reverted: Number(summary && summary.reverted || 0),
      policy_holds: Number(summary && summary.policy_holds || 0),
      yield_rate: Number(yieldRate.toFixed(4)),
      drift_proxy: Number(driftRate.toFixed(4)),
      integrity_alert: integrityAlert,
      integrity_violation_total: integrityViolationTotal,
      integrity_severity: String(summary && summary.integrity_severity || (integrityAlert ? 'critical' : 'ok')),
      constitution_alignment_score: Number(constitution.alignment_score || 0),
      constitution_alignment_band: String(constitution.alignment_band || 'gray'),
      constitution_proposals_sampled: Number(constitution.proposals_sampled || 0),
      evolution_commit_velocity_30d: Number(evolution.commit_velocity_30d || 0),
      evolution_stability_score: Number(evolution.stability_score || 0),
      evolution_commits_30d: Number(evolution.commits_30d || 0),
      fractal_harmony_score: Number(fractal.harmony_score || 0),
      fractal_symbiosis_plans: Number(fractal.symbiosis_plans || 0),
      fractal_predator_candidates: Number(fractal.predator_candidates || 0),
      fractal_restructure_candidates: Number(fractal.restructure_candidates || 0),
      fractal_epigenetic_tags: Number(fractal.epigenetic_tags || 0),
      fractal_archetypes: Number(fractal.archetypes || 0),
      fractal_pheromones: Number(fractal.pheromones || 0),
      black_box_rows: Number(fractal.black_box_rows || 0),
      continuum_events_24h: Number(continuum.events_24h_total || 0),
      continuum_training_queue_rows_24h: Number(continuum.training_queue_rows_24h || 0),
      continuum_last_trit: Number(continuum.last_trit || 0),
      continuum_last_trit_label: String(continuum.last_trit_label || ''),
      continuum_last_skipped: continuum.last_skipped === true ? 1 : 0,
      continuum_pulse_age_sec: Number(continuum.pulse_age_sec || 0),
      change_pending_push: pendingPush,
      change_just_pushed: justPushed,
      change_active_modules: Number(changeSummary.active_modules || 0),
      change_dirty_files_total: Number(changeSummary.dirty_files_total || 0),
      change_staged_files_total: Number(changeSummary.staged_files_total || 0),
      change_ahead_count: Number(changeSummary.ahead_count || 0)
    }
  };
}

function buildPayload(hours) {
  const telemetry = loadRecentTelemetry(hours, MAX_EVENTS);
  const directives = loadDirectiveSummary();
  const strategy = loadStrategySummary();
  const integrity = loadIntegrityStatus(telemetry.window_hours);
  const baseSummary = buildSummary(telemetry.runs, telemetry.audits, telemetry.window_hours, integrity);
  const constitution = buildConstitutionSnapshot(telemetry.runs, directives, strategy);
  const evolution = loadEvolutionSnapshot();
  const fractalSnapshot = loadFractalSnapshot();
  const continuumSnapshot = loadContinuumSnapshot();
  const fractal = {
    harmony_score: Number(fractalSnapshot && fractalSnapshot.organism ? fractalSnapshot.organism.harmony_score || 0 : 0),
    symbiosis_plans: Number(fractalSnapshot && fractalSnapshot.organism ? fractalSnapshot.organism.symbiosis_plans || 0 : 0),
    predator_candidates: Number(fractalSnapshot && fractalSnapshot.organism ? fractalSnapshot.organism.predator_candidates || 0 : 0),
    restructure_candidates: Number(fractalSnapshot && fractalSnapshot.introspection ? fractalSnapshot.introspection.restructure_candidates || 0 : 0),
    epigenetic_tags: Number(fractalSnapshot && fractalSnapshot.epigenetic ? fractalSnapshot.epigenetic.tags || 0 : 0),
    archetypes: Number(fractalSnapshot && fractalSnapshot.archetypes ? fractalSnapshot.archetypes.count || 0 : 0),
    pheromones: Number(fractalSnapshot && fractalSnapshot.pheromones ? fractalSnapshot.pheromones.packets || 0 : 0),
    genome_rows: Number(fractalSnapshot && fractalSnapshot.genome ? fractalSnapshot.genome.rows || 0 : 0),
    genome_last_hash: String(fractalSnapshot && fractalSnapshot.genome ? fractalSnapshot.genome.last_hash || '' : ''),
    black_box_rows: Number(fractalSnapshot && fractalSnapshot.black_box ? fractalSnapshot.black_box.rows || 0 : 0),
    black_box_last_hash: String(fractalSnapshot && fractalSnapshot.black_box ? fractalSnapshot.black_box.last_hash || '' : '')
  };
  const continuum = {
    available: continuumSnapshot && continuumSnapshot.available === true,
    last_pulse_ts: String(continuumSnapshot && continuumSnapshot.last_pulse_ts || ''),
    pulse_age_sec: Number(continuumSnapshot && continuumSnapshot.pulse_age_sec || 0),
    last_profile: String(continuumSnapshot && continuumSnapshot.last_profile || ''),
    last_trit: Number(continuumSnapshot && continuumSnapshot.last_trit || 0),
    last_trit_label: String(continuumSnapshot && continuumSnapshot.last_trit_label || ''),
    last_skipped: continuumSnapshot && continuumSnapshot.last_skipped === true,
    last_skip_reasons: Array.isArray(continuumSnapshot && continuumSnapshot.last_skip_reasons)
      ? continuumSnapshot.last_skip_reasons.slice(0, 6)
      : [],
    tasks_executed_last: Number(continuumSnapshot && continuumSnapshot.tasks_executed_last || 0),
    events_24h_total: Number(continuumSnapshot && continuumSnapshot.events_24h_total || 0),
    events_24h_by_stage: continuumSnapshot && continuumSnapshot.events_24h_by_stage && typeof continuumSnapshot.events_24h_by_stage === 'object'
      ? continuumSnapshot.events_24h_by_stage
      : {},
    training_queue_rows_24h: Number(continuumSnapshot && continuumSnapshot.training_queue_rows_24h || 0),
    anticipation_drafts_last: Number(continuumSnapshot && continuumSnapshot.anticipation_drafts_last || 0),
    anticipation_candidates_last: Number(continuumSnapshot && continuumSnapshot.anticipation_candidates_last || 0),
    red_team_cases_last: Number(continuumSnapshot && continuumSnapshot.red_team_cases_last || 0),
    red_team_critical_last: Number(continuumSnapshot && continuumSnapshot.red_team_critical_last || 0),
    observer_mood_last: String(continuumSnapshot && continuumSnapshot.observer_mood_last || '')
  };
  const summary = {
    ...baseSummary,
    constitution: {
      alignment_score: Number(constitution.alignment_score || 0),
      alignment_band: String(constitution.alignment_band || 'gray'),
      alignment_bands: constitution.alignment_bands || { green: 0, gray: 0, red: 0 },
      proposals_sampled: Number(constitution.proposals_sampled || 0),
      directives_total: Number(constitution.directives_total || directives.length || 0),
      tier1_total: Number(constitution.tier1_total || 0),
      tier2_total: Number(constitution.tier2_total || 0),
      top_proposals: Array.isArray(constitution.top_proposals) ? constitution.top_proposals.slice(0, 20) : []
    },
    evolution,
    fractal,
    continuum
  };
  const graph = buildGraph(telemetry.runs, directives, strategy);
  const holo = buildHoloModel(telemetry.runs, summary);
  return {
    ok: true,
    generated_at: nowIso(),
    summary,
    graph,
    holo,
    constitution,
    evolution,
    fractal,
    continuum,
    fractal_snapshot: fractalSnapshot,
    continuum_snapshot: continuumSnapshot,
    incidents: {
      integrity
    }
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
    incidents: payload.incidents || {},
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
  watchTarget(CONTINUUM_RUNS_DIR);
  watchTarget(CONTINUUM_EVENTS_DIR);

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

function resolveWorkspacePath(rawPath) {
  const input = String(rawPath || '').trim();
  if (!input) return null;
  const abs = path.isAbsolute(input)
    ? path.resolve(input)
    : path.resolve(REPO_ROOT, input);
  const rootPrefix = REPO_ROOT.endsWith(path.sep) ? REPO_ROOT : `${REPO_ROOT}${path.sep}`;
  if (abs !== REPO_ROOT && !abs.startsWith(rootPrefix)) return null;
  return abs;
}

function likelyTextBuffer(buf) {
  if (!Buffer.isBuffer(buf)) return false;
  if (buf.length === 0) return true;
  const sampleLen = Math.min(buf.length, 4096);
  let controlCount = 0;
  for (let i = 0; i < sampleLen; i += 1) {
    const b = buf[i];
    if (b === 0) return false;
    if (b < 7 || (b > 13 && b < 32)) controlCount += 1;
  }
  return (controlCount / sampleLen) < 0.25;
}

function readCodePreview(rawPath) {
  const abs = resolveWorkspacePath(rawPath);
  if (!abs) {
    return {
      ok: false,
      error: 'path_outside_workspace'
    };
  }
  let stat = null;
  try {
    stat = fs.statSync(abs);
  } catch {
    return {
      ok: false,
      error: 'not_found',
      path: abs
    };
  }
  const rel = normalizeRelPath(path.relative(REPO_ROOT, abs));
  if (stat.isDirectory()) {
    return {
      ok: true,
      path: abs,
      rel,
      is_file: false,
      is_dir: true,
      size_bytes: 0,
      truncated: false,
      content: ''
    };
  }
  if (!stat.isFile()) {
    return {
      ok: true,
      path: abs,
      rel,
      is_file: false,
      is_dir: false,
      size_bytes: 0,
      truncated: false,
      content: ''
    };
  }

  const sizeBytes = Math.max(0, Number(stat.size || 0));
  const readBytes = Math.max(0, Math.min(sizeBytes, CODE_PREVIEW_MAX_BYTES));
  let fileBuffer = Buffer.alloc(0);
  if (readBytes > 0) {
    const fd = fs.openSync(abs, 'r');
    try {
      fileBuffer = Buffer.alloc(readBytes);
      const bytesRead = fs.readSync(fd, fileBuffer, 0, readBytes, 0);
      if (bytesRead < readBytes) fileBuffer = fileBuffer.slice(0, bytesRead);
    } finally {
      try { fs.closeSync(fd); } catch {}
    }
  }
  if (!likelyTextBuffer(fileBuffer)) {
    return {
      ok: true,
      path: abs,
      rel,
      is_file: true,
      is_dir: false,
      size_bytes: sizeBytes,
      truncated: sizeBytes > readBytes,
      binary: true,
      content: ''
    };
  }
  return {
    ok: true,
    path: abs,
    rel,
    is_file: true,
    is_dir: false,
    size_bytes: sizeBytes,
    truncated: sizeBytes > readBytes,
    binary: false,
    content: String(fileBuffer.toString('utf8') || '')
  };
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
          holo: payload.holo,
          incidents: payload.incidents || {}
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
    if (pathname === '/api/file') {
      const rawPath = String(parsed.searchParams.get('path') || '');
      const payload = readCodePreview(rawPath);
      if (!payload.ok) {
        const err = String(payload.error || 'bad_request');
        const code = err === 'not_found' ? 404 : (err === 'path_outside_workspace' ? 403 : 400);
        sendJson(res, code, payload);
        return;
      }
      sendJson(res, 200, payload);
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
