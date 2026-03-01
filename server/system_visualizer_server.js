#!/usr/bin/env node
/**
 * Read-only system visualizer server.
 *
 * Serves:
 * - GET /api/graph?hours=24&live_mode=1&live_minutes=6
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
const WORKFLOW_ORCHESTRON_DIR = path.join(REPO_ROOT, 'state', 'adaptive', 'workflows', 'orchestron');
const WORKFLOW_BIRTH_EVENTS_PATH = path.join(WORKFLOW_ORCHESTRON_DIR, 'birth_events.jsonl');
const WORKFLOW_ORCHESTRON_LATEST_PATH = path.join(WORKFLOW_ORCHESTRON_DIR, 'latest.json');
const AUTOTEST_DIR = path.join(REPO_ROOT, 'state', 'ops', 'autotest');
const AUTOTEST_LATEST_PATH = path.join(AUTOTEST_DIR, 'latest.json');
const AUTOTEST_STATUS_PATH = path.join(AUTOTEST_DIR, 'status.json');
const AUTOTEST_EVENTS_PATH = path.join(AUTOTEST_DIR, 'events.jsonl');
const SYSTEM_HEALTH_DIR = path.join(REPO_ROOT, 'state', 'ops', 'system_health');
const SYSTEM_HEALTH_EVENTS_PATH = path.join(SYSTEM_HEALTH_DIR, 'events.jsonl');
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
const DEFAULT_LIVE_MINUTES = 6;
const MAX_EVENTS = 6000;
const MAX_PROPOSALS = 80;
const WS_PATH = '/ws/holo';
const WS_TICK_MS = 1300;
const WS_WATCH_DEBOUNCE_MS = 220;
const WS_PING_MS = 15000;
const MODULE_SCAN_CACHE_TTL_MS = 7000;
const MAX_LAYER_MODULES = Math.round(clampNumber(process.env.HOLO_LAYER_MODULE_LIMIT, 8, 256, 48));
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
const WORKFLOW_BIRTH_CACHE_TTL_MS = 3000;
const DOCTOR_HEALTH_CACHE_TTL_MS = 3000;
const CODEBASE_SIZE_MAX_FILES = 2400;
const CODEBASE_SIZE_MAX_DEPTH = 10;
const CODE_PREVIEW_MAX_BYTES = 180 * 1024;
const CODEGRAPH_CACHE_TTL_MS = 20000;
const CODEGRAPH_MAX_FILES = 3200;
const CODEGRAPH_MAX_DEPTH = 14;
const CODEGRAPH_MAX_FILE_BYTES = 96 * 1024;
const CODEGRAPH_DEFAULT_QUERY_LIMIT = 24;
const LIVE_MINUTES_MIN = 1;
const LIVE_MINUTES_MAX = 24 * 60;
const RUNTIME_STALE_MULTIPLIER = clampNumber(process.env.HOLO_RUNTIME_STALE_MULTIPLIER, 2, 12, 5);
const RUNTIME_STALE_MIN_SEC = clampNumber(process.env.HOLO_RUNTIME_STALE_MIN_SEC, 60, 12 * 60 * 60, 20 * 60);
const SPINE_PENDING_RUN_GRACE_SEC = clampNumber(process.env.HOLO_SPINE_PENDING_RUN_GRACE_SEC, 120, 12 * 60 * 60, 90 * 60);
const RUNTIME_SIGNAL_LOOKBACK_HOURS = clampNumber(process.env.HOLO_RUNTIME_SIGNAL_LOOKBACK_HOURS, 1, 24 * 30, 24);
const SPINE_SIGNAL_SCAN_MAX_ROWS = Math.round(clampNumber(process.env.HOLO_SPINE_SIGNAL_SCAN_MAX_ROWS, 120, 6000, 1200));
const SPINE_SIGNAL_CACHE_TTL_MS = Math.round(clampNumber(process.env.HOLO_SPINE_SIGNAL_CACHE_TTL_MS, 250, 15000, 1500));
const TERMINAL_OUTPUT_MAX_CHARS = 20000;
const TERMINAL_CMD_TIMEOUT_MS = 20000;
const TERMINAL_MAX_BUFFER_BYTES = 2 * 1024 * 1024;
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
const CODEGRAPH_SKIP_DIRS = new Set([
  ...Array.from(CODEBASE_SKIP_DIRS),
  '.openclaw',
  'state',
  'memory',
  'logs',
  'tmp'
]);
const CODEGRAPH_FILE_EXTS = new Set([
  '.js', '.ts', '.jsx', '.tsx', '.mjs', '.cjs',
  '.py', '.go', '.rs', '.java', '.kt', '.swift',
  '.rb', '.php', '.sh', '.zsh', '.bash',
  '.json', '.yaml', '.yml', '.toml',
  '.md', '.sql', '.html', '.css', '.scss', '.sass'
]);
const CODEGRAPH_IMPORT_SCAN_EXTS = new Set([
  '.js', '.ts', '.jsx', '.tsx', '.mjs', '.cjs', '.py'
]);
const DOCTOR_WOUNDED_CODES = new Set([
  'autotest_doctor_wounded_module',
  'autotest_doctor_destructive_repair_blocked',
  'autotest_doctor_kill_switch',
  'autotest_doctor_kill_same_signature'
]);
const DOCTOR_HEALING_CODES = new Set([
  'autotest_doctor_healing_attempt'
]);
const DOCTOR_REGROWTH_CODES = new Set([
  'autotest_doctor_regrowth'
]);
const DOCTOR_ROLLBACK_CODES = new Set([
  'autotest_doctor_rollback_cut'
]);
const SPINE_RUN_TERMINAL_TYPES = new Set([
  'spine_run_ok',
  'spine_run_failed',
  'spine_run_error',
  'spine_run_stopped',
  'spine_run_halt'
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

let WORKFLOW_BIRTH_CACHE = {
  ts: 0,
  hours: 0,
  payload: null
};

let DOCTOR_HEALTH_CACHE = {
  ts: 0,
  hours: 0,
  payload: null
};

let CODEGRAPH_CACHE = {
  ts: 0,
  payload: null
};
let SPINE_SIGNAL_CACHE = {
  ts: 0,
  payload: null
};
const TERMINAL_STATE = {
  cwd: REPO_ROOT,
  updated_at: nowIso(),
  last_exit_code: 0,
  last_command: ''
};

let TYPESCRIPT_MODULE_CACHE = {
  tried: false,
  mod: null
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
  const out = {
    host: DEFAULT_HOST,
    port: DEFAULT_PORT,
    hours: DEFAULT_HOURS,
    live_minutes: DEFAULT_LIVE_MINUTES
  };
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
    if (tok === '--live-minutes' && argv[i + 1]) {
      const v = Number(argv[i + 1]);
      if (Number.isFinite(v) && v > 0) out.live_minutes = Math.round(v);
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

function clampLiveMinutes(v, fallback = DEFAULT_LIVE_MINUTES) {
  return clampNumber(v, LIVE_MINUTES_MIN, LIVE_MINUTES_MAX, fallback);
}

function parseQueryNumber(searchParams, key) {
  if (!searchParams || typeof searchParams.get !== 'function') return NaN;
  const raw = searchParams.get(String(key || ''));
  if (raw == null) return NaN;
  const text = String(raw).trim();
  if (!text) return NaN;
  const n = Number(text);
  return Number.isFinite(n) ? n : NaN;
}

function parseBoolish(value, fallback = false) {
  if (typeof value === 'boolean') return value;
  const text = String(value == null ? '' : value).trim().toLowerCase();
  if (!text) return fallback;
  if (text === '1' || text === 'true' || text === 'yes' || text === 'on') return true;
  if (text === '0' || text === 'false' || text === 'no' || text === 'off') return false;
  return fallback;
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

function severityRank(raw) {
  const sev = String(raw || '').trim().toLowerCase();
  if (sev === 'critical') return 4;
  if (sev === 'high') return 3;
  if (sev === 'medium') return 2;
  if (sev === 'low') return 1;
  return 0;
}

function doctorStateFromEvent(row) {
  const explicit = String(row && row.viz_state || '').trim().toLowerCase();
  if (explicit === 'wounded' || explicit === 'healing' || explicit === 'regrowth' || explicit === 'rollback_cut') {
    return explicit;
  }
  const code = String(row && row.code || '').trim().toLowerCase();
  if (!code) return '';
  if (DOCTOR_ROLLBACK_CODES.has(code)) return 'rollback_cut';
  if (DOCTOR_WOUNDED_CODES.has(code)) return 'wounded';
  if (DOCTOR_HEALING_CODES.has(code)) return 'healing';
  if (DOCTOR_REGROWTH_CODES.has(code)) return 'regrowth';
  if (code.includes('rollback')) return 'rollback_cut';
  if (code.includes('regrowth')) return 'regrowth';
  if (code.includes('healing')) return 'healing';
  if (code.includes('kill') || code.includes('blocked') || code.includes('wounded')) return 'wounded';
  return '';
}

function resolveAliasPath(aliasToId, rawAlias) {
  const map = aliasToId && typeof aliasToId === 'object' ? aliasToId : {};
  let key = normalizeRelPath(rawAlias).replace(/^\/+/, '').toLowerCase();
  if (!key) return null;
  if (map[key]) return map[key];
  while (key.includes('/')) {
    const idx = key.lastIndexOf('/');
    if (idx <= 0) break;
    key = key.slice(0, idx);
    if (map[key]) return map[key];
  }
  return map[key] || null;
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
  const h = clampNumber(hours, 1 / 60, 24 * 30, DEFAULT_HOURS);
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

function latestEventMs(events) {
  const rows = Array.isArray(events) ? events : [];
  for (const evt of rows) {
    const ms = parseTsMs(evt && evt.ts);
    if (ms != null) return ms;
  }
  return null;
}

function loadSpineRuntimeSignal(lookbackHours = RUNTIME_SIGNAL_LOOKBACK_HOURS) {
  const nowMs = Date.now();
  if (
    SPINE_SIGNAL_CACHE.payload
    && (nowMs - Number(SPINE_SIGNAL_CACHE.ts || 0)) < SPINE_SIGNAL_CACHE_TTL_MS
  ) {
    return { ...SPINE_SIGNAL_CACHE.payload };
  }
  const lookbackMs = clampNumber(lookbackHours, 1, 24 * 30, RUNTIME_SIGNAL_LOOKBACK_HOURS) * 60 * 60 * 1000;
  const cutoffMs = nowMs - lookbackMs;
  const files = listSpineRunFilesDesc().slice(0, 4);
  let latestMs = null;
  let latestType = '';
  const signalRows = [];
  let done = false;
  for (const file of files) {
    if (done) break;
    const fp = path.join(SPINE_RUNS_DIR, file);
    if (!fs.existsSync(fp)) continue;
    const lines = String(fs.readFileSync(fp, 'utf8') || '').split('\n');
    for (let i = lines.length - 1; i >= 0; i -= 1) {
      const line = String(lines[i] || '').trim();
      if (!line) continue;
      const evt = safeJsonParse(line);
      if (!evt || typeof evt !== 'object') continue;
      const ms = parseTsMs(evt.ts);
      if (ms == null) continue;
      if (ms < cutoffMs) break;
      const type = String(evt.type || '').trim();
      if (latestMs == null || ms > latestMs) {
        latestMs = ms;
        latestType = type;
      }
      if (type === 'spine_run_started' || SPINE_RUN_TERMINAL_TYPES.has(type)) {
        signalRows.push({ ms, type });
      }
      if (signalRows.length >= SPINE_SIGNAL_SCAN_MAX_ROWS) {
        done = true;
        break;
      }
    }
  }
  signalRows.sort((a, b) => a.ms - b.ms);
  let latestStartMs = null;
  let latestTerminalMs = null;
  for (const row of signalRows) {
    if (!row || typeof row !== 'object') continue;
    if (row.type === 'spine_run_started') {
      latestStartMs = row.ms;
      continue;
    }
    if (SPINE_RUN_TERMINAL_TYPES.has(row.type)) latestTerminalMs = row.ms;
  }
  const pendingRun = latestStartMs != null && (latestTerminalMs == null || latestTerminalMs < latestStartMs);
  const pendingAgeSec = pendingRun
    ? Math.max(0, Math.round((nowMs - latestStartMs) / 1000))
    : null;
  const payload = {
    latest_ms: latestMs,
    latest_ts: latestMs != null ? new Date(latestMs).toISOString() : '',
    latest_type: latestType,
    pending_run: pendingRun,
    pending_run_started_ts: latestStartMs != null ? new Date(latestStartMs).toISOString() : '',
    pending_run_age_sec: pendingAgeSec
  };
  SPINE_SIGNAL_CACHE = {
    ts: nowMs,
    payload
  };
  return { ...payload };
}

function runtimeWindowMinutesFromInput(hours, liveMinutes, liveMode = true) {
  if (liveMode === true) {
    return clampLiveMinutes(liveMinutes, DEFAULT_LIVE_MINUTES);
  }
  return clampLiveMinutes(Math.round(clampNumber(hours, 1, 24 * 30, DEFAULT_HOURS) * 60), DEFAULT_HOURS * 60);
}

function buildRuntimeStatus(windowMinutes, spineEvents, continuumSnapshot, spineSignal = null) {
  const nowMs = Date.now();
  const liveM = clampLiveMinutes(windowMinutes, DEFAULT_LIVE_MINUTES);
  const onlineMaxSec = Math.max(45, Math.round(liveM * 60));
  const staleMaxSec = Math.max(
    onlineMaxSec + 30,
    Math.round(onlineMaxSec * RUNTIME_STALE_MULTIPLIER),
    Math.round(RUNTIME_STALE_MIN_SEC)
  );
  const spineSignalSafe = spineSignal && typeof spineSignal === 'object' ? spineSignal : {};
  const latestSpineMs = Math.max(
    latestEventMs(spineEvents) || 0,
    parseTsMs(spineSignalSafe.latest_ts) || Number(spineSignalSafe.latest_ms || 0) || 0
  ) || null;
  const latestContinuumMs = parseTsMs(continuumSnapshot && continuumSnapshot.last_pulse_ts);
  const latestMs = Math.max(latestSpineMs || 0, latestContinuumMs || 0) || null;
  const ageSec = latestMs != null ? Math.max(0, Math.round((nowMs - latestMs) / 1000)) : null;
  let status = ageSec == null
    ? 'offline'
    : ageSec <= onlineMaxSec
      ? 'online'
      : ageSec <= staleMaxSec
        ? 'stale'
        : 'offline';
  let statusReason = ageSec == null
    ? 'no_recent_runtime_signal'
    : status === 'online'
      ? 'recent_runtime_signal'
      : status === 'stale'
        ? 'runtime_signal_aging'
        : 'runtime_signal_expired';
  const pendingRunAgeSec = spineSignalSafe.pending_run_age_sec == null
    ? null
    : Number(spineSignalSafe.pending_run_age_sec);
  const pendingRun = spineSignalSafe.pending_run === true
    && Number.isFinite(pendingRunAgeSec)
    && pendingRunAgeSec >= 0
    && pendingRunAgeSec <= Math.max(staleMaxSec + 30, SPINE_PENDING_RUN_GRACE_SEC);
  if (status === 'offline' && pendingRun) {
    status = pendingRunAgeSec <= onlineMaxSec ? 'online' : 'stale';
    statusReason = 'spine_run_in_progress';
  }
  const source = (latestSpineMs != null && latestContinuumMs != null)
    ? (latestSpineMs >= latestContinuumMs ? 'spine' : 'continuum')
    : (latestSpineMs != null ? 'spine' : (latestContinuumMs != null ? 'continuum' : 'none'));
  const eventCount = Array.isArray(spineEvents) ? spineEvents.length : 0;
  const activityScale = status === 'online' ? 1 : (status === 'stale' ? 0.58 : 0.18);
  return {
    status,
    online: status === 'online',
    stale: status === 'stale',
    offline: status === 'offline',
    reason: statusReason,
    source,
    live_window_minutes: Number(liveM),
    online_max_sec: Number(onlineMaxSec),
    stale_max_sec: Number(staleMaxSec),
    latest_signal_ts: latestMs != null ? new Date(latestMs).toISOString() : '',
    latest_spine_signal_ts: String(spineSignalSafe.latest_ts || ''),
    latest_spine_signal_type: String(spineSignalSafe.latest_type || ''),
    signal_age_sec: ageSec == null ? null : Number(ageSec),
    spine_pending_run: pendingRun,
    spine_pending_run_started_ts: String(spineSignalSafe.pending_run_started_ts || ''),
    spine_pending_run_age_sec: pendingRunAgeSec == null ? null : Number(pendingRunAgeSec),
    spine_event_count_window: Number(eventCount),
    activity_scale: Number(activityScale)
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
    autotest_validation: 0,
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

  const autotestLatest = safeJsonRead(AUTOTEST_LATEST_PATH, null);
  const autotestLatestPayload = autotestLatest && typeof autotestLatest === 'object' ? autotestLatest : {};
  const autotestStatus = safeJsonRead(AUTOTEST_STATUS_PATH, null);
  const autotestStatusPayload = autotestStatus && typeof autotestStatus === 'object' ? autotestStatus : {};
  const autotestRun = autotestLatestPayload.run && typeof autotestLatestPayload.run === 'object'
    ? autotestLatestPayload.run
    : autotestLatestPayload;
  const autotestTs = String(autotestRun.ts || autotestLatestPayload.ts || autotestStatusPayload.last_run || '');
  const autotestEvents = readJsonlRows(AUTOTEST_EVENTS_PATH);
  let autotestAlerts24h = 0;
  let autotestFailed24h = 0;
  let autotestGuardBlocked24h = 0;
  for (let i = autotestEvents.length - 1; i >= 0; i -= 1) {
    const row = autotestEvents[i];
    const tsMs = parseTsMs(row && row.ts);
    if (tsMs != null && tsMs < cutoffMs) break;
    if (String(row && row.type || '') !== 'autotest_alert') continue;
    autotestAlerts24h += 1;
    autotestFailed24h += Number(row && row.failed || 0);
    autotestGuardBlocked24h += Number(row && row.guard_blocked || 0);
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
    autotest_available: fs.existsSync(AUTOTEST_LATEST_PATH) || fs.existsSync(AUTOTEST_STATUS_PATH),
    autotest_last_run_ts: autotestTs || '',
    autotest_last_scope: String(autotestRun.scope || ''),
    autotest_selected_tests_last: Number(autotestRun.selected_tests || 0),
    autotest_failed_last: Number(autotestRun.failed || 0),
    autotest_guard_blocked_last: Number(autotestRun.guard_blocked || 0),
    autotest_untested_modules_last: Number(autotestRun.untested_modules || 0),
    autotest_modules_total: Number(autotestStatusPayload.modules_total || 0),
    autotest_modules_changed: Number(autotestStatusPayload.modules_changed || 0),
    autotest_tests_failed: Number(autotestStatusPayload.tests_failed || 0),
    autotest_tests_total: Number(autotestStatusPayload.tests_total || 0),
    autotest_alerts_24h: autotestAlerts24h,
    autotest_failed_24h: autotestFailed24h,
    autotest_guard_blocked_24h: autotestGuardBlocked24h,
    history_rows: historyRows.length
  };

  CONTINUUM_CACHE = {
    ts: nowMs,
    payload
  };
  return cloneJson(payload);
}

function loadWorkflowBirthSnapshot(hours = DEFAULT_HOURS) {
  const h = clampNumber(hours, 1, 24 * 30, DEFAULT_HOURS);
  const nowMs = Date.now();
  if (
    WORKFLOW_BIRTH_CACHE.payload
    && Number(WORKFLOW_BIRTH_CACHE.hours || 0) === Number(h)
    && (nowMs - Number(WORKFLOW_BIRTH_CACHE.ts || 0)) < WORKFLOW_BIRTH_CACHE_TTL_MS
  ) {
    return cloneJson(WORKFLOW_BIRTH_CACHE.payload);
  }

  const cutoffMs = nowMs - (h * 60 * 60 * 1000);
  const rows = readJsonlRows(WORKFLOW_BIRTH_EVENTS_PATH);
  const stageCounts = {};
  const runCounts = {};
  const candidateMap = {};
  const recentEvents = [];

  for (let i = rows.length - 1; i >= 0; i -= 1) {
    const row = rows[i];
    if (!row || typeof row !== 'object') continue;
    if (String(row.type || '') !== 'orchestron_birth_event') continue;
    const tsMs = parseTsMs(row.ts);
    if (tsMs != null && tsMs < cutoffMs) break;
    const stage = String(row.stage || 'unknown').trim() || 'unknown';
    stageCounts[stage] = Number(stageCounts[stage] || 0) + 1;

    const runId = String(row.run_id || '').trim();
    if (runId) runCounts[runId] = Number(runCounts[runId] || 0) + 1;

    if (recentEvents.length < 180) {
      recentEvents.push({
        ts: String(row.ts || ''),
        stage,
        run_id: runId || null,
        candidate_id: String(row.candidate_id || '').trim() || null,
        parent_candidate_id: String(row.parent_candidate_id || '').trim() || null,
        proposal_type: String(row.proposal_type || '').trim() || null,
        mutation_kind: String(row.mutation_kind || '').trim() || null
      });
    }

    const candidateId = String(row.candidate_id || '').trim();
    if (!candidateId) continue;
    if (!candidateMap[candidateId]) {
      candidateMap[candidateId] = {
        candidate_id: candidateId,
        parent_candidate_id: null,
        fractal_depth: 0,
        proposal_type: '',
        mutation_kind: '',
        run_id: '',
        last_stage: '',
        last_ts: '',
        stage_counts: {},
        scorecard: {
          trit_alignment: null,
          composite_score: null,
          predicted_drift_delta: null,
          predicted_yield_delta: null,
          critical_failures: null,
          non_critical_findings: null,
          adversarial_pass: null
        }
      };
    }
    const cand = candidateMap[candidateId];
    if (runId) cand.run_id = runId;
    const parentId = String(row.parent_candidate_id || '').trim();
    if (parentId) cand.parent_candidate_id = parentId;
    if (Number.isFinite(Number(row.fractal_depth))) cand.fractal_depth = Number(row.fractal_depth || 0);
    if (!cand.proposal_type && row.proposal_type) cand.proposal_type = String(row.proposal_type);
    if (!cand.mutation_kind && row.mutation_kind) cand.mutation_kind = String(row.mutation_kind);
    cand.last_stage = stage;
    cand.last_ts = String(row.ts || cand.last_ts || '');
    cand.stage_counts[stage] = Number(cand.stage_counts[stage] || 0) + 1;
    if (Number.isFinite(Number(row.trit_alignment))) cand.scorecard.trit_alignment = Number(row.trit_alignment);
    if (Number.isFinite(Number(row.composite_score))) cand.scorecard.composite_score = Number(row.composite_score);
    if (Number.isFinite(Number(row.predicted_drift_delta))) cand.scorecard.predicted_drift_delta = Number(row.predicted_drift_delta);
    if (Number.isFinite(Number(row.predicted_yield_delta))) cand.scorecard.predicted_yield_delta = Number(row.predicted_yield_delta);
    if (Number.isFinite(Number(row.critical_failures))) cand.scorecard.critical_failures = Number(row.critical_failures);
    if (Number.isFinite(Number(row.non_critical_findings))) cand.scorecard.non_critical_findings = Number(row.non_critical_findings);
    if (typeof row.pass === 'boolean') cand.scorecard.adversarial_pass = row.pass === true;
  }

  const candidates = Object.values(candidateMap);
  const topRuns = topCounts(runCounts, 5);
  const latestRunId = topRuns.length ? String(topRuns[0][0] || '') : '';

  const candidateById = {};
  for (const row of candidates) candidateById[row.candidate_id] = row;
  for (const row of candidates) {
    const lineage = [];
    const seen = new Set();
    let cur = String(row.candidate_id || '');
    while (cur && !seen.has(cur) && lineage.length < 12) {
      seen.add(cur);
      lineage.push(cur);
      const next = candidateById[cur] && String(candidateById[cur].parent_candidate_id || '').trim();
      if (!next) break;
      cur = next;
    }
    row.lineage_path = lineage.reverse();
  }

  const sortedCandidates = candidates
    .sort((a, b) => {
      const da = Number(a.fractal_depth || 0);
      const db = Number(b.fractal_depth || 0);
      if (Math.abs(da - db) > 0.0001) return da - db;
      const sa = Number(a.scorecard && a.scorecard.composite_score || -999);
      const sb = Number(b.scorecard && b.scorecard.composite_score || -999);
      if (Math.abs(sa - sb) > 0.0001) return sb - sa;
      return String(a.candidate_id || '').localeCompare(String(b.candidate_id || ''));
    })
    .slice(0, 220)
    .map((row) => ({
      candidate_id: String(row.candidate_id || ''),
      parent_candidate_id: row.parent_candidate_id ? String(row.parent_candidate_id) : null,
      fractal_depth: Number(row.fractal_depth || 0),
      proposal_type: String(row.proposal_type || ''),
      mutation_kind: String(row.mutation_kind || ''),
      run_id: String(row.run_id || ''),
      last_stage: String(row.last_stage || ''),
      last_ts: String(row.last_ts || ''),
      stage_counts: row.stage_counts && typeof row.stage_counts === 'object' ? row.stage_counts : {},
      lineage_path: Array.isArray(row.lineage_path) ? row.lineage_path.slice(0, 12) : [],
      scorecard: row.scorecard && typeof row.scorecard === 'object' ? row.scorecard : {}
    }));
  const visibleCandidateIds = new Set(sortedCandidates.map((row) => String(row.candidate_id || '')));
  const lineageEdges = [];
  for (const row of sortedCandidates) {
    const from = String(row.parent_candidate_id || '').trim();
    const to = String(row.candidate_id || '').trim();
    if (!from || !to || !visibleCandidateIds.has(from) || !visibleCandidateIds.has(to)) continue;
    lineageEdges.push({
      from,
      to,
      relation: 'spawned'
    });
  }

  const latest = safeJsonRead(WORKFLOW_ORCHESTRON_LATEST_PATH, null);
  const latestPayload = latest && typeof latest === 'object' ? latest : {};
  const payload = {
    generated_at: nowIso(),
    available: fs.existsSync(WORKFLOW_BIRTH_EVENTS_PATH),
    file: fs.existsSync(WORKFLOW_BIRTH_EVENTS_PATH)
      ? path.relative(REPO_ROOT, WORKFLOW_BIRTH_EVENTS_PATH).replace(/\\/g, '/')
      : '',
    latest_file: fs.existsSync(WORKFLOW_ORCHESTRON_LATEST_PATH)
      ? path.relative(REPO_ROOT, WORKFLOW_ORCHESTRON_LATEST_PATH).replace(/\\/g, '/')
      : '',
    window_hours: h,
    events_total: recentEvents.length,
    stage_counts: stageCounts,
    runs_total: Object.keys(runCounts).length,
    latest_run_id: latestRunId || String(latestPayload.run_id || ''),
    candidates_total: sortedCandidates.length,
    lineage_nodes: sortedCandidates,
    lineage_edges: lineageEdges,
    events_recent: recentEvents.slice(0, 120)
  };

  WORKFLOW_BIRTH_CACHE = {
    ts: nowMs,
    hours: Number(h),
    payload
  };
  return cloneJson(payload);
}

function loadDoctorHealthSnapshot(hours = DEFAULT_HOURS) {
  const h = clampNumber(hours, 1, 24 * 30, DEFAULT_HOURS);
  const nowMs = Date.now();
  if (
    DOCTOR_HEALTH_CACHE.payload
    && Number(DOCTOR_HEALTH_CACHE.hours || 0) === Number(h)
    && (nowMs - Number(DOCTOR_HEALTH_CACHE.ts || 0)) < DOCTOR_HEALTH_CACHE_TTL_MS
  ) {
    return cloneJson(DOCTOR_HEALTH_CACHE.payload);
  }

  const cutoffMs = nowMs - (h * 60 * 60 * 1000);
  const rows = readJsonlRows(SYSTEM_HEALTH_EVENTS_PATH);
  const codeCounts = {};
  const stateCounts = {};
  const modulesMap = {};
  const recentEvents = [];
  let totalEvents = 0;

  for (let i = rows.length - 1; i >= 0; i -= 1) {
    const row = rows[i];
    if (!row || typeof row !== 'object') continue;
    if (String(row.type || '') !== 'system_health_event') continue;
    const code = String(row.code || '').trim().toLowerCase();
    const source = String(row.source || '').trim().toLowerCase();
    if (source !== 'autotest_doctor' && !code.startsWith('autotest_doctor_')) continue;
    const tsMs = parseTsMs(row.ts);
    if (tsMs != null && tsMs < cutoffMs) break;

    totalEvents += 1;
    if (code) codeCounts[code] = Number(codeCounts[code] || 0) + 1;
    const state = doctorStateFromEvent(row);
    if (state) stateCounts[state] = Number(stateCounts[state] || 0) + 1;

    const moduleRel = normalizeRelPath(
      row.module
      || row.module_path
      || row.rel
      || row.path
      || row.file
      || ''
    ).replace(/^\/+/, '');
    const signatureId = String(row.signature_id || '').trim();
    const severity = String(row.severity || 'medium').trim().toLowerCase() || 'medium';
    const summary = String(row.summary || '').trim();
    const risk = String(row.risk || '').trim().toLowerCase() || '';

    if (recentEvents.length < 220) {
      recentEvents.push({
        ts: String(row.ts || ''),
        code,
        state,
        severity,
        risk,
        module: moduleRel || '',
        signature_id: signatureId || '',
        summary,
        details: String(row.details || '').trim().slice(0, 220)
      });
    }

    const moduleKey = moduleRel || (signatureId ? `signature:${signatureId}` : '');
    if (!moduleKey) continue;
    if (!modulesMap[moduleKey]) {
      modulesMap[moduleKey] = {
        module: moduleRel || '',
        signature_id: signatureId || '',
        events_total: 0,
        latest_ts: '',
        latest_code: '',
        latest_state: '',
        latest_summary: '',
        latest_severity: 'low',
        max_severity: 'low',
        max_severity_rank: 0,
        state_counts: {},
        code_counts: {}
      };
    }
    const agg = modulesMap[moduleKey];
    agg.events_total += 1;
    if (signatureId && !agg.signature_id) agg.signature_id = signatureId;
    if (moduleRel && !agg.module) agg.module = moduleRel;
    if (code) agg.code_counts[code] = Number(agg.code_counts[code] || 0) + 1;
    if (state) agg.state_counts[state] = Number(agg.state_counts[state] || 0) + 1;
    const sevRank = severityRank(severity);
    if (sevRank >= Number(agg.max_severity_rank || 0)) {
      agg.max_severity_rank = sevRank;
      agg.max_severity = severity || agg.max_severity;
    }
    if (!agg.latest_ts) {
      agg.latest_ts = String(row.ts || '');
      agg.latest_code = code;
      agg.latest_state = state;
      agg.latest_summary = summary;
      agg.latest_severity = severity;
    }
  }

  const modules = Object.values(modulesMap)
    .map((row) => {
      const latestState = String(row.latest_state || '').trim().toLowerCase();
      return {
        module: String(row.module || '').trim(),
        signature_id: String(row.signature_id || '').trim(),
        events_total: Number(row.events_total || 0),
        latest_ts: String(row.latest_ts || ''),
        latest_code: String(row.latest_code || ''),
        latest_state: latestState,
        latest_summary: String(row.latest_summary || ''),
        latest_severity: String(row.latest_severity || row.max_severity || 'low'),
        max_severity: String(row.max_severity || 'low'),
        state_counts: row.state_counts && typeof row.state_counts === 'object' ? row.state_counts : {},
        code_counts: row.code_counts && typeof row.code_counts === 'object' ? row.code_counts : {},
        active: latestState === 'wounded' || latestState === 'rollback_cut',
        healing: latestState === 'healing',
        regrowth: latestState === 'regrowth'
      };
    })
    .sort((a, b) => {
      const aActive = a.active ? 1 : 0;
      const bActive = b.active ? 1 : 0;
      if (aActive !== bActive) return bActive - aActive;
      const at = Number(parseTsMs(a.latest_ts) || 0);
      const bt = Number(parseTsMs(b.latest_ts) || 0);
      if (Math.abs(at - bt) > 0.0001) return bt - at;
      return String(a.module || a.signature_id || '').localeCompare(String(b.module || b.signature_id || ''));
    })
    .slice(0, 260);

  const payload = {
    generated_at: nowIso(),
    available: fs.existsSync(SYSTEM_HEALTH_EVENTS_PATH),
    file: fs.existsSync(SYSTEM_HEALTH_EVENTS_PATH)
      ? path.relative(REPO_ROOT, SYSTEM_HEALTH_EVENTS_PATH).replace(/\\/g, '/')
      : '',
    window_hours: h,
    events_total: totalEvents,
    code_counts: codeCounts,
    state_counts: stateCounts,
    wounded_active: modules.filter((row) => row.active).length,
    healing_active: modules.filter((row) => row.healing).length,
    regrowth_recent: modules.filter((row) => row.regrowth).length,
    modules_total: modules.length,
    modules,
    events_recent: recentEvents.slice(0, 140)
  };

  DOCTOR_HEALTH_CACHE = {
    ts: nowMs,
    hours: Number(h),
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

function invalidateLayerModelCache() {
  MODULE_SCAN_CACHE = {
    ts: 0,
    payload: null
  };
  CHANGE_STATE_CACHE = {
    ts: 0,
    payload: null
  };
}

function invalidateCodegraphCache() {
  CODEGRAPH_CACHE = {
    ts: 0,
    payload: null
  };
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
  const bumpRel = (relPath, weight) => {
    const id = resolveAliasPath(aliasToId, relPath);
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
    bumpAlias('systems/ops', Math.max(0.2, Number(continuum.autotest_alerts_24h || 0) * 0.12));
    bumpAlias('state/ops', Math.max(0.15, Number(continuum.autotest_modules_changed || 0) * 0.08));
  }
  const doctor = summary && summary.doctor && typeof summary.doctor === 'object'
    ? summary.doctor
    : {};
  const doctorModules = Array.isArray(doctor.modules) ? doctor.modules : [];
  if (doctorModules.length > 0) {
    bumpAlias('systems/ops', Math.max(0.2, Number(doctor.events_total || 0) * 0.03));
    bumpAlias('state/ops', Math.max(0.15, Number(doctor.wounded_active || 0) * 0.16));
  }
  for (const row of doctorModules.slice(0, 180)) {
    const moduleRel = String(row && row.module || '').trim();
    if (!moduleRel) continue;
    const latestState = String(row && row.latest_state || '').trim().toLowerCase();
    const events = Math.max(1, Number(row && row.events_total || 1));
    let weight = 0.24 + (Math.min(8, events) * 0.05);
    if (latestState === 'wounded' || latestState === 'rollback_cut') {
      weight += 0.55;
    } else if (latestState === 'healing') {
      weight += 0.25;
    } else if (latestState === 'regrowth') {
      weight += 0.12;
    }
    bumpRel(moduleRel, weight);
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
        block_reason: '',
        doctor_state: ''
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
      const doctorState = String(meta.doctor_state || '').trim().toLowerCase();
      if (doctorState && !edge.doctor_state) edge.doctor_state = doctorState;
    }
  };
  const byAlias = (alias) => aliasToId[String(alias || '').toLowerCase()] || null;
  const byAliasPath = (relPath) => resolveAliasPath(aliasToId, relPath);

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
  const doctor = summary && summary.doctor && typeof summary.doctor === 'object'
    ? summary.doctor
    : {};
  const doctorModules = Array.isArray(doctor.modules) ? doctor.modules : [];
  if (doctorModules.length > 0) {
    const doctorSource = byAlias('systems/ops/autotest_doctor')
      || byAlias('systems/ops')
      || byAlias('systems');
    const doctorState = byAlias('state/ops') || byAlias('state') || byAlias('systems');
    if (doctorSource && doctorState) {
      add(doctorSource, doctorState, Math.max(0.4, Number(doctor.events_total || 0) * 0.12), 'doctor', {
        packet_tokens: Math.max(160, Number(doctor.events_total || 0) * 52),
        event_count: Math.max(1, Number(doctor.events_total || 0)),
        blocked: Number(doctor.wounded_active || 0) > 0,
        block_reason: Number(doctor.wounded_active || 0) > 0 ? 'doctor_active_wounded_modules' : '',
        doctor_state: Number(doctor.wounded_active || 0) > 0 ? 'wounded' : ''
      });
    }
    for (const row of doctorModules.slice(0, 160)) {
      const moduleRel = String(row && row.module || '').trim();
      if (!moduleRel || !doctorSource) continue;
      const target = byAliasPath(moduleRel);
      if (!target || target === doctorSource) continue;
      const latestState = String(row && row.latest_state || '').trim().toLowerCase();
      const events = Math.max(1, Number(row && row.events_total || 1));
      const blocked = latestState === 'wounded' || latestState === 'rollback_cut';
      const baseCount = latestState === 'wounded' || latestState === 'rollback_cut'
        ? 1.2
        : (latestState === 'healing' ? 0.9 : 0.7);
      add(doctorSource, target, Math.max(0.4, baseCount + (Math.min(8, events) * 0.08)), 'doctor', {
        packet_tokens: Math.max(150, events * 64),
        event_count: events,
        blocked,
        block_reason: blocked ? String(row && row.latest_code || 'doctor_blocked') : '',
        doctor_state: latestState
      });
    }
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
  const runtime = summary && summary.runtime && typeof summary.runtime === 'object'
    ? summary.runtime
    : {};
  const workflowBirth = summary && summary.workflow_birth && typeof summary.workflow_birth === 'object'
    ? summary.workflow_birth
    : {};
  const doctor = summary && summary.doctor && typeof summary.doctor === 'object'
    ? summary.doctor
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
      runtime_status: String(runtime.status || 'unknown'),
      runtime_online: runtime.online === true ? 1 : 0,
      runtime_stale: runtime.stale === true ? 1 : 0,
      runtime_signal_age_sec: Number(runtime.signal_age_sec == null ? 0 : runtime.signal_age_sec),
      runtime_live_window_minutes: Number(runtime.live_window_minutes || 0),
      runtime_activity_scale: Number(runtime.activity_scale || 0),
      continuum_autotest_failed_last: Number(continuum.autotest_failed_last || 0),
      continuum_autotest_guard_blocked_last: Number(continuum.autotest_guard_blocked_last || 0),
      continuum_autotest_untested_modules_last: Number(continuum.autotest_untested_modules_last || 0),
      continuum_autotest_modules_changed: Number(continuum.autotest_modules_changed || 0),
      continuum_autotest_alerts_24h: Number(continuum.autotest_alerts_24h || 0),
      continuum_autotest_failed_24h: Number(continuum.autotest_failed_24h || 0),
      continuum_autotest_guard_blocked_24h: Number(continuum.autotest_guard_blocked_24h || 0),
      workflow_birth_events_24h: Number(workflowBirth.events_total || 0),
      workflow_birth_candidates: Number(workflowBirth.candidates_total || 0),
      workflow_birth_grafted: Number(workflowBirth.stage_counts && workflowBirth.stage_counts.grafted || 0),
      workflow_birth_rewrites: Number(workflowBirth.stage_counts && workflowBirth.stage_counts.candidate_indexed || 0),
      doctor_events_24h: Number(doctor.events_total || 0),
      doctor_wounded_active: Number(doctor.wounded_active || 0),
      doctor_healing_active: Number(doctor.healing_active || 0),
      doctor_regrowth_recent: Number(doctor.regrowth_recent || 0),
      doctor_modules_total: Number(doctor.modules_total || 0),
      change_pending_push: pendingPush,
      change_just_pushed: justPushed,
      change_active_modules: Number(changeSummary.active_modules || 0),
      change_dirty_files_total: Number(changeSummary.dirty_files_total || 0),
      change_staged_files_total: Number(changeSummary.staged_files_total || 0),
      change_ahead_count: Number(changeSummary.ahead_count || 0)
    },
    doctor: {
      available: doctor.available === true,
      window_hours: Number(doctor.window_hours || 0),
      events_total: Number(doctor.events_total || 0),
      wounded_active: Number(doctor.wounded_active || 0),
      healing_active: Number(doctor.healing_active || 0),
      regrowth_recent: Number(doctor.regrowth_recent || 0),
      modules_total: Number(doctor.modules_total || 0),
      code_counts: doctor.code_counts && typeof doctor.code_counts === 'object' ? doctor.code_counts : {},
      state_counts: doctor.state_counts && typeof doctor.state_counts === 'object' ? doctor.state_counts : {},
      modules: Array.isArray(doctor.modules) ? doctor.modules.slice(0, 260) : [],
      events_recent: Array.isArray(doctor.events_recent) ? doctor.events_recent.slice(0, 140) : []
    },
    workflow_birth: {
      available: workflowBirth.available === true,
      window_hours: Number(workflowBirth.window_hours || 0),
      events_total: Number(workflowBirth.events_total || 0),
      candidates_total: Number(workflowBirth.candidates_total || 0),
      runs_total: Number(workflowBirth.runs_total || 0),
      latest_run_id: String(workflowBirth.latest_run_id || ''),
      stage_counts: workflowBirth.stage_counts && typeof workflowBirth.stage_counts === 'object'
        ? workflowBirth.stage_counts
        : {},
      lineage_nodes: Array.isArray(workflowBirth.lineage_nodes) ? workflowBirth.lineage_nodes.slice(0, 220) : [],
      lineage_edges: Array.isArray(workflowBirth.lineage_edges) ? workflowBirth.lineage_edges.slice(0, 320) : [],
      events_recent: Array.isArray(workflowBirth.events_recent) ? workflowBirth.events_recent.slice(0, 120) : []
    }
  };
}

function buildPayload(hours, liveMinutes = DEFAULT_LIVE_MINUTES, liveMode = true) {
  const telemetry = loadRecentTelemetry(hours, MAX_EVENTS);
  const directives = loadDirectiveSummary();
  const strategy = loadStrategySummary();
  const integrity = loadIntegrityStatus(telemetry.window_hours);
  const baseSummary = buildSummary(telemetry.runs, telemetry.audits, telemetry.window_hours, integrity);
  const constitution = buildConstitutionSnapshot(telemetry.runs, directives, strategy);
  const evolution = loadEvolutionSnapshot();
  const fractalSnapshot = loadFractalSnapshot();
  const continuumSnapshot = loadContinuumSnapshot();
  const runtimeWindowMinutes = runtimeWindowMinutesFromInput(telemetry.window_hours, liveMinutes, liveMode === true);
  const runtimeSpineEvents = loadRecentSpineEvents(Math.max(1 / 60, runtimeWindowMinutes / 60), 240);
  const runtimeSpineSignal = loadSpineRuntimeSignal(RUNTIME_SIGNAL_LOOKBACK_HOURS);
  const runtimeStatus = buildRuntimeStatus(runtimeWindowMinutes, runtimeSpineEvents, continuumSnapshot, runtimeSpineSignal);
  const workflowBirthSnapshot = loadWorkflowBirthSnapshot(telemetry.window_hours);
  const doctorSnapshot = loadDoctorHealthSnapshot(telemetry.window_hours);
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
    observer_mood_last: String(continuumSnapshot && continuumSnapshot.observer_mood_last || ''),
    autotest_available: continuumSnapshot && continuumSnapshot.autotest_available === true,
    autotest_last_run_ts: String(continuumSnapshot && continuumSnapshot.autotest_last_run_ts || ''),
    autotest_last_scope: String(continuumSnapshot && continuumSnapshot.autotest_last_scope || ''),
    autotest_selected_tests_last: Number(continuumSnapshot && continuumSnapshot.autotest_selected_tests_last || 0),
    autotest_failed_last: Number(continuumSnapshot && continuumSnapshot.autotest_failed_last || 0),
    autotest_guard_blocked_last: Number(continuumSnapshot && continuumSnapshot.autotest_guard_blocked_last || 0),
    autotest_untested_modules_last: Number(continuumSnapshot && continuumSnapshot.autotest_untested_modules_last || 0),
    autotest_modules_total: Number(continuumSnapshot && continuumSnapshot.autotest_modules_total || 0),
    autotest_modules_changed: Number(continuumSnapshot && continuumSnapshot.autotest_modules_changed || 0),
    autotest_tests_failed: Number(continuumSnapshot && continuumSnapshot.autotest_tests_failed || 0),
    autotest_tests_total: Number(continuumSnapshot && continuumSnapshot.autotest_tests_total || 0),
    autotest_alerts_24h: Number(continuumSnapshot && continuumSnapshot.autotest_alerts_24h || 0),
    autotest_failed_24h: Number(continuumSnapshot && continuumSnapshot.autotest_failed_24h || 0),
    autotest_guard_blocked_24h: Number(continuumSnapshot && continuumSnapshot.autotest_guard_blocked_24h || 0)
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
    continuum,
    runtime: runtimeStatus,
    workflow_birth: {
      available: workflowBirthSnapshot.available === true,
      file: String(workflowBirthSnapshot.file || ''),
      window_hours: Number(workflowBirthSnapshot.window_hours || telemetry.window_hours || 0),
      events_total: Number(workflowBirthSnapshot.events_total || 0),
      runs_total: Number(workflowBirthSnapshot.runs_total || 0),
      latest_run_id: String(workflowBirthSnapshot.latest_run_id || ''),
      candidates_total: Number(workflowBirthSnapshot.candidates_total || 0),
      stage_counts: workflowBirthSnapshot.stage_counts && typeof workflowBirthSnapshot.stage_counts === 'object'
        ? workflowBirthSnapshot.stage_counts
        : {},
      lineage_nodes: Array.isArray(workflowBirthSnapshot.lineage_nodes) ? workflowBirthSnapshot.lineage_nodes.slice(0, 220) : [],
      lineage_edges: Array.isArray(workflowBirthSnapshot.lineage_edges) ? workflowBirthSnapshot.lineage_edges.slice(0, 320) : [],
      events_recent: Array.isArray(workflowBirthSnapshot.events_recent) ? workflowBirthSnapshot.events_recent.slice(0, 120) : []
    },
    doctor: {
      available: doctorSnapshot.available === true,
      file: String(doctorSnapshot.file || ''),
      window_hours: Number(doctorSnapshot.window_hours || telemetry.window_hours || 0),
      events_total: Number(doctorSnapshot.events_total || 0),
      wounded_active: Number(doctorSnapshot.wounded_active || 0),
      healing_active: Number(doctorSnapshot.healing_active || 0),
      regrowth_recent: Number(doctorSnapshot.regrowth_recent || 0),
      modules_total: Number(doctorSnapshot.modules_total || 0),
      code_counts: doctorSnapshot.code_counts && typeof doctorSnapshot.code_counts === 'object'
        ? doctorSnapshot.code_counts
        : {},
      state_counts: doctorSnapshot.state_counts && typeof doctorSnapshot.state_counts === 'object'
        ? doctorSnapshot.state_counts
        : {},
      modules: Array.isArray(doctorSnapshot.modules) ? doctorSnapshot.modules.slice(0, 260) : [],
      events_recent: Array.isArray(doctorSnapshot.events_recent) ? doctorSnapshot.events_recent.slice(0, 140) : []
    }
  };
  const graph = buildGraph(telemetry.runs, directives, strategy);
  const holo = buildHoloModel(telemetry.runs, summary);
  return {
    ok: true,
    generated_at: nowIso(),
    live_mode: liveMode === true,
    live_minutes: Number(runtimeWindowMinutes),
    runtime: runtimeStatus,
    summary,
    graph,
    holo,
    constitution,
    evolution,
    fractal,
    continuum,
    workflow_birth: workflowBirthSnapshot,
    doctor_health: doctorSnapshot,
    fractal_snapshot: fractalSnapshot,
    continuum_snapshot: continuumSnapshot,
    incidents: {
      integrity
    }
  };
}

function buildSpinePulse(liveMinutes = DEFAULT_LIVE_MINUTES) {
  const liveM = clampLiveMinutes(liveMinutes, DEFAULT_LIVE_MINUTES);
  const events = loadRecentSpineEvents(Math.max(1 / 60, liveM / 60), 220);
  const typeCounts = {};
  for (const evt of events) {
    const type = String(evt && evt.type || 'unknown');
    typeCounts[type] = Number(typeCounts[type] || 0) + 1;
  }
  return {
    generated_at: nowIso(),
    live_window_minutes: Number(liveM),
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

function createHoloSnapshot(hours, liveMinutes, liveMode, reason) {
  const h = clampNumber(hours, 1, 24 * 30, DEFAULT_HOURS);
  const liveM = clampLiveMinutes(liveMinutes, DEFAULT_LIVE_MINUTES);
  const payload = buildPayload(h, liveM, liveMode === true);
  return {
    type: 'holo_snapshot',
    reason: String(reason || 'tick'),
    generated_at: payload.generated_at,
    live_mode: payload.live_mode === true,
    live_minutes: Number(payload.live_minutes || liveM),
    runtime: payload.runtime || (payload.summary && payload.summary.runtime) || null,
    summary: payload.summary,
    holo: payload.holo,
    incidents: payload.incidents || {},
    spine_pulse: buildSpinePulse(liveM)
  };
}

function createWsHub(server, defaultHours, defaultLiveMinutes) {
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
  const liveMinutesFor = (ws) => clampLiveMinutes(ws && ws.sub_live_minutes, defaultLiveMinutes);
  const liveModeFor = (ws) => (ws && ws.sub_live_mode === false ? false : true);

  const sendSnapshot = (ws, reason) => {
    const snapshot = createHoloSnapshot(hoursFor(ws), liveMinutesFor(ws), liveModeFor(ws), reason);
    wsSendJson(ws, snapshot);
  };

  const broadcast = (reason = 'tick') => {
    const openClients = Array.from(clients).filter((ws) => ws.readyState === 1);
    if (!openClients.length) return;
    const snapshotBySubscription = {};
    for (const ws of openClients) {
      const h = hoursFor(ws);
      const liveM = liveMinutesFor(ws);
      const liveMode = liveModeFor(ws);
      const key = `${h}:${liveM}:${liveMode ? 1 : 0}`;
      if (!snapshotBySubscription[key]) {
        snapshotBySubscription[key] = createHoloSnapshot(h, liveM, liveMode, reason);
      }
      wsSendJson(ws, snapshotBySubscription[key]);
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

  const watchTarget = (absDir, options = {}) => {
    if (!fs.existsSync(absDir)) return;
    const opts = options && typeof options === 'object' ? options : {};
    const invalidateLayerModel = opts.invalidate_layer_model === true;
    const invalidateCodegraph = opts.invalidate_codegraph === true;
    const reason = String(opts.reason || 'fswatch') || 'fswatch';
    const onFsChange = () => {
      if (invalidateLayerModel) invalidateLayerModelCache();
      if (invalidateCodegraph) invalidateCodegraphCache();
      scheduleBroadcast(reason);
    };
    try {
      const watcher = fs.watch(absDir, { persistent: false, recursive: true }, onFsChange);
      watchers.push(watcher);
    } catch {
      try {
        const watcher = fs.watch(absDir, { persistent: false }, onFsChange);
        watchers.push(watcher);
      } catch {
        // ignore watcher failures
      }
    }
  };
  watchTarget(RUNS_DIR);
  watchTarget(SPINE_RUNS_DIR);
  watchTarget(CONTINUUM_RUNS_DIR);
  watchTarget(CONTINUUM_EVENTS_DIR);
  for (const layerDef of LAYER_ROOTS) {
    const layerAbs = path.join(REPO_ROOT, layerDef.rel);
    watchTarget(layerAbs, {
      reason: 'module_tree_change',
      invalidate_layer_model: true,
      invalidate_codegraph: true
    });
  }

  wss.on('connection', (ws, req) => {
    let subHours = defaultHours;
    let subLiveMinutes = defaultLiveMinutes;
    let subLiveMode = true;
    try {
      const parsed = new URL(String(req && req.url || WS_PATH), `http://${DEFAULT_HOST}:${DEFAULT_PORT}`);
      const qHours = parseQueryNumber(parsed.searchParams, 'hours');
      if (Number.isFinite(qHours)) subHours = clampNumber(qHours, 1, 24 * 30, defaultHours);
      const qLiveMinutes = parseQueryNumber(parsed.searchParams, 'live_minutes');
      if (Number.isFinite(qLiveMinutes)) subLiveMinutes = clampLiveMinutes(qLiveMinutes, defaultLiveMinutes);
      const qLiveMode = parsed.searchParams.get('live_mode');
      if (qLiveMode != null) subLiveMode = parseBoolish(qLiveMode, true);
    } catch {
      // ignore malformed URL
    }
    ws.sub_hours = subHours;
    ws.sub_live_minutes = subLiveMinutes;
    ws.sub_live_mode = subLiveMode;
    clients.add(ws);
    sendSnapshot(ws, 'connect');

    ws.on('message', (raw) => {
      const msg = safeParseMessage(raw);
      if (!msg || typeof msg !== 'object') return;
      const type = String(msg.type || '').trim().toLowerCase();
      if (type === 'subscribe' || type === 'set_hours' || type === 'set_live') {
        const h = Number(msg.hours);
        if (Number.isFinite(h)) ws.sub_hours = clampNumber(h, 1, 24 * 30, defaultHours);
        const liveM = Number(msg.live_minutes);
        if (Number.isFinite(liveM)) ws.sub_live_minutes = clampLiveMinutes(liveM, defaultLiveMinutes);
        if (msg.live_mode != null) ws.sub_live_mode = parseBoolish(msg.live_mode, true);
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

function workspaceContainsPath(absPath) {
  const abs = path.resolve(String(absPath || REPO_ROOT));
  const rootPrefix = REPO_ROOT.endsWith(path.sep) ? REPO_ROOT : `${REPO_ROOT}${path.sep}`;
  return abs === REPO_ROOT || abs.startsWith(rootPrefix);
}

function normalizeTerminalCwd(absPath, fallback = REPO_ROOT) {
  const abs = path.resolve(String(absPath || fallback || REPO_ROOT));
  if (!workspaceContainsPath(abs)) return path.resolve(String(fallback || REPO_ROOT));
  return abs;
}

function terminalRel(absPath) {
  const rel = normalizeRelPath(path.relative(REPO_ROOT, String(absPath || REPO_ROOT)));
  return rel || '.';
}

function clipText(raw, limit = TERMINAL_OUTPUT_MAX_CHARS) {
  const text = String(raw == null ? '' : raw).replace(/\r\n/g, '\n');
  const maxChars = Math.max(512, Number(limit || TERMINAL_OUTPUT_MAX_CHARS));
  if (text.length <= maxChars) {
    return {
      text,
      truncated: false
    };
  }
  const headChars = Math.max(160, Math.round(maxChars * 0.82));
  const tailChars = Math.max(80, maxChars - headChars);
  const clipped = `${text.slice(0, headChars)}\n... [output truncated] ...\n${text.slice(-tailChars)}`;
  return {
    text: clipped,
    truncated: true
  };
}

function parseTerminalCwdMarker(rawStdout, marker) {
  const text = String(rawStdout == null ? '' : rawStdout);
  if (!marker) return { cleaned: text, cwd: null };
  const lines = text.split('\n');
  let cwd = null;
  const keep = [];
  for (const line of lines) {
    const row = String(line || '');
    if (row.startsWith(`${marker}=`)) {
      cwd = row.slice(marker.length + 1).trim();
      continue;
    }
    keep.push(row);
  }
  let cleaned = keep.join('\n');
  cleaned = cleaned.replace(/\n{3,}$/g, '\n\n');
  return {
    cleaned,
    cwd
  };
}

function terminalStatePayload(extra = null) {
  const cwdAbs = normalizeTerminalCwd(TERMINAL_STATE.cwd, REPO_ROOT);
  const payload = {
    ok: true,
    cwd_abs: cwdAbs,
    cwd_rel: terminalRel(cwdAbs),
    updated_at: String(TERMINAL_STATE.updated_at || nowIso()),
    last_exit_code: Number(TERMINAL_STATE.last_exit_code || 0),
    last_command: String(TERMINAL_STATE.last_command || '')
  };
  if (extra && typeof extra === 'object') Object.assign(payload, extra);
  return payload;
}

function resolveTerminalTarget(rawPath, baseCwd) {
  const text = String(rawPath || '').trim();
  if (!text) return normalizeTerminalCwd(baseCwd, REPO_ROOT);
  const cwdBase = normalizeTerminalCwd(baseCwd, REPO_ROOT);
  const abs = path.isAbsolute(text)
    ? path.resolve(text)
    : path.resolve(cwdBase, text);
  if (!workspaceContainsPath(abs)) return null;
  let stat = null;
  try {
    stat = fs.statSync(abs);
  } catch {
    return null;
  }
  if (stat && stat.isDirectory()) return abs;
  if (stat && stat.isFile()) return path.dirname(abs);
  return null;
}

function setTerminalCwd(rawPath, source = 'api') {
  const next = resolveTerminalTarget(rawPath, TERMINAL_STATE.cwd);
  if (!next) {
    return {
      ok: false,
      error: 'invalid_cwd_target'
    };
  }
  TERMINAL_STATE.cwd = normalizeTerminalCwd(next, REPO_ROOT);
  TERMINAL_STATE.updated_at = nowIso();
  return terminalStatePayload({
    source: String(source || 'api')
  });
}

function execTerminalCommand(rawCmd) {
  const command = String(rawCmd == null ? '' : rawCmd).trim();
  if (!command) {
    return {
      ok: false,
      error: 'empty_command'
    };
  }

  const cwdBefore = normalizeTerminalCwd(TERMINAL_STATE.cwd, REPO_ROOT);
  const marker = `__PROTHEUS_CWD_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}__`;
  const script = `${command}\nprintf '\\n${marker}=%s\\n' "$PWD"`;
  const proc = spawnSync('/bin/zsh', ['-lc', script], {
    cwd: cwdBefore,
    encoding: 'utf8',
    timeout: TERMINAL_CMD_TIMEOUT_MS,
    maxBuffer: TERMINAL_MAX_BUFFER_BYTES,
    env: process.env
  });

  const parsed = parseTerminalCwdMarker(proc.stdout, marker);
  const stdoutClip = clipText(parsed.cleaned, TERMINAL_OUTPUT_MAX_CHARS);
  const stderrClip = clipText(proc.stderr, TERMINAL_OUTPUT_MAX_CHARS);
  const markerCwd = parsed.cwd && workspaceContainsPath(parsed.cwd)
    ? normalizeTerminalCwd(parsed.cwd, cwdBefore)
    : cwdBefore;
  TERMINAL_STATE.cwd = markerCwd;
  TERMINAL_STATE.updated_at = nowIso();
  TERMINAL_STATE.last_command = command;
  TERMINAL_STATE.last_exit_code = Number(proc.status == null ? 1 : proc.status);
  const timedOut = Boolean(proc.error && String(proc.error.code || '').toUpperCase() === 'ETIMEDOUT');

  return terminalStatePayload({
    executed: true,
    command,
    exit_code: Number(proc.status == null ? 1 : proc.status),
    timed_out: timedOut,
    signal: proc.signal ? String(proc.signal) : '',
    error: proc.error ? String(proc.error && proc.error.message ? proc.error.message : proc.error) : '',
    stdout: stdoutClip.text,
    stderr: stderrClip.text,
    stdout_truncated: stdoutClip.truncated === true,
    stderr_truncated: stderrClip.truncated === true,
    cwd_before_abs: cwdBefore,
    cwd_before_rel: terminalRel(cwdBefore)
  });
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

function codegraphNodeId(rel) {
  return `file:${normalizeRelPath(rel)}`;
}

function codegraphLayerFromRel(rel) {
  const clean = normalizeRelPath(rel);
  if (!clean) return '';
  const seg = String(clean.split('/')[0] || '').trim();
  return seg;
}

function codegraphModuleFromRel(rel) {
  const clean = normalizeRelPath(rel);
  if (!clean) return '';
  const parts = clean.split('/').filter(Boolean);
  if (!parts.length) return '';
  if (parts.length === 1) return parts[0];
  return `${parts[0]}/${parts[1]}`;
}

function codegraphIsPathLikeToken(text) {
  const s = String(text || '').trim();
  if (!s) return false;
  if (s.includes('/')) return true;
  if (/\.[a-z0-9]{1,6}$/i.test(s)) return true;
  return false;
}

function shouldIncludeCodegraphFile(fileName) {
  const name = String(fileName || '').trim();
  if (!name || name.startsWith('.')) return false;
  const ext = path.extname(name).toLowerCase();
  if (!ext) return false;
  return CODEGRAPH_FILE_EXTS.has(ext);
}

function safeReadTextPrefix(absPath, maxBytes = CODEGRAPH_MAX_FILE_BYTES) {
  if (!absPath || !fs.existsSync(absPath)) return '';
  let stat = null;
  try {
    stat = fs.statSync(absPath);
  } catch {
    stat = null;
  }
  if (!stat || !stat.isFile()) return '';
  const readBytes = Math.max(0, Math.min(Number(stat.size || 0), Number(maxBytes || CODEGRAPH_MAX_FILE_BYTES)));
  if (readBytes <= 0) return '';
  let fileBuffer = Buffer.alloc(0);
  const fd = fs.openSync(absPath, 'r');
  try {
    fileBuffer = Buffer.alloc(readBytes);
    const bytesRead = fs.readSync(fd, fileBuffer, 0, readBytes, 0);
    if (bytesRead < readBytes) fileBuffer = fileBuffer.slice(0, bytesRead);
  } catch {
    return '';
  } finally {
    try { fs.closeSync(fd); } catch {}
  }
  if (!likelyTextBuffer(fileBuffer)) return '';
  return String(fileBuffer.toString('utf8') || '');
}

function safeReadTextFullUnderLimit(absPath, maxBytes = CODEGRAPH_MAX_FILE_BYTES) {
  if (!absPath || !fs.existsSync(absPath)) return '';
  let stat = null;
  try {
    stat = fs.statSync(absPath);
  } catch {
    stat = null;
  }
  if (!stat || !stat.isFile()) return '';
  const size = Math.max(0, Number(stat.size || 0));
  const cap = Math.max(1, Number(maxBytes || CODEGRAPH_MAX_FILE_BYTES));
  if (size <= 0 || size > cap) return '';
  let fileBuffer = Buffer.alloc(0);
  try {
    fileBuffer = fs.readFileSync(absPath);
  } catch {
    return '';
  }
  if (!likelyTextBuffer(fileBuffer)) return '';
  return String(fileBuffer.toString('utf8') || '');
}

function loadTypeScriptModule() {
  if (TYPESCRIPT_MODULE_CACHE.tried) return TYPESCRIPT_MODULE_CACHE.mod;
  TYPESCRIPT_MODULE_CACHE.tried = true;
  let mod = null;
  try {
    mod = require('typescript');
  } catch {
    mod = null;
  }
  TYPESCRIPT_MODULE_CACHE.mod = mod;
  return mod;
}

function tsScriptKindForExt(ts, ext) {
  const e = String(ext || '').toLowerCase();
  if (e === '.ts') return ts.ScriptKind.TS;
  if (e === '.tsx') return ts.ScriptKind.TSX;
  if (e === '.js') return ts.ScriptKind.JS;
  if (e === '.jsx') return ts.ScriptKind.JSX;
  if (e === '.mjs') return ts.ScriptKind.JS;
  if (e === '.cjs') return ts.ScriptKind.JS;
  return ts.ScriptKind.Unknown;
}

function tsNodeHasModifier(ts, node, kind) {
  if (!node || !Array.isArray(node.modifiers)) return false;
  for (const mod of node.modifiers) {
    if (mod && mod.kind === kind) return true;
  }
  return false;
}

function tsCollectBindingNames(ts, nameNode, out) {
  const rows = Array.isArray(out) ? out : [];
  if (!nameNode) return rows;
  if (ts.isIdentifier(nameNode)) {
    rows.push(String(nameNode.text || '').trim());
    return rows;
  }
  if (ts.isObjectBindingPattern(nameNode) || ts.isArrayBindingPattern(nameNode)) {
    for (const el of nameNode.elements || []) {
      if (!el) continue;
      if (ts.isBindingElement(el)) {
        tsCollectBindingNames(ts, el.name, rows);
      }
    }
  }
  return rows;
}

function parseTsAstInfo(sourceText, ext, fileName = 'file.ts') {
  const ts = loadTypeScriptModule();
  const src = String(sourceText || '');
  const result = {
    ok: false,
    imports: [],
    import_bindings: [],
    exports: [],
    symbols: [],
    call_hints: []
  };
  if (!ts || !src.trim()) return result;

  try {
    const importSet = new Set();
    const exportSet = new Set();
    const symbolSet = new Set();
    const bindingByLocal = {};
    const callRows = [];
    const scriptKind = tsScriptKindForExt(ts, ext);
    const sf = ts.createSourceFile(
      String(fileName || 'file.ts'),
      src,
      ts.ScriptTarget.Latest,
      true,
      scriptKind
    );

    const addImport = (raw) => {
      const spec = String(raw || '').trim();
      if (!spec) return;
      if (spec.startsWith('node:')) return;
      importSet.add(spec);
    };
    const addBinding = (localRaw, specRaw, importedRaw, kindRaw) => {
      const local = String(localRaw || '').trim();
      const spec = String(specRaw || '').trim();
      const imported = String(importedRaw || '').trim();
      const kind = String(kindRaw || '').trim() || 'named';
      if (!local || !spec) return;
      bindingByLocal[local] = {
        local,
        spec,
        imported: imported || local,
        kind
      };
    };
    const addSymbol = (raw) => {
      const name = String(raw || '').trim();
      if (!name) return;
      symbolSet.add(name);
    };
    const addExport = (raw) => {
      const name = String(raw || '').trim();
      if (!name) return;
      exportSet.add(name);
    };
    const addCallHint = (payload) => {
      if (!payload || typeof payload !== 'object') return;
      const spec = String(payload.spec || '').trim();
      const imported = String(payload.imported || '').trim();
      const local = String(payload.local || '').trim();
      const via = String(payload.via || '').trim() || 'binding';
      if (!spec) return;
      callRows.push({
        spec,
        imported,
        local,
        via,
        call_name: String(payload.call_name || '').trim()
      });
    };

    const visit = (node) => {
      if (!node) return;
      if (ts.isImportDeclaration(node) && node.moduleSpecifier && ts.isStringLiteral(node.moduleSpecifier)) {
        const spec = String(node.moduleSpecifier.text || '').trim();
        addImport(spec);
        const clause = node.importClause;
        if (clause) {
          if (clause.name) {
            addBinding(String(clause.name.text || ''), spec, 'default', 'default');
          }
          const named = clause.namedBindings;
          if (named) {
            if (ts.isNamespaceImport(named)) {
              addBinding(String(named.name && named.name.text || ''), spec, '*', 'namespace');
            } else if (ts.isNamedImports(named)) {
              for (const el of named.elements || []) {
                if (!el) continue;
                const local = String(el.name && el.name.text || '').trim();
                const imported = String(el.propertyName && el.propertyName.text || el.name && el.name.text || '').trim();
                addBinding(local, spec, imported, 'named');
              }
            }
          }
        }
      } else if (ts.isExportDeclaration(node)) {
        if (node.moduleSpecifier && ts.isStringLiteral(node.moduleSpecifier)) {
          addImport(String(node.moduleSpecifier.text || ''));
        }
        if (node.exportClause && ts.isNamedExports(node.exportClause)) {
          for (const el of node.exportClause.elements || []) {
            if (!el) continue;
            addExport(String(el.name && el.name.text || ''));
          }
        }
      } else if (ts.isFunctionDeclaration(node)) {
        if (node.name) addSymbol(String(node.name.text || ''));
        if (tsNodeHasModifier(ts, node, ts.SyntaxKind.ExportKeyword) && node.name) {
          addExport(String(node.name.text || ''));
        }
        if (tsNodeHasModifier(ts, node, ts.SyntaxKind.DefaultKeyword)) addExport('default');
      } else if (ts.isClassDeclaration(node)) {
        if (node.name) addSymbol(String(node.name.text || ''));
        if (tsNodeHasModifier(ts, node, ts.SyntaxKind.ExportKeyword) && node.name) {
          addExport(String(node.name.text || ''));
        }
        if (tsNodeHasModifier(ts, node, ts.SyntaxKind.DefaultKeyword)) addExport('default');
      } else if (ts.isInterfaceDeclaration(node) || ts.isTypeAliasDeclaration(node) || ts.isEnumDeclaration(node)) {
        if (node.name) addSymbol(String(node.name.text || ''));
        if (tsNodeHasModifier(ts, node, ts.SyntaxKind.ExportKeyword) && node.name) {
          addExport(String(node.name.text || ''));
        }
      } else if (ts.isVariableStatement(node)) {
        const declList = node.declarationList;
        for (const decl of declList.declarations || []) {
          if (!decl) continue;
          const names = tsCollectBindingNames(ts, decl.name, []);
          for (const name of names) {
            addSymbol(name);
            if (tsNodeHasModifier(ts, node, ts.SyntaxKind.ExportKeyword)) addExport(name);
          }
          const init = decl.initializer;
          if (
            init
            && ts.isCallExpression(init)
            && ts.isIdentifier(init.expression)
            && String(init.expression.text || '').trim() === 'require'
          ) {
            const arg = Array.isArray(init.arguments) ? init.arguments[0] : null;
            if (arg && ts.isStringLiteral(arg)) {
              const spec = String(arg.text || '').trim();
              addImport(spec);
              if (ts.isIdentifier(decl.name)) {
                addBinding(String(decl.name.text || '').trim(), spec, '*', 'namespace');
              } else if (ts.isObjectBindingPattern(decl.name)) {
                for (const el of decl.name.elements || []) {
                  if (!el || !ts.isBindingElement(el)) continue;
                  const local = String(el.name && el.name.text || '').trim();
                  const imported = String(el.propertyName && el.propertyName.text || el.name && el.name.text || '').trim();
                  addBinding(local, spec, imported || local, 'named');
                }
              }
            }
          }
        }
      } else if (ts.isCallExpression(node)) {
        if (node.expression && node.expression.kind === ts.SyntaxKind.ImportKeyword) {
          const arg = Array.isArray(node.arguments) ? node.arguments[0] : null;
          if (arg && ts.isStringLiteral(arg)) addImport(String(arg.text || ''));
        } else if (ts.isIdentifier(node.expression)) {
          const callee = String(node.expression.text || '').trim();
          if (callee === 'require') {
            const arg = Array.isArray(node.arguments) ? node.arguments[0] : null;
            if (arg && ts.isStringLiteral(arg)) addImport(String(arg.text || ''));
          }
          const binding = bindingByLocal[callee];
          if (binding) {
            addCallHint({
              spec: binding.spec,
              imported: binding.imported,
              local: binding.local,
              via: 'binding',
              call_name: callee
            });
          }
        } else if (ts.isPropertyAccessExpression(node.expression)) {
          const expr = node.expression;
          if (ts.isIdentifier(expr.expression)) {
            const root = String(expr.expression.text || '').trim();
            const prop = String(expr.name && expr.name.text || '').trim();
            const binding = bindingByLocal[root];
            if (binding && binding.kind === 'namespace') {
              addCallHint({
                spec: binding.spec,
                imported: prop,
                local: root,
                via: 'namespace',
                call_name: `${root}.${prop}`
              });
            }
          }
        }
      }
      ts.forEachChild(node, visit);
    };
    visit(sf);

    result.ok = true;
    result.imports = Array.from(importSet).slice(0, 220);
    result.import_bindings = Object.values(bindingByLocal).slice(0, 260);
    result.exports = Array.from(exportSet).slice(0, 220);
    result.symbols = Array.from(symbolSet).slice(0, 320);
    result.call_hints = callRows.slice(0, 320);
    return result;
  } catch {
    return result;
  }
}

function scanCodegraphFiles() {
  const files = [];
  const stack = [{ abs: REPO_ROOT, rel: '', depth: 0 }];
  let truncated = false;
  while (stack.length > 0) {
    const cur = stack.pop();
    if (!cur) continue;
    if (cur.depth > CODEGRAPH_MAX_DEPTH) {
      truncated = true;
      continue;
    }
    const entries = safeReadDirWithTypes(cur.abs).sort(direntSort);
    for (const ent of entries) {
      if (files.length >= CODEGRAPH_MAX_FILES) {
        truncated = true;
        break;
      }
      const name = String(ent && ent.name || '').trim();
      if (!name || name.startsWith('.')) continue;
      const rel = normalizeRelPath(cur.rel ? `${cur.rel}/${name}` : name);
      const abs = path.join(cur.abs, name);
      if (ent.isDirectory()) {
        if (CODEGRAPH_SKIP_DIRS.has(name.toLowerCase())) continue;
        stack.push({
          abs,
          rel,
          depth: cur.depth + 1
        });
        continue;
      }
      if (!ent.isFile()) continue;
      if (!shouldIncludeCodegraphFile(name)) continue;
      files.push(rel);
    }
    if (truncated) break;
  }
  files.sort((a, b) => a.localeCompare(b));
  return {
    files: Array.from(new Set(files)),
    truncated
  };
}

function parseImportSpecsFromSource(sourceText, ext) {
  const src = String(sourceText || '');
  const out = new Set();
  if (!src.trim()) return [];
  const e = String(ext || '').toLowerCase();
  const push = (raw) => {
    const spec = String(raw || '').trim();
    if (!spec) return;
    if (spec.startsWith('node:')) return;
    out.add(spec);
  };

  if (e === '.py') {
    const fromRe = /^\s*from\s+([a-zA-Z0-9_./-]+)\s+import\s+/gm;
    const importRe = /^\s*import\s+([a-zA-Z0-9_.,\s/-]+)$/gm;
    let m = null;
    while ((m = fromRe.exec(src)) != null) push(m[1]);
    while ((m = importRe.exec(src)) != null) {
      const terms = String(m[1] || '').split(',').map((t) => String(t || '').trim()).filter(Boolean);
      for (const term of terms) push(term);
    }
    return Array.from(out).slice(0, 140);
  }

  const importFromRe = /(?:import|export)\s+(?:[^'"]+?\s+from\s+)?["']([^"']+)["']/g;
  const requireRe = /require\(\s*["']([^"']+)["']\s*\)/g;
  const dynamicImportRe = /import\(\s*["']([^"']+)["']\s*\)/g;
  let m = null;
  while ((m = importFromRe.exec(src)) != null) push(m[1]);
  while ((m = requireRe.exec(src)) != null) push(m[1]);
  while ((m = dynamicImportRe.exec(src)) != null) push(m[1]);
  return Array.from(out).slice(0, 160);
}

function resolveCodegraphPathCandidate(candidateRel, relSet) {
  const raw = normalizeRelPath(candidateRel).replace(/^\//, '');
  if (!raw) return '';
  if (relSet.has(raw)) return raw;
  const ext = path.extname(raw).toLowerCase();
  if (!ext) {
    for (const ex of CODEGRAPH_FILE_EXTS) {
      const withExt = `${raw}${ex}`;
      if (relSet.has(withExt)) return withExt;
    }
    for (const ex of CODEGRAPH_FILE_EXTS) {
      const idx = `${raw}/index${ex}`;
      if (relSet.has(idx)) return idx;
    }
  }
  return '';
}

function resolveCodegraphImport(fromRel, importSpec, relSet) {
  const spec = String(importSpec || '').trim();
  if (!spec) return '';
  if (spec.startsWith('.')) {
    const baseDir = normalizeRelPath(path.dirname(String(fromRel || '')));
    const joined = normalizeRelPath(path.join(baseDir, spec));
    return resolveCodegraphPathCandidate(joined, relSet);
  }
  if (spec.startsWith('/')) {
    return resolveCodegraphPathCandidate(spec, relSet);
  }
  const parts = spec.split('/').filter(Boolean);
  if (!parts.length) return '';
  const head = String(parts[0] || '').toLowerCase();
  const layerKnown = LAYER_ROOTS.some((row) => String(row && row.rel || '').toLowerCase() === head);
  if (layerKnown || codegraphIsPathLikeToken(spec)) {
    return resolveCodegraphPathCandidate(spec, relSet);
  }
  return '';
}

function codegraphTokenize(text) {
  return String(text || '')
    .toLowerCase()
    .split(/[^a-z0-9_./-]+/g)
    .map((s) => s.trim())
    .filter((s) => s.length >= 2);
}

function buildCodegraph() {
  const scan = scanCodegraphFiles();
  const files = Array.isArray(scan.files) ? scan.files : [];
  const relSet = new Set(files);
  const tsMod = loadTypeScriptModule();
  const astEnabled = Boolean(tsMod);
  let astAttemptedFiles = 0;
  let astParsedFiles = 0;
  let astFailedFiles = 0;
  const nodes = [];
  const edges = [];
  const externalCounts = {};

  for (const rel of files) {
    const abs = path.join(REPO_ROOT, rel);
    let stat = null;
    try {
      stat = fs.statSync(abs);
    } catch {
      stat = null;
    }
    const layer = codegraphLayerFromRel(rel);
    const moduleName = codegraphModuleFromRel(rel);
    const fileName = path.basename(rel);
    const node = {
      id: codegraphNodeId(rel),
      rel,
      abs,
      layer,
      module: moduleName,
      file_name: fileName,
      ext: path.extname(fileName).toLowerCase(),
      size_bytes: stat && stat.isFile() ? Math.max(0, Number(stat.size || 0)) : 0,
      mtime_ms: stat && stat.mtimeMs ? Number(stat.mtimeMs) : 0,
      parser: 'none',
      ast_parsed: false,
      import_specs: [],
      exports: [],
      symbols: [],
      tokens: codegraphTokenize(`${rel} ${layer} ${moduleName} ${fileName}`),
      imports_count: 0,
      exports_count: 0,
      symbols_count: 0
    };
    nodes.push(node);
  }

  const nodeByRel = {};
  for (const node of nodes) nodeByRel[String(node.rel || '')] = node;

  for (const node of nodes) {
    const ext = String(node.ext || '').toLowerCase();
    const canScanImports = CODEGRAPH_IMPORT_SCAN_EXTS.has(ext);
    if (!canScanImports) continue;
    let specs = [];
    let exportsList = [];
    let symbolsList = [];
    let callHints = [];
    let parsedViaAst = false;

    const isTsAstTarget = ext === '.ts'
      || ext === '.tsx'
      || ext === '.js'
      || ext === '.jsx'
      || ext === '.mjs'
      || ext === '.cjs';
    const srcFull = safeReadTextFullUnderLimit(node.abs, CODEGRAPH_MAX_FILE_BYTES);
    if (isTsAstTarget && astEnabled && srcFull) {
      astAttemptedFiles += 1;
      const parsed = parseTsAstInfo(srcFull, ext, node.rel);
      if (parsed.ok) {
        parsedViaAst = true;
        astParsedFiles += 1;
        specs = Array.isArray(parsed.imports) ? parsed.imports : [];
        callHints = Array.isArray(parsed.call_hints) ? parsed.call_hints : [];
        exportsList = Array.isArray(parsed.exports) ? parsed.exports : [];
        symbolsList = Array.isArray(parsed.symbols) ? parsed.symbols : [];
      } else {
        astFailedFiles += 1;
      }
    }
    if (!parsedViaAst) {
      const src = srcFull || safeReadTextPrefix(node.abs, CODEGRAPH_MAX_FILE_BYTES);
      if (!src) continue;
      specs = parseImportSpecsFromSource(src, ext);
    }

    node.parser = parsedViaAst ? 'typescript_ast' : 'regex_fallback';
    node.ast_parsed = parsedViaAst;
    node.import_specs = Array.from(new Set(specs.map((v) => String(v || '').trim()).filter(Boolean))).slice(0, 260);
    node.exports = Array.from(new Set(exportsList.map((v) => String(v || '').trim()).filter(Boolean))).slice(0, 260);
    node.symbols = Array.from(new Set(symbolsList.map((v) => String(v || '').trim()).filter(Boolean))).slice(0, 420);
    node.imports_count = node.import_specs.length;
    node.exports_count = node.exports.length;
    node.symbols_count = node.symbols.length;
    if (node.symbols.length || node.exports.length) {
      node.tokens = codegraphTokenize([
        `${node.rel} ${node.layer} ${node.module} ${node.file_name}`,
        node.symbols.join(' '),
        node.exports.join(' ')
      ].join(' '));
    }

    for (const spec of node.import_specs) {
      const resolvedRel = resolveCodegraphImport(node.rel, spec, relSet);
      if (resolvedRel) {
        const toNode = nodeByRel[resolvedRel];
        if (!toNode) continue;
        edges.push({
          id: `${node.id}->${toNode.id}|import|${String(spec || '')}`,
          kind: 'import',
          from_id: node.id,
          to_id: toNode.id,
          from_rel: node.rel,
          to_rel: toNode.rel,
          spec: String(spec || '')
        });
      } else {
        const key = String(spec.split('/')[0] || spec || '').trim().toLowerCase();
        if (!key) continue;
        externalCounts[key] = Number(externalCounts[key] || 0) + 1;
      }
    }

    if (callHints.length > 0) {
      for (const hint of callHints) {
        const spec = String(hint && hint.spec || '').trim();
        if (!spec) continue;
        const resolvedRel = resolveCodegraphImport(node.rel, spec, relSet);
        if (!resolvedRel) continue;
        const toNode = nodeByRel[resolvedRel];
        if (!toNode) continue;
        const symbol = String(hint && hint.imported || '').trim();
        edges.push({
          id: `${node.id}->${toNode.id}|call|${symbol || 'unknown'}`,
          kind: 'call',
          from_id: node.id,
          to_id: toNode.id,
          from_rel: node.rel,
          to_rel: toNode.rel,
          spec,
          symbol,
          via: String(hint && hint.via || '').trim() || 'binding',
          call_name: String(hint && hint.call_name || '').trim()
        });
      }
    }
  }

  const dedup = {};
  const uniqEdges = [];
  for (const edge of edges) {
    const key = `${edge.from_id}|${edge.to_id}|${edge.kind}|${String(edge.spec || '')}|${String(edge.symbol || '')}`;
    if (dedup[key]) continue;
    dedup[key] = true;
    uniqEdges.push(edge);
  }
  const importEdgeCount = uniqEdges.filter((row) => String(row && row.kind || '') === 'import').length;
  const callEdgeCount = uniqEdges.filter((row) => String(row && row.kind || '') === 'call').length;

  const externalTop = Object.keys(externalCounts)
    .map((key) => [key, Number(externalCounts[key] || 0)])
    .sort((a, b) => {
      if (Math.abs(Number(a[1] || 0) - Number(b[1] || 0)) > 0.0001) return Number(b[1] || 0) - Number(a[1] || 0);
      return String(a[0] || '').localeCompare(String(b[0] || ''));
    })
    .slice(0, 24)
    .map(([name, count]) => ({ name, count }));

  return {
    generated_at: nowIso(),
    nodes,
    edges: uniqEdges,
    external_top: externalTop,
    files_scanned: files.length,
    files_truncated: scan.truncated === true,
    import_edge_count: importEdgeCount,
    call_edge_count: callEdgeCount,
    ast_enabled: astEnabled,
    ast_attempted_files: astAttemptedFiles,
    ast_parsed_files: astParsedFiles,
    ast_failed_files: astFailedFiles
  };
}

function loadCodegraphCached(force = false) {
  const nowMs = Date.now();
  if (
    !force
    && CODEGRAPH_CACHE.payload
    && (nowMs - Number(CODEGRAPH_CACHE.ts || 0)) < CODEGRAPH_CACHE_TTL_MS
  ) {
    return cloneJson(CODEGRAPH_CACHE.payload);
  }
  const payload = buildCodegraph();
  CODEGRAPH_CACHE = {
    ts: nowMs,
    payload
  };
  return cloneJson(payload);
}

function scoreCodegraphNode(node, queryLower, queryTokens) {
  const rel = String(node && node.rel || '').toLowerCase();
  const layer = String(node && node.layer || '').toLowerCase();
  const moduleName = String(node && node.module || '').toLowerCase();
  const fileName = String(node && node.file_name || '').toLowerCase();
  const symbols = Array.isArray(node && node.symbols) ? node.symbols : [];
  const exportsList = Array.isArray(node && node.exports) ? node.exports : [];
  const symbolsText = symbols.map((v) => String(v || '').toLowerCase()).join(' ');
  const exportsText = exportsList.map((v) => String(v || '').toLowerCase()).join(' ');
  const all = `${rel} ${layer} ${moduleName} ${fileName} ${symbolsText} ${exportsText}`;
  let score = 0;
  if (queryLower && rel.includes(queryLower)) score += 8;
  if (queryLower && fileName.includes(queryLower)) score += 4;
  if (queryLower && symbolsText.includes(queryLower)) score += 4.2;
  if (queryLower && exportsText.includes(queryLower)) score += 4.6;
  for (const tok of queryTokens) {
    if (!tok) continue;
    if (rel.includes(tok)) score += 2.6;
    else if (all.includes(tok)) score += 1.2;
    if (fileName.startsWith(tok)) score += 1.4;
    if (symbolsText.includes(tok)) score += 1.8;
    if (exportsText.includes(tok)) score += 1.9;
  }
  if (codegraphIsPathLikeToken(queryLower) && rel.endsWith(queryLower)) score += 4;
  return score;
}

function inferCodegraphMode(query, modeRaw) {
  const mode = String(modeRaw || '').trim().toLowerCase();
  if (mode === 'search' || mode === 'callers' || mode === 'callees') return mode;
  const q = String(query || '').toLowerCase();
  if (/(callers?\s+of|used\s+by|imports?\s+of|depends?\s+on\s+me|who\s+calls?|called\s+by)/.test(q)) return 'callers';
  if (/(dependencies?\s+of|what\s+does.+import|imports?\s+from|outgoing|callees?\s+of|what\s+does.+call|calls?\s+from)/.test(q)) return 'callees';
  if (/(who|which)\s+.*(imports?|uses?|depends?|calls?)/.test(q)) return 'callers';
  return 'search';
}

function inferCodegraphRelationKindHint(query) {
  const q = String(query || '').toLowerCase();
  if (!q) return '';
  if (/\b(call|calls|called|callee|callees|outgoing call)\b/.test(q)) return 'call';
  if (/\b(import|imports|dependency|dependencies|depends)\b/.test(q)) return 'import';
  return '';
}

function extractQueryQuotedTerm(query) {
  const q = String(query || '');
  const match = q.match(/["'`](.+?)["'`]/);
  if (!match) return '';
  return String(match[1] || '').trim();
}

function extractRelationTarget(query, mode) {
  const q = String(query || '').trim();
  if (!q) return '';
  const quoted = extractQueryQuotedTerm(q);
  if (quoted) return quoted;
  const lower = q.toLowerCase();
  if (String(mode || '').toLowerCase() === 'callees') {
    const m = lower.match(/\b(?:calls?\s+from|callees?\s+of|dependencies?\s+of|imports?\s+from)\s+([a-z0-9_./-]+(?:\.[a-z0-9_]+)?)/i);
    if (m && m[1]) return String(m[1]).trim();
  }
  if (String(mode || '').toLowerCase() === 'callers') {
    const m = lower.match(/\b(?:callers?\s+of|called\s+by|used\s+by|imports?\s+of)\s+([a-z0-9_./-]+(?:\.[a-z0-9_]+)?)/i);
    if (m && m[1]) return String(m[1]).trim();
  }
  const callMatch = lower.match(/\b(?:calls?|called\s+by|uses?|imports?|depends?\s+on)\s+([a-z0-9_./-]+(?:\.[a-z0-9_]+)?)/i);
  if (callMatch && callMatch[1]) return String(callMatch[1]).trim();
  const ofMatch = lower.match(/\bof\s+([a-z0-9_./-]+(?:\.[a-z0-9_]+)?)/i);
  if (ofMatch && ofMatch[1]) return String(ofMatch[1]).trim();
  const byMatch = lower.match(/\bby\s+([a-z0-9_./-]+(?:\.[a-z0-9_]+)?)/i);
  if (mode === 'callers' && byMatch && byMatch[1]) return String(byMatch[1]).trim();
  const pathLike = q
    .split(/\s+/g)
    .map((row) => String(row || '').trim().replace(/[),.;:!?]+$/g, ''))
    .filter((row) => codegraphIsPathLikeToken(row))
    .sort((a, b) => b.length - a.length);
  if (pathLike.length) return pathLike[0];
  return q;
}

function scoreNodeRows(nodes, queryText, queryTokens) {
  const qLower = String(queryText || '').toLowerCase();
  const out = [];
  for (const node of nodes) {
    const score = scoreCodegraphNode(node, qLower, queryTokens);
    if (score <= 0) continue;
    out.push({
      id: String(node.id || ''),
      rel: String(node.rel || ''),
      layer: String(node.layer || ''),
      module: String(node.module || ''),
      file_name: String(node.file_name || ''),
      size_bytes: Math.max(0, Number(node.size_bytes || 0)),
      score: Number(score.toFixed(3))
    });
  }
  out.sort((a, b) => {
    if (Math.abs(Number(a.score || 0) - Number(b.score || 0)) > 0.0001) return Number(b.score || 0) - Number(a.score || 0);
    return String(a.rel || '').localeCompare(String(b.rel || ''));
  });
  return out;
}

function edgeRowsFromScope(graph, scopeNodeIds, direction, limit, kindHint = '') {
  const nodeScope = new Set(Array.isArray(scopeNodeIds) ? scopeNodeIds.map((id) => String(id || '')) : []);
  const rows = [];
  const byId = {};
  const kindNeed = String(kindHint || '').trim().toLowerCase();
  for (const node of graph.nodes || []) byId[String(node.id || '')] = node;
  for (const edge of graph.edges || []) {
    const fromId = String(edge && edge.from_id || '');
    const toId = String(edge && edge.to_id || '');
    if (!fromId || !toId) continue;
    if (kindNeed && String(edge && edge.kind || '').toLowerCase() !== kindNeed) continue;
    if (direction === 'callers' && !nodeScope.has(toId)) continue;
    if (direction === 'callees' && !nodeScope.has(fromId)) continue;
    const fromNode = byId[fromId];
    const toNode = byId[toId];
    rows.push({
      id: String(edge.id || ''),
      kind: String(edge.kind || 'import'),
      from_id: fromId,
      to_id: toId,
      from_rel: String((fromNode && fromNode.rel) || edge.from_rel || ''),
      to_rel: String((toNode && toNode.rel) || edge.to_rel || ''),
      spec: String(edge.spec || ''),
      score: 1
    });
    if (rows.length >= limit) break;
  }
  return rows;
}

function queryCodegraph(graph, options = {}) {
  const query = String(options.query || '').trim();
  const mode = inferCodegraphMode(query, options.mode);
  const limit = clampNumber(options.limit, 1, 160, CODEGRAPH_DEFAULT_QUERY_LIMIT);
  const queryTokens = codegraphTokenize(query).slice(0, 18);
  const nodes = Array.isArray(graph && graph.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph && graph.edges) ? graph.edges : [];
  const summary = {
    files_scanned: Math.max(0, Number(graph && graph.files_scanned || 0)),
    files_truncated: graph && graph.files_truncated === true,
    node_count: nodes.length,
    edge_count: edges.length,
    import_edge_count: Math.max(0, Number(graph && graph.import_edge_count || 0)),
    call_edge_count: Math.max(0, Number(graph && graph.call_edge_count || 0)),
    ast_enabled: graph && graph.ast_enabled === true,
    ast_attempted_files: Math.max(0, Number(graph && graph.ast_attempted_files || 0)),
    ast_parsed_files: Math.max(0, Number(graph && graph.ast_parsed_files || 0)),
    ast_failed_files: Math.max(0, Number(graph && graph.ast_failed_files || 0))
  };
  if (!query) {
    return {
      ok: true,
      generated_at: nowIso(),
      mode: 'summary',
      query: '',
      summary,
      matches: {
        nodes: [],
        links: [],
        explanation: 'no_query_supplied',
        top_external: Array.isArray(graph && graph.external_top) ? graph.external_top.slice(0, 10) : []
      }
    };
  }

  const scoredNodes = scoreNodeRows(nodes, query, queryTokens);
  if (mode === 'callers' || mode === 'callees') {
    const target = extractRelationTarget(query, mode);
    const kindHint = inferCodegraphRelationKindHint(query);
    const targetRows = scoreNodeRows(nodes, target || query, codegraphTokenize(target || query).slice(0, 18)).slice(0, 8);
    const targetIds = targetRows.map((row) => String(row.id || ''));
    let edgeRows = edgeRowsFromScope(graph, targetIds, mode, Math.max(limit * 2, 24), kindHint);
    if (edgeRows.length === 0 && kindHint) {
      edgeRows = edgeRowsFromScope(graph, targetIds, mode, Math.max(limit * 2, 24), '');
    }
    const nodeById = {};
    for (const node of nodes) nodeById[String(node.id || '')] = node;
    const relationNodeIds = new Set(targetIds);
    for (const edge of edgeRows) {
      relationNodeIds.add(String(edge.from_id || ''));
      relationNodeIds.add(String(edge.to_id || ''));
    }
    const relationNodes = [];
    for (const id of relationNodeIds) {
      const node = nodeById[id];
      if (!node) continue;
      relationNodes.push({
        id: String(node.id || ''),
        rel: String(node.rel || ''),
        layer: String(node.layer || ''),
        module: String(node.module || ''),
        file_name: String(node.file_name || ''),
        size_bytes: Math.max(0, Number(node.size_bytes || 0)),
        score: targetIds.includes(id) ? 1.2 : 1
      });
    }
    relationNodes.sort((a, b) => String(a.rel || '').localeCompare(String(b.rel || '')));
    return {
      ok: true,
      generated_at: nowIso(),
      mode,
      query,
      summary,
      matches: {
        target: target || query,
        nodes: relationNodes.slice(0, Math.max(limit, 10)),
        links: edgeRows.slice(0, Math.max(limit, 10)),
        explanation: `${mode}_query`,
        top_external: Array.isArray(graph && graph.external_top) ? graph.external_top.slice(0, 10) : []
      }
    };
  }

  const topNodes = scoredNodes.slice(0, Math.max(limit * 2, 20));
  const nodeScoreById = {};
  for (const row of topNodes) nodeScoreById[String(row.id || '')] = Number(row.score || 0);
  const edgeRows = [];
  for (const edge of edges) {
    const fromScore = Number(nodeScoreById[String(edge && edge.from_id || '')] || 0);
    const toScore = Number(nodeScoreById[String(edge && edge.to_id || '')] || 0);
    let score = (fromScore * 0.52) + (toScore * 0.65);
    if (query && String(edge && edge.spec || '').toLowerCase().includes(query.toLowerCase())) score += 2;
    if (score <= 0) continue;
    edgeRows.push({
      id: String(edge.id || ''),
      kind: String(edge.kind || 'import'),
      from_id: String(edge.from_id || ''),
      to_id: String(edge.to_id || ''),
      from_rel: String(edge.from_rel || ''),
      to_rel: String(edge.to_rel || ''),
      spec: String(edge.spec || ''),
      score: Number(score.toFixed(3))
    });
  }
  edgeRows.sort((a, b) => {
    if (Math.abs(Number(a.score || 0) - Number(b.score || 0)) > 0.0001) return Number(b.score || 0) - Number(a.score || 0);
    return String(a.id || '').localeCompare(String(b.id || ''));
  });

  return {
    ok: true,
    generated_at: nowIso(),
    mode: 'search',
    query,
    summary,
    matches: {
      nodes: topNodes.slice(0, limit),
      links: edgeRows.slice(0, limit),
      explanation: 'token_ranked_search',
      top_external: Array.isArray(graph && graph.external_top) ? graph.external_top.slice(0, 10) : []
    }
  };
}

function codegraphIndexMaps(graph) {
  const nodeById = {};
  const outById = {};
  const inById = {};
  const nodes = Array.isArray(graph && graph.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph && graph.edges) ? graph.edges : [];
  for (const node of nodes) {
    const id = String(node && node.id || '').trim();
    if (!id) continue;
    nodeById[id] = node;
    if (!outById[id]) outById[id] = [];
    if (!inById[id]) inById[id] = [];
  }
  for (const edge of edges) {
    const fromId = String(edge && edge.from_id || '').trim();
    const toId = String(edge && edge.to_id || '').trim();
    if (!fromId || !toId) continue;
    if (!outById[fromId]) outById[fromId] = [];
    if (!inById[toId]) inById[toId] = [];
    outById[fromId].push(edge);
    inById[toId].push(edge);
  }
  return {
    node_by_id: nodeById,
    out_by_id: outById,
    in_by_id: inById
  };
}

function resolveImpactTargetRows(graph, targetText, limit = 8) {
  const target = String(targetText || '').trim();
  if (!target) return [];
  const nodes = Array.isArray(graph && graph.nodes) ? graph.nodes : [];
  const byPath = normalizeRelPath(target).toLowerCase();
  const exactRows = [];
  for (const node of nodes) {
    const rel = normalizeRelPath(node && node.rel || '').toLowerCase();
    if (!rel || !byPath) continue;
    if (rel === byPath) {
      exactRows.push({
        id: String(node.id || ''),
        rel: String(node.rel || ''),
        layer: String(node.layer || ''),
        module: String(node.module || ''),
        file_name: String(node.file_name || ''),
        size_bytes: Math.max(0, Number(node.size_bytes || 0)),
        score: 999
      });
    }
  }
  if (exactRows.length) return exactRows.slice(0, Math.max(1, Number(limit || 8)));
  const scored = scoreNodeRows(
    nodes,
    target,
    codegraphTokenize(target).slice(0, 18)
  );
  return scored.slice(0, Math.max(1, Number(limit || 8)));
}

function traverseImpactGraph(graph, startNodeIds, options = {}) {
  const direction = String(options.direction || 'reverse').toLowerCase() === 'forward'
    ? 'forward'
    : 'reverse';
  const maxDepth = clampNumber(options.max_depth, 1, 24, 6);
  const nodeCap = clampNumber(options.node_cap, 20, 5000, 1200);
  const kindNeed = String(options.kind || '').trim().toLowerCase();
  const idx = codegraphIndexMaps(graph);
  const startIds = Array.isArray(startNodeIds)
    ? Array.from(new Set(startNodeIds.map((id) => String(id || '').trim()).filter(Boolean)))
    : [];
  const startSet = new Set(startIds);
  const queue = [];
  const seenDepth = {};
  const rowsById = {};

  for (const id of startIds) {
    queue.push({ id, depth: 0 });
    seenDepth[id] = 0;
  }

  while (queue.length > 0) {
    const cur = queue.shift();
    if (!cur) continue;
    const curId = String(cur.id || '');
    const curDepth = Math.max(0, Number(cur.depth || 0));
    if (!curId) continue;
    if (curDepth >= maxDepth) continue;
    const edgeRows = direction === 'forward'
      ? (Array.isArray(idx.out_by_id[curId]) ? idx.out_by_id[curId] : [])
      : (Array.isArray(idx.in_by_id[curId]) ? idx.in_by_id[curId] : []);
    for (const edge of edgeRows) {
      if (!edge || typeof edge !== 'object') continue;
      const edgeKind = String(edge.kind || '').toLowerCase();
      if (kindNeed && edgeKind && edgeKind !== kindNeed) continue;
      const nextId = direction === 'forward'
        ? String(edge.to_id || '')
        : String(edge.from_id || '');
      if (!nextId || startSet.has(nextId)) continue;
      const nextDepth = curDepth + 1;
      if (!(nextId in seenDepth) || nextDepth < Number(seenDepth[nextId] || 9999)) {
        seenDepth[nextId] = nextDepth;
        if (Object.keys(rowsById).length < nodeCap && nextDepth < maxDepth) {
          queue.push({ id: nextId, depth: nextDepth });
        }
      }
      const row = rowsById[nextId] || {
        id: nextId,
        depth: nextDepth,
        direct: nextDepth === 1,
        incoming_edges: 0,
        via_kinds: {},
        parents: {},
        via_symbols: {}
      };
      row.depth = Math.min(Number(row.depth || nextDepth), nextDepth);
      row.direct = row.direct || nextDepth === 1;
      row.incoming_edges += 1;
      row.via_kinds[edgeKind || 'import'] = Number(row.via_kinds[edgeKind || 'import'] || 0) + 1;
      const parentId = curId;
      row.parents[parentId] = true;
      const symbol = String(edge.symbol || '').trim();
      if (symbol) row.via_symbols[symbol] = true;
      rowsById[nextId] = row;
      if (Object.keys(rowsById).length >= nodeCap) break;
    }
    if (Object.keys(rowsById).length >= nodeCap) break;
  }

  const rows = [];
  for (const id of Object.keys(rowsById)) {
    const meta = rowsById[id];
    const node = idx.node_by_id[id];
    if (!meta || !node) continue;
    rows.push({
      id,
      rel: String(node.rel || ''),
      layer: String(node.layer || ''),
      module: String(node.module || ''),
      file_name: String(node.file_name || ''),
      size_bytes: Math.max(0, Number(node.size_bytes || 0)),
      depth: Math.max(1, Number(meta.depth || 1)),
      direct: meta.direct === true,
      incoming_edges: Math.max(0, Number(meta.incoming_edges || 0)),
      via_kinds: Object.keys(meta.via_kinds).sort(),
      parent_count: Object.keys(meta.parents || {}).length,
      via_symbols: Object.keys(meta.via_symbols || {}).slice(0, 24)
    });
  }
  rows.sort((a, b) => {
    if (Math.abs(Number(a.depth || 0) - Number(b.depth || 0)) > 0.0001) return Number(a.depth || 0) - Number(b.depth || 0);
    if (Math.abs(Number(a.incoming_edges || 0) - Number(b.incoming_edges || 0)) > 0.0001) return Number(b.incoming_edges || 0) - Number(a.incoming_edges || 0);
    return String(a.rel || '').localeCompare(String(b.rel || ''));
  });
  return rows;
}

function computeImpactRiskRows(rows, limit = 25) {
  const src = Array.isArray(rows) ? rows : [];
  const ranked = [];
  for (const row of src) {
    const depth = Math.max(1, Number(row && row.depth || 1));
    const incoming = Math.max(0, Number(row && row.incoming_edges || 0));
    const kinds = Array.isArray(row && row.via_kinds) ? row.via_kinds : [];
    const calls = kinds.includes('call');
    const imports = kinds.includes('import');
    let base = 1;
    if (calls) base += 0.65;
    if (imports) base += 0.35;
    if (calls && imports) base += 0.22;
    const fanIn = Math.min(1.4, incoming * 0.18);
    const depthPenalty = 1 / (1 + ((depth - 1) * 0.55));
    const score = Number(((base + fanIn) * depthPenalty).toFixed(4));
    ranked.push({
      ...row,
      risk_score: score
    });
  }
  ranked.sort((a, b) => {
    if (Math.abs(Number(a.risk_score || 0) - Number(b.risk_score || 0)) > 0.0001) return Number(b.risk_score || 0) - Number(a.risk_score || 0);
    return String(a.rel || '').localeCompare(String(b.rel || ''));
  });
  return ranked.slice(0, Math.max(1, Number(limit || 25)));
}

function summarizeImpactRows(rows) {
  const src = Array.isArray(rows) ? rows : [];
  const byLayer = {};
  const byKind = {};
  let direct = 0;
  for (const row of src) {
    if (!row || typeof row !== 'object') continue;
    if (row.direct === true) direct += 1;
    const layer = String(row.layer || '').trim() || 'unknown';
    byLayer[layer] = Number(byLayer[layer] || 0) + 1;
    for (const kind of Array.isArray(row.via_kinds) ? row.via_kinds : []) {
      const k = String(kind || '').trim() || 'import';
      byKind[k] = Number(byKind[k] || 0) + 1;
    }
  }
  return {
    total: src.length,
    direct,
    transitive: Math.max(0, src.length - direct),
    by_layer: byLayer,
    by_kind: byKind
  };
}

function impactCodegraph(graph, options = {}) {
  const target = String(options.target || '').trim();
  const maxDepth = clampNumber(options.max_depth, 1, 24, 6);
  const limit = clampNumber(options.limit, 1, 240, 80);
  const kindHint = String(options.kind || '').trim().toLowerCase();
  const targetRows = resolveImpactTargetRows(graph, target, 8);
  const targetIds = targetRows.map((row) => String(row.id || '')).filter(Boolean);
  const summary = {
    files_scanned: Math.max(0, Number(graph && graph.files_scanned || 0)),
    files_truncated: graph && graph.files_truncated === true,
    node_count: Array.isArray(graph && graph.nodes) ? graph.nodes.length : 0,
    edge_count: Array.isArray(graph && graph.edges) ? graph.edges.length : 0,
    import_edge_count: Math.max(0, Number(graph && graph.import_edge_count || 0)),
    call_edge_count: Math.max(0, Number(graph && graph.call_edge_count || 0))
  };

  if (!targetIds.length) {
    return {
      ok: true,
      generated_at: nowIso(),
      target,
      target_nodes: [],
      summary,
      impact: {
        mode: 'none',
        max_depth: maxDepth,
        kind_filter: kindHint || '',
        dependents: { total: 0, direct: 0, transitive: 0, by_layer: {}, by_kind: {} },
        dependencies: { total: 0, direct: 0, transitive: 0, by_layer: {}, by_kind: {} },
        top_dependents: [],
        top_dependencies: [],
        unresolved_target: true
      }
    };
  }

  const dependentsAll = traverseImpactGraph(graph, targetIds, {
    direction: 'reverse',
    max_depth: maxDepth,
    kind: kindHint,
    node_cap: 2600
  });
  const dependenciesAll = traverseImpactGraph(graph, targetIds, {
    direction: 'forward',
    max_depth: maxDepth,
    kind: kindHint,
    node_cap: 2600
  });
  const topDependents = computeImpactRiskRows(dependentsAll, limit);
  const topDependencies = computeImpactRiskRows(dependenciesAll, limit);
  return {
    ok: true,
    generated_at: nowIso(),
    target,
    target_nodes: targetRows,
    summary,
    impact: {
      mode: 'blast_radius',
      max_depth: maxDepth,
      kind_filter: kindHint || '',
      dependents: summarizeImpactRows(dependentsAll),
      dependencies: summarizeImpactRows(dependenciesAll),
      top_dependents: topDependents,
      top_dependencies: topDependencies,
      unresolved_target: false
    }
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
  const defaultLiveMinutes = clampLiveMinutes(args.live_minutes, DEFAULT_LIVE_MINUTES);

  const server = http.createServer((req, res) => {
    const parsed = new URL(req.url || '/', `http://${req.headers.host || `${host}:${port}`}`);
    const pathname = parsed.pathname || '/';
    if (req.method !== 'GET') {
      sendText(res, 405, 'method_not_allowed\n');
      return;
    }
    if (pathname === '/api/graph') {
      const qHours = parseQueryNumber(parsed.searchParams, 'hours');
      const hours = Number.isFinite(qHours) ? qHours : defaultHours;
      const qLiveMinutes = parseQueryNumber(parsed.searchParams, 'live_minutes');
      const liveMinutes = Number.isFinite(qLiveMinutes) ? qLiveMinutes : defaultLiveMinutes;
      const liveMode = parseBoolish(parsed.searchParams.get('live_mode'), true);
      try {
        sendJson(res, 200, buildPayload(hours, liveMinutes, liveMode));
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
      const qHours = parseQueryNumber(parsed.searchParams, 'hours');
      const hours = Number.isFinite(qHours) ? qHours : defaultHours;
      const qLiveMinutes = parseQueryNumber(parsed.searchParams, 'live_minutes');
      const liveMinutes = Number.isFinite(qLiveMinutes) ? qLiveMinutes : defaultLiveMinutes;
      const liveMode = parseBoolish(parsed.searchParams.get('live_mode'), true);
      try {
        const payload = buildPayload(hours, liveMinutes, liveMode);
        sendJson(res, 200, {
          ok: true,
          generated_at: payload.generated_at,
          live_mode: payload.live_mode === true,
          live_minutes: Number(payload.live_minutes || liveMinutes),
          runtime: payload.runtime || null,
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
    if (pathname === '/api/codegraph/reindex') {
      try {
        const graph = loadCodegraphCached(true);
        sendJson(res, 200, {
          ok: true,
          generated_at: nowIso(),
          summary: {
            files_scanned: Number(graph.files_scanned || 0),
            files_truncated: graph.files_truncated === true,
            node_count: Array.isArray(graph.nodes) ? graph.nodes.length : 0,
            edge_count: Array.isArray(graph.edges) ? graph.edges.length : 0,
            import_edge_count: Number(graph.import_edge_count || 0),
            call_edge_count: Number(graph.call_edge_count || 0),
            ast_enabled: graph.ast_enabled === true,
            ast_attempted_files: Number(graph.ast_attempted_files || 0),
            ast_parsed_files: Number(graph.ast_parsed_files || 0),
            ast_failed_files: Number(graph.ast_failed_files || 0)
          },
          top_external: Array.isArray(graph.external_top) ? graph.external_top.slice(0, 16) : []
        });
      } catch (err) {
        sendJson(res, 500, {
          ok: false,
          error: String(err && err.message || err || 'codegraph_reindex_failed'),
          ts: nowIso()
        });
      }
      return;
    }
    if (pathname === '/api/codegraph/query' || pathname === '/api/codegraph') {
      const query = String(parsed.searchParams.get('q') || '').trim();
      const mode = String(parsed.searchParams.get('mode') || '').trim().toLowerCase();
      const limitRaw = Number(parsed.searchParams.get('limit'));
      const limit = Number.isFinite(limitRaw) ? limitRaw : CODEGRAPH_DEFAULT_QUERY_LIMIT;
      const force = String(parsed.searchParams.get('reindex') || '').trim() === '1';
      try {
        const graph = loadCodegraphCached(force);
        const payload = queryCodegraph(graph, {
          query,
          mode,
          limit
        });
        sendJson(res, 200, payload);
      } catch (err) {
        sendJson(res, 500, {
          ok: false,
          error: String(err && err.message || err || 'codegraph_query_failed'),
          ts: nowIso()
        });
      }
      return;
    }
    if (pathname === '/api/codegraph/impact') {
      const target = String(parsed.searchParams.get('target') || parsed.searchParams.get('q') || '').trim();
      const kind = String(parsed.searchParams.get('kind') || '').trim().toLowerCase();
      const maxDepthRaw = Number(parsed.searchParams.get('max_depth'));
      const maxDepth = Number.isFinite(maxDepthRaw) ? maxDepthRaw : 6;
      const limitRaw = Number(parsed.searchParams.get('limit'));
      const limit = Number.isFinite(limitRaw) ? limitRaw : 80;
      const force = String(parsed.searchParams.get('reindex') || '').trim() === '1';
      if (!target) {
        sendJson(res, 400, {
          ok: false,
          error: 'target_required',
          hint: 'Use /api/codegraph/impact?target=systems/spine/spine.ts',
          ts: nowIso()
        });
        return;
      }
      try {
        const graph = loadCodegraphCached(force);
        const payload = impactCodegraph(graph, {
          target,
          kind,
          max_depth: maxDepth,
          limit
        });
        sendJson(res, 200, payload);
      } catch (err) {
        sendJson(res, 500, {
          ok: false,
          error: String(err && err.message || err || 'codegraph_impact_failed'),
          ts: nowIso()
        });
      }
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
    if (pathname === '/api/terminal/state') {
      sendJson(res, 200, terminalStatePayload());
      return;
    }
    if (pathname === '/api/terminal/cwd') {
      const rawPath = String(parsed.searchParams.get('path') || '').trim();
      if (!rawPath) {
        sendJson(res, 400, {
          ok: false,
          error: 'path_required',
          ts: nowIso()
        });
        return;
      }
      const payload = setTerminalCwd(rawPath, 'selection');
      if (!payload.ok) {
        sendJson(res, 400, {
          ...payload,
          ts: nowIso()
        });
        return;
      }
      sendJson(res, 200, payload);
      return;
    }
    if (pathname === '/api/terminal/exec') {
      const command = String(parsed.searchParams.get('cmd') || '');
      const payload = execTerminalCommand(command);
      if (!payload.ok) {
        sendJson(res, 400, {
          ...payload,
          ts: nowIso()
        });
        return;
      }
      sendJson(res, 200, payload);
      return;
    }
    if (!serveStatic(pathname, res)) {
      sendText(res, 404, 'not_found\n');
    }
  });
  const wsHub = createWsHub(server, defaultHours, defaultLiveMinutes);
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
      default_live_minutes: defaultLiveMinutes,
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
