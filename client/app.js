const QUALITY_LEVELS = ['low', 'medium', 'high', 'ultra'];
const QUALITY_PROFILES = {
  low: {
    label: 'TRON-LEGACY',
    max_layers: 5,
    max_modules_per_layer: 8,
    max_submodules_per_module: 4,
    particles_per_link: 1,
    max_particles: 220,
    trail_length: 0,
    tube_alpha: 0.06,
    dpr_cap: 1,
    motion_dt_blend: 0.8,
    spin_lerp_gain: 0.28,
    spin_substeps: 4
  },
  medium: {
    label: 'TRON-EVOLVED',
    max_layers: 7,
    max_modules_per_layer: 12,
    max_submodules_per_module: 6,
    particles_per_link: 2,
    max_particles: 650,
    trail_length: 4,
    tube_alpha: 0.075,
    dpr_cap: 1.35,
    motion_dt_blend: 0.62,
    spin_lerp_gain: 0.42,
    spin_substeps: 4
  },
  high: {
    label: 'ULTRON-LITE',
    max_layers: 9,
    max_modules_per_layer: 18,
    max_submodules_per_module: 10,
    particles_per_link: 4,
    max_particles: 1400,
    trail_length: 9,
    tube_alpha: 0.095,
    dpr_cap: 1.8,
    motion_dt_blend: 0.44,
    spin_lerp_gain: 0.58,
    spin_substeps: 4
  },
  ultra: {
    label: 'ULTRON-CINEMATIC',
    max_layers: 12,
    max_modules_per_layer: 24,
    max_submodules_per_module: 14,
    particles_per_link: 7,
    max_particles: 2600,
    trail_length: 16,
    tube_alpha: 0.115,
    dpr_cap: 2.2,
    motion_dt_blend: 0.26,
    spin_lerp_gain: 0.74,
    spin_substeps: 4
  }
};

const BASE_ORANGE = [255, 159, 31];
const BASE_BLUE = [28, 140, 255];
const SPINE_NODE_ID = 'spine:core';
const SPINE_NODE_PATH = 'systems/spine';
const SYSTEM_ROOT_ID = 'system:root';
const WORKSPACE_ROOT_PATH = '/Users/jay/.openclaw/workspace';
const PACKET_RADIUS_FLOOR = 0.45;
const PACKET_RADIUS_CEILING = 3.8;
const PACKET_PATHWAY_OPACITY_SCALE = 0.32;
const PACKET_MOTION_SPEED_SCALE = 1.8;
const MODULE_RADIUS_MIN = 10;
const MODULE_RADIUS_MAX = 34;
const MODULE_RADIUS_SIZE_BLEND = 0.42;
const ROOT_MIN_SCREEN_HEIGHT_RATIO = 0.7;
const SHELL_ORDER_SWAP_COOLDOWN_MS = 10 * 60 * 1000;
const SHELL_INTRO_INITIAL_DELAY_MS = 110;
const SHELL_INTRO_LAYER_DELAY_MS = 70;
const SHELL_INTRO_LAYER_DURATION_MS = 240;
const RESOLVE_FLASH_MS = 4200;
const CODEGRAPH_QUERY_LIMIT = 28;

const state = {
  canvas: null,
  ctx: null,
  width: 0,
  height: 0,
  dpr: 1,
  payload: null,
  scene: null,
  particles: [],
  particle_cursor: 0,
  selected: null,
  selected_subfractal: null,
  selected_link: null,
  workflow_birth_selection_id: '',
  hover: null,
  refresh_ms: 6000,
  refresh_timer: null,
  last_frame_ts: 0,
  motion_initialized: false,
  motion_frame_count: 0,
  motion_dt_smoothed: 16.7,
  motion_jitter_ema: 0,
  motion_smoothness_ema: 1,
  fps: 0,
  fps_smoothed: 60,
  gpu: null,
  quality_tier: 'low',
  quality_profile: QUALITY_PROFILES.low,
  adaptive_downgrade_streak: 0,
  ws: null,
  ws_connected: false,
  ws_retry_timer: null,
  ws_backoff_ms: 900,
  transport: 'poll',
  live_mode: true,
  live_minutes: 6,
  runtime: {
    status: 'unknown',
    online: false,
    stale: false,
    offline: true,
    signal_age_sec: null,
    live_window_minutes: 6,
    activity_scale: 1
  },
  spine_event_count: 0,
  spine_event_top: '',
  spine_burst_until: 0,
  incidents: {
    integrity: null
  },
  orbit_lock: {
    layer_id: '',
    runtime_by_layer: Object.create(null)
  },
  module_spin_lock: {
    runtime_by_module: Object.create(null)
  },
  layer_order_lock: {
    order: [],
    last_swap_ms: 0
  },
  shell_intro: {
    started: false,
    played: false,
    start_ts: 0,
    initial_delay_ms: SHELL_INTRO_INITIAL_DELAY_MS,
    layer_delay_ms: SHELL_INTRO_LAYER_DELAY_MS,
    layer_duration_ms: SHELL_INTRO_LAYER_DURATION_MS,
    layer_ids: []
  },
  focus: null,
  preview_tab: 'preview',
  terminal: {
    cwd_abs: WORKSPACE_ROOT_PATH,
    cwd_rel: '.',
    loaded: false,
    loading: false,
    running: false,
    output: '',
    last_exit_code: 0,
    last_command: '',
    selection_sync_key: '',
    auto_follow: false,
    scroll_top: 0,
    scroll_listener_bound: false,
    suppress_scroll_event: false
  },
  codegraph: {
    query: '',
    mode: '',
    running: false,
    error: '',
    notice: '',
    last_result: null,
    matched_node_ids: new Set(),
    matched_link_ids: new Set(),
    matched_node_paths: [],
    matched_link_paths: []
  },
  code_preview: {
    selection_key: '',
    path: '',
    loading: false,
    is_file: false,
    is_dir: false,
    truncated: false,
    error: '',
    content: '',
    request_id: 0
  },
  code_preview_cache: Object.create(null),
  resolved_flash: {
    nodes: Object.create(null),
    links: Object.create(null)
  },
  particle_signature: '',
  camera: {
    zoom: 1,
    min_zoom: 0.72,
    max_zoom: 16,
    pan_x: 0,
    pan_y: 0,
    focus_mode: false,
    focus_target_id: null,
    restore_zoom: 1,
    restore_pan_x: 0,
    restore_pan_y: 0,
    map_mode: false,
    map_owner_id: '',
    map_return_zoom: 1,
    map_return_pan_x: 0,
    map_return_pan_y: 0,
    module_return_selection: null,
    module_return_selection_for: '',
    submodule_return_selection: null,
    submodule_return_selection_for: '',
    transition: null,
    panning: false,
    pan_started: false,
    drag_px: 0,
    last_x: 0,
    last_y: 0
  }
};

function byId(id) {
  return document.getElementById(id);
}

function escapeHtml(text) {
  return String(text == null ? '' : text)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function clamp(v, lo, hi) {
  const n = Number(v);
  if (!Number.isFinite(n)) return lo;
  if (n < lo) return lo;
  if (n > hi) return hi;
  return n;
}

function mix(a, b, t) {
  return a + (b - a) * t;
}

function easeOutCubic(v) {
  const t = clamp(v, 0, 1);
  return 1 - Math.pow(1 - t, 3);
}

function normalizeRelPathText(raw) {
  return String(raw || '')
    .trim()
    .replace(/\\/g, '/')
    .replace(/^\.\//, '')
    .replace(/\/{2,}/g, '/')
    .replace(/\/$/, '');
}

function relPathsOverlap(a, b) {
  const x = normalizeRelPathText(a).toLowerCase();
  const y = normalizeRelPathText(b).toLowerCase();
  if (!x || !y) return false;
  if (x === y) return true;
  if (x.startsWith(`${y}/`)) return true;
  if (y.startsWith(`${x}/`)) return true;
  return false;
}

function codegraphState() {
  const cg = state.codegraph && typeof state.codegraph === 'object'
    ? state.codegraph
    : null;
  if (cg) return cg;
  state.codegraph = {
    query: '',
    mode: '',
    running: false,
    error: '',
    notice: '',
    last_result: null,
    matched_node_ids: new Set(),
    matched_link_ids: new Set(),
    matched_node_paths: [],
    matched_link_paths: []
  };
  return state.codegraph;
}

function codegraphHasMatches() {
  const cg = codegraphState();
  return Boolean(
    (cg.matched_node_ids instanceof Set && cg.matched_node_ids.size > 0)
    || (cg.matched_link_ids instanceof Set && cg.matched_link_ids.size > 0)
  );
}

function isCodegraphNodeMatched(id) {
  const cg = codegraphState();
  if (!codegraphHasMatches()) return false;
  const key = String(id || '').trim();
  if (!key) return false;
  return cg.matched_node_ids instanceof Set && cg.matched_node_ids.has(key);
}

function isCodegraphLinkMatched(id) {
  const cg = codegraphState();
  if (!codegraphHasMatches()) return false;
  const key = String(id || '').trim();
  if (!key) return false;
  return cg.matched_link_ids instanceof Set && cg.matched_link_ids.has(key);
}

function codegraphNodeAlphaScale(nodeId) {
  if (!codegraphHasMatches()) return 1;
  if (isCodegraphNodeMatched(nodeId)) return 1;
  return 0.22;
}

function codegraphLinkAlphaScale(link) {
  if (!codegraphHasMatches()) return 1;
  const row = link && typeof link === 'object' ? link : {};
  const linkId = String(row.id || '');
  if (isCodegraphLinkMatched(linkId)) return 1;
  if (isCodegraphNodeMatched(String(row.from_id || '')) || isCodegraphNodeMatched(String(row.to_id || ''))) return 0.6;
  return 0.12;
}

function hasNodeIssueSignal(node) {
  if (!node || typeof node !== 'object') return false;
  if (node.error_state_active === true || node.integrity_alert === true) return true;
  if (node.change_active === true) return true;
  const change = normalizeChangeState(node.change_state);
  return change.changed === true;
}

function hasLinkIssueSignal(link) {
  if (!link || typeof link !== 'object') return false;
  const errW = clamp(Number(link.error_weight || 0), 0, 1);
  const blockedRatio = clamp(Number(link.blocked_ratio || 0), 0, 1);
  return errW > 0.045 || link.flow_blocked === true || blockedRatio > 0.02;
}

function registerResolvedFlashes(prevScene, nextScene, nowTs = performance.now()) {
  const prev = prevScene && typeof prevScene === 'object' ? prevScene : null;
  const next = nextScene && typeof nextScene === 'object' ? nextScene : null;
  if (!prev || !next) return;
  const flash = state.resolved_flash && typeof state.resolved_flash === 'object'
    ? state.resolved_flash
    : (state.resolved_flash = { nodes: Object.create(null), links: Object.create(null) });
  const nodesMap = flash.nodes && typeof flash.nodes === 'object'
    ? flash.nodes
    : (flash.nodes = Object.create(null));
  const linksMap = flash.links && typeof flash.links === 'object'
    ? flash.links
    : (flash.links = Object.create(null));
  const ttl = Math.max(400, Number(RESOLVE_FLASH_MS || 4200));
  const expiresAt = Number(nowTs || 0) + ttl;

  const prevNodes = prev.node_by_id && typeof prev.node_by_id === 'object' ? prev.node_by_id : Object.create(null);
  const nextNodes = next.node_by_id && typeof next.node_by_id === 'object' ? next.node_by_id : Object.create(null);
  for (const id of Object.keys(nextNodes)) {
    const before = prevNodes[id];
    const after = nextNodes[id];
    if (!before || !after) continue;
    if (hasNodeIssueSignal(before) && !hasNodeIssueSignal(after)) {
      nodesMap[id] = expiresAt;
    }
  }

  const prevLinks = prev.link_by_id && typeof prev.link_by_id === 'object' ? prev.link_by_id : Object.create(null);
  const nextLinks = next.link_by_id && typeof next.link_by_id === 'object' ? next.link_by_id : Object.create(null);
  for (const id of Object.keys(nextLinks)) {
    const before = prevLinks[id];
    const after = nextLinks[id];
    if (!before || !after) continue;
    if (hasLinkIssueSignal(before) && !hasLinkIssueSignal(after)) {
      linksMap[id] = expiresAt;
    }
  }
}

function resolvedFlashAlpha(kind, id, nowTs = performance.now()) {
  const bucket = state.resolved_flash && kind === 'link'
    ? state.resolved_flash.links
    : state.resolved_flash && kind === 'node'
      ? state.resolved_flash.nodes
      : null;
  if (!bucket || typeof bucket !== 'object') return 0;
  const key = String(id || '').trim();
  if (!key) return 0;
  const until = Number(bucket[key] || 0);
  if (!Number.isFinite(until) || until <= 0) return 0;
  const remain = until - Number(nowTs || 0);
  if (remain <= 0) {
    delete bucket[key];
    return 0;
  }
  const t = clamp(remain / Math.max(100, Number(RESOLVE_FLASH_MS || 4200)), 0, 1);
  const pulse = 0.72 + (0.28 * (0.5 + (0.5 * Math.sin((Number(nowTs || 0) * 0.008) + (stableHash(key) % 31)))));
  return clamp(t * pulse, 0, 1);
}

function syncCameraTransition(ts = performance.now()) {
  const cam = state.camera;
  const tr = cam.transition && typeof cam.transition === 'object' ? cam.transition : null;
  if (!tr) return;
  const startTs = Number(tr.start_ts || ts);
  const durationMs = Math.max(1, Number(tr.duration_ms || 1));
  const p = clamp((Number(ts || startTs) - startTs) / durationMs, 0, 1);
  const e = easeOutCubic(p);
  const fromZoom = Number(tr.from_zoom || cam.zoom);
  const fromPanX = Number(tr.from_pan_x || cam.pan_x);
  const fromPanY = Number(tr.from_pan_y || cam.pan_y);
  const toZoom = Number(tr.to_zoom || fromZoom);
  const toPanX = Number(tr.to_pan_x || fromPanX);
  const toPanY = Number(tr.to_pan_y || fromPanY);
  const allowOutOfBounds = tr.allow_out_of_bounds === true;
  cam.zoom = mix(fromZoom, toZoom, e);
  cam.pan_x = mix(fromPanX, toPanX, e);
  cam.pan_y = mix(fromPanY, toPanY, e);
  if (!allowOutOfBounds) clampCameraPanInPlace();
  if (p >= 1) {
    cam.zoom = toZoom;
    cam.pan_x = toPanX;
    cam.pan_y = toPanY;
    if (!allowOutOfBounds) clampCameraPanInPlace();
    cam.transition = null;
  }
}

function stopCameraTransition() {
  syncCameraTransition(performance.now());
  state.camera.transition = null;
}

function startCameraTransition(targetZoom, targetPanX, targetPanY, durationMs = 170, options = {}) {
  const cam = state.camera;
  syncCameraTransition(performance.now());
  const allowOutOfBounds = options && options.allow_out_of_bounds === true;
  const desiredZoom = clampZoomToSceneBounds(Number(targetZoom));
  let toZoom = desiredZoom;
  let toPanX = Number(targetPanX);
  let toPanY = Number(targetPanY);
  if (!allowOutOfBounds) {
    const panCandidate = clampPanToSceneBounds(Number(targetPanX), Number(targetPanY), desiredZoom);
    toZoom = Number(panCandidate.zoom || desiredZoom);
    toPanX = Number(panCandidate.pan_x);
    toPanY = Number(panCandidate.pan_y);
  }
  if (!Number.isFinite(toZoom) || !Number.isFinite(toPanX) || !Number.isFinite(toPanY)) return;
  const fromZoom = Number(cam.zoom || 1);
  const fromPanX = Number(cam.pan_x || 0);
  const fromPanY = Number(cam.pan_y || 0);
  const dist = Math.hypot(toPanX - fromPanX, toPanY - fromPanY);
  const zoomDelta = Math.abs(toZoom - fromZoom);
  const dur = Math.max(1, Number(durationMs || 1));
  if (dur <= 1 || (dist < 0.8 && zoomDelta < 0.0009)) {
    cam.zoom = toZoom;
    cam.pan_x = toPanX;
    cam.pan_y = toPanY;
    if (!allowOutOfBounds) clampCameraPanInPlace();
    cam.transition = null;
    return;
  }
  cam.transition = {
    start_ts: performance.now(),
    duration_ms: dur,
    from_zoom: fromZoom,
    from_pan_x: fromPanX,
    from_pan_y: fromPanY,
    to_zoom: toZoom,
    to_pan_x: toPanX,
    to_pan_y: toPanY,
    allow_out_of_bounds: allowOutOfBounds
  };
}

function activityRgb(activity) {
  const t = clamp(activity, 0, 1);
  return {
    r: Math.round(mix(BASE_ORANGE[0], BASE_BLUE[0], t)),
    g: Math.round(mix(BASE_ORANGE[1], BASE_BLUE[1], t)),
    b: Math.round(mix(BASE_ORANGE[2], BASE_BLUE[2], t))
  };
}

function colorFromActivity(activity, alpha = 1) {
  const { r, g, b } = activityRgb(activity);
  return `rgba(${r},${g},${b},${clamp(alpha, 0, 1)})`;
}

function colorFromActivityBright(activity, alpha = 1, amount = 0.12) {
  const { r: br, g: bg, b: bb } = activityRgb(activity);
  const boost = clamp(amount, 0, 1);
  const r = Math.round(mix(br, 255, boost));
  const g = Math.round(mix(bg, 255, boost));
  const b = Math.round(mix(bb, 255, boost));
  return `rgba(${r},${g},${b},${clamp(alpha, 0, 1)})`;
}

function softHighlightColor(alpha = 1) {
  return `rgba(236,242,246,${clamp(alpha, 0, 1)})`;
}

function brightenChannel(value, amount = 0.12) {
  return Math.round(mix(Number(value || 0), 255, clamp(amount, 0, 1)));
}

function layerStableId(layer, fallback = '') {
  return String(
    (layer && (layer.id || layer.key || layer.name))
    || fallback
    || ''
  ).trim();
}

function applyLayerOrderLock(layerRows, nowMs = Date.now()) {
  const rows = Array.isArray(layerRows) ? layerRows.slice() : [];
  if (rows.length <= 1) return rows;
  const lock = state.layer_order_lock && typeof state.layer_order_lock === 'object'
    ? state.layer_order_lock
    : (state.layer_order_lock = { order: [], last_swap_ms: 0 });
  const byId = new Map();
  const desiredIds = [];
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];
    const id = layerStableId(row, `layer:${i}`);
    if (!id) continue;
    desiredIds.push(id);
    if (!byId.has(id)) byId.set(id, row);
  }
  if (!desiredIds.length) return rows;

  const prevIdsRaw = Array.isArray(lock.order) ? lock.order : [];
  const prevIds = prevIdsRaw
    .map((id) => String(id || '').trim())
    .filter((id) => id && byId.has(id));
  const prevSet = new Set(prevIds);
  const appended = desiredIds.filter((id) => !prevSet.has(id));
  const preservedOrderIds = [...prevIds, ...appended];
  const hasReorder = preservedOrderIds.length === desiredIds.length
    && preservedOrderIds.some((id, idx) => id !== desiredIds[idx]);
  const now = Number(nowMs || Date.now());

  if (!hasReorder || prevIds.length === 0) {
    lock.order = desiredIds.slice();
    if (!Number.isFinite(Number(lock.last_swap_ms)) || Number(lock.last_swap_ms) <= 0) {
      lock.last_swap_ms = now;
    }
    return rows;
  }

  const canSwap = (now - Number(lock.last_swap_ms || 0)) >= SHELL_ORDER_SWAP_COOLDOWN_MS;
  if (canSwap) {
    lock.order = desiredIds.slice();
    lock.last_swap_ms = now;
    return rows;
  }

  lock.order = preservedOrderIds.slice();
  return preservedOrderIds.map((id) => byId.get(id)).filter(Boolean);
}

function primeShellIntro(scene, ts = performance.now()) {
  const intro = state.shell_intro && typeof state.shell_intro === 'object'
    ? state.shell_intro
    : (state.shell_intro = {
      started: false,
      played: false,
      start_ts: 0,
      initial_delay_ms: SHELL_INTRO_INITIAL_DELAY_MS,
      layer_delay_ms: SHELL_INTRO_LAYER_DELAY_MS,
      layer_duration_ms: SHELL_INTRO_LAYER_DURATION_MS,
      layer_ids: []
    });
  if (intro.played || intro.started || !scene) return;
  const nodes = Array.isArray(scene.nodes) ? scene.nodes : [];
  const orderedLayerIds = nodes
    .filter((n) => n && n.type === 'layer')
    .slice()
    .sort((a, b) => Number(a.radius || 0) - Number(b.radius || 0))
    .map((n) => String(n.id || '').trim())
    .filter(Boolean);
  if (!orderedLayerIds.length) {
    intro.played = true;
    return;
  }
  intro.layer_ids = orderedLayerIds;
  intro.start_ts = Number(ts || performance.now()) + Number(intro.initial_delay_ms || 0);
  intro.started = true;
}

function computeLayerIntroScale(scene, ts = performance.now()) {
  const intro = state.shell_intro && typeof state.shell_intro === 'object' ? state.shell_intro : null;
  if (!intro || intro.played || !scene) return null;
  if (!intro.started) primeShellIntro(scene, ts);
  if (!intro.started) return null;
  const ids = Array.isArray(intro.layer_ids) ? intro.layer_ids : [];
  if (!ids.length) {
    intro.played = true;
    return null;
  }
  const elapsed = Number(ts || performance.now()) - Number(intro.start_ts || 0);
  const layerDelay = Math.max(1, Number(intro.layer_delay_ms || SHELL_INTRO_LAYER_DELAY_MS));
  const layerDuration = Math.max(1, Number(intro.layer_duration_ms || SHELL_INTRO_LAYER_DURATION_MS));
  const layerStride = layerDuration + layerDelay;
  const scaleById = Object.create(null);
  let complete = elapsed >= 0;
  for (let i = 0; i < ids.length; i += 1) {
    const id = String(ids[i] || '').trim();
    if (!id) continue;
    const localElapsed = elapsed - (i * layerStride);
    const raw = localElapsed / layerDuration;
    const p = clamp(raw, 0, 1);
    if (p < 1) complete = false;
    scaleById[id] = p <= 0 ? 0 : easeOutCubic(p);
  }
  if (complete) intro.played = true;
  return scaleById;
}

function isShellIntroActive(ts = performance.now()) {
  const intro = state.shell_intro && typeof state.shell_intro === 'object' ? state.shell_intro : null;
  if (!intro || intro.played || !intro.started) return false;
  const ids = Array.isArray(intro.layer_ids) ? intro.layer_ids : [];
  if (!ids.length) return false;
  const layerDelay = Math.max(1, Number(intro.layer_delay_ms || SHELL_INTRO_LAYER_DELAY_MS));
  const layerDuration = Math.max(1, Number(intro.layer_duration_ms || SHELL_INTRO_LAYER_DURATION_MS));
  const total = (ids.length * layerDuration) + (Math.max(0, ids.length - 1) * layerDelay);
  const elapsed = Number(ts || performance.now()) - Number(intro.start_ts || 0);
  if (elapsed >= total) {
    intro.played = true;
    return false;
  }
  return true;
}

function fmtNum(v) {
  const n = Number(v || 0);
  if (!Number.isFinite(n)) return '0';
  if (Math.abs(n) >= 1000) return n.toLocaleString();
  if (Math.abs(n) >= 10) return n.toFixed(1);
  return n.toFixed(2);
}

function stableHash(text) {
  const s = String(text || '');
  let h = 2166136261;
  for (let i = 0; i < s.length; i += 1) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0);
}

function detectGpuTier() {
  const dmem = Number(navigator.deviceMemory || 0);
  const cores = Number(navigator.hardwareConcurrency || 0);
  const pixel_load = (window.innerWidth * window.innerHeight) / 1000000;
  let score = 0;
  let renderer = 'unknown';
  let max_texture = 0;
  try {
    const probe = document.createElement('canvas');
    const gl = probe.getContext('webgl') || probe.getContext('experimental-webgl');
    if (gl) {
      max_texture = Number(gl.getParameter(gl.MAX_TEXTURE_SIZE) || 0);
      const debug = gl.getExtension('WEBGL_debug_renderer_info');
      const name = debug ? gl.getParameter(debug.UNMASKED_RENDERER_WEBGL) : gl.getParameter(gl.RENDERER);
      renderer = String(name || 'unknown').toLowerCase();
    }
  } catch {
    renderer = 'unknown';
  }

  score += clamp(dmem, 0, 16) * 2;
  score += clamp(cores, 0, 24) * 1.2;
  score += clamp(max_texture / 2048, 0, 8) * 2.1;

  if (renderer.includes('nvidia') || renderer.includes('radeon') || renderer.includes('apple m')) score += 12;
  else if (renderer.includes('intel') || renderer.includes('iris') || renderer.includes('uhd')) score += 5;
  else if (renderer.includes('swiftshader')) score -= 8;

  if (pixel_load > 3.8) score -= 3;

  let tier = 'low';
  if (score >= 36) tier = 'ultra';
  else if (score >= 25) tier = 'high';
  else if (score >= 15) tier = 'medium';

  return {
    tier,
    score: Number(score.toFixed(2)),
    renderer,
    max_texture,
    memory_gb: dmem,
    cores
  };
}

function setQualityTier(tier) {
  const normalized = QUALITY_LEVELS.includes(tier) ? tier : 'low';
  state.quality_tier = normalized;
  state.quality_profile = QUALITY_PROFILES[normalized];
  const badge = byId('qualityBadge');
  badge.textContent = `GPU: ${state.quality_profile.label}`;
}

function maybeAutoTuneQuality() {
  const smoothness = clamp(Number(state.motion_smoothness_ema || 0), 0, 1);
  const fpsLow = state.fps_smoothed < 23;
  const smoothLow = smoothness < 0.42;
  if (fpsLow || smoothLow) {
    const penalty = smoothness < 0.26 ? 2 : 1;
    state.adaptive_downgrade_streak += penalty;
    if (state.adaptive_downgrade_streak > 140) {
      const idx = QUALITY_LEVELS.indexOf(state.quality_tier);
      if (idx > 0) {
        setQualityTier(QUALITY_LEVELS[idx - 1]);
      }
      state.adaptive_downgrade_streak = 0;
    }
    return;
  }
  if (state.fps_smoothed > 52 && smoothness > 0.78) {
    state.adaptive_downgrade_streak = Math.max(0, state.adaptive_downgrade_streak - 4);
  } else {
    state.adaptive_downgrade_streak = Math.max(0, state.adaptive_downgrade_streak - 1);
  }
}

function resizeCanvas() {
  const profile = state.quality_profile;
  const dprTarget = Math.min(window.devicePixelRatio || 1, profile.dpr_cap);
  state.dpr = clamp(dprTarget, 1, 3);
  state.width = Math.max(320, Math.floor(window.innerWidth));
  state.height = Math.max(240, Math.floor(window.innerHeight));
  state.canvas.width = Math.floor(state.width * state.dpr);
  state.canvas.height = Math.floor(state.height * state.dpr);
  state.canvas.style.width = `${state.width}px`;
  state.canvas.style.height = `${state.height}px`;
  state.ctx.setTransform(state.dpr, 0, 0, state.dpr, 0, 0);
}

function screenToWorld(x, y) {
  const cam = state.camera;
  return {
    x: (x - cam.pan_x) / cam.zoom,
    y: (y - cam.pan_y) / cam.zoom
  };
}

function sceneWorldBounds(scene) {
  const src = scene && typeof scene === 'object' ? scene : null;
  if (!src) return null;
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  const includeCircle = (x, y, r) => {
    const cx = Number(x);
    const cy = Number(y);
    const cr = Math.max(0, Number(r || 0));
    if (!Number.isFinite(cx) || !Number.isFinite(cy) || !Number.isFinite(cr)) return;
    minX = Math.min(minX, cx - cr);
    maxX = Math.max(maxX, cx + cr);
    minY = Math.min(minY, cy - cr);
    maxY = Math.max(maxY, cy + cr);
  };
  const targets = Array.isArray(src.hit_targets) ? src.hit_targets : [];
  for (const t of targets) {
    if (!t) continue;
    const kind = String(t.kind || '').toLowerCase();
    if (kind === 'ring' || kind === 'shell_fill') {
      includeCircle(Number(t.cx || 0), Number(t.cy || 0), Math.max(0, Number(t.outer || 0)));
    } else {
      includeCircle(Number(t.x || 0), Number(t.y || 0), Math.max(0, Number(t.r || 0)));
    }
  }
  const links = Array.isArray(src.links) ? src.links : [];
  for (const link of links) {
    if (!link) continue;
    for (const key of ['p0', 'p1', 'p2', 'p3']) {
      const p = link[key];
      if (!p) continue;
      includeCircle(Number(p.x || 0), Number(p.y || 0), 3.5);
    }
  }
  if (!(maxX > minX) || !(maxY > minY)) {
    const center = src.center && typeof src.center === 'object'
      ? src.center
      : { x: state.width * 0.52, y: state.height * 0.5 };
    const fallbackOuter = Math.max(64, Number(src.outer_shell_boundary || 0), Number(src.outer_radius || 0));
    includeCircle(Number(center.x || 0), Number(center.y || 0), fallbackOuter);
  }
  if (!(maxX > minX) || !(maxY > minY)) return null;
  const pad = 10;
  minX -= pad;
  minY -= pad;
  maxX += pad;
  maxY += pad;
  return {
    minX,
    minY,
    maxX,
    maxY,
    width: Math.max(1, maxX - minX),
    height: Math.max(1, maxY - minY),
    cx: (minX + maxX) * 0.5,
    cy: (minY + maxY) * 0.5
  };
}

function effectiveMinZoom(scene = state.scene) {
  const cam = state.camera;
  const baseMin = Math.max(0.001, Number(cam.min_zoom || 0.72));
  const bounds = sceneWorldBounds(scene);
  if (!bounds) return baseMin;
  const ratio = clamp(Number(ROOT_MIN_SCREEN_HEIGHT_RATIO || 0.7), 0.1, 1);
  const targetHeightPx = Math.max(1, state.height * ratio);
  const byHeight = targetHeightPx / Math.max(1, Number(bounds.height || 1));
  if (!Number.isFinite(byHeight) || byHeight <= 0) return baseMin;
  return Math.max(baseMin, byHeight);
}

function clampZoomToSceneBounds(nextZoom, scene = state.scene) {
  const cam = state.camera;
  const minZoom = effectiveMinZoom(scene);
  const maxZoom = Math.max(minZoom, Number(cam.max_zoom || minZoom));
  return clamp(Number(nextZoom), minZoom, maxZoom);
}

function clampPanToSceneBounds(panX, panY, zoom, scene = state.scene) {
  const z = clampZoomToSceneBounds(zoom, scene);
  const bounds = sceneWorldBounds(scene);
  let nextPanX = Number(panX || 0);
  let nextPanY = Number(panY || 0);
  if (!bounds) {
    return { pan_x: nextPanX, pan_y: nextPanY, zoom: z };
  }
  const worldWpx = bounds.width * z;
  const worldHpx = bounds.height * z;
  if (worldWpx <= state.width) {
    nextPanX = (state.width * 0.5) - (bounds.cx * z);
  } else {
    const minPanX = state.width - (bounds.maxX * z);
    const maxPanX = -(bounds.minX * z);
    nextPanX = clamp(nextPanX, minPanX, maxPanX);
  }
  if (worldHpx <= state.height) {
    nextPanY = (state.height * 0.5) - (bounds.cy * z);
  } else {
    const minPanY = state.height - (bounds.maxY * z);
    const maxPanY = -(bounds.minY * z);
    nextPanY = clamp(nextPanY, minPanY, maxPanY);
  }
  return { pan_x: nextPanX, pan_y: nextPanY, zoom: z };
}

function clampCameraPanInPlace() {
  const cam = state.camera;
  const clamped = clampPanToSceneBounds(cam.pan_x, cam.pan_y, cam.zoom);
  cam.pan_x = clamped.pan_x;
  cam.pan_y = clamped.pan_y;
}

function setZoomAt(screenX, screenY, nextZoom) {
  const cam = state.camera;
  stopCameraTransition();
  const prevZoom = cam.zoom;
  const target = clampZoomToSceneBounds(nextZoom);
  if (Math.abs(target - prevZoom) < 0.0005) return;
  const worldX = (screenX - cam.pan_x) / prevZoom;
  const worldY = (screenY - cam.pan_y) / prevZoom;
  cam.zoom = target;
  cam.pan_x = screenX - (worldX * target);
  cam.pan_y = screenY - (worldY * target);
  clampCameraPanInPlace();
}

async function fetchPayload(hours, options = {}) {
  const h = Math.max(1, Number(hours || 24));
  const liveMode = options && options.live_mode === false ? 0 : 1;
  const liveMinutes = Math.max(1, Number(options && options.live_minutes || 6));
  const q = new URLSearchParams();
  q.set('hours', String(h));
  q.set('live_mode', String(liveMode));
  q.set('live_minutes', String(liveMinutes));
  const res = await fetch(`/api/holo?${q.toString()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`api_http_${res.status}`);
  return res.json();
}

async function fetchCodePreview(pathText) {
  const p = String(pathText || '').trim();
  if (!p) {
    return {
      ok: true,
      path: '',
      is_file: false,
      is_dir: false,
      truncated: false,
      content: ''
    };
  }
  const res = await fetch(`/api/file?path=${encodeURIComponent(p)}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`file_http_${res.status}`);
  return res.json();
}

async function fetchTerminalState() {
  const res = await fetch('/api/terminal/state', { cache: 'no-store' });
  if (!res.ok) throw new Error(`terminal_state_http_${res.status}`);
  return res.json();
}

async function fetchTerminalSetCwd(pathText) {
  const p = String(pathText || '').trim();
  if (!p) throw new Error('terminal_cwd_path_required');
  const res = await fetch(`/api/terminal/cwd?path=${encodeURIComponent(p)}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`terminal_cwd_http_${res.status}`);
  return res.json();
}

async function fetchTerminalExec(commandText) {
  const cmd = String(commandText || '').trim();
  if (!cmd) throw new Error('terminal_command_required');
  const res = await fetch(`/api/terminal/exec?cmd=${encodeURIComponent(cmd)}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`terminal_exec_http_${res.status}`);
  return res.json();
}

async function fetchCodegraphQuery(queryText, options = {}) {
  const q = String(queryText || '').trim();
  const mode = String(options.mode || '').trim();
  const limit = Math.max(1, Number(options.limit || CODEGRAPH_QUERY_LIMIT));
  const force = options.reindex === true;
  const params = new URLSearchParams();
  params.set('q', q);
  params.set('limit', String(limit));
  if (mode) params.set('mode', mode);
  if (force) params.set('reindex', '1');
  const res = await fetch(`/api/codegraph/query?${params.toString()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`codegraph_http_${res.status}`);
  return res.json();
}

async function fetchCodegraphReindex() {
  const res = await fetch('/api/codegraph/reindex', { cache: 'no-store' });
  if (!res.ok) throw new Error(`codegraph_reindex_http_${res.status}`);
  return res.json();
}

function codegraphRows(payload, key) {
  const src = payload && payload.matches && typeof payload.matches === 'object'
    ? payload.matches
    : {};
  const rows = Array.isArray(src[key]) ? src[key] : [];
  return rows.slice(0, 160);
}

function mapCodegraphMatchesToScene(scene, payload) {
  const mapped = {
    node_ids: new Set(),
    link_ids: new Set(),
    node_paths: [],
    link_paths: []
  };
  if (!scene || !scene.node_by_id || !payload || typeof payload !== 'object') return mapped;
  const nodeRows = codegraphRows(payload, 'nodes');
  const linkRows = codegraphRows(payload, 'links');
  const nodePathList = nodeRows
    .map((row) => normalizeRelPathText(row && row.rel))
    .filter(Boolean);
  const linkPathList = linkRows
    .map((row) => ({
      from_rel: normalizeRelPathText(row && row.from_rel),
      to_rel: normalizeRelPathText(row && row.to_rel),
      kind: String(row && row.kind || '').toLowerCase()
    }))
    .filter((row) => row.from_rel || row.to_rel);
  mapped.node_paths = nodePathList.slice(0, 24);
  mapped.link_paths = linkPathList
    .slice(0, 24)
    .map((row) => `${row.from_rel || '?'} -> ${row.to_rel || '?'}`);
  const sceneNodes = Array.isArray(scene.nodes) ? scene.nodes : [];
  for (const node of sceneNodes) {
    const nodeId = String(node && node.id || '');
    const nodeRel = normalizeRelPathText(node && node.rel);
    if (!nodeId || !nodeRel) continue;
    if (nodePathList.some((rel) => relPathsOverlap(nodeRel, rel))) {
      mapped.node_ids.add(nodeId);
    }
  }
  const sceneLinks = Array.isArray(scene.links) ? scene.links : [];
  const nodeById = scene.node_by_id && typeof scene.node_by_id === 'object'
    ? scene.node_by_id
    : Object.create(null);
  for (const link of sceneLinks) {
    if (!link) continue;
    const linkId = String(link.id || '');
    const fromId = String(link.from_id || '');
    const toId = String(link.to_id || '');
    const fromNode = nodeById[fromId];
    const toNode = nodeById[toId];
    const fromRel = normalizeRelPathText(fromNode && fromNode.rel);
    const toRel = normalizeRelPathText(toNode && toNode.rel);
    const kind = String(link.kind || '').toLowerCase();
    let matched = false;
    for (const row of linkPathList) {
      const kindMatch = !row.kind || row.kind === kind;
      const fromMatch = !row.from_rel || relPathsOverlap(fromRel, row.from_rel);
      const toMatch = !row.to_rel || relPathsOverlap(toRel, row.to_rel);
      if (kindMatch && fromMatch && toMatch) {
        matched = true;
        break;
      }
    }
    if (!matched && (mapped.node_ids.has(fromId) || mapped.node_ids.has(toId))) {
      matched = true;
    }
    if (matched && linkId) {
      mapped.link_ids.add(linkId);
      mapped.node_ids.add(fromId);
      mapped.node_ids.add(toId);
    }
  }
  return mapped;
}

function applyCodegraphMatches(scene = state.scene) {
  const cg = codegraphState();
  const payload = cg.last_result && typeof cg.last_result === 'object'
    ? cg.last_result
    : null;
  if (!payload || !scene) {
    cg.matched_node_ids = new Set();
    cg.matched_link_ids = new Set();
    cg.matched_node_paths = [];
    cg.matched_link_paths = [];
    return;
  }
  const mapped = mapCodegraphMatchesToScene(scene, payload);
  cg.matched_node_ids = mapped.node_ids;
  cg.matched_link_ids = mapped.link_ids;
  cg.matched_node_paths = mapped.node_paths;
  cg.matched_link_paths = mapped.link_paths;
}

function renderCodegraphStatus() {
  const cg = codegraphState();
  const el = byId('queryStatus');
  if (!el) return;
  if (cg.running) {
    el.textContent = 'CodeGraph: querying...';
    return;
  }
  if (cg.error) {
    el.textContent = `CodeGraph: ${cg.error}`;
    return;
  }
  if (cg.notice) {
    el.textContent = `CodeGraph: ${cg.notice}`;
    return;
  }
  if (cg.query) {
    const nodeCount = cg.matched_node_ids instanceof Set ? cg.matched_node_ids.size : 0;
    const linkCount = cg.matched_link_ids instanceof Set ? cg.matched_link_ids.size : 0;
    const mode = String(cg.mode || 'search');
    el.textContent = `CodeGraph: ${mode} | nodes ${nodeCount} | links ${linkCount}`;
    return;
  }
  el.textContent = 'CodeGraph: idle';
}

async function runCodegraphQuery(options = {}) {
  const cg = codegraphState();
  const inputEl = byId('queryInput');
  const rawQuery = options && options.query != null
    ? String(options.query)
    : String(inputEl && inputEl.value || '');
  const query = rawQuery.trim();
  if (!query) {
    cg.error = 'query_required';
    cg.notice = '';
    cg.last_result = null;
    cg.query = '';
    cg.mode = '';
    cg.matched_node_ids = new Set();
    cg.matched_link_ids = new Set();
    cg.matched_node_paths = [];
    cg.matched_link_paths = [];
    syncParticlePool(true);
    renderCodegraphStatus();
    renderStats();
    return;
  }
  cg.running = true;
  cg.error = '';
  cg.notice = '';
  renderCodegraphStatus();
  renderStats();
  try {
    const payload = await fetchCodegraphQuery(query, {
      mode: String(options.mode || '').trim(),
      limit: CODEGRAPH_QUERY_LIMIT,
      reindex: options.reindex === true
    });
    if (!payload || payload.ok !== true) {
      throw new Error(String(payload && payload.error || 'codegraph_query_failed'));
    }
    cg.query = query;
    cg.mode = String(payload.mode || 'search');
    cg.last_result = payload;
    cg.error = '';
    cg.notice = '';
    cg.running = false;
    if (inputEl) inputEl.value = query;
    applyCodegraphMatches(state.scene);
    syncParticlePool(true);
    renderCodegraphStatus();
    renderStats();
  } catch (err) {
    cg.running = false;
    cg.error = String(err && err.message || err || 'codegraph_query_failed');
    cg.notice = '';
    renderCodegraphStatus();
    renderStats();
  }
}

async function reindexCodegraph() {
  const cg = codegraphState();
  cg.running = true;
  cg.error = '';
  cg.notice = '';
  renderCodegraphStatus();
  renderStats();
  try {
    const payload = await fetchCodegraphReindex();
    const summary = payload && payload.summary && typeof payload.summary === 'object'
      ? payload.summary
      : {};
    const files = Math.max(0, Number(summary.files_scanned || 0));
    const nodes = Math.max(0, Number(summary.node_count || 0));
    const edges = Math.max(0, Number(summary.edge_count || 0));
    cg.notice = `reindexed ${files} files (${nodes} nodes, ${edges} links)`;
    cg.running = false;
    renderCodegraphStatus();
    if (cg.query) {
      await runCodegraphQuery({ query: cg.query, mode: cg.mode || '', reindex: true });
      return;
    }
    renderStats();
  } catch (err) {
    cg.running = false;
    cg.error = String(err && err.message || err || 'codegraph_reindex_failed');
    cg.notice = '';
    renderCodegraphStatus();
    renderStats();
  }
}

function parseJsonSafe(raw) {
  try {
    return JSON.parse(String(raw || ''));
  } catch {
    return null;
  }
}

function currentHours() {
  return Math.max(1, Number(byId('hours').value || 24));
}

function currentLiveMode() {
  const el = byId('liveMode');
  if (!el) return state.live_mode !== false;
  return el.checked !== false;
}

function currentLiveMinutes() {
  const el = byId('liveMinutes');
  return Math.max(1, Number(el && el.value || state.live_minutes || 6));
}

function currentLiveConfig() {
  return {
    live_mode: currentLiveMode(),
    live_minutes: currentLiveMinutes()
  };
}

function syncLiveControlState() {
  const liveMinutesEl = byId('liveMinutes');
  if (!liveMinutesEl) return;
  liveMinutesEl.disabled = !currentLiveMode();
}

function wsEndpoint(hours, options = {}) {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const h = Math.max(1, Number(hours || 24));
  const liveMode = options && options.live_mode === false ? 0 : 1;
  const liveMinutes = Math.max(1, Number(options && options.live_minutes || state.live_minutes || 6));
  const q = new URLSearchParams();
  q.set('hours', String(h));
  q.set('live_mode', String(liveMode));
  q.set('live_minutes', String(liveMinutes));
  return `${proto}//${window.location.host}/ws/holo?${q.toString()}`;
}

function runtimeSnapshotFromPayload(payload) {
  const summary = payload && payload.summary && typeof payload.summary === 'object'
    ? payload.summary
    : {};
  const runtime = summary.runtime && typeof summary.runtime === 'object'
    ? summary.runtime
    : (payload && payload.runtime && typeof payload.runtime === 'object' ? payload.runtime : {});
  const status = String(runtime.status || '').trim().toLowerCase() || 'unknown';
  const online = runtime.online === true || status === 'online';
  const stale = runtime.stale === true || status === 'stale';
  const offline = runtime.offline === true || status === 'offline';
  const signalAge = runtime.signal_age_sec == null ? null : Number(runtime.signal_age_sec);
  const liveWindowMinutes = Math.max(1, Number(runtime.live_window_minutes || payload && payload.live_minutes || state.live_minutes || 6));
  const activityScale = clamp(Number(runtime.activity_scale == null ? (offline ? 0.2 : (stale ? 0.58 : 1)) : runtime.activity_scale), 0.12, 1.2);
  return {
    status,
    online,
    stale,
    offline,
    signal_age_sec: Number.isFinite(signalAge) ? signalAge : null,
    live_window_minutes: liveWindowMinutes,
    reason: String(runtime.reason || ''),
    source: String(runtime.source || ''),
    latest_signal_ts: String(runtime.latest_signal_ts || ''),
    activity_scale: activityScale
  };
}

function sendWs(msg) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return false;
  try {
    state.ws.send(JSON.stringify(msg));
    return true;
  } catch {
    return false;
  }
}

function applySpinePulse(pulse) {
  const p = pulse && typeof pulse === 'object' ? pulse : {};
  const count = Math.max(0, Number(p.event_count || 0));
  const top = Array.isArray(p.top_types) && p.top_types.length ? String(p.top_types[0][0] || '') : '';
  state.spine_event_count = count;
  state.spine_event_top = top;
  const burstMs = 750 + Math.min(2200, count * 12);
  state.spine_burst_until = performance.now() + burstMs;
}

function applySnapshotMessage(msg) {
  if (!msg || typeof msg !== 'object') return;
  const summary = msg.summary && typeof msg.summary === 'object' ? msg.summary : null;
  const holo = msg.holo && typeof msg.holo === 'object' ? msg.holo : null;
  const incidents = msg.incidents && typeof msg.incidents === 'object' ? msg.incidents : {};
  if (summary && holo) {
    state.live_mode = msg.live_mode !== false;
    state.live_minutes = Math.max(1, Number(msg.live_minutes || state.live_minutes || 6));
    state.runtime = runtimeSnapshotFromPayload({
      summary,
      runtime: msg.runtime || null,
      live_minutes: state.live_minutes
    });
    setPayload({
      ok: true,
      generated_at: msg.generated_at || new Date().toISOString(),
      live_mode: state.live_mode,
      live_minutes: state.live_minutes,
      runtime: state.runtime,
      summary,
      holo,
      incidents
    });
  }
  applySpinePulse(msg.spine_pulse || null);
}

function scheduleWsReconnect() {
  if (state.ws_retry_timer) clearTimeout(state.ws_retry_timer);
  const wait = Math.max(600, Math.min(15000, Number(state.ws_backoff_ms || 900)));
  state.ws_retry_timer = setTimeout(() => {
    connectWebSocket();
  }, wait);
  state.ws_backoff_ms = Math.min(15000, Math.round(wait * 1.55));
}

function connectWebSocket() {
  if (state.ws_retry_timer) {
    clearTimeout(state.ws_retry_timer);
    state.ws_retry_timer = null;
  }
  if (state.ws) {
    try { state.ws.close(); } catch {}
    state.ws = null;
  }
  let socket = null;
  const liveCfg = currentLiveConfig();
  try {
    socket = new WebSocket(wsEndpoint(currentHours(), liveCfg));
  } catch {
    state.ws_connected = false;
    state.transport = 'poll';
    scheduleWsReconnect();
    return;
  }
  state.ws = socket;
  socket.addEventListener('open', () => {
    state.ws_connected = true;
    state.transport = 'ws';
    state.ws_backoff_ms = 900;
    const cfg = currentLiveConfig();
    state.live_mode = cfg.live_mode;
    state.live_minutes = cfg.live_minutes;
    sendWs({ type: 'subscribe', hours: currentHours(), live_mode: cfg.live_mode, live_minutes: cfg.live_minutes });
  });
  socket.addEventListener('message', (evt) => {
    const msg = parseJsonSafe(evt.data);
    if (!msg || typeof msg !== 'object') return;
    if (String(msg.type || '') === 'holo_snapshot') {
      applySnapshotMessage(msg);
    }
  });
  socket.addEventListener('close', () => {
    if (state.ws !== socket) return;
    state.ws_connected = false;
    state.transport = 'poll';
    scheduleWsReconnect();
  });
  socket.addEventListener('error', () => {
    // close handler will handle reconnect
  });
}

function bezierPoint(edge, t) {
  const s = 1 - t;
  const x = (s * s * s * edge.p0.x)
    + (3 * s * s * t * edge.p1.x)
    + (3 * s * t * t * edge.p2.x)
    + (t * t * t * edge.p3.x);
  const y = (s * s * s * edge.p0.y)
    + (3 * s * s * t * edge.p1.y)
    + (3 * s * t * t * edge.p2.y)
    + (t * t * t * edge.p3.y);
  return { x, y };
}

function setLinkGeometry(link, from, to, center) {
  if (!link || !from || !to || !center) return;
  const dx = Number(to.x || 0) - Number(from.x || 0);
  const dy = Number(to.y || 0) - Number(from.y || 0);
  const dist = Math.max(1, Math.hypot(dx, dy));
  const kind = String(link.kind || 'flow');
  const bend = Math.min(160, Math.max(40, dist * 0.28));
  const nx = -dy / dist;
  const ny = dx / dist;
  const p0 = { x: Number(from.x || 0), y: Number(from.y || 0) };
  const p3 = { x: Number(to.x || 0), y: Number(to.y || 0) };
  let p1 = { x: p0.x + (dx * 0.33) + (nx * bend), y: p0.y + (dy * 0.33) + (ny * bend) };
  let p2 = { x: p0.x + (dx * 0.66) + (nx * bend), y: p0.y + (dy * 0.66) + (ny * bend) };

  if (kind !== 'hierarchy' && kind !== 'fractal') {
    const side = Number(link.arc_side || 0) >= 0 ? 1 : -1;
    const spineBias = kind === 'ingress' || kind === 'egress' ? 0.76 : 0.67;
    const spineOffset = Math.min(90, Math.max(20, dist * 0.12)) * side;
    p1 = {
      x: p0.x + ((center.x - p0.x) * spineBias) + (nx * spineOffset),
      y: p0.y + ((center.y - p0.y) * spineBias) + (ny * spineOffset)
    };
    p2 = {
      x: p3.x + ((center.x - p3.x) * spineBias) + (nx * spineOffset),
      y: p3.y + ((center.y - p3.y) * spineBias) + (ny * spineOffset)
    };
  }

  link.p0 = p0;
  link.p1 = p1;
  link.p2 = p2;
  link.p3 = p3;
}

function computeNodeErrorHint(...parts) {
  const text = parts
    .map((p) => String(p == null ? '' : p).toLowerCase())
    .join(' ');
  if (!text.trim()) return 0;
  let score = 0;
  if (/(error|fail|panic|revert|reject|exception)/.test(text)) score += 0.52;
  if (/(failsafe|fallback|hold|blocked|block|guard|gate|policy)/.test(text)) score += 0.34;
  if (/(safety|security|risk)/.test(text)) score += 0.22;
  if (/(route|router|ingress|egress|health)/.test(text)) score += 0.12;
  if (/\bdrift\b/.test(text)) score += 0.1;
  return clamp(score, 0, 1);
}

function hasActiveErrorState(rawNode, errorHint = 0, errorSignal = 0) {
  const row = rawNode && typeof rawNode === 'object' ? rawNode : {};
  const explicit = Boolean(
    row.error_state === true
    || row.error_active === true
    || row.has_error === true
    || row.error === true
  );
  if (explicit) return true;
  const status = String(row.status || row.state || '').toLowerCase();
  if (/(error|failed|failure|panic|exception|fault|degraded)/.test(status)) return true;
  const hint = clamp(Number(errorHint || 0), 0, 1);
  const signal = clamp(Number(errorSignal || 0), 0, 1);
  return hint * signal >= 0.16;
}

function computeErrorSignal(summary) {
  const s = summary && typeof summary === 'object' ? summary : {};
  const runEvents = Math.max(1, Number(s.run_events || 0));
  const policyHolds = Math.max(0, Number(s.policy_holds || 0));
  const routeBlocked = Math.max(0, Number(s.route_blocked || 0));
  const confidenceFallback = Math.max(0, Number(s.confidence_fallback || 0));
  const reverted = Math.max(0, Number(s.reverted || 0));

  const basePressure = (policyHolds + routeBlocked + confidenceFallback + (reverted * 1.35)) / runEvents;
  const gateRows = Array.isArray(s.top_rejected_gates) ? s.top_rejected_gates : [];
  let gateTotal = 0;
  for (const row of gateRows) {
    const count = Array.isArray(row) ? Number(row[1] || 0) : 0;
    if (Number.isFinite(count) && count > 0) gateTotal += count;
  }
  const gatePressure = gateTotal / (runEvents * 4.5);
  return clamp((basePressure * 0.78) + (gatePressure * 0.22), 0, 1);
}

function asStringArray(rows, limit = 12) {
  const src = Array.isArray(rows) ? rows : [];
  const out = [];
  for (const row of src) {
    if (out.length >= limit) break;
    const value = String(row || '').trim();
    if (!value) continue;
    out.push(value);
  }
  return out;
}

function normalizeIntegrityIncident(payload) {
  const root = payload && typeof payload === 'object' ? payload : {};
  const summary = root.summary && typeof root.summary === 'object' ? root.summary : {};
  const incidents = root.incidents && typeof root.incidents === 'object' ? root.incidents : {};
  const integrity = incidents.integrity && typeof incidents.integrity === 'object'
    ? incidents.integrity
    : {};
  const holoMetrics = root.holo && root.holo.metrics && typeof root.holo.metrics === 'object'
    ? root.holo.metrics
    : {};
  const active = (
    integrity.active_alert === true
    || summary.integrity_active_alert === true
    || Number(holoMetrics.integrity_alert || 0) > 0
  );
  const violationTotal = Math.max(
    0,
    Number(
      integrity.violation_total
      || summary.integrity_violation_total
      || 0
    ) || 0
  );
  const severityRaw = String(
    integrity.severity
    || summary.integrity_severity
    || holoMetrics.integrity_severity
    || (active ? 'critical' : 'ok')
  ).trim().toLowerCase();
  const severity = severityRaw || (active ? 'critical' : 'ok');
  const violationsRaw = Array.isArray(integrity.violations) ? integrity.violations : [];
  const violations = [];
  for (const row of violationsRaw) {
    const file = String(row && row.file || '').trim();
    const type = String(row && row.type || 'unknown').trim() || 'unknown';
    const detail = String(row && row.detail || '').trim();
    if (!file) continue;
    violations.push({
      file,
      type,
      detail: detail ? detail.slice(0, 180) : ''
    });
    if (violations.length >= 16) break;
  }
  if (!violations.length) {
    const fallbackFiles = asStringArray(
      integrity.top_files
      || summary.integrity_top_files
      || [],
      16
    );
    for (const file of fallbackFiles) {
      violations.push({ file, type: 'hash_mismatch', detail: '' });
    }
  }
  return {
    active,
    severity,
    violation_total: violationTotal,
    top_files: asStringArray(
      integrity.top_files
      || summary.integrity_top_files
      || []
    ),
    last_violation_ts: String(
      integrity.last_violation_ts
      || summary.integrity_last_violation_ts
      || ''
    ).trim(),
    last_reseal_ts: String(
      integrity.last_reseal_ts
      || summary.integrity_last_reseal_ts
      || ''
    ).trim(),
    policy_path: String(integrity.policy_path || summary.integrity_policy_path || '').trim(),
    policy_version: String(integrity.policy_version || summary.integrity_policy_version || '').trim(),
    violations
  };
}

function hasIntegrityPathMatch(relPath, files) {
  const rel = String(relPath || '').trim().toLowerCase();
  if (!rel) return false;
  const rows = Array.isArray(files) ? files : [];
  for (const fileRow of rows) {
    const file = String(fileRow || '').trim().toLowerCase();
    if (!file) continue;
    if (file === rel) return true;
    if (file.startsWith(`${rel}/`)) return true;
    if (rel.startsWith(`${file}/`)) return true;
  }
  return false;
}

function integrityRowsForSelection(incident, selectedType, selectedNode) {
  const out = [];
  const alert = incident && typeof incident === 'object' ? incident : {};
  if (alert.active !== true) return out;
  const type = String(selectedType || '').toLowerCase();
  const node = selectedNode && typeof selectedNode === 'object' ? selectedNode : null;
  const allRows = Array.isArray(alert.violations) ? alert.violations : [];
  let matched = [];

  if (type === 'spine' || type === 'system') {
    matched = allRows.slice(0, 6);
  } else if (node) {
    const rel = String(node.rel || '').trim();
    if (rel) {
      matched = allRows.filter((row) => hasIntegrityPathMatch(rel, [String(row && row.file || '')]));
    }
    matched = matched.slice(0, 6);
  }

  if (!matched.length) {
    if (type === 'spine' || type === 'system') {
      out.push(['Integrity Errors', 'No mapped errors for selection']);
      return out;
    }
    const nodeHasLocalAlert = !!(node && (node.error_state_active === true || node.integrity_alert === true));
    if (nodeHasLocalAlert) {
      out.push(['Integrity Errors', 'Local alert active (file mapping unavailable)']);
    }
    return out;
  }

  out.push(['Integrity Errors', `${fmtNum(matched.length)} mapped`]);
  for (let i = 0; i < matched.length; i += 1) {
    const row = matched[i];
    const file = String(row && row.file || '').trim();
    const vType = String(row && row.type || 'violation').trim();
    const detail = String(row && row.detail || '').trim();
    const value = detail
      ? `${vType}: ${file} (${detail})`
      : `${vType}: ${file}`;
    out.push([`Error ${i + 1}`, value]);
  }
  return out;
}

function isPreviewErrorRow(key, value) {
  const k = String(key || '').trim();
  const v = String(value == null ? '' : value).trim().toLowerCase();
  if (!k) return false;
  if (k.toLowerCase() === 'runtime' && v.includes('offline')) return true;
  if (k === 'Errors') return true;
  if (k === 'Error Source') return true;
  if (k === 'Error Reason') return true;
  if (k === 'Error State') return true;
  if (k === 'Integrity Errors') return true;
  if (/^Error\s+\d+$/i.test(k)) return true;
  return false;
}

function isPreviewWarningRow(key, value) {
  const k = String(key || '').trim().toLowerCase();
  const v = String(value == null ? '' : value).trim().toLowerCase();
  if (!k && !v) return false;
  if (isPreviewErrorRow(key, value)) return false;
  if (k === 'runtime' && (v.includes('offline') || v.includes('no signal'))) return true;
  if (k === 'runtime' && v.includes('stale')) return true;
  if (k === 'continuum pulse' && (v.includes('unavailable') || v.includes('skipped') || v.includes('stale'))) return true;
  if (k === 'continuum trit' && (v.includes('pain') || v.includes('unknown') || v.includes('(0)') || v.includes('(-1)'))) return true;
  if (k === 'continuum red-team critical') {
    const n = Number(String(value == null ? '' : value).replace(/[^0-9.\-]/g, ''));
    if (Number.isFinite(n) && n > 0) return true;
  }
  if (k === 'blocked links') {
    const n = Number(String(value == null ? '' : value).replace(/[^0-9.\-]/g, ''));
    if (Number.isFinite(n) && n > 0) return true;
  }
  if (k === 'max block ratio') {
    const n = Number(String(value == null ? '' : value).replace(/[^0-9.\-]/g, ''));
    if (Number.isFinite(n) && n > 0) return true;
  }
  if (k === 'motion smoothness') {
    const n = Number(String(value == null ? '' : value).replace(/[^0-9.\-]/g, ''));
    if (Number.isFinite(n) && n < 65) return true;
  }
  if (k === 'fractal harmony') {
    const n = Number(String(value == null ? '' : value).replace(/[^0-9.\-]/g, ''));
    if (Number.isFinite(n) && n < 45) return true;
  }
  if (k === 'black-box rows') {
    const n = Number(String(value == null ? '' : value).replace(/[^0-9.\-]/g, ''));
    if (Number.isFinite(n) && n <= 0) return true;
  }
  if (k === 'integrity' && (v.includes('warning') || v.includes('recent'))) return true;
  if (k === 'health' && v.includes('watch')) return true;
  if (k === 'flow status' && (v.includes('degraded') || v.includes('partial') || v.includes('blocked'))) return true;
  if (k.includes('warning') || k.includes('warn')) return true;
  if (v.includes('warning') || v.includes('warn')) return true;
  if (v.includes('watch')) return true;
  if (v.includes('recent')) return true;
  return false;
}

function isPreviewGoodRow(key, value) {
  const k = String(key || '').trim().toLowerCase();
  const v = String(value == null ? '' : value).trim().toLowerCase();
  if (!k && !v) return false;
  if (k === 'runtime' && v.includes('online')) return true;
  if (k === 'continuum pulse' && v.startsWith('active')) return true;
  if (k === 'continuum trit' && (v.includes('ok (1)') || v.includes('true (1)'))) return true;
  if (k === 'continuum red-team critical') {
    const n = Number(String(value == null ? '' : value).replace(/[^0-9.\-]/g, ''));
    if (Number.isFinite(n) && n === 0) return true;
  }
  if (k === 'constitution alignment' && (v.includes('green') || v.includes('aligned'))) return true;
  if (k === 'evolution trajectory' && v.includes('accelerating')) return true;
  if (k === 'fractal harmony') {
    const n = Number(String(value == null ? '' : value).replace(/[^0-9.\-]/g, ''));
    if (Number.isFinite(n) && n >= 70) return true;
  }
  if (k === 'black-box rows') {
    const n = Number(String(value == null ? '' : value).replace(/[^0-9.\-]/g, ''));
    if (Number.isFinite(n) && n > 0) return true;
  }
  if (k === 'integrity' && v === 'ok') return true;
  if (k === 'health' && v.includes('nominal')) return true;
  if (k === 'flow status' && v === 'clear') return true;
  return false;
}

function nodeErrorRowsForSelection(node, integrityRows = [], errorSignal = 0) {
  const out = [];
  const row = node && typeof node === 'object' ? node : null;
  if (!row) return out;
  const hasIntegrityRows = Array.isArray(integrityRows) && integrityRows.length > 0;
  const doctorState = String(row.doctor_state || '').trim().toLowerCase();
  const hasDoctorState = Boolean(doctorState);
  const hasNodeError = row.error_state_active === true || row.integrity_alert === true;
  if (!hasNodeError && !hasDoctorState) return out;

  if (hasDoctorState) {
    const doctorCode = String(row.doctor_code || 'autotest_doctor_event').trim();
    const doctorReason = String(
      row.doctor_summary
      || (
        doctorState === 'rollback_cut'
          ? 'Doctor triggered rollback cut due to failed repair/regression.'
          : doctorState === 'wounded'
            ? 'Doctor marked this module as wounded due to destructive/blocked repair signal.'
            : doctorState === 'healing'
              ? 'Doctor healing attempt in progress.'
              : doctorState === 'regrowth'
                ? 'Doctor observed successful regrowth for this module.'
                : 'Doctor event mapped to selected node.'
      )
    ).trim();
    const stateLabel = doctorState
      ? doctorState.replace(/_/g, ' ')
      : 'active';
    out.push(['Errors', stateLabel]);
    out.push(['Error Source', `autotest_doctor/${doctorCode}`]);
    out.push(['Error Reason', doctorReason]);
    if (row.doctor_severity) {
      out.push(['Error Severity', String(row.doctor_severity).toLowerCase()]);
    }
    if (row.doctor_module) {
      out.push(['Error Module', String(row.doctor_module)]);
    }
    if (!hasNodeError) return out;
  }

  if (hasIntegrityRows && !hasDoctorState) return out;

  const source = row.integrity_alert === true
    ? 'integrity'
    : 'runtime';
  const reason = source === 'integrity'
    ? 'Integrity alert active for selected path.'
    : 'Selected node is in active error state (runtime signal).';

  out.push(['Errors', 'Active']);
  out.push(['Error Source', source]);
  out.push(['Error Reason', reason]);
  const hint = clamp(Number(row.error_hint || 0), 0, 1);
  if (hint > 0) out.push(['Error Hint', `${fmtNum(hint * 100)}%`]);
  const signal = clamp(Number(errorSignal || 0), 0, 1);
  if (signal > 0) out.push(['Error Signal', `${fmtNum(signal * 100)}%`]);
  return out;
}

function linkErrorRowsForSelection(link) {
  const out = [];
  const row = link && typeof link === 'object' ? link : null;
  if (!row) return out;
  const doctorState = String(row.doctor_state || '').trim().toLowerCase();
  const kind = String(row.kind || '').trim().toLowerCase();
  const errWeight = clamp(Number(row.error_weight || 0), 0, 1);
  const blockedRatio = clamp(Number(row.blocked_ratio || 0), 0, 1);
  const blocked = row.flow_blocked === true || blockedRatio > 0.02;
  const degraded = errWeight >= 0.045;
  const doctorPath = kind === 'doctor' || Boolean(doctorState);
  if (!blocked && !degraded && !doctorPath) return out;

  if (doctorPath) {
    const stateLabel = doctorState || (blocked ? 'wounded' : 'healing');
    out.push(['Errors', stateLabel.replace(/_/g, ' ')]);
    out.push(['Error Source', 'autotest_doctor/doctor_link']);
    if (blocked) {
      out.push(['Error Reason', String(row.block_reason || 'Doctor flow blocked by rollback/kill switch signal.')]);
    } else if (doctorState === 'regrowth') {
      out.push(['Error Reason', 'Doctor regrowth link indicates module recovered.']);
    } else if (doctorState === 'healing') {
      out.push(['Error Reason', 'Doctor healing attempt flow active.']);
    } else {
      out.push(['Error Reason', `Doctor path watch signal at ${fmtNum(errWeight * 100)}% weight.`]);
    }
    return out;
  }

  out.push(['Errors', blocked ? 'Blocked Flow' : 'Degraded Flow']);
  out.push(['Error Source', blocked ? 'flow_block' : 'error_weight']);
  if (blocked) {
    out.push(['Error Reason', String(row.block_reason || 'Blocked by policy/gate/route guard.')]);
  } else {
    out.push(['Error Reason', `Link error weight ${fmtNum(errWeight * 100)}% exceeded watch threshold.`]);
  }
  return out;
}

function normalizeChangeState(raw) {
  const row = raw && typeof raw === 'object' ? raw : {};
  const topFiles = Array.isArray(row.top_files)
    ? row.top_files.map((v) => String(v || '').trim()).filter(Boolean).slice(0, 10)
    : [];
  const activeWrite = row.active_write === true;
  const dirty = row.dirty === true;
  const staged = row.staged === true;
  const pendingPush = row.pending_push === true;
  const justPushed = row.just_pushed === true;
  const changed = row.changed === true || activeWrite || dirty || staged || pendingPush || justPushed;
  return {
    active_write: activeWrite,
    dirty,
    staged,
    pending_push: pendingPush,
    just_pushed: justPushed,
    changed,
    file_count: Math.max(0, Number(row.file_count || topFiles.length || 0) || 0),
    dirty_file_count: Math.max(0, Number(row.dirty_file_count || 0) || 0),
    staged_file_count: Math.max(0, Number(row.staged_file_count || 0) || 0),
    pending_push_file_count: Math.max(0, Number(row.pending_push_file_count || 0) || 0),
    active_write_file_count: Math.max(0, Number(row.active_write_file_count || 0) || 0),
    top_files: topFiles,
    last_push_ts: String(row.last_push_ts || '').trim()
  };
}

function changeStateLabel(change) {
  const c = normalizeChangeState(change);
  const flags = [];
  if (c.active_write) flags.push('mutating');
  if (c.dirty) flags.push('dirty');
  if (c.staged) flags.push('staged');
  if (c.pending_push) flags.push('pending_push');
  if (c.just_pushed) flags.push('just_pushed');
  return flags.length ? flags.join(', ') : 'none';
}

function changeRowsForSelection(change, selectedType, selectedNode) {
  const rows = [];
  const c = normalizeChangeState(change);
  if (!c.changed && !c.just_pushed) return rows;
  const type = String(selectedType || '').toLowerCase();
  if (type !== 'module' && type !== 'submodule' && type !== 'spine' && type !== 'system') return rows;
  rows.push(['Change State', changeStateLabel(c)]);
  if (c.file_count > 0) rows.push(['Changed Files', fmtNum(c.file_count)]);
  if (c.top_files.length) rows.push(['Change Paths', c.top_files.slice(0, 3).join(' | ')]);
  if (c.last_push_ts) rows.push(['Last Push', new Date(c.last_push_ts).toLocaleString()]);
  return rows;
}

function updateHitTargetCircle(scene, id, x, y, radius) {
  if (!scene || !scene.hit_target_by_id) return;
  const target = scene.hit_target_by_id[String(id || '')];
  if (!target || target.kind !== 'circle') return;
  target.x = Number(x || 0);
  target.y = Number(y || 0);
  if (Number.isFinite(radius)) target.r = Number(radius);
}

function nodeIntroScale(node) {
  const raw = Number(node && node.intro_scale);
  if (!Number.isFinite(raw)) return 1;
  return clamp(raw, 0, 1);
}

function selectionTypeFrom(node, kindHint = '') {
  const nodeType = String(node && node.type || '').toLowerCase();
  if (nodeType) return nodeType;
  const kind = String(kindHint || '').toLowerCase();
  if (kind === 'ring' || kind === 'shell_fill') return 'layer';
  if (kind === 'system') return 'system';
  return '';
}

function cloneSelectionRef(selection) {
  const src = selection && typeof selection === 'object' ? selection : null;
  if (!src) return null;
  const id = String(src.id || '').trim();
  if (!id) return null;
  return {
    id,
    name: String(src.name || id),
    path: String(src.path || ''),
    kind: String(src.kind || ''),
    type: String(src.type || '')
  };
}

function subfractalChildrenForNode(node) {
  const rows = node && typeof node === 'object' && Array.isArray(node.subfractal_children)
    ? node.subfractal_children
    : [];
  const out = [];
  for (const row of rows) {
    const child = row && typeof row === 'object' ? row : {};
    const id = String(child.id || '').trim();
    const name = String(child.name || '').trim();
    if (!id || !name) continue;
    out.push({
      id,
      name,
      rel: String(child.rel || '').trim()
    });
  }
  return out;
}

function setSelectedSubfractal(next) {
  const row = next && typeof next === 'object' ? next : null;
  if (!row) {
    state.selected_subfractal = null;
    return;
  }
  const id = String(row.id || '').trim();
  const parentId = String(row.parent_id || '').trim();
  if (!id || !parentId) {
    state.selected_subfractal = null;
    return;
  }
  state.selected_subfractal = {
    id,
    parent_id: parentId,
    index: Math.max(0, Math.floor(Number(row.index || 0) || 0)),
    count: Math.max(0, Math.floor(Number(row.count || 0) || 0)),
    name: String(row.name || id),
    path: String(row.path || '')
  };
}

function clearSelectedSubfractal() {
  state.selected_subfractal = null;
}

function validateSelectedSubfractal(scene) {
  const selected = state.selected_subfractal && typeof state.selected_subfractal === 'object'
    ? state.selected_subfractal
    : null;
  if (!selected || !scene || !scene.node_by_id) {
    if (selected) state.selected_subfractal = null;
    return null;
  }
  const parentId = String(selected.parent_id || '').trim();
  const selectedId = String(state.selected && state.selected.id || '').trim();
  const selectedType = String(state.selected && state.selected.type || '').toLowerCase();
  if (!parentId || selectedId !== parentId || selectedType !== 'submodule') {
    state.selected_subfractal = null;
    return null;
  }
  const node = scene.node_by_id[parentId];
  if (!node || String(node.type || '').toLowerCase() !== 'submodule') {
    state.selected_subfractal = null;
    return null;
  }
  const children = subfractalChildrenForNode(node);
  if (!children.length) {
    state.selected_subfractal = null;
    return null;
  }
  const currentId = String(selected.id || '').trim();
  const idx = children.findIndex((child) => String(child.id || '') === currentId);
  if (idx < 0) {
    state.selected_subfractal = null;
    return null;
  }
  const child = children[idx];
  const normalized = {
    id: String(child.id || ''),
    parent_id: parentId,
    index: idx,
    count: children.length,
    name: String(child.name || child.id || ''),
    path: String(child.rel || '')
  };
  state.selected_subfractal = normalized;
  return normalized;
}

function subfractalAnchorForSelection(node, selectedSubfractal) {
  const subNode = node && typeof node === 'object' ? node : null;
  const selected = selectedSubfractal && typeof selectedSubfractal === 'object'
    ? selectedSubfractal
    : null;
  if (!subNode || !selected) return null;
  const children = subfractalChildrenForNode(subNode);
  if (!children.length) return null;
  const idx = clamp(Math.floor(Number(selected.index || 0) || 0), 0, children.length - 1);
  const start = Number(subNode.shell_start || 0);
  const end = Number(subNode.shell_end || (start + (Math.PI * 0.35)));
  const a = angleAtArcFraction(start, end, (idx + 0.5) / children.length);
  const px = Number(subNode.parent_x || subNode.x || 0);
  const py = Number(subNode.parent_y || subNode.y || 0);
  const inner = Math.max(0.5, Number(subNode.shell_inner || (Number(subNode.radius || 2) * 2.2)));
  const outer = Math.max(inner + 0.5, Number(subNode.shell_outer || (Number(subNode.radius || 2) * 3.1)));
  const r = inner + ((outer - inner) * 0.56);
  return {
    x: px + (Math.cos(a) * r),
    y: py + (Math.sin(a) * r)
  };
}

function subfractalSelectionExtent(node, selectedSubfractal) {
  const subNode = node && typeof node === 'object' ? node : null;
  const selected = selectedSubfractal && typeof selectedSubfractal === 'object'
    ? selectedSubfractal
    : null;
  if (!subNode || !selected) return null;
  const children = subfractalChildrenForNode(subNode);
  if (!children.length) return null;
  const idx = clamp(Math.floor(Number(selected.index || 0) || 0), 0, children.length - 1);
  const inner = Math.max(0.5, Number(subNode.shell_inner || (Number(subNode.radius || 2) * 2.2)));
  const outer = Math.max(inner + 0.5, Number(subNode.shell_outer || (Number(subNode.radius || 2) * 3.1)));
  const start = Number(subNode.shell_start || 0);
  const end = Number(subNode.shell_end || (start + (Math.PI * 0.35)));
  const segStart = angleAtArcFraction(start, end, idx / children.length);
  const segEnd = angleAtArcFraction(start, end, (idx + 1) / children.length);
  const span = positiveAngleSpan(segStart, segEnd);
  const arcLen = span * ((inner + outer) * 0.5);
  const radial = outer - inner;
  const extent = Math.max(6, arcLen, radial);
  return {
    extent,
    inner,
    outer,
    seg_start: segStart,
    seg_end: segEnd
  };
}

function nodeWithAnchor(node, anchor) {
  const src = node && typeof node === 'object' ? node : null;
  const a = anchor && typeof anchor === 'object' ? anchor : null;
  if (!src || !a || !Number.isFinite(Number(a.x)) || !Number.isFinite(Number(a.y))) return src;
  return {
    ...src,
    x: Number(a.x),
    y: Number(a.y)
  };
}

function updateSceneMotion(scene, ts) {
  if (!scene || !Number.isFinite(ts)) return;
  const nodes = Array.isArray(scene.nodes) ? scene.nodes : [];
  const links = Array.isArray(scene.links) ? scene.links : [];
  const profile = state.quality_profile || QUALITY_PROFILES.low;
  const center = scene.center && typeof scene.center === 'object'
    ? scene.center
    : { x: state.width * 0.52, y: state.height * 0.5 };
  const nodeById = scene.node_by_id && typeof scene.node_by_id === 'object'
    ? scene.node_by_id
    : Object.create(null);
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selectedRef && selectedRef.id || '');
  const selectedNode = selectedId ? nodeById[selectedId] : null;
  const selectedTypeHint = String(selectedRef && selectedRef.type || '').toLowerCase();
  const selectedType = selectedTypeHint || String(selectedNode && selectedNode.type || '').toLowerCase();
  const selectedSubfractalRef = state.selected_subfractal && typeof state.selected_subfractal === 'object'
    ? state.selected_subfractal
    : null;
  const selectedSubfractalParentId = String(selectedSubfractalRef && selectedSubfractalRef.parent_id || '');
  const selectedSubfractalParentNode = selectedSubfractalParentId ? nodeById[selectedSubfractalParentId] : null;
  const selectedSubfractalModuleId = String(selectedSubfractalParentNode && selectedSubfractalParentNode.parent_id || '');
  const focusId = state.camera && state.camera.focus_mode
    ? String(state.camera.focus_target_id || '')
    : '';
  const focusNode = focusId ? nodeById[focusId] : null;
  const focusType = String(focusNode && focusNode.type || '').toLowerCase();
  const selectedSubmoduleParentId = selectedType === 'submodule'
    ? String(selectedNode && selectedNode.parent_id || '')
    : '';
  const focusSubmoduleParentId = focusType === 'submodule'
    ? String(focusNode && focusNode.parent_id || '')
    : '';
  const moduleSpinLock = state.module_spin_lock && typeof state.module_spin_lock === 'object'
    ? state.module_spin_lock
    : (state.module_spin_lock = { runtime_by_module: Object.create(null) });
  const moduleSpinRuntimeById = moduleSpinLock.runtime_by_module && typeof moduleSpinLock.runtime_by_module === 'object'
    ? moduleSpinLock.runtime_by_module
    : (moduleSpinLock.runtime_by_module = Object.create(null));
  let frozenLayerId = '';
  if (selectedType === 'layer') {
    frozenLayerId = String(selectedNode && selectedNode.id || '');
  } else if (selectedType === 'module') {
    frozenLayerId = String(selectedNode && selectedNode.parent_id || '');
  } else if (selectedType === 'submodule') {
    const parentModule = nodeById[String(selectedNode && selectedNode.parent_id || '')];
    frozenLayerId = String(parentModule && parentModule.parent_id || '');
  } else if (selectedSubfractalModuleId) {
    const parentModule = nodeById[selectedSubfractalModuleId];
    frozenLayerId = String(parentModule && parentModule.parent_id || '');
  }
  const layerOrbitState = scene.layer_orbit_state && typeof scene.layer_orbit_state === 'object'
    ? scene.layer_orbit_state
    : (scene.layer_orbit_state = Object.create(null));
  const orbitLock = state.orbit_lock && typeof state.orbit_lock === 'object'
    ? state.orbit_lock
    : (state.orbit_lock = { layer_id: '', runtime_by_layer: Object.create(null) });
  if (String(orbitLock.layer_id || '') !== frozenLayerId) {
    orbitLock.layer_id = frozenLayerId;
  }
  const lockMap = orbitLock.runtime_by_layer && typeof orbitLock.runtime_by_layer === 'object'
    ? orbitLock.runtime_by_layer
    : (orbitLock.runtime_by_layer = Object.create(null));
  const advancedLayers = new Set();
  const prevTs = Number(scene.last_motion_ts || ts);
  const rawDtMs = clamp(ts - prevTs, 0, 80);
  const smoothDtMs = clamp(Number(state.motion_dt_smoothed || rawDtMs || 16.7), 1, 80);
  const motionDtBlend = clamp(Number(profile.motion_dt_blend || 0), 0, 0.95);
  const dtMs = (rawDtMs * (1 - motionDtBlend)) + (smoothDtMs * motionDtBlend);
  const spinSubsteps = clamp(Math.round(Number(profile.spin_substeps || 1)), 1, 8);
  scene.last_motion_ts = ts;
  const layerIntroScale = computeLayerIntroScale(scene, ts);

  for (const node of nodes) {
    if (!node || node.type !== 'layer') continue;
    const layerId = String(node.id || '');
    const scale = layerIntroScale && Object.prototype.hasOwnProperty.call(layerIntroScale, layerId)
      ? clamp(Number(layerIntroScale[layerId]), 0, 1)
      : 1;
    node.intro_scale = scale;
    node.render_radius = Number(node.radius || 0) * scale;
  }

  for (const node of nodes) {
    if (!node || node.type !== 'module') continue;
    const layerId = String(node.parent_id || '');
    const layerScale = layerIntroScale && Object.prototype.hasOwnProperty.call(layerIntroScale, layerId)
      ? clamp(Number(layerIntroScale[layerId]), 0, 1)
      : 1;
    node.intro_scale = layerScale;
    const orbitSpeed = Number(node.orbit_speed || 0);
    let orbitLayer = layerOrbitState[layerId];
    if (!orbitLayer) {
      orbitLayer = {
        runtime: ts * orbitSpeed,
        speed: orbitSpeed,
        frozen: false
      };
      layerOrbitState[layerId] = orbitLayer;
    }
    if (!Number.isFinite(orbitLayer.runtime)) orbitLayer.runtime = ts * orbitSpeed;
    orbitLayer.speed = orbitSpeed;
    if (!advancedLayers.has(layerId)) {
      const freezeLayer = Boolean(frozenLayerId && layerId && layerId === frozenLayerId);
      orbitLayer.frozen = freezeLayer;
      if (freezeLayer) {
        if (Number.isFinite(lockMap[layerId])) {
          orbitLayer.runtime = Number(lockMap[layerId]);
        } else {
          lockMap[layerId] = Number(orbitLayer.runtime);
        }
      } else {
        if (Object.prototype.hasOwnProperty.call(lockMap, layerId)) {
          delete lockMap[layerId];
        }
        if (orbitSpeed !== 0) {
          orbitLayer.runtime += dtMs * orbitSpeed;
          if (Math.abs(orbitLayer.runtime) > 1e6) {
            orbitLayer.runtime = orbitLayer.runtime % (Math.PI * 2);
          }
        }
      }
      advancedLayers.add(layerId);
    }
    const orbitRadius = Number(node.orbit_radius || 0) * layerScale;
    if (orbitRadius <= 0) continue;
    const orbitAngle = Number(node.base_angle || 0) + Number(orbitLayer.runtime || 0);
    node.angle = orbitAngle;
    node.x = center.x + (Math.cos(orbitAngle) * orbitRadius);
    node.y = center.y + (Math.sin(orbitAngle) * orbitRadius);
    updateHitTargetCircle(scene, node.id, node.x, node.y, Number(node.radius || 0) + 5);

    const childIds = Array.isArray(node.child_ids) ? node.child_ids : [];
    if (!childIds.length) continue;
    const spinSpeed = Number(node.spin_speed || 0);
    const spinKey = String(node.id || '');
    const storedSpin = Number(moduleSpinRuntimeById[spinKey]);
    if (Number.isFinite(storedSpin)) {
      node.spin_runtime = storedSpin;
      node.spin_target = storedSpin;
    }
    if (!Number.isFinite(node.spin_runtime)) {
      let seededSpin = Number(node.spin_base || 0) + (Number(ts || 0) * spinSpeed);
      if (Math.abs(seededSpin) > 1e6) seededSpin %= (Math.PI * 2);
      node.spin_runtime = seededSpin;
    }
    if (!Number.isFinite(node.spin_target)) {
      node.spin_target = Number(node.spin_runtime || node.spin_base || 0);
    }
    const freezeSpin = Boolean(
      (selectedType === 'module' && selectedId && selectedId === String(node.id || ''))
      || (focusType === 'module' && focusId && focusId === String(node.id || ''))
      || (selectedSubmoduleParentId && selectedSubmoduleParentId === String(node.id || ''))
      || (focusSubmoduleParentId && focusSubmoduleParentId === String(node.id || ''))
      || (selectedSubfractalModuleId && selectedSubfractalModuleId === String(node.id || ''))
    );
    let spinAngle = Number(node.spin_runtime || node.spin_base || 0);
    if (freezeSpin) {
      node.spin_locked = true;
      node.spin_lock_angle = spinAngle;
      node.spin_target = spinAngle;
    } else if (spinSpeed !== 0) {
      node.spin_locked = false;
      const spinStepDt = dtMs / spinSubsteps;
      for (let step = 0; step < spinSubsteps; step += 1) {
        node.spin_runtime += spinStepDt * spinSpeed;
        if (Math.abs(node.spin_runtime) > 1e6) {
          node.spin_runtime = node.spin_runtime % (Math.PI * 2);
        }
      }
      node.spin_target = Number(node.spin_runtime || 0);
      spinAngle = Number(node.spin_runtime || 0);
    }
    node.spin_angle = spinAngle;
    moduleSpinRuntimeById[spinKey] = Number(spinAngle || 0);
    for (const childId of childIds) {
      const sub = nodeById[String(childId || '')];
      if (!sub) continue;
      const localStart = Number(sub.local_shell_start || 0);
      const localEnd = Number(sub.local_shell_end || 0);
      const shellMid = Number(sub.shell_mid || ((Number(sub.shell_inner || 0) + Number(sub.shell_outer || 0)) * 0.5));
      const moduleOrientation = Number(node.base_angle || 0);
      const start = moduleOrientation + localStart + spinAngle;
      const end = moduleOrientation + localEnd + spinAngle;
      const mid = (start + end) * 0.5;
      sub.parent_x = node.x;
      sub.parent_y = node.y;
      sub.shell_start = start;
      sub.shell_end = end;
      sub.x = node.x + (Math.cos(mid) * shellMid);
      sub.y = node.y + (Math.sin(mid) * shellMid);
      sub.intro_scale = layerScale;
      updateHitTargetCircle(scene, sub.id, sub.x, sub.y, Math.max(12, Number(sub.radius || 0) + 8));
    }
  }

  const selectedSubfractal = validateSelectedSubfractal(scene);
  const selectedSubmoduleId = selectedType === 'submodule' ? String(selectedNode && selectedNode.id || '') : '';
  const selectedModuleId = selectedType === 'submodule' ? String(selectedNode && selectedNode.parent_id || '') : '';
  const selectedSubfractalAnchor = selectedSubmoduleId && selectedSubfractal
    ? subfractalAnchorForSelection(selectedNode, selectedSubfractal)
    : null;

  for (const link of links) {
    const fromId = String(link && link.from_id || '');
    const toId = String(link && link.to_id || '');
    const from = nodeById[fromId];
    const to = nodeById[toId];
    if (!from || !to) continue;
    let fromNode = from;
    let toNode = to;
    if (selectedSubmoduleId) {
      if (selectedSubfractalAnchor) {
        if (fromId === selectedSubmoduleId || (selectedModuleId && fromId === selectedModuleId)) {
          fromNode = nodeWithAnchor(fromNode, selectedSubfractalAnchor);
        }
        if (toId === selectedSubmoduleId || (selectedModuleId && toId === selectedModuleId)) {
          toNode = nodeWithAnchor(toNode, selectedSubfractalAnchor);
        }
      } else {
        const subAnchor = { x: Number(selectedNode && selectedNode.x || 0), y: Number(selectedNode && selectedNode.y || 0) };
        if (selectedModuleId && fromId === selectedModuleId) fromNode = nodeWithAnchor(fromNode, subAnchor);
        if (selectedModuleId && toId === selectedModuleId) toNode = nodeWithAnchor(toNode, subAnchor);
      }
    }
    setLinkGeometry(link, fromNode, toNode, center);
  }
}

function buildScene(payload) {
  const holo = payload && payload.holo && typeof payload.holo === 'object' ? payload.holo : null;
  if (!holo) return null;
  const summary = payload && payload.summary && typeof payload.summary === 'object' ? payload.summary : {};
  const runtimeStatus = runtimeSnapshotFromPayload(payload);
  const workflowBirth = holo.workflow_birth && typeof holo.workflow_birth === 'object'
    ? holo.workflow_birth
    : (summary.workflow_birth && typeof summary.workflow_birth === 'object' ? summary.workflow_birth : {});
  const doctor = holo.doctor && typeof holo.doctor === 'object'
    ? holo.doctor
    : (summary.doctor && typeof summary.doctor === 'object' ? summary.doctor : {});
  const doctorModules = Array.isArray(doctor.modules) ? doctor.modules : [];
  const doctorWoundedActive = Number(doctor.wounded_active || 0);
  const doctorCoreAlert = doctorWoundedActive > 0;
  const errorSignal = computeErrorSignal(summary);
  const integrity = normalizeIntegrityIncident(payload);
  const integrityAlert = integrity.active === true;
  const integrityFiles = Array.isArray(integrity.top_files) ? integrity.top_files : [];
  const changeSummaryRaw = holo && holo.change && typeof holo.change === 'object' ? holo.change : {};
  const changeSummary = {
    dirty_files_total: Math.max(0, Number(changeSummaryRaw.dirty_files_total || 0) || 0),
    staged_files_total: Math.max(0, Number(changeSummaryRaw.staged_files_total || 0) || 0),
    pending_push_files_total: Math.max(0, Number(changeSummaryRaw.pending_push_files_total || 0) || 0),
    active_write_files_total: Math.max(0, Number(changeSummaryRaw.active_write_files_total || 0) || 0),
    active_modules: Math.max(0, Number(changeSummaryRaw.active_modules || 0) || 0),
    active_submodules: Math.max(0, Number(changeSummaryRaw.active_submodules || 0) || 0),
    ahead_count: Math.max(0, Number(changeSummaryRaw.ahead_count || 0) || 0),
    pending_push: changeSummaryRaw.pending_push === true,
    just_pushed: changeSummaryRaw.just_pushed === true,
    top_files: Array.isArray(changeSummaryRaw.top_files)
      ? changeSummaryRaw.top_files.map((v) => String(v || '').trim()).filter(Boolean).slice(0, 10)
      : [],
    has_upstream: changeSummaryRaw.has_upstream === true,
    upstream: String(changeSummaryRaw.upstream || '').trim(),
    last_push_ts: String(changeSummaryRaw.last_push_ts || '').trim(),
    last_commit_ts: String(changeSummaryRaw.last_commit_ts || '').trim()
  };
  const spineChangeState = normalizeChangeState({
    active_write: changeSummary.active_write_files_total > 0,
    dirty: changeSummary.dirty_files_total > 0,
    staged: changeSummary.staged_files_total > 0,
    pending_push: changeSummary.pending_push === true || changeSummary.ahead_count > 0,
    just_pushed: changeSummary.just_pushed === true,
    changed: (changeSummary.active_write_files_total + changeSummary.dirty_files_total + changeSummary.staged_files_total) > 0
      || changeSummary.pending_push === true
      || changeSummary.just_pushed === true,
    file_count: changeSummary.top_files.length,
    dirty_file_count: changeSummary.dirty_files_total,
    staged_file_count: changeSummary.staged_files_total,
    pending_push_file_count: changeSummary.pending_push_files_total,
    active_write_file_count: changeSummary.active_write_files_total,
    top_files: changeSummary.top_files,
    last_push_ts: changeSummary.last_push_ts
  });
  const profile = state.quality_profile;
  const layersRaw = Array.isArray(holo.layers) ? holo.layers : [];
  const layerPriorityBase = {
    systems: 1.0,
    adaptive: 0.94,
    memory: 0.88,
    state: 0.84,
    habits: 0.72,
    config: 0.62,
    lib: 0.58
  };
  const layerRowsUnordered = layersRaw
    .map((layer) => {
      const key = String(layer && (layer.key || layer.name || '')).toLowerCase();
      const modules = Array.isArray(layer && layer.modules) ? layer.modules : [];
      let subCount = 0;
      for (const mod of modules) {
        subCount += Array.isArray(mod && mod.submodules) ? mod.submodules.length : 0;
      }
      const contentScore = (modules.length * 1.0) + (subCount * 0.55);
      return {
        layer,
        key,
        priority: Number(layerPriorityBase[key] || 0.45),
        content_score: contentScore
      };
    })
    .sort((a, b) => {
      if (Math.abs(a.content_score - b.content_score) > 0.0001) return b.content_score - a.content_score;
      if (Math.abs(a.priority - b.priority) > 0.0001) return b.priority - a.priority;
      return a.key.localeCompare(b.key);
    })
    .slice(0, profile.max_layers)
    .map((row) => row.layer);
  const layerRows = applyLayerOrderLock(layerRowsUnordered, Date.now());
  const ioIn = Array.isArray(holo.io && holo.io.inputs) ? holo.io.inputs : [];
  const ioOut = Array.isArray(holo.io && holo.io.outputs) ? holo.io.outputs : [];

  const center = { x: state.width * 0.52, y: state.height * 0.5 };
  const outerRadius = Math.min(state.width, state.height) * 0.46;
  const minRing = Math.max(56, outerRadius * 0.16);
  const ringSpan = Math.max(0, outerRadius - minRing);
  const nominalRingStep = layerRows.length > 1
    ? (ringSpan / Math.max(1, layerRows.length - 1))
    : (outerRadius * 0.12);

  const nodes = [];
  const hitTargets = [];
  const nodeById = Object.create(null);
  const hitTargetById = Object.create(null);
  const internalLinks = [];
  const layerNodesForHit = [];
  let outerShellBoundary = 0;

  const addCircleHitTarget = (id, name, pathText, x, y, r) => {
    const target = {
      id: String(id || ''),
      name: String(name || ''),
      path: String(pathText || ''),
      kind: 'circle',
      x: Number(x || 0),
      y: Number(y || 0),
      r: Number(r || 0)
    };
    hitTargets.push(target);
    hitTargetById[target.id] = target;
  };

  const addRingHitTarget = (id, name, pathText, cx, cy, inner, outer) => {
    const target = {
      id: String(id || ''),
      name: String(name || ''),
      path: String(pathText || ''),
      kind: 'ring',
      cx: Number(cx || 0),
      cy: Number(cy || 0),
      inner: Math.max(0, Number(inner || 0)),
      outer: Math.max(0, Number(outer || 0))
    };
    hitTargets.push(target);
    if (!hitTargetById[target.id]) hitTargetById[target.id] = target;
  };

  const addShellFillHitTarget = (id, name, pathText, cx, cy, inner, outer) => {
    const target = {
      id: String(id || ''),
      name: String(name || ''),
      path: String(pathText || ''),
      kind: 'shell_fill',
      cx: Number(cx || 0),
      cy: Number(cy || 0),
      inner: Math.max(0, Number(inner || 0)),
      outer: Math.max(0, Number(outer || 0))
    };
    hitTargets.push(target);
  };

  const spineNode = {
    id: SPINE_NODE_ID,
    type: 'spine',
    name: 'Spine Core',
    rel: SPINE_NODE_PATH,
    x: center.x,
    y: center.y,
    radius: 15,
    activity: clamp(holo.metrics && holo.metrics.drift_proxy, 0, 1),
    error_hint: clamp(0.1 + (doctorCoreAlert ? 0.26 : 0), 0, 1),
    integrity_alert: integrityAlert,
    integrity_severity: integrity.severity,
    error_state_active: integrityAlert || doctorCoreAlert,
    doctor_state: doctorCoreAlert ? 'wounded' : '',
    doctor_code: doctorCoreAlert ? 'autotest_doctor_wounded_module' : '',
    doctor_summary: doctorCoreAlert
      ? `Doctor reports ${fmtNum(doctorWoundedActive)} wounded module(s).`
      : '',
    doctor_severity: doctorCoreAlert ? 'high' : '',
    doctor_module: '',
    change_state: spineChangeState,
    change_active: spineChangeState.changed === true
  };
  nodes.push(spineNode);
  nodeById[spineNode.id] = spineNode;
  addCircleHitTarget(spineNode.id, spineNode.name, spineNode.rel, spineNode.x, spineNode.y, spineNode.radius + 5);

  const systemNode = {
    id: SYSTEM_ROOT_ID,
    type: 'system',
    name: 'System Root',
    rel: WORKSPACE_ROOT_PATH,
    x: center.x,
    y: center.y,
    radius: outerRadius,
    activity: 0.5,
    integrity_alert: integrityAlert,
    integrity_severity: integrity.severity,
    error_state_active: integrityAlert || doctorCoreAlert,
    doctor_state: doctorCoreAlert ? 'wounded' : '',
    doctor_code: doctorCoreAlert ? 'autotest_doctor_wounded_module' : '',
    doctor_summary: doctorCoreAlert
      ? `Doctor reports ${fmtNum(doctorWoundedActive)} wounded module(s).`
      : '',
    doctor_severity: doctorCoreAlert ? 'high' : '',
    doctor_module: '',
    change_state: spineChangeState,
    change_active: spineChangeState.changed === true
  };
  nodes.push(systemNode);
  nodeById[systemNode.id] = systemNode;

  const preparedLayers = layerRows.map((layer, li) => {
    const modules = Array.isArray(layer.modules) ? layer.modules.slice(0, profile.max_modules_per_layer) : [];
    const preparedModules = modules.map((mod) => {
      const subs = Array.isArray(mod.submodules)
        ? mod.submodules.slice(0, profile.max_submodules_per_module)
        : [];
      const fractalCount = subs.length;
      const moduleBaseRadius = Math.max(MODULE_RADIUS_MIN, Math.min(MODULE_RADIUS_MAX, nominalRingStep * 0.62));
      const fractalScale = 1 + Math.min(0.62, fractalCount * 0.052);
      const fractalRadius = clamp(
        moduleBaseRadius * fractalScale,
        MODULE_RADIUS_MIN,
        MODULE_RADIUS_MAX
      );
      const codebaseSizeBytes = Math.max(0, Number(mod && mod.codebase_size_bytes || 0) || 0);
      return {
        mod,
        subs,
        fractal_count: fractalCount,
        module_radius: fractalRadius,
        codebase_size_bytes: codebaseSizeBytes,
        codebase_size_norm: 0.5
      };
    });
    return {
      layer,
      index: li,
      prepared_modules: preparedModules,
      max_module_radius: 0,
      min_by_circ: 0
    };
  });

  let minCodebaseSize = Infinity;
  let maxCodebaseSize = 0;
  for (const row of preparedLayers) {
    for (const modRow of row.prepared_modules) {
      const size = Math.max(0, Number(modRow.codebase_size_bytes || 0));
      if (size <= 0) continue;
      minCodebaseSize = Math.min(minCodebaseSize, size);
      maxCodebaseSize = Math.max(maxCodebaseSize, size);
    }
  }
  if (!Number.isFinite(minCodebaseSize) || minCodebaseSize <= 0 || maxCodebaseSize <= 0) {
    minCodebaseSize = 1;
    maxCodebaseSize = 1;
  }
  const codebaseSpan = Math.max(0, maxCodebaseSize - minCodebaseSize);

  for (const row of preparedLayers) {
    for (const modRow of row.prepared_modules) {
      const codeSize = Math.max(0, Number(modRow.codebase_size_bytes || 0));
      const sizeNorm = codebaseSpan > 0
        ? clamp((codeSize - minCodebaseSize) / codebaseSpan, 0, 1)
        : 0.5;
      const sizeRadius = MODULE_RADIUS_MIN + ((MODULE_RADIUS_MAX - MODULE_RADIUS_MIN) * sizeNorm);
      const fractalRadius = clamp(
        Number(modRow.module_radius || MODULE_RADIUS_MIN),
        MODULE_RADIUS_MIN,
        MODULE_RADIUS_MAX
      );
      modRow.codebase_size_norm = sizeNorm;
      modRow.module_radius = clamp(
        (fractalRadius * (1 - MODULE_RADIUS_SIZE_BLEND)) + (sizeRadius * MODULE_RADIUS_SIZE_BLEND),
        MODULE_RADIUS_MIN,
        MODULE_RADIUS_MAX
      );
    }
    const maxModuleRadius = row.prepared_modules.reduce((acc, modRow) => (
      Math.max(acc, Number(modRow.module_radius || 0))
    ), 0);
    const requiredCirc = row.prepared_modules.reduce(
      (acc, modRow) => acc + (Math.max(8, Number(modRow.module_radius || 0)) * 2) + 9,
      0
    );
    row.max_module_radius = maxModuleRadius;
    row.min_by_circ = requiredCirc > 0 ? (requiredCirc / (Math.PI * 2)) : 0;
  }

  const plannedRadii = [];
  for (let li = 0; li < preparedLayers.length; li += 1) {
    const row = preparedLayers[li];
    const ringWidth = Math.max(13, nominalRingStep * 0.5);
    const required = Math.max(
      minRing + (li * Math.max(8, nominalRingStep * 0.4)),
      Number(row.min_by_circ || 0) + Number(row.max_module_radius || 0) + 7
    );
    if (li === 0) {
      plannedRadii.push(required);
      continue;
    }
    const prev = preparedLayers[li - 1];
    const gap = Number(prev.max_module_radius || 0)
      + Number(row.max_module_radius || 0)
      + (ringWidth * 0.85)
      + 12;
    plannedRadii.push(Math.max(required, Number(plannedRadii[li - 1] || 0) + gap));
  }

  let maxPlanned = plannedRadii.length
    ? plannedRadii.reduce((acc, r) => Math.max(acc, Number(r || 0)), 0)
    : minRing;
  const targetOuter = outerRadius;
  if (maxPlanned > targetOuter && maxPlanned > 0) {
    const scale = targetOuter / maxPlanned;
    for (let i = 0; i < plannedRadii.length; i += 1) plannedRadii[i] *= scale;
    for (const row of preparedLayers) {
      row.max_module_radius *= scale;
      for (const modRow of row.prepared_modules) {
        modRow.module_radius = Math.max(8, Number(modRow.module_radius || 0) * scale);
      }
    }
    maxPlanned = targetOuter;
  } else if (maxPlanned < targetOuter && plannedRadii.length > 1) {
    const slack = targetOuter - maxPlanned;
    for (let i = 0; i < plannedRadii.length; i += 1) {
      const t = i / Math.max(1, plannedRadii.length - 1);
      plannedRadii[i] += slack * t * 0.94;
    }
  }

  for (let li = 0; li < preparedLayers.length; li += 1) {
    const row = preparedLayers[li];
    const layer = row.layer;
    const layerRadius = Number(plannedRadii[li] || (minRing + (li * nominalRingStep)));
    const layerId = String(layer.id || `layer:${li}`);
    const layerRel = String(layer.rel || layer.key || layerId);
    const layerKey = String(layer.key || layer.name || layerRel || '').trim().toLowerCase();
    const orbitSeed = stableHash(`${layerId}|orbit`);
    const layerOrbitDirection = (orbitSeed % 2 === 0) ? 1 : -1;
    const layerOrbitSpeed = layerOrbitDirection * (0.000018 + ((orbitSeed % 6) * 0.000001));
    const layerIntegrityAlert = integrityAlert && (layerKey === 'systems' || layerRel.toLowerCase().includes('systems'));
    const layerDoctor = doctorModuleMatchForRel(doctorModules, layerRel);
    const layerDoctorState = String(layerDoctor && layerDoctor.latest_state || '').trim().toLowerCase();
    const layerDoctorError = doctorStateIsError(layerDoctorState);
    const layerErrorHint = clamp(
      computeNodeErrorHint(layerId, layerRel, layer.name, layer.key) + doctorStateHintBoost(layerDoctorState),
      0,
      1
    );
    const layerNode = {
      id: layerId,
      type: 'layer',
      name: String(layer.name || layer.key || layerId),
      rel: layerRel,
      key: layerKey,
      x: center.x,
      y: center.y,
      radius: layerRadius,
      ring_width: Math.max(13, nominalRingStep * 0.5),
      activity: clamp(layer.activity, 0, 1),
      error_hint: layerErrorHint,
      integrity_alert: layerIntegrityAlert,
      integrity_severity: integrity.severity,
      error_state_active: layerIntegrityAlert || layerDoctorError,
      doctor_state: layerDoctorState,
      doctor_code: String(layerDoctor && layerDoctor.latest_code || ''),
      doctor_summary: String(layerDoctor && layerDoctor.latest_summary || ''),
      doctor_severity: String(layerDoctor && layerDoctor.latest_severity || ''),
      doctor_module: String(layerDoctor && layerDoctor.module || '')
    };
    nodes.push(layerNode);
    nodeById[layerNode.id] = layerNode;
    layerNodesForHit.push(layerNode);
    addRingHitTarget(
      layerNode.id,
      `Layer / ${layerNode.name}`,
      layerNode.rel,
      layerNode.x,
      layerNode.y,
      layerNode.radius - (layerNode.ring_width * 0.9),
      layerNode.radius + (layerNode.ring_width * 0.9)
    );

    const modules = row.prepared_modules;
    const count = Math.max(1, modules.length);
    const phase = ((stableHash(layerId) % 1000) / 1000) * (Math.PI * 2);
    for (let mi = 0; mi < modules.length; mi += 1) {
      const modRow = modules[mi];
      const mod = modRow.mod;
      const subs = modRow.subs;
      const angle = phase + ((mi / count) * Math.PI * 2);
      const moduleRadius = Number(
        modRow.module_radius
        || Math.max(MODULE_RADIUS_MIN, Math.min(MODULE_RADIUS_MAX, layerNode.ring_width * 0.62))
      );
      const x = center.x + (Math.cos(angle) * layerRadius);
      const y = center.y + (Math.sin(angle) * layerRadius);
      const modId = String(mod.id || `${layerId}/m${mi}`);
      const modRel = String(mod.rel || `${layerRel}/${String(mod.name || modId)}`);
      const moduleIntegrityAlert = integrityAlert && hasIntegrityPathMatch(modRel, integrityFiles);
      const moduleDoctor = doctorModuleMatchForRel(doctorModules, modRel);
      const moduleDoctorState = String(moduleDoctor && moduleDoctor.latest_state || '').trim().toLowerCase();
      const moduleDoctorError = doctorStateIsError(moduleDoctorState);
      const moduleChangeState = normalizeChangeState(mod && mod.change_state);
      const spinSeed = (stableHash(`${modId}|spin`) % 1000) / 1000;
      const spinBase = (stableHash(`${modId}|spinbase`) % 6283) / 1000;
      const moduleNode = {
        id: modId,
        parent_id: layerId,
        type: 'module',
        name: String(mod.name || modId),
        rel: modRel,
        x,
        y,
        radius: moduleRadius,
        angle,
        base_angle: angle,
        orbit_radius: layerRadius,
        orbit_speed: layerOrbitSpeed,
        spin_base: spinBase,
        spin_speed: modRow.fractal_count > 0
          ? (0.000045 + (Math.min(14, modRow.fractal_count) * 0.0000022) + (spinSeed * 0.0000034))
          : 0,
        spin_angle: spinBase,
        fractal_count: modRow.fractal_count,
        codebase_size_bytes: Math.max(0, Number(modRow.codebase_size_bytes || 0)),
        codebase_size_norm: clamp(Number(modRow.codebase_size_norm || 0.5), 0, 1),
        child_ids: [],
        activity: clamp(mod.activity, 0, 1),
        error_hint: clamp(
          computeNodeErrorHint(modId, modRel, mod.name, layerNode.name) + doctorStateHintBoost(moduleDoctorState),
          0,
          1
        ),
        integrity_alert: moduleIntegrityAlert,
        integrity_severity: integrity.severity,
        error_state_active: moduleIntegrityAlert || moduleDoctorError,
        doctor_state: moduleDoctorState,
        doctor_code: String(moduleDoctor && moduleDoctor.latest_code || ''),
        doctor_summary: String(moduleDoctor && moduleDoctor.latest_summary || ''),
        doctor_severity: String(moduleDoctor && moduleDoctor.latest_severity || ''),
        doctor_module: String(moduleDoctor && moduleDoctor.module || ''),
        change_state: moduleChangeState,
        change_active: moduleChangeState.changed === true
      };
      nodes.push(moduleNode);
      nodeById[moduleNode.id] = moduleNode;
      addCircleHitTarget(
        moduleNode.id,
        `${layerNode.name} / ${moduleNode.name}`,
        moduleNode.rel,
        moduleNode.x,
        moduleNode.y,
        moduleNode.radius + 5
      );

      const shellOuter = moduleRadius * 1.12;
      const shellInner = moduleRadius * 0.74;
      const subSpan = (Math.PI * 2) / Math.max(1, subs.length);
      for (let si = 0; si < subs.length; si += 1) {
        const sub = subs[si];
        const localStart = (si * subSpan) + 0.06;
        const localEnd = localStart + (subSpan * 0.68);
        const start = angle + localStart + spinBase;
        const end = angle + localEnd + spinBase;
        const mid = (start + end) * 0.5;
        const shellMid = (shellInner + shellOuter) * 0.5;
        const sx = x + (Math.cos(mid) * shellMid);
        const sy = y + (Math.sin(mid) * shellMid);
        const subId = String(sub.id || `${modId}/s${si}`);
        const subRel = String(sub.rel || `${modRel}/${String(sub.name || subId)}`);
        const subfolderRows = Array.isArray(sub && sub.subfolders) ? sub.subfolders : [];
        const subfractalChildren = [];
        for (let ci = 0; ci < subfolderRows.length; ci += 1) {
          const child = subfolderRows[ci] && typeof subfolderRows[ci] === 'object'
            ? subfolderRows[ci]
            : {};
          const childName = String(child.name || '').trim();
          if (!childName) continue;
          const childPath = String(child.rel || `${subRel}/${childName}`).trim();
          subfractalChildren.push({
            id: `${subId}/child:${ci}:${childName}`,
            name: childName,
            rel: childPath
          });
        }
        const subErrorHint = computeNodeErrorHint(subId, subRel, sub.name, moduleNode.name);
        const subIntegrityAlert = integrityAlert && hasIntegrityPathMatch(subRel, integrityFiles);
        const subDoctor = doctorModuleMatchForRel(doctorModules, subRel);
        const subDoctorState = String(subDoctor && subDoctor.latest_state || '').trim().toLowerCase();
        const subDoctorError = doctorStateIsError(subDoctorState);
        const subErrorActive = subIntegrityAlert || subDoctorError || hasActiveErrorState(sub, subErrorHint, errorSignal);
        const subChangeState = normalizeChangeState(sub && sub.change_state);
        const subNode = {
          id: subId,
          parent_id: modId,
          type: 'submodule',
          name: String(sub.name || subId),
          rel: subRel,
          x: sx,
          y: sy,
          radius: Math.max(2, moduleRadius * 0.15),
          parent_x: x,
          parent_y: y,
          shell_inner: shellInner,
          shell_outer: shellOuter,
          shell_mid: shellMid,
          local_shell_start: localStart,
          local_shell_end: localEnd,
          shell_start: start,
          shell_end: end,
          subfractal_children: subfractalChildren,
          subfractal_count: subfractalChildren.length,
          activity: clamp(sub.activity, 0, 1),
          error_hint: clamp(subErrorHint + doctorStateHintBoost(subDoctorState), 0, 1),
          error_state_active: subErrorActive,
          integrity_alert: subIntegrityAlert,
          integrity_severity: integrity.severity,
          doctor_state: subDoctorState,
          doctor_code: String(subDoctor && subDoctor.latest_code || ''),
          doctor_summary: String(subDoctor && subDoctor.latest_summary || ''),
          doctor_severity: String(subDoctor && subDoctor.latest_severity || ''),
          doctor_module: String(subDoctor && subDoctor.module || ''),
          change_state: subChangeState,
          change_active: subChangeState.changed === true
        };
        nodes.push(subNode);
        nodeById[subNode.id] = subNode;
        moduleNode.child_ids.push(subNode.id);
        internalLinks.push({
          from: moduleNode.id,
          to: subNode.id,
          kind: 'fractal',
          count: 1,
          activity: clamp((moduleNode.activity * 0.42) + (subNode.activity * 0.58), 0, 1)
        });
        addCircleHitTarget(
          subNode.id,
          `${layerNode.name} / ${moduleNode.name} / ${subNode.name}`,
          subNode.rel,
          subNode.x,
          subNode.y,
          Math.max(12, subNode.radius + 8)
        );
      }
    }
  }

  // Layer fill hit-zones: clicking/hovering empty shell interior selects the shell.
  if (layerNodesForHit.length) {
    const sorted = layerNodesForHit.slice().sort((a, b) => Number(a.radius || 0) - Number(b.radius || 0));
    for (let i = 0; i < sorted.length; i += 1) {
      const node = sorted[i];
      const prev = i > 0 ? sorted[i - 1] : null;
      const next = i < sorted.length - 1 ? sorted[i + 1] : null;
      const inner = prev
        ? Math.max(0, (Number(prev.radius || 0) + Number(node.radius || 0)) * 0.5)
        : 0;
      const outer = next
        ? Math.max(inner + 1, (Number(node.radius || 0) + Number(next.radius || 0)) * 0.5)
        : Math.max(inner + 1, Number(node.radius || 0) + Math.max(22, Number(node.ring_width || 12) * 1.8));
      outerShellBoundary = Math.max(outerShellBoundary, outer);
      addShellFillHitTarget(
        node.id,
        `Layer / ${node.name}`,
        node.rel,
        node.x,
        node.y,
        inner,
        outer
      );
    }
  }

  const ioNodes = [];
  const ioRadius = outerRadius + Math.max(40, Math.min(120, state.width * 0.06));
  const allIo = [...ioIn.map((row) => ({ ...row, io_type: 'input' })), ...ioOut.map((row) => ({ ...row, io_type: 'output' }))];
  for (let i = 0; i < allIo.length; i += 1) {
    const row = allIo[i];
    const baseAngle = (i / Math.max(1, allIo.length)) * (Math.PI * 2);
    const angle = baseAngle + ((row.io_type === 'input') ? -0.45 : 0.4);
    const x = center.x + Math.cos(angle) * ioRadius;
    const y = center.y + Math.sin(angle) * ioRadius;
    const node = {
      id: String(row.id || `io:${i}`),
      type: row.io_type === 'input' ? 'io_input' : 'io_output',
      name: String(row.name || row.id || 'IO'),
      rel: String(row.id || `io:${i}`),
      x,
      y,
      radius: 7,
      angle,
      activity: clamp(row.activity, 0, 1),
      count: Number(row.count || 0),
      error_hint: computeNodeErrorHint(row.id, row.name, row.io_type)
    };
    ioNodes.push(node);
    nodeById[node.id] = node;
    addCircleHitTarget(
      node.id,
      `${row.io_type === 'input' ? 'Input' : 'Output'} / ${node.name}`,
      node.rel,
      node.x,
      node.y,
      node.radius + 5
    );
  }

  const rawLinks = [
    ...(Array.isArray(holo.links) ? holo.links : []),
    ...internalLinks
  ];
  const linkRows = [];
  const linkById = Object.create(null);
  for (const row of rawLinks) {
    const from = nodeById[String(row.from || '')];
    const to = nodeById[String(row.to || '')];
    if (!from || !to) continue;
    const kind = String(row.kind || 'flow');
    const link = {
      id: `${row.from}|${row.to}|${row.kind || 'flow'}`,
      from_id: String(row.from),
      to_id: String(row.to),
      p0: { x: Number(from.x || 0), y: Number(from.y || 0) },
      p1: { x: Number(from.x || 0), y: Number(from.y || 0) },
      p2: { x: Number(to.x || 0), y: Number(to.y || 0) },
      p3: { x: Number(to.x || 0), y: Number(to.y || 0) },
      activity: clamp(row.activity, 0, 1),
      count: Number(row.count || 0),
      packet_size_tokens: Math.max(0, Number(row.packet_size_tokens || 0) || 0),
      packet_size_norm: clamp(Number(row.packet_size_norm || 0), 0, 1),
      blocked_ratio: clamp(Number(row.blocked_ratio || 0), 0, 1),
      flow_blocked: row.flow_blocked === true,
      block_reason: String(row.block_reason || '').trim(),
      doctor_state: String(row.doctor_state || '').trim().toLowerCase(),
      kind,
      arc_side: (stableHash(`${row.from}|${row.to}|${kind}`) % 2) ? 1 : -1
    };
    const fromHint = Number(from.error_hint || 0);
    const toHint = Number(to.error_hint || 0);
    const endpointHint = Math.max(fromHint, toHint);
    const kindHint = (kind === 'ingress' || kind === 'egress') ? 0.18 : 0;
    const channelHint = Math.max(endpointHint, kindHint);
    const integrityPathAlert = integrityAlert && (
      from.error_state_active === true
      || to.error_state_active === true
      || from.integrity_alert === true
      || to.integrity_alert === true
      || hasIntegrityPathMatch(String(from.rel || ''), integrityFiles)
      || hasIntegrityPathMatch(String(to.rel || ''), integrityFiles)
    );
    link.error_weight = channelHint > 0
      ? clamp(errorSignal * channelHint * (0.7 + (link.activity * 0.3)), 0, 1)
      : 0;
    if (link.flow_blocked || link.blocked_ratio > 0.01) {
      link.error_weight = Math.max(link.error_weight, clamp(0.16 + (link.blocked_ratio * 0.74), 0.16, 0.96));
    }
    if (kind === 'doctor') {
      const doctorState = String(link.doctor_state || '').trim().toLowerCase();
      if (doctorState === 'wounded' || doctorState === 'rollback_cut') {
        link.error_weight = Math.max(link.error_weight, 0.68);
      } else if (doctorState === 'healing') {
        link.error_weight = Math.max(link.error_weight, 0.2);
      } else if (doctorState === 'regrowth') {
        link.error_weight = Math.max(link.error_weight, 0.08);
      }
    }
    if (integrityPathAlert) {
      link.error_weight = Math.max(link.error_weight, clamp(0.52 + (link.activity * 0.38), 0.52, 0.95));
    }
    setLinkGeometry(link, from, to, center);
    linkRows.push(link);
    linkById[link.id] = link;
  }

  return {
    center,
    layers: layerRows,
    nodes,
    io_nodes: ioNodes,
    links: linkRows,
    link_by_id: linkById,
    hit_targets: hitTargets,
    hit_target_by_id: hitTargetById,
    node_by_id: nodeById,
    spine_id: SPINE_NODE_ID,
    system_id: SYSTEM_ROOT_ID,
    system_target: {
      id: SYSTEM_ROOT_ID,
      name: 'System Root',
      path: WORKSPACE_ROOT_PATH,
      kind: 'system'
    },
    outer_shell_boundary: Math.max(outerShellBoundary, 0),
    summary,
    runtime_status: runtimeStatus,
    error_signal: errorSignal,
    metrics: holo.metrics && typeof holo.metrics === 'object' ? holo.metrics : {},
    change_summary: changeSummary,
    integrity_alert: integrityAlert,
    integrity_severity: integrity.severity,
    integrity_violation_total: Number(integrity.violation_total || 0),
    integrity_top_files: integrityFiles,
    doctor,
    workflow_birth: workflowBirth
  };
}

function rebuildParticles() {
  const scene = state.scene;
  if (!scene) return;
  const profile = state.quality_profile;
  const links = visibleLinksForScene(scene);
  const targetCount = Math.min(
    profile.max_particles,
    Math.max(60, links.length * profile.particles_per_link * 6)
  );
  const particles = [];
  if (!links.length) {
    state.particles = particles;
    return;
  }
  for (let i = 0; i < targetCount; i += 1) {
    const link = links[i % links.length];
    const flowPenalty = clamp(Number(link.blocked_ratio || 0), 0, 1);
    const p = {
      id: i,
      link_id: link.id,
      t: ((stableHash(`${link.id}:${i}`) % 1000) / 1000),
      speed: (0.045 + (link.activity * 0.12) + (((i % 7) * 0.004))) * (1 - (flowPenalty * 0.75)),
      radius: PACKET_RADIUS_FLOOR,
      trail: []
    };
    particles.push(p);
  }
  state.particles = particles;
}

function packetMetricForLink(link) {
  const row = link && typeof link === 'object' ? link : {};
  const packetTokens = Number(row.packet_size_tokens || 0);
  if (Number.isFinite(packetTokens) && packetTokens > 0) return packetTokens;
  const count = Number(row.count || 0);
  if (Number.isFinite(count) && count > 0) return 100 + (count * 120);
  return 0;
}

function packetMetricRange(links) {
  const rows = Array.isArray(links) ? links : [];
  let min = Infinity;
  let max = 0;
  for (const link of rows) {
    const metric = packetMetricForLink(link);
    if (!Number.isFinite(metric) || metric <= 0) continue;
    min = Math.min(min, metric);
    max = Math.max(max, metric);
  }
  if (!Number.isFinite(min) || min <= 0 || max <= 0) {
    return { min: 1, max: 1, span: 0 };
  }
  return {
    min,
    max,
    span: Math.max(0, max - min)
  };
}

function packetRadiusForLink(link, range) {
  const metric = packetMetricForLink(link);
  if (!Number.isFinite(metric) || metric <= 0) return PACKET_RADIUS_FLOOR;
  const r = range && typeof range === 'object' ? range : { min: 1, max: 1, span: 0 };
  const span = Math.max(0, Number(r.span || 0));
  const t = span > 0
    ? clamp((metric - Number(r.min || metric)) / span, 0, 1)
    : 0.5;
  return PACKET_RADIUS_FLOOR + ((PACKET_RADIUS_CEILING - PACKET_RADIUS_FLOOR) * t);
}

function visibleLinksForScene(scene) {
  if (!scene) return [];
  let linksAll = Array.isArray(scene.links) ? scene.links : [];
  const focusLinks = state.focus && state.focus.links instanceof Set ? state.focus.links : null;
  if (focusLinks) {
    linksAll = linksAll.filter((link) => focusLinks.has(link.id));
  }
  if (!codegraphHasMatches()) return linksAll;
  const cg = codegraphState();
  const linkSet = cg.matched_link_ids instanceof Set ? cg.matched_link_ids : null;
  const nodeSet = cg.matched_node_ids instanceof Set ? cg.matched_node_ids : null;
  if ((!linkSet || linkSet.size <= 0) && (!nodeSet || nodeSet.size <= 0)) return linksAll;
  return linksAll.filter((link) => {
    const lid = String(link && link.id || '');
    if (linkSet && linkSet.size > 0 && lid && linkSet.has(lid)) return true;
    if (nodeSet && nodeSet.size > 0) {
      const fromId = String(link && link.from_id || '');
      const toId = String(link && link.to_id || '');
      if (nodeSet.has(fromId) || nodeSet.has(toId)) return true;
    }
    return false;
  });
}

function particleSignature(scene) {
  const links = visibleLinksForScene(scene);
  const ids = links.map((link) => String(link.id || '')).sort();
  return `${state.quality_tier}|${ids.length}|${ids.join(',')}`;
}

function syncParticlePool(force = false) {
  const sig = particleSignature(state.scene);
  if (force || !state.particles.length || state.particle_signature !== sig) {
    rebuildParticles();
    state.particle_signature = sig;
  }
}

function computeSelectionFocus(scene, selectedId) {
  const sid = String(selectedId || '').trim();
  if (!sid) return null;
  const nodes = [
    ...(Array.isArray(scene && scene.nodes) ? scene.nodes : []),
    ...(Array.isArray(scene && scene.io_nodes) ? scene.io_nodes : [])
  ];
  const links = Array.isArray(scene && scene.links) ? scene.links : [];
  const nodeById = Object.create(null);
  const linksByNode = Object.create(null);
  const linkById = Object.create(null);

  for (const node of nodes) {
    const id = String(node && node.id || '').trim();
    if (!id) continue;
    nodeById[id] = node;
  }

  for (const link of links) {
    const lid = String(link && link.id || '').trim();
    const from = String(link && link.from_id || '').trim();
    const to = String(link && link.to_id || '').trim();
    if (!lid || !from || !to) continue;
    linkById[lid] = link;
    if (!linksByNode[from]) linksByNode[from] = [];
    if (!linksByNode[to]) linksByNode[to] = [];
    linksByNode[from].push(lid);
    linksByNode[to].push(lid);
  }

  if (sid === SPINE_NODE_ID) {
    const focusNodes = new Set([sid]);
    const focusLinks = new Set();
    const spineDirectLinkIds = Array.isArray(linksByNode[sid]) ? linksByNode[sid] : [];
    const selectedLinks = [];
    for (const lid of spineDirectLinkIds) {
      const link = linkById[lid];
      if (link) selectedLinks.push(link);
    }
    if (!selectedLinks.length) {
      // Fallback for datasets that do not emit explicit spine endpoints:
      // use immediate ingress/egress + IO boundary links as inferred spine-adjacent flow.
      for (const link of links) {
        if (!link) continue;
        const fromId = String(link.from_id || '');
        const toId = String(link.to_id || '');
        const fromNode = nodeById[fromId];
        const toNode = nodeById[toId];
        const fromType = String(fromNode && fromNode.type || '').toLowerCase();
        const toType = String(toNode && toNode.type || '').toLowerCase();
        const kind = String(link.kind || '').toLowerCase();
        const touchesIo = fromType.startsWith('io_') || toType.startsWith('io_');
        if (kind === 'ingress' || kind === 'egress' || touchesIo) {
          selectedLinks.push(link);
        }
      }
    }
    if (!selectedLinks.length) {
      // Last-resort compact view instead of opening full graph.
      const ranked = links
        .slice()
        .sort((a, b) => {
          const av = Number(a && a.activity || 0);
          const bv = Number(b && b.activity || 0);
          if (Math.abs(av - bv) > 0.0001) return bv - av;
          return String(a && a.id || '').localeCompare(String(b && b.id || ''));
        })
        .slice(0, 12);
      for (const link of ranked) {
        if (link) selectedLinks.push(link);
      }
    }
    for (const link of selectedLinks) {
      const lid = String(link && link.id || '').trim();
      if (!lid) continue;
      focusLinks.add(lid);
      focusNodes.add(String(link && link.from_id || ''));
      focusNodes.add(String(link && link.to_id || ''));
    }
    return { nodes: focusNodes, links: focusLinks };
  }
  if (sid === SYSTEM_ROOT_ID) {
    const focusNodes = new Set();
    const focusLinks = new Set();
    for (const node of nodes) {
      const id = String(node && node.id || '').trim();
      if (id) focusNodes.add(id);
    }
    for (const link of links) {
      const lid = String(link && link.id || '').trim();
      if (!lid) continue;
      focusLinks.add(lid);
      focusNodes.add(String(link.from_id || ''));
      focusNodes.add(String(link.to_id || ''));
    }
    return { nodes: focusNodes, links: focusLinks };
  }

  if (!nodeById[sid] && !linksByNode[sid]) return null;

  const selectedNode = nodeById[sid];
  if (selectedNode && String(selectedNode.type || '') === 'layer') {
    const moduleIds = [];
    const submoduleIds = [];
    for (const node of nodes) {
      if (!node) continue;
      if (String(node.type || '') === 'module' && String(node.parent_id || '') === sid) {
        moduleIds.push(String(node.id || ''));
      }
    }
    const moduleSet = new Set(moduleIds);
    for (const node of nodes) {
      if (!node) continue;
      if (String(node.type || '') === 'submodule' && moduleSet.has(String(node.parent_id || ''))) {
        submoduleIds.push(String(node.id || ''));
      }
    }
    const includeIds = new Set([sid, ...moduleIds, ...submoduleIds]);
    const focusNodes = new Set(includeIds);
    const focusLinks = new Set();
    for (const link of links) {
      const from = String(link && link.from_id || '');
      const to = String(link && link.to_id || '');
      if (includeIds.has(from) || includeIds.has(to)) {
        const lid = String(link && link.id || '');
        if (!lid) continue;
        focusLinks.add(lid);
        focusNodes.add(from);
        focusNodes.add(to);
      }
    }
    return { nodes: focusNodes, links: focusLinks };
  }

  const focusNodes = new Set([sid]);
  const focusLinks = new Set();
  const directLinks = Array.isArray(linksByNode[sid]) ? linksByNode[sid] : [];

  for (const lid of directLinks) {
    const link = linkById[lid];
    if (!link) continue;
    focusLinks.add(lid);
    focusNodes.add(String(link.from_id || ''));
    focusNodes.add(String(link.to_id || ''));
  }

  const parentId = String(selectedNode && selectedNode.parent_id || '').trim();
  if (parentId) {
    const parentLinks = Array.isArray(linksByNode[parentId]) ? linksByNode[parentId] : [];
    for (const lid of parentLinks) {
      const link = linkById[lid];
      if (!link) continue;
      const kind = String(link.kind || '').toLowerCase();
      if (kind !== 'flow' && kind !== 'route' && kind !== 'ingress' && kind !== 'egress') continue;
      focusLinks.add(lid);
      focusNodes.add(String(link.from_id || ''));
      focusNodes.add(String(link.to_id || ''));
    }
  }

  return { nodes: focusNodes, links: focusLinks };
}

function applySelectionFocus(rebuild = true) {
  const sid = String(state.selected && state.selected.id || '').trim();
  const lid = String(state.selected_link && state.selected_link.id || '').trim();
  if (!state.scene) {
    state.focus = null;
    clearSelectedSubfractal();
    if (rebuild) syncParticlePool(true);
    return;
  }
  validateSelectedSubfractal(state.scene);
  if (lid) {
    const link = state.scene.link_by_id && state.scene.link_by_id[lid]
      ? state.scene.link_by_id[lid]
      : null;
    if (link) {
      const mapOwnerId = state.camera && state.camera.map_mode
        ? String(state.camera.map_owner_id || '').trim()
        : '';
      const mapOwnerNode = mapOwnerId && state.scene.node_by_id
        ? state.scene.node_by_id[mapOwnerId]
        : null;
      const mapOwnerType = String(mapOwnerNode && mapOwnerNode.type || '').toLowerCase();
      if (mapOwnerId && (mapOwnerType === 'module' || mapOwnerType === 'submodule')) {
        state.focus = computeSelectionFocus(state.scene, mapOwnerId);
      }
      if (!state.focus) {
        state.focus = {
          nodes: new Set([String(link.from_id || ''), String(link.to_id || '')]),
          links: new Set([String(link.id || '')])
        };
      }
    } else {
      state.selected_link = null;
      state.focus = null;
    }
    if (rebuild) syncParticlePool(true);
    return;
  }
  if (sid) {
    state.focus = computeSelectionFocus(state.scene, sid);
    if (!state.focus) {
      state.selected = null;
      renderSelectionTag();
    }
    if (rebuild) syncParticlePool(true);
    return;
  }
  state.focus = null;
  if (rebuild) syncParticlePool(true);
}

function setPayload(payload) {
  const prevScene = state.scene;
  state.payload = payload;
  state.live_mode = payload && payload.live_mode !== false;
  state.live_minutes = Math.max(1, Number(payload && payload.live_minutes || state.live_minutes || 6));
  state.runtime = runtimeSnapshotFromPayload(payload);
  const liveModeEl = byId('liveMode');
  const liveMinutesEl = byId('liveMinutes');
  if (liveModeEl) liveModeEl.checked = state.live_mode !== false;
  if (liveMinutesEl) liveMinutesEl.value = String(state.live_minutes);
  syncLiveControlState();
  state.incidents = payload && payload.incidents && typeof payload.incidents === 'object'
    ? payload.incidents
    : { integrity: null };
  const nextScene = buildScene(payload);
  registerResolvedFlashes(prevScene, nextScene, performance.now());
  state.scene = nextScene;
  if (state.scene) primeShellIntro(state.scene, performance.now());
  applyCodegraphMatches(state.scene);
  clampCameraPanInPlace();
  applySelectionFocus(false);
  syncParticlePool(false);
  renderSelectionTag();
  renderCodegraphStatus();
  renderStats();
}

function drawBackground(ts) {
  const ctx = state.ctx;
  const runtime = runtimeStatusForScene(state.scene);
  ctx.clearRect(0, 0, state.width, state.height);

  const pulse = 0.5 + (0.5 * Math.sin(ts * 0.00025));
  const grad = ctx.createRadialGradient(
    state.width * 0.52,
    state.height * 0.5,
    state.height * 0.08,
    state.width * 0.52,
    state.height * 0.5,
    state.height * 0.75
  );
  grad.addColorStop(0, `rgba(10,30,52,${0.6 + pulse * 0.1})`);
  grad.addColorStop(1, 'rgba(3,8,14,0.94)');
  ctx.fillStyle = grad;
  ctx.fillRect(0, 0, state.width, state.height);

  ctx.save();
  ctx.globalAlpha = 0.06;
  ctx.strokeStyle = '#6ecbff';
  ctx.lineWidth = 1;
  for (let y = 0; y < state.height; y += 6) {
    ctx.beginPath();
    ctx.moveTo(0, y + ((ts / 120) % 6));
    ctx.lineTo(state.width, y + ((ts / 120) % 6));
    ctx.stroke();
  }
  ctx.restore();

  const integrityIncident = normalizeIntegrityIncident(state.payload);
  if (integrityIncident.active) {
    const alpha = 0.08 + (Math.sin(ts * 0.0032) * 0.025);
    const pulseGrad = ctx.createRadialGradient(
      state.width * 0.52,
      state.height * 0.5,
      state.height * 0.08,
      state.width * 0.52,
      state.height * 0.5,
      state.height * 0.7
    );
    pulseGrad.addColorStop(0, `rgba(255,82,82,${clamp(alpha * 1.6, 0.06, 0.24)})`);
    pulseGrad.addColorStop(1, 'rgba(255,82,82,0)');
    ctx.fillStyle = pulseGrad;
    ctx.fillRect(0, 0, state.width, state.height);
  } else if (runtime.offline) {
    const offlineGlow = ctx.createRadialGradient(
      state.width * 0.52,
      state.height * 0.5,
      state.height * 0.08,
      state.width * 0.52,
      state.height * 0.5,
      state.height * 0.72
    );
    const alpha = 0.045 + (0.02 * (0.5 + Math.sin(ts * 0.0017)));
    offlineGlow.addColorStop(0, `rgba(255,94,94,${clamp(alpha * 1.5, 0.03, 0.14)})`);
    offlineGlow.addColorStop(1, 'rgba(255,94,94,0)');
    ctx.fillStyle = offlineGlow;
    ctx.fillRect(0, 0, state.width, state.height);
  } else if (runtime.stale) {
    const staleGlow = ctx.createRadialGradient(
      state.width * 0.52,
      state.height * 0.5,
      state.height * 0.08,
      state.width * 0.52,
      state.height * 0.5,
      state.height * 0.72
    );
    const alpha = 0.032 + (0.015 * (0.5 + Math.sin(ts * 0.0019)));
    staleGlow.addColorStop(0, `rgba(255,172,118,${clamp(alpha * 1.4, 0.02, 0.1)})`);
    staleGlow.addColorStop(1, 'rgba(255,172,118,0)');
    ctx.fillStyle = staleGlow;
    ctx.fillRect(0, 0, state.width, state.height);
  }
}

function drawLayerRing(layerNode, ts) {
  const ctx = state.ctx;
  const selected = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selected && selected.id || '');
  const selectedType = String(selected && selected.type || '').toLowerCase();
  const isSelected = selectedId
    && selectedId === String(layerNode.id || '')
    && (!selectedType || selectedType === 'layer');
  const queryMatch = isCodegraphNodeMatched(String(layerNode.id || ''));
  const queryAlphaScale = codegraphNodeAlphaScale(String(layerNode.id || ''));
  const introScale = clamp(Number(layerNode.intro_scale == null ? 1 : layerNode.intro_scale), 0, 1);
  const baseRadius = Number(layerNode.render_radius || layerNode.radius || 0);
  if (baseRadius <= 0.5) return;
  const integrityAlert = layerNode && (layerNode.integrity_alert === true || layerNode.error_state_active === true);
  const jitter = Math.sin(ts * 0.00035 + layerNode.radius * 0.02) * 1.5 * introScale;
  const radius = Math.max(0.2, baseRadius + jitter);
  const alphaScale = 0.25 + (introScale * 0.75);
  const primaryAlpha = (0.2 + (layerNode.activity * 0.22)) * alphaScale * queryAlphaScale;
  const secondaryAlpha = 0.11 * queryAlphaScale;
  const resolvedAlpha = resolvedFlashAlpha('node', String(layerNode && layerNode.id || ''), ts);
  const integrityPrimary = `rgba(255,82,82,${clamp(0.28 + (primaryAlpha * 0.92), 0.2, 0.92)})`;
  const integritySecondary = `rgba(255,112,112,${clamp(0.2 + (secondaryAlpha * 1.25), 0.16, 0.84)})`;
  const resolvedPrimary = `rgba(122,255,166,${clamp(0.3 + (resolvedAlpha * 0.62), 0.16, 0.96)})`;
  const resolvedSecondary = `rgba(154,255,188,${clamp(0.22 + (resolvedAlpha * 0.46), 0.12, 0.86)})`;
  const stroke = resolvedAlpha > 0
    ? resolvedPrimary
    : (isSelected
      ? softHighlightColor(Math.min(0.92, primaryAlpha + 0.11))
      : queryMatch
        ? colorFromActivityBright(layerNode.activity, Math.min(0.88, primaryAlpha + 0.3), 0.28)
      : integrityAlert
        ? integrityPrimary
      : colorFromActivity(layerNode.activity, primaryAlpha));
  ctx.strokeStyle = stroke;
  ctx.lineWidth = Math.max(0.8, layerNode.ring_width * 0.08 * (0.32 + (introScale * 0.68)));
  ctx.beginPath();
  ctx.arc(layerNode.x, layerNode.y, radius, 0, Math.PI * 2);
  ctx.stroke();

  ctx.strokeStyle = resolvedAlpha > 0
    ? resolvedSecondary
    : (isSelected
      ? softHighlightColor(Math.min(0.8, secondaryAlpha + 0.22))
      : queryMatch
        ? colorFromActivityBright(layerNode.activity, Math.min(0.7, secondaryAlpha + 0.34), 0.26)
      : integrityAlert
        ? integritySecondary
      : colorFromActivity(layerNode.activity, secondaryAlpha));
  ctx.lineWidth = Math.max(0.7, layerNode.ring_width * 0.03 * (0.35 + (introScale * 0.65)));
  for (let i = 0; i < 24; i += 1) {
    const a0 = (i / 24) * Math.PI * 2;
    const a1 = a0 + 0.12;
    ctx.beginPath();
    ctx.arc(layerNode.x, layerNode.y, radius + (layerNode.ring_width * 0.2 * (0.2 + (introScale * 0.8))), a0, a1);
    ctx.stroke();
  }
}

function runtimeStatusForScene(scene) {
  const sceneRuntime = scene && scene.runtime_status && typeof scene.runtime_status === 'object'
    ? scene.runtime_status
    : {};
  const base = state.runtime && typeof state.runtime === 'object' ? state.runtime : {};
  const merged = {
    ...base,
    ...sceneRuntime
  };
  const status = String(merged.status || '').trim().toLowerCase() || 'unknown';
  const online = merged.online === true || status === 'online';
  const stale = merged.stale === true || status === 'stale';
  const offline = merged.offline === true || status === 'offline';
  return {
    ...merged,
    status,
    online,
    stale,
    offline,
    activity_scale: clamp(Number(merged.activity_scale == null ? (offline ? 0.2 : (stale ? 0.58 : 1)) : merged.activity_scale), 0.12, 1.2)
  };
}

function runtimeLinkAlphaScale(scene) {
  const runtime = runtimeStatusForScene(scene);
  if (runtime.offline) return 0.22;
  if (runtime.stale) return 0.62;
  return 1;
}

function drawSpineHub(scene, ts) {
  const ctx = state.ctx;
  const c = scene.center;
  const runtime = runtimeStatusForScene(scene);
  const selected = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selected && selected.id || '');
  const selectedType = String(selected && selected.type || '').toLowerCase();
  const spineSelected = selectedId
    && selectedId === SPINE_NODE_ID
    && (!selectedType || selectedType === 'spine');
  const queryMatch = isCodegraphNodeMatched(SPINE_NODE_ID);
  const queryAlphaScale = codegraphNodeAlphaScale(SPINE_NODE_ID);
  const pulse = 1 + (Math.sin(ts * 0.00125) * 0.08);
  const drift = clamp(Number(scene.metrics && scene.metrics.drift_proxy || 0), 0, 1);
  const runtimeOffline = runtime.offline === true;
  const runtimeStale = !runtimeOffline && runtime.stale === true;
  const integrityAlert = scene && (
    scene.integrity_alert === true
    || Number(scene.metrics && scene.metrics.integrity_alert || 0) > 0
  );
  const base = 13 * pulse;
  const normalPrimary = spineSelected
    ? `rgba(${brightenChannel(110, 0.2)},${brightenChannel(203, 0.2)},${brightenChannel(255, 0.2)},0.86)`
    : `rgba(110, 203, 255, ${0.72 * queryAlphaScale})`;
  const normalSecondary = spineSelected
    ? `rgba(${brightenChannel(110, 0.22)},${brightenChannel(203, 0.22)},${brightenChannel(255, 0.22)},0.5)`
    : `rgba(110, 203, 255, ${0.34 * queryAlphaScale})`;
  const alertPrimary = spineSelected ? 'rgba(255,236,236,0.9)' : 'rgba(255,86,86,0.86)';
  const alertSecondary = spineSelected ? 'rgba(255,226,226,0.58)' : 'rgba(255,104,104,0.46)';
  const stalePrimary = spineSelected ? 'rgba(255,236,226,0.86)' : 'rgba(255,166,96,0.78)';
  const staleSecondary = spineSelected ? 'rgba(255,230,216,0.56)' : 'rgba(255,186,112,0.42)';
  const offlinePrimary = spineSelected ? 'rgba(255,230,230,0.84)' : 'rgba(255,92,92,0.66)';
  const offlineSecondary = spineSelected ? 'rgba(255,222,222,0.52)' : 'rgba(255,112,112,0.36)';
  const resolvedAlpha = resolvedFlashAlpha('node', SPINE_NODE_ID, ts);
  const resolvedPrimary = `rgba(122,255,166,${clamp(0.34 + (resolvedAlpha * 0.62), 0.14, 0.98)})`;
  const resolvedSecondary = `rgba(156,255,190,${clamp(0.24 + (resolvedAlpha * 0.44), 0.12, 0.88)})`;
  ctx.strokeStyle = resolvedAlpha > 0
    ? resolvedPrimary
    : (queryMatch
      ? `rgba(222,242,255,${clamp(0.7 * queryAlphaScale, 0.2, 0.92)})`
      : (integrityAlert ? alertPrimary : (runtimeOffline ? offlinePrimary : (runtimeStale ? stalePrimary : normalPrimary))));
  ctx.lineWidth = 1.2;
  ctx.beginPath();
  ctx.arc(c.x, c.y, base, 0, Math.PI * 2);
  ctx.stroke();
  ctx.strokeStyle = resolvedAlpha > 0
    ? resolvedSecondary
    : (queryMatch
      ? `rgba(205,236,252,${clamp(0.52 * queryAlphaScale, 0.16, 0.8)})`
      : (integrityAlert ? alertSecondary : (runtimeOffline ? offlineSecondary : (runtimeStale ? staleSecondary : normalSecondary))));
  ctx.beginPath();
  ctx.arc(c.x, c.y, base + 7, 0, Math.PI * 2);
  ctx.stroke();
  if (resolvedAlpha > 0) {
    const settledPulse = 0.16 + (0.08 * (0.5 + Math.sin(ts * 0.0032)));
    ctx.fillStyle = `rgba(130,255,176,${settledPulse + (resolvedAlpha * 0.2)})`;
    ctx.beginPath();
    ctx.arc(c.x, c.y, base + 3.2, 0, Math.PI * 2);
    ctx.fill();
  } else if (integrityAlert) {
    const alarmPulse = 0.32 + (0.16 * (0.5 + Math.sin(ts * 0.004)));
    ctx.fillStyle = `rgba(255,76,76,${alarmPulse})`;
    ctx.beginPath();
    ctx.arc(c.x, c.y, base + 3.5, 0, Math.PI * 2);
    ctx.fill();
  } else if (runtimeOffline) {
    const idlePulse = 0.2 + (0.08 * (0.5 + Math.sin(ts * 0.0024)));
    ctx.fillStyle = `rgba(255,92,92,${idlePulse})`;
    ctx.beginPath();
    ctx.arc(c.x, c.y, base + 3.1, 0, Math.PI * 2);
    ctx.fill();
  } else if (runtimeStale) {
    const stalePulse = 0.18 + (0.08 * (0.5 + Math.sin(ts * 0.0029)));
    ctx.fillStyle = `rgba(255,170,110,${stalePulse})`;
    ctx.beginPath();
    ctx.arc(c.x, c.y, base + 3.1, 0, Math.PI * 2);
    ctx.fill();
  }
  drawNodeChangeIndicator({
    id: SPINE_NODE_ID,
    x: c.x,
    y: c.y,
    radius: base + 1.4,
    change_state: scene && scene.change_summary && typeof scene.change_summary === 'object'
      ? {
          active_write: Number(scene.change_summary.active_write_files_total || 0) > 0,
          dirty: Number(scene.change_summary.dirty_files_total || 0) > 0,
          staged: Number(scene.change_summary.staged_files_total || 0) > 0,
          pending_push: scene.change_summary.pending_push === true || Number(scene.change_summary.ahead_count || 0) > 0,
          just_pushed: scene.change_summary.just_pushed === true,
          changed: Number(scene.change_summary.active_write_files_total || 0) > 0
            || Number(scene.change_summary.dirty_files_total || 0) > 0
            || Number(scene.change_summary.staged_files_total || 0) > 0
            || scene.change_summary.pending_push === true
            || scene.change_summary.just_pushed === true,
          file_count: Number(scene.change_summary.top_files && scene.change_summary.top_files.length || 0),
          top_files: Array.isArray(scene.change_summary.top_files) ? scene.change_summary.top_files : [],
          last_push_ts: String(scene.change_summary.last_push_ts || '')
        }
      : {}
  }, ts, 1.05);
  if (drift > 0.02) {
    ctx.fillStyle = `rgba(255, 90, 64, ${0.08 + (drift * 0.2)})`;
    ctx.beginPath();
    ctx.arc(c.x, c.y, base + 3 + (drift * 10), 0, Math.PI * 2);
    ctx.fill();
  }
}

function drawDriftOverlay(scene, ts) {
  const ctx = state.ctx;
  const drift = clamp(Number(scene.metrics && scene.metrics.drift_proxy || 0), 0, 1);
  if (drift <= 0.001) return;
  const layerNodes = scene.nodes.filter((n) => n.type === 'layer');
  if (!layerNodes.length) return;
  const maxR = layerNodes.reduce((acc, n) => Math.max(acc, Number(n.render_radius || n.radius || 0)), 0);
  if (maxR <= 0.5) return;
  const c = scene.center;
  const ringR = maxR + 18;
  const totalSegments = 18;
  const activeSegments = Math.max(1, Math.round(totalSegments * drift));
  const swing = Math.sin(ts * 0.0007) * 0.18;
  for (let i = 0; i < totalSegments; i += 1) {
    if (i >= activeSegments) break;
    const a0 = ((i / totalSegments) * Math.PI * 2) + swing;
    const a1 = a0 + (Math.PI * 2 / totalSegments) * 0.7;
    const alpha = 0.16 + ((i / Math.max(1, activeSegments)) * 0.22) + (drift * 0.22);
    ctx.strokeStyle = `rgba(255,92,56,${alpha})`;
    ctx.lineWidth = 1.4 + (drift * 1.4);
    ctx.beginPath();
    ctx.arc(c.x, c.y, ringR, a0, a1);
    ctx.stroke();
  }
}

function drawNodeChangeIndicator(node, ts, baseScale = 1) {
  const ctx = state.ctx;
  const n = node && typeof node === 'object' ? node : {};
  const change = normalizeChangeState(n.change_state);
  if (!change.changed && !change.just_pushed) return;
  const x = Number(n.x || 0);
  const y = Number(n.y || 0);
  const radius = Math.max(3, Number(n.radius || 8));
  const scale = Math.max(0.25, Number(baseScale || 1));
  const phase = (ts * 0.0018) + (stableHash(String(n.id || 'node')) % 23);
  const base = radius + (4 * scale);
  ctx.save();
  ctx.lineWidth = Math.max(0.9, 1 * scale);
  ctx.shadowBlur = 0;

  if (change.active_write) {
    const pulseR = base + (Math.sin(phase * 2.2) * 1.2 * scale);
    ctx.strokeStyle = `rgba(244,248,255,${0.68 + (0.14 * (0.5 + Math.sin(phase * 1.7)))})`;
    ctx.beginPath();
    ctx.arc(x, y, pulseR, 0, Math.PI * 2);
    ctx.stroke();
  }

  if (change.dirty) {
    ctx.setLineDash([3 * scale, 2.4 * scale]);
    ctx.strokeStyle = 'rgba(255,176,94,0.84)';
    ctx.beginPath();
    ctx.arc(x, y, base + (3.6 * scale), 0, Math.PI * 2);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  if (change.staged) {
    ctx.strokeStyle = 'rgba(118,226,255,0.88)';
    ctx.beginPath();
    ctx.arc(x, y, base + (6.2 * scale), 0, Math.PI * 2);
    ctx.stroke();
  }

  if (change.pending_push) {
    ctx.setLineDash([1.8 * scale, 2.6 * scale]);
    ctx.strokeStyle = 'rgba(162,208,255,0.86)';
    ctx.beginPath();
    ctx.arc(x, y, base + (8.6 * scale), 0, Math.PI * 2);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  if (change.just_pushed) {
    const ripple = base + (11 * scale) + ((0.5 + (0.5 * Math.sin(phase * 2.8))) * 4.8 * scale);
    const alpha = 0.46 + (0.2 * (0.5 + Math.sin(phase * 2.4)));
    ctx.strokeStyle = `rgba(134,255,186,${alpha})`;
    ctx.beginPath();
    ctx.arc(x, y, ripple, 0, Math.PI * 2);
    ctx.stroke();
  }

  ctx.restore();
}

function drawModuleNode(node, ts) {
  const ctx = state.ctx;
  const selected = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selected && selected.id || '');
  const selectedType = String(selected && selected.type || '').toLowerCase();
  const isSelected = selectedId
    && selectedId === String(node.id || '')
    && (!selectedType || selectedType === 'module');
  const queryMatch = isCodegraphNodeMatched(String(node.id || ''));
  const queryAlphaScale = codegraphNodeAlphaScale(String(node.id || ''));
  const introScale = nodeIntroScale(node);
  if (introScale <= 0.02) return;
  const errorActive = Boolean(node && node.error_state_active === true);
  const resolvedAlpha = resolvedFlashAlpha('node', String(node && node.id || ''), ts);
  const glow = 0.17 + (node.activity * 0.35);
  const pulse = 0.88 + (Math.sin(ts * 0.001 + node.x * 0.01) * 0.1);
  const r = node.radius * pulse;
  const spinOffset = Number(node.fractal_count || 0) > 0 ? Number(node.spin_angle || 0) * 0.42 : 0;
  ctx.save();
  ctx.globalAlpha *= (0.2 + (introScale * 0.8)) * queryAlphaScale;
  ctx.strokeStyle = resolvedAlpha > 0
    ? `rgba(122,255,166,${clamp(0.34 + (resolvedAlpha * 0.56), 0.18, 0.96)})`
    : (isSelected
      ? softHighlightColor(0.9)
      : queryMatch
        ? colorFromActivityBright(node.activity, 0.9, 0.3)
      : (errorActive ? 'rgba(255,96,96,0.92)' : colorFromActivity(node.activity, 0.65)));
  ctx.lineWidth = 1.2;
  ctx.beginPath();
  ctx.arc(node.x, node.y, r, 0, Math.PI * 2);
  ctx.stroke();

  ctx.fillStyle = resolvedAlpha > 0
    ? `rgba(132,255,176,${clamp(0.18 + (resolvedAlpha * 0.24), 0.12, 0.52)})`
    : (isSelected
      ? softHighlightColor(Math.min(0.34, 0.2 + (glow * 0.16)))
      : queryMatch
        ? colorFromActivityBright(node.activity, Math.min(0.4, (glow * 0.24) + 0.12), 0.26)
      : (errorActive ? 'rgba(255,88,88,0.22)' : colorFromActivity(node.activity, glow * 0.2)));
  ctx.beginPath();
  ctx.arc(node.x, node.y, r * 0.72, 0, Math.PI * 2);
  ctx.fill();

  ctx.strokeStyle = resolvedAlpha > 0
    ? `rgba(154,255,188,${clamp(0.24 + (resolvedAlpha * 0.44), 0.14, 0.84)})`
    : (isSelected
      ? softHighlightColor(0.74)
      : queryMatch
        ? colorFromActivityBright(node.activity, 0.74, 0.26)
      : (errorActive ? 'rgba(255,132,132,0.74)' : colorFromActivity(node.activity, 0.36)));
  ctx.lineWidth = 1;
  for (let i = 0; i < 3; i += 1) {
    const start = ((i / 3) * Math.PI * 2) + (ts * 0.00016) + (stableHash(node.id) % 10) * 0.07 + spinOffset;
    ctx.beginPath();
    ctx.arc(node.x, node.y, r * (0.82 + i * 0.09), start, start + 0.78);
    ctx.stroke();
  }
  drawNodeChangeIndicator({ ...node, radius: r }, ts, 1);
  ctx.restore();
}

function drawSubmoduleNode(node) {
  const ctx = state.ctx;
  const selected = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selected && selected.id || '');
  const selectedType = String(selected && selected.type || '').toLowerCase();
  const isSelected = selectedId
    && selectedId === String(node.id || '')
    && (!selectedType || selectedType === 'submodule');
  const queryMatch = isCodegraphNodeMatched(String(node.id || ''));
  const queryAlphaScale = codegraphNodeAlphaScale(String(node.id || ''));
  const subfractalChildren = subfractalChildrenForNode(node);
  const hasSubfractals = subfractalChildren.length > 0;
  const selectedSubfractal = isSelected
    && state.selected_subfractal
    && String(state.selected_subfractal.parent_id || '') === String(node.id || '')
    ? state.selected_subfractal
    : null;
  const errorActive = Boolean(node && node.error_state_active === true);
  const resolvedAlpha = resolvedFlashAlpha('node', String(node && node.id || ''));
  const introScale = nodeIntroScale(node);
  if (introScale <= 0.02) return;
  ctx.save();
  ctx.globalAlpha *= (0.18 + (introScale * 0.82)) * queryAlphaScale;
  if (state.quality_tier === 'low' && !(isSelected && hasSubfractals)) {
    const s = Math.max(1.4, node.radius * 1.45);
    ctx.fillStyle = resolvedAlpha > 0
      ? `rgba(124,255,170,${clamp(0.54 + (resolvedAlpha * 0.34), 0.24, 0.96)})`
      : (errorActive
        ? 'rgba(255,72,72,0.92)'
        : queryMatch
          ? colorFromActivityBright(node.activity, 0.88, 0.26)
        : colorFromActivity(node.activity, 0.82));
    ctx.fillRect(node.x - s, node.y - s, s * 2, s * 2);
    if (resolvedAlpha > 0 || isSelected || errorActive) {
      ctx.strokeStyle = resolvedAlpha > 0
        ? `rgba(168,255,198,${clamp(0.36 + (resolvedAlpha * 0.54), 0.2, 0.94)})`
        : (queryMatch ? colorFromActivityBright(node.activity, 0.88, 0.28) : softHighlightColor(0.88));
      ctx.lineWidth = 1;
      ctx.strokeRect(node.x - s, node.y - s, s * 2, s * 2);
    }
    ctx.restore();
    return;
  }
  const px = Number(node.parent_x || node.x);
  const py = Number(node.parent_y || node.y);
  const inner = Math.max(1, Number(node.shell_inner || (node.radius * 2.2)));
  const outer = Math.max(inner + 1, Number(node.shell_outer || (node.radius * 3.1)));
  const start = Number(node.shell_start || 0);
  const end = Number(node.shell_end || (Math.PI * 0.4));
  ctx.fillStyle = resolvedAlpha > 0
    ? `rgba(132,255,176,${clamp(0.18 + (resolvedAlpha * 0.24), 0.1, 0.52)})`
    : (errorActive
      ? `rgba(255,68,68,${0.18 + (node.activity * 0.24)})`
      : queryMatch
        ? colorFromActivityBright(node.activity, 0.26 + (node.activity * 0.18), 0.24)
      : colorFromActivity(node.activity, 0.16 + (node.activity * 0.22)));
  ctx.strokeStyle = resolvedAlpha > 0
    ? `rgba(122,255,166,${clamp(0.36 + (resolvedAlpha * 0.56), 0.18, 0.96)})`
    : (isSelected
      ? softHighlightColor(0.9)
      : queryMatch
        ? colorFromActivityBright(node.activity, 0.92, 0.28)
      : (errorActive ? 'rgba(255,102,102,0.95)' : colorFromActivity(node.activity, 0.68)));
  ctx.lineWidth = (isSelected && hasSubfractals) ? 0.72 : 1;
  ctx.beginPath();
  ctx.arc(px, py, outer, start, end);
  ctx.arc(px, py, inner, end, start, true);
  ctx.closePath();
  ctx.fill();
  ctx.stroke();

  if (isSelected && hasSubfractals) {
    const count = subfractalChildren.length;
    const selectedIdxRaw = selectedSubfractal
      ? Number(selectedSubfractal.index || 0)
      : -1;
    const selectedIdx = clamp(Math.floor(selectedIdxRaw), 0, Math.max(0, count - 1));
    if (selectedSubfractal && selectedIdx >= 0 && selectedIdx < count) {
      const segStart = angleAtArcFraction(start, end, selectedIdx / count);
      const segEnd = angleAtArcFraction(start, end, (selectedIdx + 1) / count);
      ctx.fillStyle = softHighlightColor(0.15);
      ctx.beginPath();
      ctx.arc(px, py, outer, segStart, segEnd);
      ctx.arc(px, py, inner, segEnd, segStart, true);
      ctx.closePath();
      ctx.fill();
    }
    ctx.strokeStyle = softHighlightColor(0.48);
    ctx.lineWidth = 0.32;
    for (let i = 1; i < count; i += 1) {
      const a = angleAtArcFraction(start, end, i / count);
      ctx.beginPath();
      ctx.moveTo(px + (Math.cos(a) * inner), py + (Math.sin(a) * inner));
      ctx.lineTo(px + (Math.cos(a) * outer), py + (Math.sin(a) * outer));
      ctx.stroke();
    }
  }

  if (state.quality_tier === 'high' || state.quality_tier === 'ultra') {
    const step = (end - start) / 3;
    ctx.strokeStyle = resolvedAlpha > 0
      ? `rgba(154,255,188,${clamp(0.24 + (resolvedAlpha * 0.44), 0.14, 0.86)})`
      : (isSelected
        ? softHighlightColor(0.7)
        : queryMatch
          ? colorFromActivityBright(node.activity, 0.72, 0.24)
        : (errorActive ? 'rgba(255,122,122,0.85)' : colorFromActivity(node.activity, 0.4)));
    ctx.lineWidth = 0.8;
    for (let i = 1; i <= 2; i += 1) {
      const a = start + (step * i);
      ctx.beginPath();
      ctx.moveTo(px + (Math.cos(a) * inner), py + (Math.sin(a) * inner));
      ctx.lineTo(px + (Math.cos(a) * outer), py + (Math.sin(a) * outer));
      ctx.stroke();
    }
    const midR = inner + ((outer - inner) * 0.52);
    ctx.beginPath();
    ctx.arc(px, py, midR, start + 0.04, end - 0.04);
    ctx.stroke();
  }
  ctx.restore();
}

function drawIoNode(node, ts) {
  const ctx = state.ctx;
  const spin = ts * 0.0014;
  const size = node.radius + (Math.sin(spin + node.angle * 2) * 1.2);
  const queryAlphaScale = codegraphNodeAlphaScale(String(node && node.id || ''));
  if (queryAlphaScale <= 0.02) return;
  ctx.save();
  ctx.globalAlpha *= queryAlphaScale;
  ctx.translate(node.x, node.y);
  ctx.rotate(spin + node.angle);
  ctx.strokeStyle = node.type === 'io_input'
    ? colorFromActivity(node.activity, 0.9)
    : 'rgba(255,255,255,0.9)';
  ctx.lineWidth = 1.4;
  ctx.beginPath();
  ctx.moveTo(0, -size);
  ctx.lineTo(size, 0);
  ctx.lineTo(0, size);
  ctx.lineTo(-size, 0);
  ctx.closePath();
  ctx.stroke();
  ctx.restore();
}

function drawLinks(scene) {
  const ctx = state.ctx;
  ctx.save();
  ctx.globalAlpha *= PACKET_PATHWAY_OPACITY_SCALE;
  const profile = state.quality_profile;
  const nowTs = performance.now();
  const runtimeScale = runtimeLinkAlphaScale(scene);
  const focusLinks = state.focus && state.focus.links instanceof Set ? state.focus.links : null;
  const hasCodegraph = codegraphHasMatches();
  const nodeById = scene.node_by_id && typeof scene.node_by_id === 'object'
    ? scene.node_by_id
    : Object.create(null);
  for (const link of scene.links) {
    if (focusLinks && !focusLinks.has(link.id)) continue;
    const codegraphScale = codegraphLinkAlphaScale(link);
    if (hasCodegraph && codegraphScale <= 0.001) continue;
    const codegraphMatch = codegraphScale >= 0.999;
    const fromNode = nodeById[String(link.from_id || '')];
    const toNode = nodeById[String(link.to_id || '')];
    const introScale = Math.min(nodeIntroScale(fromNode), nodeIntroScale(toNode));
    if (introScale <= 0.02) continue;
    const kind = String(link.kind || '').toLowerCase();
    const doctorState = String(link.doctor_state || '').trim().toLowerCase();
    const doctorLink = kind === 'doctor' || Boolean(doctorState);
    if (kind === 'fractal') {
      const alpha = clamp((0.2 + (Number(link.activity || 0) * 0.35)) * (0.2 + introScale * 0.8) * codegraphScale * runtimeScale, 0.06, 0.8);
      const resolvedAlpha = resolvedFlashAlpha('link', String(link && link.id || ''), nowTs);
      ctx.shadowBlur = 0;
      ctx.strokeStyle = resolvedAlpha > 0
        ? `rgba(136,255,180,${clamp(alpha + (resolvedAlpha * 0.35), 0.14, 0.96)})`
        : (codegraphMatch
          ? `rgba(230,242,252,${clamp(alpha + 0.26, 0.14, 0.96)})`
          : `rgba(196,232,255,${alpha})`);
      ctx.lineWidth = (0.7 + (Number(link.activity || 0) * 0.95)) * (0.35 + (introScale * 0.65));
      ctx.beginPath();
      ctx.moveTo(link.p0.x, link.p0.y);
      ctx.bezierCurveTo(link.p1.x, link.p1.y, link.p2.x, link.p2.y, link.p3.x, link.p3.y);
      ctx.stroke();
      continue;
    }
    const errW = clamp(Number(link.error_weight || 0), 0, 1);
    const errActive = errW > 0.045;
    const blockedRatio = clamp(Number(link.blocked_ratio || 0), 0, 1);
    const blockedActive = Boolean(link.flow_blocked === true || blockedRatio > 0.02);
    const resolvedAlpha = resolvedFlashAlpha('link', String(link && link.id || ''), nowTs);
    const alpha = (profile.tube_alpha + (link.activity * 0.12)) * (0.22 + (introScale * 0.78)) * codegraphScale * runtimeScale;
    const baseLineWidth = (0.8 + (link.activity * 1.8)) * (0.35 + (introScale * 0.65));
    if (state.quality_tier === 'high' || state.quality_tier === 'ultra') {
      if (doctorLink && doctorState === 'healing') {
        ctx.shadowColor = `rgba(246,246,238,${clamp((0.2 + (link.activity * 0.25)) * codegraphScale, 0.08, 0.72)})`;
        ctx.shadowBlur = (6 + (link.activity * 12)) * (0.2 + (introScale * 0.8));
      } else if (doctorLink && doctorState === 'regrowth') {
        ctx.shadowColor = `rgba(132,255,176,${clamp((0.22 + (link.activity * 0.28)) * codegraphScale, 0.08, 0.78)})`;
        ctx.shadowBlur = (6 + (link.activity * 13)) * (0.2 + (introScale * 0.8));
      } else if (errActive || blockedActive) {
        ctx.shadowColor = `rgba(255,78,78,${(0.24 + (errW * 0.4)) * codegraphScale})`;
        ctx.shadowBlur = (8 + (errW * 22)) * (0.2 + (introScale * 0.8));
      } else {
        ctx.shadowColor = codegraphMatch
          ? `rgba(218,241,255,${clamp(0.34 * codegraphScale, 0.08, 0.64)})`
          : colorFromActivity(link.activity, 0.26 * codegraphScale);
        ctx.shadowBlur = (8 + (link.activity * 16)) * (0.2 + (introScale * 0.8));
      }
    } else {
      ctx.shadowBlur = 0;
    }
    if (resolvedAlpha > 0) {
      ctx.strokeStyle = `rgba(128,255,174,${clamp(0.2 + alpha + (resolvedAlpha * 0.42), 0.12, 0.98)})`;
      ctx.lineWidth = baseLineWidth;
    } else if (doctorLink && doctorState === 'healing') {
      ctx.strokeStyle = `rgba(244,242,235,${clamp(0.16 + alpha + 0.2, 0.08, 0.96)})`;
      ctx.lineWidth = baseLineWidth;
    } else if (doctorLink && doctorState === 'regrowth') {
      ctx.strokeStyle = `rgba(130,255,174,${clamp(0.14 + alpha + 0.18, 0.08, 0.96)})`;
      ctx.lineWidth = baseLineWidth;
    } else if (codegraphMatch) {
      ctx.strokeStyle = `rgba(232,246,255,${clamp(0.2 + alpha + 0.28, 0.1, 0.98)})`;
      ctx.lineWidth = baseLineWidth * 1.1;
    } else if (errActive) {
      ctx.strokeStyle = `rgba(255,72,72,${clamp(0.14 + alpha + (errW * 0.55), 0.08, 0.96)})`;
      ctx.lineWidth = baseLineWidth;
    } else {
      ctx.strokeStyle = colorFromActivity(link.activity, alpha);
      ctx.lineWidth = baseLineWidth;
    }
    ctx.beginPath();
    ctx.moveTo(link.p0.x, link.p0.y);
    ctx.bezierCurveTo(link.p1.x, link.p1.y, link.p2.x, link.p2.y, link.p3.x, link.p3.y);
    ctx.stroke();

    if (blockedActive) {
      const blockedAlpha = clamp((0.24 + (blockedRatio * 0.64)) * (0.22 + (introScale * 0.78)) * codegraphScale, 0.08, 0.92);
      ctx.save();
      ctx.setLineDash([4.5 + (blockedRatio * 6.5), 4.5]);
      ctx.lineDashOffset = -(nowTs * (0.013 + (blockedRatio * 0.018)));
      ctx.strokeStyle = `rgba(255,158,76,${blockedAlpha})`;
      ctx.lineWidth = Math.max(0.8, baseLineWidth * 0.88);
      ctx.beginPath();
      ctx.moveTo(link.p0.x, link.p0.y);
      ctx.bezierCurveTo(link.p1.x, link.p1.y, link.p2.x, link.p2.y, link.p3.x, link.p3.y);
      ctx.stroke();
      ctx.restore();

      const mid = bezierPoint(link, 0.52);
      const markerR = 2.1 + (blockedRatio * 3.1);
      ctx.strokeStyle = `rgba(255,108,108,${clamp((0.45 + (blockedRatio * 0.4)) * Math.max(0.4, codegraphScale), 0.2, 0.92)})`;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.arc(mid.x, mid.y, markerR, 0, Math.PI * 2);
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(mid.x - markerR * 0.72, mid.y + markerR * 0.72);
      ctx.lineTo(mid.x + markerR * 0.72, mid.y - markerR * 0.72);
      ctx.stroke();
    }
  }
  ctx.shadowBlur = 0;
  ctx.restore();
}

function isModuleDepthPacketView(scene) {
  if (!scene || !scene.node_by_id) return false;
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selectedRef && selectedRef.id || '').trim();
  const selectedNode = selectedId ? scene.node_by_id[selectedId] : null;
  const selectedTypeHint = String(selectedRef && selectedRef.type || '').toLowerCase();
  const selectedType = selectedTypeHint || String((selectedNode && selectedNode.type) || '').toLowerCase();
  if (selectedType === 'module' || selectedType === 'submodule') return true;
  const focusId = state.camera && state.camera.focus_mode
    ? String(state.camera.focus_target_id || '').trim()
    : '';
  if (!focusId) return false;
  const focusNode = scene.node_by_id[focusId];
  const focusType = String((focusNode && focusNode.type) || '').toLowerCase();
  return focusType === 'module' || focusType === 'submodule';
}

function drawParticles(dt) {
  const scene = state.scene;
  if (!scene || !state.particles.length) return;
  const runtime = runtimeStatusForScene(scene);
  if (runtime.offline) {
    for (const p of state.particles) {
      if (p && Array.isArray(p.trail)) p.trail.length = 0;
    }
    return;
  }
  const runtimeFlowScale = runtime.stale ? 0.62 : 1;
  const runtimeAlphaScale = runtime.stale ? 0.7 : 1;
  const ctx = state.ctx;
  const trailLength = state.quality_profile.trail_length;
  const nowTs = performance.now();
  const burstBoost = nowTs < state.spine_burst_until ? 1.45 : 1;
  const focusLinks = state.focus && state.focus.links instanceof Set ? state.focus.links : null;
  const visibleLinks = visibleLinksForScene(scene);
  const visibleLinkIds = new Set(visibleLinks.map((link) => String(link && link.id || '')).filter(Boolean));
  const nodeById = scene.node_by_id && typeof scene.node_by_id === 'object'
    ? scene.node_by_id
    : Object.create(null);

  const linkById = {};
  for (const link of scene.links) linkById[link.id] = link;
  const linksForSizing = visibleLinks;
  const packetRange = packetMetricRange(linksForSizing);
  const forceMinPacketSize = isModuleDepthPacketView(scene);

  const linkIntroScaleById = Object.create(null);
  for (const link of scene.links) {
    const fromNode = nodeById[String(link.from_id || '')];
    const toNode = nodeById[String(link.to_id || '')];
    linkIntroScaleById[link.id] = Math.min(nodeIntroScale(fromNode), nodeIntroScale(toNode));
  }

  for (const p of state.particles) {
    const link = linkById[p.link_id];
    if (!link) continue;
    const introScale = clamp(Number(linkIntroScaleById[link.id]), 0, 1);
    if (introScale <= 0.03) {
      p.trail.length = 0;
      continue;
    }
    if (focusLinks && !focusLinks.has(link.id)) {
      p.trail.length = 0;
      continue;
    }
    if (!visibleLinkIds.has(String(link.id || ''))) {
      p.trail.length = 0;
      continue;
    }
    p.radius = forceMinPacketSize
      ? PACKET_RADIUS_FLOOR
      : packetRadiusForLink(link, packetRange);
    const blockedRatio = clamp(Number(link.blocked_ratio || 0), 0, 1);
    const blockedFlow = Boolean(link.flow_blocked === true || blockedRatio > 0.02);
    const moveGain = blockedFlow ? (0.14 + ((1 - blockedRatio) * 0.22)) : 1;
    p.t += p.speed * PACKET_MOTION_SPEED_SCALE * Math.max(0.001, dt) * (0.5 + link.activity) * burstBoost * moveGain * runtimeFlowScale;
    if (p.t > 1) p.t -= 1;
    if (blockedFlow) {
      const failEnd = 0.86;
      const failStart = Math.max(0.42, failEnd - (0.16 + (blockedRatio * 0.22)));
      const failSpan = Math.max(0.001, failEnd - failStart);
      p.fail_progress = clamp((p.t - failStart) / failSpan, 0, 1);
      p.fail_ratio = blockedRatio;
      p.fail_active = p.fail_progress > 0.001;
      if (p.t > failEnd) {
        p.t = 0.06 + ((p.id % 12) * 0.006);
        p.fail_progress = 0;
        p.fail_active = false;
      }
    } else {
      p.fail_progress = 0;
      p.fail_ratio = 0;
      p.fail_active = false;
    }
    const pt = bezierPoint(link, p.t);
    if (trailLength > 0) {
      p.trail.push(pt);
      if (p.trail.length > trailLength) p.trail.shift();
    } else {
      p.trail.length = 0;
    }
  }

  for (const p of state.particles) {
    const link = linkById[p.link_id];
    if (!link) continue;
    const introScale = clamp(Number(linkIntroScaleById[link.id]), 0, 1);
    if (introScale <= 0.03) continue;
    if (focusLinks && !focusLinks.has(link.id)) continue;
    if (!visibleLinkIds.has(String(link.id || ''))) continue;
    if (trailLength <= 0 || p.trail.length < 2) continue;
    const failProgress = clamp(Number(p.fail_progress || 0), 0, 1);
    const failActive = p.fail_active === true && failProgress > 0;
    const failRatio = clamp(Number(p.fail_ratio || 0), 0, 1);
    for (let i = 1; i < p.trail.length; i += 1) {
      const a = p.trail[i - 1];
      const b = p.trail[i];
      const alphaBase = (i / p.trail.length) * 0.3 * (0.2 + (introScale * 0.8)) * runtimeAlphaScale;
      if (failActive) {
        const flicker = 0.32 + (0.68 * Math.abs(Math.sin((nowTs * (0.028 + (failRatio * 0.016))) + (p.id * 1.97) + (i * 0.75))));
        const fade = clamp(1 - (failProgress * 0.9), 0.04, 1);
        const alpha = clamp(alphaBase * flicker * fade, 0.03, 0.9);
        const redMix = failProgress;
        const g = Math.round(255 - (redMix * 215));
        const bCh = Math.round(255 - (redMix * 215));
        ctx.strokeStyle = `rgba(255,${g},${bCh},${alpha})`;
      } else {
        ctx.strokeStyle = `rgba(255,255,255,${alphaBase})`;
      }
      ctx.lineWidth = Math.max(0.55, Number(p.radius || 1.2) * 0.42);
      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      ctx.stroke();
    }
  }

  for (const p of state.particles) {
    const link = linkById[p.link_id];
    if (!link) continue;
    const introScale = clamp(Number(linkIntroScaleById[link.id]), 0, 1);
    if (introScale <= 0.03) continue;
    if (focusLinks && !focusLinks.has(link.id)) continue;
    if (!visibleLinkIds.has(String(link.id || ''))) continue;
    const pt = bezierPoint(link, p.t);
    const failProgress = clamp(Number(p.fail_progress || 0), 0, 1);
    const failActive = p.fail_active === true && failProgress > 0;
    const failRatio = clamp(Number(p.fail_ratio || 0), 0, 1);
    let alpha = (0.45 + (link.activity * 0.5)) * (0.25 + (introScale * 0.75)) * runtimeAlphaScale;
    let radius = Math.max(PACKET_RADIUS_FLOOR, Number(p.radius || PACKET_RADIUS_FLOOR));
    if (failActive) {
      const flicker = 0.2 + (0.8 * Math.abs(Math.sin((nowTs * (0.035 + (failRatio * 0.02))) + (p.id * 2.17))));
      const fade = clamp(1 - (failProgress * 0.94), 0.02, 1);
      alpha = clamp(alpha * flicker * fade, 0.02, 0.96);
      radius = Math.max(PACKET_RADIUS_FLOOR * 0.75, radius * (1 - (failProgress * 0.38)));
      const redMix = failProgress;
      const g = Math.round(255 - (redMix * 225));
      const bCh = Math.round(255 - (redMix * 225));
      ctx.fillStyle = `rgba(255,${g},${bCh},${alpha})`;
    } else {
      if (runtime.stale) {
        ctx.fillStyle = `rgba(232,244,255,${alpha})`;
      } else {
        ctx.fillStyle = `rgba(255,255,255,${alpha})`;
      }
    }
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, radius, 0, Math.PI * 2);
    ctx.fill();
  }
}

function drawSelectionForeground(scene, ts) {
  if (!scene || !scene.node_by_id) return;
  const selected = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selected && selected.id || '').trim();
  if (!selectedId) return;
  const node = scene.node_by_id[selectedId];
  if (!node) return;
  const selectedTypeHint = String(selected && selected.type || '').toLowerCase();
  const selectedType = selectedTypeHint || String(node.type || '').toLowerCase();
  if (selectedType === 'module' && String(node.type || '').toLowerCase() === 'module') {
    drawModuleNode(node, ts);
  }
}

function drawHoverPathLabel(scene) {
  const hover = state.hover;
  if (!scene || !hover) return;
  const rawName = String(hover.name || '').trim();
  const rawPath = String(hover.path || '').trim();
  const rawLine2 = String(hover.line2 || '').trim();
  const primary = rawPath || rawName;
  if (!primary) return;
  const secondary = rawLine2 || ((rawPath && rawName && rawName !== rawPath) ? rawName : '');
  const line1 = primary.length > 120 ? `${primary.slice(0, 117)}...` : primary;
  const line2 = secondary.length > 90 ? `${secondary.slice(0, 87)}...` : secondary;
  const hasLine2 = Boolean(line2);

  const sx = Number(hover.sx || (state.width * 0.5));
  const sy = Number(hover.sy || (state.height * 0.5));
  const ctx = state.ctx;

  ctx.save();
  ctx.font = '12px ui-monospace, SFMono-Regular, Menlo, monospace';
  ctx.textBaseline = 'middle';
  const padX = 8;
  const boxH = hasLine2 ? 38 : 24;
  const boxW = Math.ceil(Math.max(
    ctx.measureText(line1).width,
    hasLine2 ? ctx.measureText(line2).width : 0
  )) + (padX * 2);
  const bx = clamp(sx - (boxW * 0.5), 8, Math.max(8, state.width - boxW - 8));
  const by = clamp(sy - boxH - 14, 8, Math.max(8, state.height - boxH - 8));

  ctx.fillStyle = 'rgba(6,16,28,0.9)';
  ctx.strokeStyle = 'rgba(118,201,255,0.58)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  if (typeof ctx.roundRect === 'function') {
    ctx.roundRect(bx, by, boxW, boxH, 6);
  } else {
    ctx.rect(bx, by, boxW, boxH);
  }
  ctx.fill();
  ctx.stroke();

  ctx.fillStyle = 'rgba(223,244,255,0.95)';
  ctx.fillText(line1, bx + padX, by + (hasLine2 ? 13 : (boxH * 0.53)));
  if (hasLine2) {
    ctx.fillStyle = 'rgba(184,228,255,0.8)';
    ctx.fillText(line2, bx + padX, by + 28);
  }
  ctx.restore();
}

function drawScene(ts) {
  const scene = state.scene;
  if (!scene) return;
  updateSceneMotion(scene, ts);
  drawBackground(ts);
  const ctx = state.ctx;
  const cam = state.camera;
  ctx.save();
  ctx.translate(cam.pan_x, cam.pan_y);
  ctx.scale(cam.zoom, cam.zoom);

  drawSpineHub(scene, ts);

  const layerNodes = scene.nodes.filter((n) => n.type === 'layer');
  for (const layerNode of layerNodes) drawLayerRing(layerNode, ts);
  drawDriftOverlay(scene, ts);

  for (const node of scene.nodes) {
    if (node.type === 'module') drawModuleNode(node, ts);
  }
  for (const node of scene.nodes) {
    if (node.type === 'submodule') drawSubmoduleNode(node);
  }
  for (const node of scene.io_nodes || []) drawIoNode(node, ts);
  drawLinks(scene);
  drawParticles(1 / Math.max(1, state.fps_smoothed));
  drawSelectionForeground(scene, ts);
  ctx.restore();
  drawHoverPathLabel(scene);
}

function describeLinkProcess(link) {
  const kind = String(link && link.kind || '').toLowerCase();
  if (kind === 'fractal') return 'Fractal submodule linkage';
  if (kind === 'hierarchy') return 'Hierarchy/containment handoff';
  if (kind === 'flow') return 'Operational packet flow';
  if (kind === 'route') return 'Route dispatch between modules';
  if (kind === 'ingress') return 'Ingress path from external input';
  if (kind === 'egress') return 'Egress path to external output';
  if (kind === 'doctor') return 'Autotest Doctor healing/rollback path';
  return 'Cross-module process path';
}

function topRejectedGateRows(summary, limit = 5) {
  const rows = [];
  const src = summary && typeof summary === 'object' && Array.isArray(summary.top_rejected_gates)
    ? summary.top_rejected_gates
    : [];
  for (const row of src) {
    if (rows.length >= Math.max(1, Number(limit || 5))) break;
    const gate = Array.isArray(row) ? String(row[0] || '').trim() : '';
    const count = Array.isArray(row) ? Number(row[1] || 0) : 0;
    if (!gate || !Number.isFinite(count) || count <= 0) continue;
    rows.push([`Bottleneck/${gate}`, fmtNum(count)]);
  }
  if (!rows.length) rows.push(['Bottleneck/none', '0']);
  return rows;
}

function blockedFlowStats(links) {
  const rows = Array.isArray(links) ? links : [];
  let blockedLinks = 0;
  let maxRatio = 0;
  for (const link of rows) {
    if (!link || typeof link !== 'object') continue;
    const ratio = clamp(Number(link.blocked_ratio || 0), 0, 1);
    const blocked = link.flow_blocked === true || ratio > 0.02;
    if (!blocked) continue;
    blockedLinks += 1;
    maxRatio = Math.max(maxRatio, ratio);
  }
  return {
    blocked_links: blockedLinks,
    max_blocked_ratio: maxRatio
  };
}

function renderIncidentBanner() {
  const banner = byId('incidentBanner');
  const badge = byId('incidentBadge');
  if (!banner || !badge) return;
  const incident = normalizeIntegrityIncident(state.payload);
  const active = incident.active === true;
  const severity = String(incident.severity || (active ? 'critical' : 'ok')).toLowerCase();
  banner.classList.remove('show', 'severity-critical', 'severity-warning', 'severity-recent', 'severity-ok');
  badge.classList.remove('show', 'severity-critical', 'severity-warning', 'severity-recent', 'severity-ok');
  if (!active) {
    banner.textContent = '';
    badge.textContent = 'Integrity: OK';
    badge.classList.add('show', 'severity-ok');
    return;
  }
  const violationText = `${fmtNum(incident.violation_total)} mismatch${incident.violation_total === 1 ? '' : 'es'}`;
  const title = severity === 'critical' ? 'SPINE INTEGRITY ALERT' : 'SPINE INTEGRITY WARNING';
  const topFile = Array.isArray(incident.top_files) && incident.top_files.length
    ? ` · ${incident.top_files[0]}`
    : '';
  banner.textContent = `${title} · ${violationText}${topFile}`;
  badge.textContent = `Integrity: ${String(severity || 'critical').toUpperCase()} (${violationText})`;
  const severityClass = severity === 'critical'
    ? 'severity-critical'
    : (severity === 'warning' ? 'severity-warning' : 'severity-recent');
  banner.classList.add('show', severityClass);
  badge.classList.add('show', severityClass);
}

function codeSelectionTarget(scene) {
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedSubfractalRef = state.selected_subfractal && typeof state.selected_subfractal === 'object'
    ? state.selected_subfractal
    : null;
  const selectedLinkRef = state.selected_link && typeof state.selected_link === 'object' ? state.selected_link : null;
  const selectedId = String(selectedRef && selectedRef.id || '').trim();
  const selectedType = String(selectedRef && selectedRef.type || '').toLowerCase();
  const selectedPath = String(selectedRef && selectedRef.path || '').trim();
  const selectedSubfractalId = String(selectedSubfractalRef && selectedSubfractalRef.id || '').trim();
  const selectedSubfractalPath = String(selectedSubfractalRef && selectedSubfractalRef.path || '').trim();
  const selectedLinkId = String(selectedLinkRef && selectedLinkRef.id || '').trim();
  const selectedLinkPath = String(selectedLinkRef && selectedLinkRef.path || '').trim();
  let rawPath = '';
  let key = '';

  if (selectedSubfractalId) {
    rawPath = selectedSubfractalPath;
    key = `subfractal:${selectedSubfractalId}`;
  } else if (selectedId) {
    rawPath = selectedPath;
    key = `node:${selectedType || 'unknown'}:${selectedId}`;
  } else if (selectedLinkId) {
    rawPath = selectedLinkPath;
    key = `link:${selectedLinkId}`;
  } else {
    rawPath = WORKSPACE_ROOT_PATH;
    key = 'root:system';
  }

  const normalizedPath = rawPath.includes(' -> ') ? '' : rawPath;
  const pathKey = normalizedPath || '__none__';
  return {
    selection_key: `${key}|${pathKey}`,
    path: normalizedPath
  };
}

function applyCodePreviewPayload(target, payload) {
  const view = state.code_preview;
  const src = payload && typeof payload === 'object' ? payload : {};
  const isFile = src.ok === true && src.is_file === true;
  const isDir = src.ok === true && src.is_dir === true;
  view.selection_key = String(target.selection_key || '');
  view.path = String(target.path || '');
  view.loading = false;
  view.error = src.ok === true ? '' : String(src.error || 'code_load_failed');
  view.is_file = isFile;
  view.is_dir = isDir;
  view.truncated = isFile && src.truncated === true;
  view.content = isFile ? String(src.content || '') : '';
}

function ensureCodePreview(scene) {
  if (state.preview_tab !== 'code') return;
  const view = state.code_preview;
  const target = codeSelectionTarget(scene);
  if (String(view.selection_key || '') === String(target.selection_key || '')) return;

  const cache = state.code_preview_cache && typeof state.code_preview_cache === 'object'
    ? state.code_preview_cache
    : (state.code_preview_cache = Object.create(null));
  view.selection_key = String(target.selection_key || '');
  view.path = String(target.path || '');
  view.is_file = false;
  view.is_dir = false;
  view.truncated = false;
  view.error = '';
  view.content = '';

  if (!target.path) {
    view.loading = false;
    return;
  }

  const cacheKey = String(target.path || '');
  if (Object.prototype.hasOwnProperty.call(cache, cacheKey)) {
    applyCodePreviewPayload(target, cache[cacheKey]);
    return;
  }

  view.loading = true;
  const requestId = Number(view.request_id || 0) + 1;
  view.request_id = requestId;
  fetchCodePreview(target.path)
    .then((payload) => {
      if (Number(state.code_preview.request_id || 0) !== requestId) return;
      cache[cacheKey] = payload;
      applyCodePreviewPayload(target, payload);
      renderStats();
    })
    .catch((err) => {
      if (Number(state.code_preview.request_id || 0) !== requestId) return;
      state.code_preview.loading = false;
      state.code_preview.error = String(err && err.message || err || 'code_load_failed');
      state.code_preview.is_file = false;
      state.code_preview.is_dir = false;
      state.code_preview.truncated = false;
      state.code_preview.content = '';
      renderStats();
    });
}

function setPreviewTab(tab) {
  const next = tab === 'code' ? 'code' : (tab === 'terminal' ? 'terminal' : 'preview');
  if (state.preview_tab === next) return;
  state.preview_tab = next;
  if (next === 'code') {
    state.code_preview.selection_key = '';
  } else if (next === 'terminal') {
    ensureTerminalState();
    if (state.scene) syncTerminalCwdFromSelection(state.scene, { silent: true });
  }
  renderStats();
}

function terminalStateRef() {
  const t = state.terminal && typeof state.terminal === 'object'
    ? state.terminal
    : null;
  if (t) return t;
  state.terminal = {
    cwd_abs: WORKSPACE_ROOT_PATH,
    cwd_rel: '.',
    loaded: false,
    loading: false,
    running: false,
    output: '',
    last_exit_code: 0,
    last_command: '',
    selection_sync_key: '',
    auto_follow: false,
    scroll_top: 0,
    scroll_listener_bound: false,
    suppress_scroll_event: false
  };
  return state.terminal;
}

function bindTerminalScrollTracking(outEl, term) {
  if (!outEl || !term || term.scroll_listener_bound === true) return;
  outEl.addEventListener('scroll', () => {
    if (term.suppress_scroll_event === true) return;
    const current = Number(outEl.scrollTop || 0);
    term.scroll_top = Number.isFinite(current) ? current : 0;
    if (term.auto_follow === true) {
      const distanceToBottom = Number(outEl.scrollHeight || 0) - (Number(outEl.clientHeight || 0) + Number(outEl.scrollTop || 0));
      if (Number.isFinite(distanceToBottom) && distanceToBottom > 24) {
        term.auto_follow = false;
        const followEl = byId('terminalAutoFollow');
        if (followEl) followEl.checked = false;
      }
    }
  });
  term.scroll_listener_bound = true;
}

function pushTerminalOutput(chunk, opts = {}) {
  const term = terminalStateRef();
  const text = String(chunk == null ? '' : chunk).replace(/\r\n/g, '\n');
  if (!text) return;
  const suffix = opts.no_newline ? '' : '\n';
  const next = `${String(term.output || '')}${text}${suffix}`;
  const maxChars = 32000;
  term.output = next.length > maxChars
    ? `... [terminal output clipped]\n${next.slice(-maxChars)}`
    : next;
}

function applyTerminalPayload(payload, opts = {}) {
  const src = payload && typeof payload === 'object' ? payload : {};
  const term = terminalStateRef();
  if (src.cwd_abs) term.cwd_abs = String(src.cwd_abs);
  if (src.cwd_rel) term.cwd_rel = String(src.cwd_rel);
  if (Number.isFinite(Number(src.last_exit_code))) term.last_exit_code = Number(src.last_exit_code);
  if (src.last_command != null) term.last_command = String(src.last_command || '');
  if (src.command != null) term.last_command = String(src.command || term.last_command || '');
  if (opts.logCommand && term.last_command) {
    pushTerminalOutput(`$ ${term.last_command}`);
  }
  const stdout = String(src.stdout || '');
  const stderr = String(src.stderr || '');
  if (stdout) pushTerminalOutput(stdout, { no_newline: stdout.endsWith('\n') });
  if (stderr) pushTerminalOutput(stderr, { no_newline: stderr.endsWith('\n') });
  if (src.timed_out === true) pushTerminalOutput('[terminal] command timed out');
  if (src.error) pushTerminalOutput(`[terminal] ${String(src.error)}`);
  term.loaded = true;
  term.loading = false;
  term.running = false;
}

function terminalPathFromSelection(scene) {
  const selected = state.selected && typeof state.selected === 'object' ? state.selected : null;
  if (!selected) return '';
  const rawPath = String(selected.path || '').trim();
  if (!rawPath) return '';
  if (rawPath.includes(' -> ')) return '';
  if (rawPath.startsWith('/')) return rawPath;
  const normalized = normalizeRelPathText(rawPath);
  if (!normalized || normalized === '.') return WORKSPACE_ROOT_PATH;
  return `${WORKSPACE_ROOT_PATH}/${normalized}`;
}

function bestNodeIdForRelPath(scene, relPath) {
  if (!scene || !scene.node_by_id) return '';
  const target = normalizeRelPathText(relPath).toLowerCase();
  if (!target || target === '.') return SYSTEM_ROOT_ID;
  let bestId = '';
  let bestScore = -Infinity;
  for (const node of scene.nodes || []) {
    if (!node || typeof node !== 'object') continue;
    const id = String(node.id || '').trim();
    const rel = normalizeRelPathText(node.rel || '').toLowerCase();
    if (!id || !rel) continue;
    let score = -Infinity;
    if (rel === target) {
      score = 2000 + rel.length;
    } else if (target.startsWith(`${rel}/`)) {
      score = 1000 + rel.length;
    } else if (rel.startsWith(`${target}/`)) {
      score = 600 + target.length;
    } else {
      continue;
    }
    const type = String(node.type || '').toLowerCase();
    if (type === 'module') score += 18;
    else if (type === 'layer') score += 12;
    else if (type === 'submodule') score += 8;
    if (score > bestScore) {
      bestScore = score;
      bestId = id;
    }
  }
  return bestId;
}

function focusSelectionForTerminalCwd() {
  const scene = state.scene;
  const term = terminalStateRef();
  if (!scene || !scene.node_by_id) return;
  const rel = normalizeRelPathText(term.cwd_rel || '');
  if (!rel) return;
  const bestId = bestNodeIdForRelPath(scene, rel);
  if (!bestId || !scene.node_by_id[bestId]) return;
  const same = state.selected && String(state.selected.id || '') === bestId;
  if (same) return;
  const node = scene.node_by_id[bestId];
  state.selected = {
    id: bestId,
    name: String(node.name || bestId),
    path: String(node.rel || ''),
    kind: String(node.type || 'node'),
    type: selectionTypeFrom(node, String(node.type || ''))
  };
  state.selected_link = null;
  applySelectionFocus(true);
  renderSelectionTag();
  renderStats();
}

function renderTerminalPane() {
  const term = terminalStateRef();
  const cwdEl = byId('terminalCwd');
  const statusEl = byId('terminalStatus');
  const outEl = byId('terminalOutput');
  const followEl = byId('terminalAutoFollow');
  if (cwdEl) cwdEl.textContent = String(term.cwd_rel || '.');
  if (statusEl) {
    if (term.running) statusEl.textContent = 'running';
    else if (term.loading) statusEl.textContent = 'loading';
    else statusEl.textContent = `exit ${fmtNum(term.last_exit_code || 0)}`;
  }
  if (followEl) followEl.checked = term.auto_follow === true;
  if (outEl) {
    bindTerminalScrollTracking(outEl, term);
    const prevTop = Number(outEl.scrollTop || 0);
    if (Number.isFinite(prevTop)) term.scroll_top = prevTop;
    outEl.textContent = String(term.output || '');
    term.suppress_scroll_event = true;
    if (term.auto_follow === true) {
      outEl.scrollTop = outEl.scrollHeight;
      term.scroll_top = Number(outEl.scrollTop || 0);
    } else {
      const target = Number.isFinite(Number(term.scroll_top)) ? Number(term.scroll_top) : 0;
      outEl.scrollTop = Math.max(0, target);
    }
    term.suppress_scroll_event = false;
  }
}

function setTerminalAutoFollow(enabled) {
  const term = terminalStateRef();
  term.auto_follow = enabled === true;
  const outEl = byId('terminalOutput');
  if (outEl && term.auto_follow === true) {
    outEl.scrollTop = outEl.scrollHeight;
    term.scroll_top = Number(outEl.scrollTop || 0);
  } else if (outEl) {
    term.scroll_top = Number(outEl.scrollTop || 0);
  }
  renderTerminalPane();
}

function ensureTerminalState() {
  const term = terminalStateRef();
  if (term.loading || term.loaded) return;
  term.loading = true;
  fetchTerminalState()
    .then((payload) => {
      applyTerminalPayload(payload);
      renderTerminalPane();
    })
    .catch((err) => {
      term.loading = false;
      term.running = false;
      term.loaded = true;
      pushTerminalOutput(`[terminal] ${String(err && err.message || err || 'state_load_failed')}`);
      renderTerminalPane();
    });
}

function syncTerminalCwdFromSelection(scene, opts = {}) {
  const term = terminalStateRef();
  const pathText = terminalPathFromSelection(scene);
  if (!pathText) return;
  const selectedId = String(state.selected && state.selected.id || '').trim();
  const syncKey = `${selectedId}|${pathText}`;
  if (syncKey === term.selection_sync_key) return;
  term.selection_sync_key = syncKey;
  fetchTerminalSetCwd(pathText)
    .then((payload) => {
      applyTerminalPayload(payload);
      if (!opts.silent) {
        pushTerminalOutput(`[terminal] cwd -> ${String(payload.cwd_rel || '.')}`);
      }
      renderTerminalPane();
    })
    .catch((err) => {
      if (!opts.silent) {
        pushTerminalOutput(`[terminal] ${String(err && err.message || err || 'cwd_sync_failed')}`);
        renderTerminalPane();
      }
    });
}

function runTerminalCommand() {
  const inputEl = byId('terminalInput');
  const term = terminalStateRef();
  if (!inputEl || term.running) return;
  const command = String(inputEl.value || '').trim();
  if (!command) return;
  inputEl.value = '';
  term.running = true;
  pushTerminalOutput(`$ ${command}`);
  renderTerminalPane();
  fetchTerminalExec(command)
    .then((payload) => {
      applyTerminalPayload(payload);
      renderTerminalPane();
      focusSelectionForTerminalCwd();
    })
    .catch((err) => {
      term.running = false;
      pushTerminalOutput(`[terminal] ${String(err && err.message || err || 'exec_failed')}`);
      renderTerminalPane();
    });
}

function clearTerminalOutput() {
  const term = terminalStateRef();
  term.output = '';
  renderTerminalPane();
}

function workflowBirthSnapshot(scene, summary) {
  const fromScene = scene && scene.workflow_birth && typeof scene.workflow_birth === 'object'
    ? scene.workflow_birth
    : null;
  const fromSummary = summary && summary.workflow_birth && typeof summary.workflow_birth === 'object'
    ? summary.workflow_birth
    : null;
  const src = fromScene || fromSummary || {};
  return {
    available: src.available === true,
    events_total: Number(src.events_total || 0),
    candidates_total: Number(src.candidates_total || 0),
    runs_total: Number(src.runs_total || 0),
    latest_run_id: String(src.latest_run_id || ''),
    stage_counts: src.stage_counts && typeof src.stage_counts === 'object' ? src.stage_counts : {},
    lineage_nodes: Array.isArray(src.lineage_nodes) ? src.lineage_nodes : [],
    lineage_edges: Array.isArray(src.lineage_edges) ? src.lineage_edges : []
  };
}

function workflowBirthNodeById(snapshot) {
  const out = Object.create(null);
  for (const row of snapshot.lineage_nodes || []) {
    const id = String(row && row.candidate_id || '').trim();
    if (!id) continue;
    out[id] = row;
  }
  return out;
}

function workflowBirthDefaultSelection(snapshot) {
  const rows = Array.isArray(snapshot && snapshot.lineage_nodes) ? snapshot.lineage_nodes : [];
  if (!rows.length) return '';
  const sorted = rows.slice().sort((a, b) => {
    const sa = Number(a && a.scorecard && a.scorecard.composite_score || -999);
    const sb = Number(b && b.scorecard && b.scorecard.composite_score || -999);
    if (Math.abs(sa - sb) > 0.0001) return sb - sa;
    const da = Number(a && a.fractal_depth || 0);
    const db = Number(b && b.fractal_depth || 0);
    if (Math.abs(da - db) > 0.0001) return da - db;
    return String(a && a.candidate_id || '').localeCompare(String(b && b.candidate_id || ''));
  });
  return String(sorted[0] && sorted[0].candidate_id || '');
}

function workflowBirthLineagePath(nodeById, node) {
  if (!node || typeof node !== 'object') return [];
  if (Array.isArray(node.lineage_path) && node.lineage_path.length) {
    return node.lineage_path.map((row) => String(row || '').trim()).filter(Boolean).slice(0, 12);
  }
  const out = [];
  const seen = new Set();
  let cur = String(node.candidate_id || '').trim();
  while (cur && !seen.has(cur) && out.length < 12) {
    seen.add(cur);
    out.push(cur);
    const parent = nodeById[cur] ? String(nodeById[cur].parent_candidate_id || '').trim() : '';
    if (!parent) break;
    cur = parent;
  }
  return out.reverse();
}

function shouldShowWorkflowBirthPanel(selectedType) {
  const t = String(selectedType || '').toLowerCase();
  if (!t) return true;
  if (t === 'link') return false;
  return true;
}

function doctorHealthSnapshot(scene, summary) {
  const fromScene = scene && scene.doctor && typeof scene.doctor === 'object'
    ? scene.doctor
    : null;
  const fromSummary = summary && summary.doctor && typeof summary.doctor === 'object'
    ? summary.doctor
    : null;
  const src = fromScene || fromSummary || {};
  return {
    available: src.available === true,
    events_total: Number(src.events_total || 0),
    wounded_active: Number(src.wounded_active || 0),
    healing_active: Number(src.healing_active || 0),
    regrowth_recent: Number(src.regrowth_recent || 0),
    modules_total: Number(src.modules_total || 0),
    modules: Array.isArray(src.modules) ? src.modules : [],
    events_recent: Array.isArray(src.events_recent) ? src.events_recent : []
  };
}

function doctorStateIsError(stateText) {
  const stateName = String(stateText || '').trim().toLowerCase();
  return stateName === 'wounded' || stateName === 'rollback_cut';
}

function doctorStateHintBoost(stateText) {
  const stateName = String(stateText || '').trim().toLowerCase();
  if (stateName === 'rollback_cut') return 0.74;
  if (stateName === 'wounded') return 0.66;
  if (stateName === 'healing') return 0.26;
  if (stateName === 'regrowth') return 0.12;
  return 0;
}

function doctorModuleMatchForRel(moduleRows, relPath) {
  const rel = normalizeRelPathText(relPath);
  if (!rel) return null;
  const rows = Array.isArray(moduleRows) ? moduleRows : [];
  let best = null;
  let bestScore = -1;
  for (const row of rows) {
    const moduleRel = normalizeRelPathText(row && row.module);
    if (!moduleRel) continue;
    if (!relPathsOverlap(rel, moduleRel)) continue;
    const score = moduleRel.length;
    if (score > bestScore) {
      best = row;
      bestScore = score;
    }
  }
  return best;
}

function buildWorkflowBirthPanelHtml(snapshot) {
  if (!snapshot || snapshot.available !== true) return '';
  const rows = Array.isArray(snapshot.lineage_nodes) ? snapshot.lineage_nodes : [];
  if (!rows.length) return '';
  const nodeById = workflowBirthNodeById(snapshot);
  const selectedIdRaw = String(state.workflow_birth_selection_id || '').trim();
  const selectedId = selectedIdRaw && nodeById[selectedIdRaw]
    ? selectedIdRaw
    : workflowBirthDefaultSelection(snapshot);
  state.workflow_birth_selection_id = selectedId || '';
  const selected = selectedId ? nodeById[selectedId] : null;
  const selectedScore = selected && selected.scorecard && typeof selected.scorecard === 'object'
    ? selected.scorecard
    : {};
  const lineage = workflowBirthLineagePath(nodeById, selected);
  const stageCounts = snapshot.stage_counts && typeof snapshot.stage_counts === 'object'
    ? snapshot.stage_counts
    : {};
  const topStages = Object.entries(stageCounts)
    .map(([stage, count]) => [String(stage || ''), Number(count || 0)])
    .filter(([stage, count]) => stage && Number.isFinite(count) && count > 0)
    .sort((a, b) => {
      if (Math.abs(Number(a[1] || 0) - Number(b[1] || 0)) > 0.0001) return Number(b[1] || 0) - Number(a[1] || 0);
      return String(a[0] || '').localeCompare(String(b[0] || ''));
    })
    .slice(0, 3)
    .map(([stage, count]) => `${stage}:${fmtNum(count)}`);
  const buttonRows = rows.slice(0, 16).map((row) => {
    const cid = String(row && row.candidate_id || '').trim();
    if (!cid) return '';
    const shortId = cid.length > 14 ? `${cid.slice(0, 14)}…` : cid;
    const stage = String(row && row.last_stage || '').trim();
    const cls = cid === selectedId ? 'birthCandidateBtn active' : 'birthCandidateBtn';
    const label = stage ? `${shortId} · ${stage}` : shortId;
    return `<button class="${cls}" type="button" data-workflow-candidate-id="${escapeHtml(cid)}">${escapeHtml(label)}</button>`;
  }).filter(Boolean).join('');
  const lineageLabel = lineage.length ? lineage.map((row) => escapeHtml(row)).join(' <span class="birthArrow">→</span> ') : 'n/a';
  const selectedType = String(selected && selected.proposal_type || 'n/a');
  const selectedStage = String(selected && selected.last_stage || 'n/a');
  const selectedMutation = String(selected && selected.mutation_kind || 'none');
  const driftDelta = selectedScore && Number.isFinite(Number(selectedScore.predicted_drift_delta))
    ? `${fmtNum(Number(selectedScore.predicted_drift_delta || 0) * 100)}%`
    : 'n/a';
  const yieldDelta = selectedScore && Number.isFinite(Number(selectedScore.predicted_yield_delta))
    ? `${fmtNum(Number(selectedScore.predicted_yield_delta || 0) * 100)}%`
    : 'n/a';
  const scorecard = selectedScore && Number.isFinite(Number(selectedScore.composite_score))
    ? fmtNum(Number(selectedScore.composite_score || 0))
    : 'n/a';
  const tritAlign = selectedScore && Number.isFinite(Number(selectedScore.trit_alignment))
    ? fmtNum(Number(selectedScore.trit_alignment || 0))
    : 'n/a';
  const pass = typeof selectedScore.adversarial_pass === 'boolean'
    ? (selectedScore.adversarial_pass ? 'pass' : 'fail')
    : 'n/a';
  const topStagesLabel = topStages.length ? topStages.join(' | ') : 'n/a';

  return [
    '<div class="workflowBirthPanel">',
    '  <div class="birthHeader">Workflow Birth Lineage</div>',
    `  <div class="birthMeta">events ${fmtNum(snapshot.events_total)} | candidates ${fmtNum(snapshot.candidates_total)} | run ${escapeHtml(snapshot.latest_run_id || 'n/a')}</div>`,
    `  <div class="birthMeta">stages ${escapeHtml(topStagesLabel)}</div>`,
    `  <div class="birthLineage">${lineageLabel}</div>`,
    '  <div class="birthButtons">',
    buttonRows || '<span class="birthEmpty">No lineage nodes in window.</span>',
    '  </div>',
    '  <div class="birthScorecard">',
    `    <div class="birthScoreRow"><span>Candidate</span><span>${escapeHtml(selectedId || 'n/a')}</span></div>`,
    `    <div class="birthScoreRow"><span>Proposal Type</span><span>${escapeHtml(selectedType)}</span></div>`,
    `    <div class="birthScoreRow"><span>Stage</span><span>${escapeHtml(selectedStage)}</span></div>`,
    `    <div class="birthScoreRow"><span>Mutation</span><span>${escapeHtml(selectedMutation)}</span></div>`,
    `    <div class="birthScoreRow"><span>Composite</span><span>${escapeHtml(scorecard)}</span></div>`,
    `    <div class="birthScoreRow"><span>Trit Align</span><span>${escapeHtml(tritAlign)}</span></div>`,
    `    <div class="birthScoreRow"><span>Drift Delta</span><span>${escapeHtml(driftDelta)}</span></div>`,
    `    <div class="birthScoreRow"><span>Yield Delta</span><span>${escapeHtml(yieldDelta)}</span></div>`,
    `    <div class="birthScoreRow"><span>Adversarial</span><span>${escapeHtml(pass)}</span></div>`,
    '  </div>',
    '</div>'
  ].join('');
}

function onStatsGridClick(ev) {
  const target = ev && ev.target && typeof ev.target.closest === 'function'
    ? ev.target.closest('[data-workflow-candidate-id]')
    : null;
  if (!target) return;
  const candidateId = String(target.getAttribute('data-workflow-candidate-id') || '').trim();
  if (!candidateId) return;
  state.workflow_birth_selection_id = candidateId;
  renderStats();
}

function renderStats() {
  const payload = state.payload || {};
  const integrityIncident = normalizeIntegrityIncident(payload);
  const summary = payload.summary && typeof payload.summary === 'object' ? payload.summary : {};
  const holoMetrics = payload.holo && payload.holo.metrics && typeof payload.holo.metrics === 'object'
    ? payload.holo.metrics
    : {};
  const constitution = summary.constitution && typeof summary.constitution === 'object'
    ? summary.constitution
    : {};
  const evolution = summary.evolution && typeof summary.evolution === 'object'
    ? summary.evolution
    : {};
  const fractal = summary.fractal && typeof summary.fractal === 'object'
    ? summary.fractal
    : {};
  const continuum = summary.continuum && typeof summary.continuum === 'object'
    ? summary.continuum
    : {};
  const runtime = runtimeSnapshotFromPayload(payload);
  state.runtime = runtime;
  const runtimeStatusLabel = runtime.online
    ? 'ONLINE'
    : (runtime.stale ? 'STALE' : 'OFFLINE');
  const runtimeDetail = runtime.signal_age_sec == null
    ? `${runtimeStatusLabel} (no signal)`
    : `${runtimeStatusLabel} (${fmtNum(runtime.signal_age_sec)}s age)`;
  const runtimeWindowLabel = `${fmtNum(runtime.live_window_minutes)}m`;
  const workflowBirth = workflowBirthSnapshot(state.scene, summary);
  const doctor = doctorHealthSnapshot(state.scene, summary);
  const workflowBirthStageCounts = workflowBirth.stage_counts && typeof workflowBirth.stage_counts === 'object'
    ? workflowBirth.stage_counts
    : {};
  const workflowBirthTopStage = Object.entries(workflowBirthStageCounts)
    .map(([stage, count]) => [String(stage || ''), Number(count || 0)])
    .filter(([stage, count]) => stage && Number.isFinite(count) && count > 0)
    .sort((a, b) => {
      if (Math.abs(Number(a[1] || 0) - Number(b[1] || 0)) > 0.0001) return Number(b[1] || 0) - Number(a[1] || 0);
      return String(a[0] || '').localeCompare(String(b[0] || ''));
    });
  const workflowBirthTopStageLabel = workflowBirthTopStage.length
    ? `${workflowBirthTopStage[0][0]}:${fmtNum(workflowBirthTopStage[0][1])}`
    : 'n/a';
  const doctorHealthLabel = Number(doctor.wounded_active || 0) > 0
    ? `Wounded ${fmtNum(doctor.wounded_active)}`
    : (Number(doctor.healing_active || 0) > 0 ? `Healing ${fmtNum(doctor.healing_active)}` : 'Nominal');
  const continuumAvailable = continuum.available === true;
  const continuumPulseAgeSec = Number(continuum.pulse_age_sec || 0);
  const continuumPulseStatus = !continuumAvailable
    ? 'Unavailable'
    : (continuum.last_skipped === true
      ? 'Skipped'
      : (continuumPulseAgeSec > 3600
        ? `Stale (${fmtNum(continuumPulseAgeSec)}s)`
        : 'Active'));
  const continuumTritLabelRaw = String(continuum.last_trit_label || '');
  const continuumTritLabel = continuumTritLabelRaw || 'unknown';
  const continuumTritValue = Number(continuum.last_trit || 0);
  const continuumEventsByStage = continuum.events_24h_by_stage && typeof continuum.events_24h_by_stage === 'object'
    ? continuum.events_24h_by_stage
    : {};
  const continuumStageRows = Object.entries(continuumEventsByStage)
    .map(([stage, count]) => [String(stage || ''), Number(count || 0)])
    .filter(([stage, count]) => stage && Number.isFinite(count) && count > 0)
    .sort((a, b) => {
      if (Math.abs(Number(a[1] || 0) - Number(b[1] || 0)) > 0.0001) return Number(b[1] || 0) - Number(a[1] || 0);
      return String(a[0] || '').localeCompare(String(b[0] || ''));
    });
  const continuumTopStage = continuumStageRows.length
    ? `${String(continuumStageRows[0][0])}:${fmtNum(continuumStageRows[0][1])}`
    : 'n/a';
  const continuumSkipReasons = Array.isArray(continuum.last_skip_reasons)
    ? continuum.last_skip_reasons.filter(Boolean).slice(0, 3).join(', ')
    : '';
  const scene = state.scene;
  const nodeCount = scene && Array.isArray(scene.nodes) ? scene.nodes.length : 0;
  const linkCount = scene && Array.isArray(scene.links) ? scene.links.length : 0;
  const errorSignal = scene ? Number(scene.error_signal || 0) : 0;
  const gpuLabel = state.quality_profile.label;
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selectedRef && selectedRef.id || '');
  const selectedNode = selectedId && scene && scene.node_by_id
    ? scene.node_by_id[selectedId]
    : null;
  const selectedTypeHint = String(selectedRef && selectedRef.type || '').toLowerCase();
  const selectedType = selectedTypeHint || String(selectedNode && selectedNode.type || '').toLowerCase();
  const selectedLinkId = String(state.selected_link && state.selected_link.id || '');
  const selectedLink = selectedLinkId && scene && scene.link_by_id
    ? scene.link_by_id[selectedLinkId]
    : null;
  const selectedSubfractal = scene ? validateSelectedSubfractal(scene) : null;
  const titleEl = byId('statsTitle');
  const statsGridEl = byId('statsGrid');
  const codePaneEl = byId('codePane');
  const terminalPaneEl = byId('terminalPane');
  const previewTabBtn = byId('tabPreview');
  const codeTabBtn = byId('tabCode');
  const terminalTabBtn = byId('tabTerminal');
  const showingCode = state.preview_tab === 'code';
  const showingTerminal = state.preview_tab === 'terminal';
  if (previewTabBtn) previewTabBtn.classList.toggle('active', !showingCode && !showingTerminal);
  if (codeTabBtn) codeTabBtn.classList.toggle('active', showingCode);
  if (terminalTabBtn) terminalTabBtn.classList.toggle('active', showingTerminal);
  let rows = [];
  const cg = codegraphState();
  const cgResult = cg.last_result && typeof cg.last_result === 'object'
    ? cg.last_result
    : null;
  const cgMatchedNodes = cg.matched_node_ids instanceof Set ? cg.matched_node_ids.size : 0;
  const cgMatchedLinks = cg.matched_link_ids instanceof Set ? cg.matched_link_ids.size : 0;
  const cgTopNodePaths = Array.isArray(cg.matched_node_paths) ? cg.matched_node_paths.slice(0, 2) : [];
  const cgTopLinkPaths = Array.isArray(cg.matched_link_paths) ? cg.matched_link_paths.slice(0, 2) : [];
  const selectedIntegrityRows = integrityRowsForSelection(integrityIncident, selectedType, selectedNode);
  const selectedNodeErrorRows = nodeErrorRowsForSelection(selectedNode, selectedIntegrityRows, errorSignal);
  const sceneChange = scene && scene.change_summary && typeof scene.change_summary === 'object'
    ? scene.change_summary
    : {};
  const selectedChangeState = (selectedType === 'spine' || selectedType === 'system')
    ? normalizeChangeState({
        active_write: Number(sceneChange.active_write_files_total || 0) > 0,
        dirty: Number(sceneChange.dirty_files_total || 0) > 0,
        staged: Number(sceneChange.staged_files_total || 0) > 0,
        pending_push: sceneChange.pending_push === true || Number(sceneChange.ahead_count || 0) > 0,
        just_pushed: sceneChange.just_pushed === true,
        changed: Number(sceneChange.active_write_files_total || 0) > 0
          || Number(sceneChange.dirty_files_total || 0) > 0
          || Number(sceneChange.staged_files_total || 0) > 0
          || sceneChange.pending_push === true
          || sceneChange.just_pushed === true,
        file_count: Number(sceneChange.top_files && sceneChange.top_files.length || 0),
        top_files: Array.isArray(sceneChange.top_files) ? sceneChange.top_files : [],
        last_push_ts: String(sceneChange.last_push_ts || '')
      })
    : normalizeChangeState(selectedNode && selectedNode.change_state);
  const selectedChangeRows = changeRowsForSelection(selectedChangeState, selectedType, selectedNode);

  if (selectedLink && scene && scene.node_by_id) {
    const fromNode = scene.node_by_id[String(selectedLink.from_id || '')];
    const toNode = scene.node_by_id[String(selectedLink.to_id || '')];
    const fromName = String((fromNode && fromNode.name) || selectedLink.from_id || 'unknown');
    const toName = String((toNode && toNode.name) || selectedLink.to_id || 'unknown');
    const fromPath = String((fromNode && fromNode.rel) || selectedLink.from_id || '');
    const toPath = String((toNode && toNode.rel) || selectedLink.to_id || '');
    const errWeight = clamp(Number(selectedLink.error_weight || 0), 0, 1);
    const blockedRatio = clamp(Number(selectedLink.blocked_ratio || 0), 0, 1);
    const blocked = Boolean(selectedLink.flow_blocked === true || blockedRatio > 0.02);
    const packetSizeTokens = Math.max(0, Number(selectedLink.packet_size_tokens || 0));
    const packetSizeLabel = packetSizeTokens > 0
      ? `${fmtNum(packetSizeTokens)} tokens avg`
      : 'n/a';
    const linkErrorRows = linkErrorRowsForSelection(selectedLink);
    const health = errWeight >= 0.35 ? 'Degraded'
      : errWeight >= 0.12 ? 'Watch'
      : 'Nominal';
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'Link Process'],
      ['From Module', fromName],
      ['To Module', toName],
      ['From Path', fromPath],
      ['To Path', toPath],
      ['Channel Kind', String(selectedLink.kind || 'flow')],
      ['Process', describeLinkProcess(selectedLink)],
      ['Health', health],
      ['Flow Status', blocked ? `Blocked (${fmtNum(blockedRatio * 100)}%)` : 'Clear'],
      ['Block Reason', blocked ? (String(selectedLink.block_reason || 'error gate / policy hold')) : 'n/a'],
      ...linkErrorRows,
      ['Activity', `${fmtNum(Number(selectedLink.activity || 0) * 100)}%`],
      ['Error Weight', `${fmtNum(errWeight * 100)}%`],
      ['Packet Count', fmtNum(selectedLink.count || 0)],
      ['Packet Size', packetSizeLabel]
    ];
  } else if (selectedType === 'spine' && scene) {
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'Core Integrity Overview'],
      ['Core', String((selectedNode && selectedNode.name) || 'Spine Core')],
      ['Core Path', String((selectedNode && selectedNode.rel) || SPINE_NODE_PATH)],
      ['Runtime', runtimeDetail],
      ['Live Window', runtimeWindowLabel],
      ['Integrity', integrityIncident.active
        ? `${String(integrityIncident.severity || 'critical').toUpperCase()} (${fmtNum(integrityIncident.violation_total)} mismatches)`
        : 'OK'],
      ['Constitution Alignment', `${String(constitution.alignment_band || 'gray').toUpperCase()} (${fmtNum(Number(constitution.alignment_score || 0) * 100)}%)`],
      ['T1/T2 Directives', `${fmtNum(constitution.tier1_total || 0)} / ${fmtNum(constitution.tier2_total || 0)}`],
      ['Evolution Commits 30d', fmtNum(evolution.commits_30d || 0)],
      ['Fractal Harmony', `${fmtNum(Number(fractal.harmony_score || 0) * 100)}%`],
      ['Symbiosis Plans', fmtNum(fractal.symbiosis_plans || 0)],
      ['Predator Candidates', fmtNum(fractal.predator_candidates || 0)],
      ['Black-Box Rows', fmtNum(fractal.black_box_rows || 0)],
      ['Continuum Pulse', continuumPulseStatus],
      ['Continuum Trit', `${continuumTritLabel} (${fmtNum(continuumTritValue)})`],
      ['Continuum Events 24h', fmtNum(continuum.events_24h_total || 0)],
      ['Continuum Top Stage', continuumTopStage],
      ['Continuum Queue Rows', fmtNum(continuum.training_queue_rows_24h || 0)],
      ['Continuum Anticipation', fmtNum(continuum.anticipation_candidates_last || 0)],
      ['Continuum Red-Team Critical', fmtNum(continuum.red_team_critical_last || 0)],
      ['Continuum Observer Mood', String(continuum.observer_mood_last || 'n/a')],
      ['Doctor Health', doctorHealthLabel],
      ['Doctor Events', fmtNum(doctor.events_total || 0)],
      ['Doctor Wounded Active', fmtNum(doctor.wounded_active || 0)],
      ['Doctor Healing Active', fmtNum(doctor.healing_active || 0)],
      ['Doctor Regrowth Recent', fmtNum(doctor.regrowth_recent || 0)],
      ['Workflow Birth Events', fmtNum(workflowBirth.events_total || 0)],
      ['Workflow Birth Candidates', fmtNum(workflowBirth.candidates_total || 0)],
      ['Workflow Birth Latest Run', String(workflowBirth.latest_run_id || 'n/a')],
      ['Workflow Birth Top Stage', workflowBirthTopStageLabel],
      ['Continuum Skip Reasons', continuumSkipReasons || 'n/a'],
      ...selectedChangeRows,
      ...selectedNodeErrorRows,
      ...selectedIntegrityRows
    ];
  } else if (selectedType === 'system' && scene && Array.isArray(scene.links)) {
    const processCounts = Object.create(null);
    for (const link of scene.links) {
      const kind = String(link && link.kind || 'flow');
      processCounts[kind] = Number(processCounts[kind] || 0) + Number(link && link.count || 0);
    }
    const processRows = Object.keys(processCounts)
      .sort((a, b) => {
        const av = Number(processCounts[a] || 0);
        const bv = Number(processCounts[b] || 0);
        if (Math.abs(av - bv) > 0.0001) return bv - av;
        return a.localeCompare(b);
      })
      .map((kind) => [`Process/${kind}`, fmtNum(processCounts[kind])]);
    const blockStats = blockedFlowStats(scene.links);
    const bottleneckRows = topRejectedGateRows(summary, 5);
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'System Process Overview'],
      ['Scope', 'All shells + children'],
      ['Runtime', runtimeDetail],
      ['Live Window', runtimeWindowLabel],
      ['Total Links', fmtNum(scene.links.length)],
      ['Blocked Links', fmtNum(blockStats.blocked_links)],
      ['Max Block Ratio', `${fmtNum(blockStats.max_blocked_ratio * 100)}%`],
      ['Constitution Alignment', `${String(constitution.alignment_band || 'gray').toUpperCase()} (${fmtNum(Number(constitution.alignment_score || 0) * 100)}%)`],
      ['T1/T2 Directives', `${fmtNum(constitution.tier1_total || 0)} / ${fmtNum(constitution.tier2_total || 0)}`],
      ['Evolution Trajectory', String(evolution.trajectory || 'flat')],
      ['Evolution Commits 30d', fmtNum(evolution.commits_30d || 0)],
      ['Evolution Stability', `${fmtNum(Number(evolution.stability_score || 0) * 100)}%`],
      ['Fractal Harmony', `${fmtNum(Number(fractal.harmony_score || 0) * 100)}%`],
      ['Symbiosis Plans', fmtNum(fractal.symbiosis_plans || 0)],
      ['Predator Candidates', fmtNum(fractal.predator_candidates || 0)],
      ['Restructure Candidates', fmtNum(fractal.restructure_candidates || 0)],
      ['Epigenetic Tags', fmtNum(fractal.epigenetic_tags || 0)],
      ['Archetypes', fmtNum(fractal.archetypes || 0)],
      ['Pheromones', fmtNum(fractal.pheromones || 0)],
      ['Black-Box Rows', fmtNum(fractal.black_box_rows || 0)],
      ['Continuum Pulse', continuumPulseStatus],
      ['Continuum Trit', `${continuumTritLabel} (${fmtNum(continuumTritValue)})`],
      ['Continuum Events 24h', fmtNum(continuum.events_24h_total || 0)],
      ['Continuum Top Stage', continuumTopStage],
      ['Continuum Queue Rows', fmtNum(continuum.training_queue_rows_24h || 0)],
      ['Continuum Anticipation', fmtNum(continuum.anticipation_candidates_last || 0)],
      ['Continuum Red-Team Critical', fmtNum(continuum.red_team_critical_last || 0)],
      ['Continuum Observer Mood', String(continuum.observer_mood_last || 'n/a')],
      ['Doctor Health', doctorHealthLabel],
      ['Doctor Events', fmtNum(doctor.events_total || 0)],
      ['Doctor Wounded Active', fmtNum(doctor.wounded_active || 0)],
      ['Doctor Healing Active', fmtNum(doctor.healing_active || 0)],
      ['Doctor Regrowth Recent', fmtNum(doctor.regrowth_recent || 0)],
      ['Workflow Birth Events', fmtNum(workflowBirth.events_total || 0)],
      ['Workflow Birth Candidates', fmtNum(workflowBirth.candidates_total || 0)],
      ['Workflow Birth Latest Run', String(workflowBirth.latest_run_id || 'n/a')],
      ['Workflow Birth Top Stage', workflowBirthTopStageLabel],
      ['Continuum Skip Reasons', continuumSkipReasons || 'n/a'],
      ...selectedChangeRows,
      ...selectedNodeErrorRows,
      ...selectedIntegrityRows,
      ...bottleneckRows,
      ...processRows
    ];
    if (!processRows.length) rows.push(['Process/none', '0']);
  } else if (selectedType === 'layer' && scene && Array.isArray(scene.nodes) && Array.isArray(scene.links)) {
    const layerId = String(selectedNode.id || '');
    const moduleIds = [];
    const submoduleIds = [];
    for (const node of scene.nodes) {
      if (!node) continue;
      if (String(node.type || '') === 'module' && String(node.parent_id || '') === layerId) {
        moduleIds.push(String(node.id || ''));
      }
    }
    const moduleSet = new Set(moduleIds);
    for (const node of scene.nodes) {
      if (!node) continue;
      if (String(node.type || '') === 'submodule' && moduleSet.has(String(node.parent_id || ''))) {
        submoduleIds.push(String(node.id || ''));
      }
    }
    const memberIds = new Set([layerId, ...moduleIds, ...submoduleIds]);
    const relevantLinks = scene.links.filter((link) => (
      memberIds.has(String(link.from_id || '')) || memberIds.has(String(link.to_id || ''))
    ));
    const processCounts = Object.create(null);
    for (const link of relevantLinks) {
      const kind = String(link && link.kind || 'flow');
      processCounts[kind] = Number(processCounts[kind] || 0) + Number(link && link.count || 0);
    }
    const processRows = Object.keys(processCounts)
      .sort((a, b) => {
        const av = Number(processCounts[a] || 0);
        const bv = Number(processCounts[b] || 0);
        if (Math.abs(av - bv) > 0.0001) return bv - av;
        return a.localeCompare(b);
      })
      .map((kind) => [`Process/${kind}`, fmtNum(processCounts[kind])]);
    const blockStats = blockedFlowStats(relevantLinks);

    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'Shell Process Overview'],
      ['Shell', String(selectedNode.name || layerId)],
      ['Shell Path', String(selectedNode.rel || layerId)],
      ['Modules', fmtNum(moduleIds.length)],
      ['Fractals', fmtNum(submoduleIds.length)],
      ['Total Child Links', fmtNum(relevantLinks.length)],
      ['Blocked Links', fmtNum(blockStats.blocked_links)],
      ['Max Block Ratio', `${fmtNum(blockStats.max_blocked_ratio * 100)}%`],
      ...selectedNodeErrorRows,
      ...selectedIntegrityRows,
      ...processRows
    ];
    if (!processRows.length) {
      rows.push(['Process/none', '0']);
    }
  } else if (selectedType === 'module' && scene && Array.isArray(scene.nodes) && Array.isArray(scene.links)) {
    const moduleId = String(selectedNode.id || '');
    const submoduleIds = [];
    for (const node of scene.nodes) {
      if (!node) continue;
      if (String(node.type || '') === 'submodule' && String(node.parent_id || '') === moduleId) {
        submoduleIds.push(String(node.id || ''));
      }
    }
    const memberIds = new Set([moduleId, ...submoduleIds]);
    const relevantLinks = scene.links.filter((link) => (
      memberIds.has(String(link.from_id || '')) || memberIds.has(String(link.to_id || ''))
    ));
    const processCounts = Object.create(null);
    for (const link of relevantLinks) {
      const kind = String(link && link.kind || 'flow');
      processCounts[kind] = Number(processCounts[kind] || 0) + Number(link && link.count || 0);
    }
    const processRows = Object.keys(processCounts)
      .sort((a, b) => {
        const av = Number(processCounts[a] || 0);
        const bv = Number(processCounts[b] || 0);
        if (Math.abs(av - bv) > 0.0001) return bv - av;
        return a.localeCompare(b);
      })
      .map((kind) => [`Process/${kind}`, fmtNum(processCounts[kind])]);
    const blockStats = blockedFlowStats(relevantLinks);
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'Module Process Overview'],
      ['Module', String(selectedNode.name || moduleId)],
      ['Module Path', String(selectedNode.rel || moduleId)],
      ['Fractals', fmtNum(submoduleIds.length)],
      ['Total Child Links', fmtNum(relevantLinks.length)],
      ['Blocked Links', fmtNum(blockStats.blocked_links)],
      ['Max Block Ratio', `${fmtNum(blockStats.max_blocked_ratio * 100)}%`],
      ...selectedChangeRows,
      ...selectedNodeErrorRows,
      ...selectedIntegrityRows,
      ...processRows
    ];
    if (!processRows.length) rows.push(['Process/none', '0']);
  } else if (selectedType === 'submodule' && scene && Array.isArray(scene.nodes) && Array.isArray(scene.links)) {
    const submoduleId = String(selectedNode.id || '');
    const subfractalActive = selectedSubfractal
      && String(selectedSubfractal.parent_id || '') === submoduleId
      ? selectedSubfractal
      : null;
    const subfractalCount = subfractalChildrenForNode(selectedNode).length;
    const moduleId = String(selectedNode.parent_id || '');
    const moduleNode = moduleId && scene.node_by_id ? scene.node_by_id[moduleId] : null;
    const layerId = String(moduleNode && moduleNode.parent_id || '');
    const layerNode = layerId && scene.node_by_id ? scene.node_by_id[layerId] : null;
    const relevantLinks = scene.links.filter((link) => (
      String(link.from_id || '') === submoduleId || String(link.to_id || '') === submoduleId
    ));
    const processCounts = Object.create(null);
    for (const link of relevantLinks) {
      const kind = String(link && link.kind || 'flow');
      processCounts[kind] = Number(processCounts[kind] || 0) + Number(link && link.count || 0);
    }
    const processRows = Object.keys(processCounts)
      .sort((a, b) => {
        const av = Number(processCounts[a] || 0);
        const bv = Number(processCounts[b] || 0);
        if (Math.abs(av - bv) > 0.0001) return bv - av;
        return a.localeCompare(b);
      })
      .map((kind) => [`Process/${kind}`, fmtNum(processCounts[kind])]);
    const blockStats = blockedFlowStats(relevantLinks);
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', subfractalActive ? 'Subfractal Process Overview' : 'Fractal Process Overview'],
      ['Fractal', String(selectedNode.name || submoduleId)],
      ['Fractal Path', String(selectedNode.rel || submoduleId)],
      ['Subfractals', fmtNum(subfractalCount)],
      ['Selected Subfractal', subfractalActive ? String(subfractalActive.name || subfractalActive.id || 'n/a') : 'none'],
      ['Selected Path', subfractalActive ? String(subfractalActive.path || 'n/a') : 'n/a'],
      ['Module', String((moduleNode && moduleNode.name) || moduleId || 'n/a')],
      ['Shell', String((layerNode && layerNode.name) || layerId || 'n/a')],
      ['Direct Links', fmtNum(relevantLinks.length)],
      ['Blocked Links', fmtNum(blockStats.blocked_links)],
      ['Max Block Ratio', `${fmtNum(blockStats.max_blocked_ratio * 100)}%`],
      ...selectedChangeRows,
      ...selectedNodeErrorRows,
      ...selectedIntegrityRows,
      ...processRows
    ];
    if (!processRows.length) rows.push(['Process/none', '0']);
  } else {
    const bottleneckRows = topRejectedGateRows(summary, 5);
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'System Overview'],
      ['GPU Tier', gpuLabel],
      ['Runtime', runtimeDetail],
      ['Live Window', runtimeWindowLabel],
      ['FPS', fmtNum(state.fps_smoothed)],
      ['Motion Smoothness', `${fmtNum(clamp(state.motion_smoothness_ema, 0, 1) * 100)}%`],
      ['Run Events', fmtNum(summary.run_events)],
      ['Executed', fmtNum(summary.executed)],
      ['Shipped', fmtNum(summary.shipped)],
      ['Policy Holds', fmtNum(summary.policy_holds)],
      ['Error Signal', `${fmtNum(errorSignal * 100)}%`],
      ['Yield', `${fmtNum(Number(holoMetrics.yield_rate || 0) * 100)}%`],
      ['Drift Proxy', `${fmtNum(Number(holoMetrics.drift_proxy || 0) * 100)}%`],
      ['Constitution Alignment', `${String(constitution.alignment_band || 'gray').toUpperCase()} (${fmtNum(Number(constitution.alignment_score || 0) * 100)}%)`],
      ['Constitution Sample', fmtNum(constitution.proposals_sampled || 0)],
      ['T1/T2 Directives', `${fmtNum(constitution.tier1_total || 0)} / ${fmtNum(constitution.tier2_total || 0)}`],
      ['Evolution Trajectory', String(evolution.trajectory || 'flat')],
      ['Evolution Commits 7/30/90', `${fmtNum(evolution.commits_7d || 0)} / ${fmtNum(evolution.commits_30d || 0)} / ${fmtNum(evolution.commits_90d || 0)}`],
      ['Evolution Velocity', `${fmtNum(evolution.commit_velocity_30d || 0)} commits/day`],
      ['Evolution Stability', `${fmtNum(Number(evolution.stability_score || 0) * 100)}%`],
      ['Fractal Harmony', `${fmtNum(Number(fractal.harmony_score || 0) * 100)}%`],
      ['Symbiosis Plans', fmtNum(fractal.symbiosis_plans || 0)],
      ['Predator Candidates', fmtNum(fractal.predator_candidates || 0)],
      ['Restructure Candidates', fmtNum(fractal.restructure_candidates || 0)],
      ['Epigenetic Tags', fmtNum(fractal.epigenetic_tags || 0)],
      ['Archetypes', fmtNum(fractal.archetypes || 0)],
      ['Pheromones', fmtNum(fractal.pheromones || 0)],
      ['Black-Box Rows', fmtNum(fractal.black_box_rows || 0)],
      ['Continuum Pulse', continuumPulseStatus],
      ['Continuum Trit', `${continuumTritLabel} (${fmtNum(continuumTritValue)})`],
      ['Continuum Events 24h', fmtNum(continuum.events_24h_total || 0)],
      ['Continuum Top Stage', continuumTopStage],
      ['Continuum Queue Rows', fmtNum(continuum.training_queue_rows_24h || 0)],
      ['Continuum Anticipation', fmtNum(continuum.anticipation_candidates_last || 0)],
      ['Continuum Red-Team Critical', fmtNum(continuum.red_team_critical_last || 0)],
      ['Continuum Observer Mood', String(continuum.observer_mood_last || 'n/a')],
      ['Doctor Health', doctorHealthLabel],
      ['Doctor Events', fmtNum(doctor.events_total || 0)],
      ['Doctor Wounded Active', fmtNum(doctor.wounded_active || 0)],
      ['Doctor Healing Active', fmtNum(doctor.healing_active || 0)],
      ['Doctor Regrowth Recent', fmtNum(doctor.regrowth_recent || 0)],
      ['Workflow Birth Events', fmtNum(workflowBirth.events_total || 0)],
      ['Workflow Birth Candidates', fmtNum(workflowBirth.candidates_total || 0)],
      ['Workflow Birth Latest Run', String(workflowBirth.latest_run_id || 'n/a')],
      ['Workflow Birth Top Stage', workflowBirthTopStageLabel],
      ['Continuum Skip Reasons', continuumSkipReasons || 'n/a'],
      ['Layer Nodes', fmtNum(nodeCount)],
      ['Links', fmtNum(linkCount)],
      ...bottleneckRows
    ];
  }
  const cgRows = [];
  if (cg.running) {
    cgRows.push(['CodeGraph', 'Query running...']);
  } else if (cg.error) {
    cgRows.push(['CodeGraph Error', String(cg.error || 'query_failed')]);
  } else if (cg.query) {
    const modeLabel = String(cg.mode || 'search');
    const explanation = String(cgResult && cgResult.matches && cgResult.matches.explanation || '').trim();
    cgRows.push(['CodeGraph Query', `${String(cg.query)} (${modeLabel})`]);
    cgRows.push(['CodeGraph Hits', `nodes ${fmtNum(cgMatchedNodes)} | links ${fmtNum(cgMatchedLinks)}`]);
    if (cgTopNodePaths.length) cgRows.push(['CodeGraph Paths', cgTopNodePaths.join(' | ')]);
    if (cgTopLinkPaths.length) cgRows.push(['CodeGraph Flows', cgTopLinkPaths.join(' | ')]);
    if (explanation) cgRows.push(['CodeGraph Mode', explanation]);
  }
  if (cg.notice && !cg.running && !cg.error) {
    cgRows.push(['CodeGraph Status', String(cg.notice)]);
  }
  if (cgRows.length) rows = [...cgRows, ...rows];

  const integrityLabel = integrityIncident.active
    ? `${String(integrityIncident.severity || 'critical').toUpperCase()} (${fmtNum(integrityIncident.violation_total)} mismatches)`
    : 'OK';
  rows.push(['Integrity', integrityLabel]);
  if (integrityIncident.active) {
    const topFiles = Array.isArray(integrityIncident.top_files)
      ? integrityIncident.top_files.slice(0, 2)
      : [];
    if (topFiles.length) rows.push(['Integrity Files', topFiles.join(' | ')]);
    if (integrityIncident.last_violation_ts) {
      rows.push(['Last Violation', new Date(integrityIncident.last_violation_ts).toLocaleString()]);
    }
  } else if (integrityIncident.last_reseal_ts) {
    rows.push(['Last Reseal', new Date(integrityIncident.last_reseal_ts).toLocaleString()]);
  }
  const rowsHtml = rows.map(([k, v]) => {
    const errorRow = isPreviewErrorRow(k, v);
    const warningRow = !errorRow && isPreviewWarningRow(k, v);
    const goodRow = !errorRow && !warningRow && isPreviewGoodRow(k, v);
    const itemClass = errorRow
      ? 'item item-error'
      : (warningRow ? 'item item-warning' : (goodRow ? 'item item-good' : 'item'));
    const valueClass = errorRow
      ? 'v v-error'
      : (warningRow ? 'v v-warning' : (goodRow ? 'v v-good' : 'v'));
    return `<div class="${itemClass}"><div class="k">${escapeHtml(String(k))}</div><div class="${valueClass}">${escapeHtml(String(v))}</div></div>`;
  }).join('');
  const workflowBirthPanelHtml = (!showingCode && !showingTerminal && shouldShowWorkflowBirthPanel(selectedType))
    ? buildWorkflowBirthPanelHtml(workflowBirth)
    : '';
  if (showingCode) {
    ensureCodePreview(scene);
    if (statsGridEl) statsGridEl.style.display = 'none';
    if (terminalPaneEl) terminalPaneEl.style.display = 'none';
    if (codePaneEl) {
      codePaneEl.style.display = 'block';
      const view = state.code_preview && typeof state.code_preview === 'object'
        ? state.code_preview
        : {};
      let codeBody = '';
      if (view.loading === true) {
        codeBody = '// Loading code...';
      } else if (String(view.error || '').trim()) {
        codeBody = `// ${String(view.error || 'code_load_failed')}`;
      } else if (view.is_file === true) {
        codeBody = String(view.content || '');
        if (view.truncated === true) {
          codeBody += '\n\n// [truncated preview]';
        }
      } else {
        codeBody = '';
      }
      codePaneEl.textContent = codeBody;
    }
    if (titleEl) titleEl.textContent = 'Code';
  } else if (showingTerminal) {
    if (statsGridEl) {
      statsGridEl.style.display = 'none';
      statsGridEl.innerHTML = '';
    }
    if (codePaneEl) {
      codePaneEl.style.display = 'none';
      codePaneEl.textContent = '';
    }
    if (terminalPaneEl) terminalPaneEl.style.display = 'block';
    ensureTerminalState();
    syncTerminalCwdFromSelection(scene, { silent: true });
    renderTerminalPane();
    if (titleEl) titleEl.textContent = 'Terminal';
  } else {
    if (statsGridEl) {
      statsGridEl.style.display = 'grid';
      statsGridEl.innerHTML = rowsHtml + workflowBirthPanelHtml;
    }
    if (codePaneEl) {
      codePaneEl.style.display = 'none';
      codePaneEl.textContent = '';
    }
    if (terminalPaneEl) terminalPaneEl.style.display = 'none';
  }
  const pulseSuffix = state.spine_event_top
    ? ` | spine ${fmtNum(state.spine_event_count)} evt (${state.spine_event_top})`
    : ` | spine ${fmtNum(state.spine_event_count)} evt`;
  const linkPreviewSuffix = selectedLink ? ` | preview: ${String(selectedLink.from_id || '?')} -> ${String(selectedLink.to_id || '?')}` : '';
  const querySuffix = cg.query
    ? ` | query ${String(cg.mode || 'search')} n${fmtNum(cgMatchedNodes)} l${fmtNum(cgMatchedLinks)}`
    : '';
  const runtimeSuffix = ` | runtime ${runtimeStatusLabel.toLowerCase()} ${runtime.signal_age_sec == null ? 'n/a' : `${fmtNum(runtime.signal_age_sec)}s`} | live ${runtimeWindowLabel}${state.live_mode ? '' : ' (off)'}`;
  const integritySuffix = integrityIncident.active
    ? ` | integrity ${String(integrityIncident.severity || 'critical')} ${fmtNum(integrityIncident.violation_total)}`
    : ' | integrity ok';
  byId('metaLine').textContent = `Updated ${new Date(payload.generated_at || Date.now()).toLocaleString()} | ${state.transport} | zoom ${fmtNum(state.camera.zoom)}x | fallback ${Math.round(state.refresh_ms / 1000)}s${pulseSuffix}${linkPreviewSuffix}${querySuffix}${runtimeSuffix}${integritySuffix}`;
  renderCodegraphStatus();
  renderIncidentBanner();
}

function renderSelectionTag() {
  const selectedTag = byId('selectedTag');
  const selectedSubfractal = state.selected_subfractal && typeof state.selected_subfractal === 'object'
    ? state.selected_subfractal
    : null;
  if (selectedSubfractal) {
    const name = escapeHtml(String(selectedSubfractal.name || 'Subfractal'));
    const pth = escapeHtml(String(selectedSubfractal.path || ''));
    selectedTag.innerHTML = pth
      ? `<div class="selName">${name}</div><div class="selPath">${pth}</div>`
      : `<div class="selName">${name}</div>`;
    selectedTag.classList.add('show');
    return;
  }
  if (state.selected) {
    const name = escapeHtml(String(state.selected.name || 'Unknown'));
    const pth = escapeHtml(String(state.selected.path || ''));
    selectedTag.innerHTML = pth
      ? `<div class="selName">${name}</div><div class="selPath">${pth}</div>`
      : `<div class="selName">${name}</div>`;
    selectedTag.classList.add('show');
    return;
  }
  if (state.selected_link) {
    const name = escapeHtml(String(state.selected_link.name || 'Link'));
    const pth = escapeHtml(String(state.selected_link.path || ''));
    selectedTag.innerHTML = pth
      ? `<div class="selName">${name}</div><div class="selPath">${pth}</div>`
      : `<div class="selName">${name}</div>`;
    selectedTag.classList.add('show');
    return;
  }
  selectedTag.innerHTML = `<div class="selName">Root</div><div class="selPath">${escapeHtml(WORKSPACE_ROOT_PATH)}</div>`;
  selectedTag.classList.add('show');
}

function nodeMatchesActiveFocusScope(scene, node) {
  if (!scene || !scene.node_by_id || !node) return false;
  const cam = state.camera && typeof state.camera === 'object' ? state.camera : null;
  if (!cam || !cam.focus_mode) return true;
  const focusId = String(cam.focus_target_id || '').trim();
  if (!focusId) return true;
  const focusNode = scene.node_by_id[focusId];
  if (!focusNode) return true;
  const focusType = String(focusNode.type || '').toLowerCase();
  const nodeType = String(node.type || '').toLowerCase();
  const nodeId = String(node.id || '');
  if (nodeType === 'spine') return true;
  if (focusType === 'layer') {
    if (nodeType === 'layer') return nodeId === focusId;
    if (nodeType === 'module') return String(node.parent_id || '') === focusId;
    if (nodeType === 'submodule') {
      const parentModule = scene.node_by_id[String(node.parent_id || '')];
      return String(parentModule && parentModule.parent_id || '') === focusId;
    }
    return false;
  }
  if (focusType === 'module') {
    const focusLayerId = String(focusNode.parent_id || '');
    if (nodeType === 'module') return String(node.parent_id || '') === focusLayerId;
    if (nodeType === 'submodule') return String(node.parent_id || '') === focusId;
    if (nodeType === 'layer') {
      const focusParent = scene.node_by_id[String(focusNode.parent_id || '')];
      return Boolean(focusParent && String(focusParent.id || '') === nodeId);
    }
    return false;
  }
  if (focusType === 'submodule') {
    const focusParentId = String(focusNode.parent_id || '');
    const focusParent = focusParentId ? scene.node_by_id[focusParentId] : null;
    const focusLayerId = String(focusParent && focusParent.parent_id || '');
    if (nodeType === 'submodule') return String(node.parent_id || '') === focusParentId;
    if (nodeType === 'module') return nodeId === focusParentId;
    if (nodeType === 'layer') return nodeId === focusLayerId;
    return false;
  }
  return true;
}

function normalizeAngleRad(v) {
  const tau = Math.PI * 2;
  let n = Number(v || 0);
  if (!Number.isFinite(n)) n = 0;
  n = n % tau;
  if (n < 0) n += tau;
  return n;
}

function positiveAngleSpan(start, end) {
  const s = normalizeAngleRad(start);
  const e = normalizeAngleRad(end);
  let span = e - s;
  if (span <= 0) span += Math.PI * 2;
  return Math.max(0.001, span);
}

function angleAtArcFraction(start, end, t) {
  const s = normalizeAngleRad(start);
  const span = positiveAngleSpan(start, end);
  return normalizeAngleRad(s + (span * clamp(t, 0, 1)));
}

function angleInArc(theta, start, end, margin = 0) {
  const m = Math.max(0, Number(margin || 0));
  const t = normalizeAngleRad(theta);
  const s = normalizeAngleRad(start - m);
  const e = normalizeAngleRad(end + m);
  if (s <= e) return t >= s && t <= e;
  return t >= s || t <= e;
}

function hitTestSelectedSubfractal(scene, worldX, worldY) {
  if (!scene || !scene.node_by_id) return null;
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selectedRef && selectedRef.id || '').trim();
  const selectedType = String(selectedRef && selectedRef.type || '').toLowerCase();
  const focusId = state.camera && state.camera.focus_mode
    ? String(state.camera.focus_target_id || '').trim()
    : '';
  const focusNode = focusId && scene.node_by_id ? scene.node_by_id[focusId] : null;
  const focusType = String(focusNode && focusNode.type || '').toLowerCase();
  let candidateId = (selectedType === 'submodule' && selectedId)
    ? selectedId
    : ((focusType === 'submodule' && focusId) ? focusId : '');
  if (!candidateId && Array.isArray(scene.nodes)) {
    const preferred = hitTestSubmoduleShell(scene, worldX, worldY);
    const preferredId = String(preferred && preferred.id || '').trim();
    const pickScore = (node) => {
      if (!node) return Infinity;
      const cx = Number(node.parent_x || node.x || 0);
      const cy = Number(node.parent_y || node.y || 0);
      const dx = Number(worldX || 0) - cx;
      const dy = Number(worldY || 0) - cy;
      const distance = Math.hypot(dx, dy);
      const inner = Math.max(0.5, Number(node.shell_inner || (Number(node.radius || 2) * 2.2)));
      const outer = Math.max(inner + 0.5, Number(node.shell_outer || (Number(node.radius || 2) * 3.1)));
      const zoom = Math.max(0.25, Number(state.camera && state.camera.zoom || 1));
      const radialPad = Math.max(3.8, 9.2 / zoom);
      if (distance < (inner - radialPad) || distance > (outer + radialPad)) return Infinity;
      const theta = Math.atan2(dy, dx);
      const start = Number(node.shell_start || 0);
      const end = Number(node.shell_end || (start + (Math.PI * 0.35)));
      if (!angleInArc(theta, start, end, 0.22)) return Infinity;
      const midRadius = (inner + outer) * 0.5;
      const radialError = Math.abs(distance - midRadius);
      const spanMid = normalizeAngleRad((start + end) * 0.5);
      let angularError = Math.abs(normalizeAngleRad(theta) - spanMid);
      if (angularError > Math.PI) angularError = (Math.PI * 2) - angularError;
      return radialError + (angularError * Math.max(6, midRadius * 0.22));
    };
    let bestId = '';
    let bestScore = Infinity;
    for (const node of scene.nodes) {
      if (!node || String(node.type || '').toLowerCase() !== 'submodule') continue;
      if (!nodeMatchesActiveFocusScope(scene, node)) continue;
      if (!subfractalChildrenForNode(node).length) continue;
      const nodeId = String(node.id || '').trim();
      if (!nodeId) continue;
      const score = pickScore(node);
      if (!Number.isFinite(score)) continue;
      const preferredBias = preferredId && nodeId === preferredId ? -2 : 0;
      const totalScore = score + preferredBias;
      if (totalScore < bestScore) {
        bestScore = totalScore;
        bestId = nodeId;
      }
    }
    if (bestId) candidateId = bestId;
  }
  if (!candidateId) return null;
  const node = scene.node_by_id[candidateId];
  if (!node || String(node.type || '').toLowerCase() !== 'submodule') return null;
  const children = subfractalChildrenForNode(node);
  if (!children.length) return null;

  const cx = Number(node.parent_x || node.x || 0);
  const cy = Number(node.parent_y || node.y || 0);
  const dx = Number(worldX || 0) - cx;
  const dy = Number(worldY || 0) - cy;
  const distance = Math.hypot(dx, dy);
  const inner = Math.max(0.5, Number(node.shell_inner || (Number(node.radius || 2) * 2.2)));
  const outer = Math.max(inner + 0.5, Number(node.shell_outer || (Number(node.radius || 2) * 3.1)));
  const zoom = Math.max(0.25, Number(state.camera && state.camera.zoom || 1));
  const radialPad = Math.max(3.8, 9.2 / zoom);
  if (distance < (inner - radialPad) || distance > (outer + radialPad)) return null;

  const theta = Math.atan2(dy, dx);
  const start = Number(node.shell_start || 0);
  const end = Number(node.shell_end || (start + (Math.PI * 0.35)));
  if (!angleInArc(theta, start, end, 0.22)) return null;

  const sNorm = normalizeAngleRad(start);
  const tNorm = normalizeAngleRad(theta);
  const span = positiveAngleSpan(start, end);
  let rel = tNorm - sNorm;
  if (rel < 0) rel += Math.PI * 2;
  const frac = clamp(rel / span, 0, 0.999999);
  const index = clamp(Math.floor(frac * children.length), 0, children.length - 1);
  const child = children[index];
  if (!child) return null;
  const subId = String(child.id || `${candidateId}/child:${index}`);
  const subName = String(child.name || `sub-${index + 1}`);
  const subPath = String(child.rel || `${String(node.rel || candidateId)}/${subName}`);
  return {
    id: candidateId,
    kind: 'subfractal',
    subfractal_id: subId,
    subfractal_index: index,
    subfractal_name: subName,
    subfractal_path: subPath,
    name: `${String(node.name || selectedId)} / ${subName}`,
    path: subPath
  };
}

function hitTestSubmoduleShell(scene, worldX, worldY) {
  if (!scene || !scene.node_by_id || !Array.isArray(scene.nodes)) return null;
  let bestNode = null;
  let bestScore = Infinity;
  const zoom = Math.max(0.2, Number(state.camera && state.camera.zoom || 1));
  const radialPad = Math.max(1.2, 2.8 / zoom);
  const angularPad = 0.05;
  for (const node of scene.nodes) {
    if (!node || String(node.type || '').toLowerCase() !== 'submodule') continue;
    if (!nodeMatchesActiveFocusScope(scene, node)) continue;
    const cx = Number(node.parent_x || node.x || 0);
    const cy = Number(node.parent_y || node.y || 0);
    const dx = worldX - cx;
    const dy = worldY - cy;
    const distance = Math.hypot(dx, dy);
    const inner = Math.max(0.5, Number(node.shell_inner || (Number(node.radius || 2) * 2.2)));
    const outer = Math.max(inner + 0.5, Number(node.shell_outer || (Number(node.radius || 2) * 3.1)));
    if (distance < (inner - radialPad) || distance > (outer + radialPad)) continue;
    const theta = Math.atan2(dy, dx);
    const start = Number(node.shell_start || 0);
    const end = Number(node.shell_end || (start + (Math.PI * 0.35)));
    if (!angleInArc(theta, start, end, angularPad)) continue;
    const midRadius = (inner + outer) * 0.5;
    const radialError = Math.abs(distance - midRadius);
    const spanMid = normalizeAngleRad((start + end) * 0.5);
    const t = normalizeAngleRad(theta);
    let angularError = Math.abs(t - spanMid);
    if (angularError > Math.PI) angularError = (Math.PI * 2) - angularError;
    const score = radialError + (angularError * Math.max(6, midRadius * 0.26));
    if (score < bestScore) {
      bestScore = score;
      bestNode = node;
    }
  }
  if (!bestNode) return null;
  const nodeId = String(bestNode.id || '').trim();
  const target = scene.hit_target_by_id && scene.hit_target_by_id[nodeId]
    ? scene.hit_target_by_id[nodeId]
    : null;
  if (target) {
    return {
      id: String(target.id || nodeId),
      name: String(target.name || bestNode.name || nodeId),
      path: String(target.path || bestNode.rel || ''),
      kind: 'submodule_shell'
    };
  }
  return {
    id: nodeId,
    name: String(bestNode.name || nodeId),
    path: String(bestNode.rel || ''),
    kind: 'submodule_shell'
  };
}

function hitTest(x, y) {
  const scene = state.scene;
  if (!scene) return null;
  const world = screenToWorld(x, y);
  const subfractalHit = hitTestSelectedSubfractal(scene, world.x, world.y);
  if (subfractalHit) return subfractalHit;
  const submoduleShellHit = hitTestSubmoduleShell(scene, world.x, world.y);
  if (submoduleShellHit) return submoduleShellHit;
  const center = scene.center && typeof scene.center === 'object'
    ? scene.center
    : { x: state.width * 0.52, y: state.height * 0.5 };
  let circleFound = null;
  let circleBest = Infinity;
  let scopedCircleFound = null;
  let scopedCircleBest = Infinity;
  let zoneFound = null;
  let zoneBest = Infinity;
  const cam = state.camera && typeof state.camera === 'object' ? state.camera : null;
  const scopeActive = Boolean(
    cam && cam.focus_mode && scene.node_by_id
    && (() => {
      const fid = String(cam.focus_target_id || '').trim();
      if (!fid) return false;
      const fnode = scene.node_by_id[fid];
      const ftype = String((fnode && fnode.type) || '').toLowerCase();
      return ftype === 'layer' || ftype === 'module' || ftype === 'submodule';
    })()
  );
  for (const t of scene.hit_targets || []) {
    if (t.kind === 'ring' || t.kind === 'shell_fill') {
      const dx = world.x - Number(t.cx || 0);
      const dy = world.y - Number(t.cy || 0);
      const d = Math.hypot(dx, dy);
      const inner = Number(t.inner || 0);
      const outer = Number(t.outer || 0);
      if (d >= inner && d <= outer) {
        const mid = (inner + outer) * 0.5;
        const ringBias = t.kind === 'ring' ? 0 : 8;
        const score = Math.abs(d - mid) + ringBias;
        if (score < zoneBest) {
          zoneBest = score;
          zoneFound = t;
        }
      }
      continue;
    }
    const dx = world.x - Number(t.x || 0);
    const dy = world.y - Number(t.y || 0);
    const d = Math.hypot(dx, dy);
    if (d <= Number(t.r || 0) && d < circleBest) {
      circleBest = d;
      circleFound = t;
    }
    const node = scene.node_by_id ? scene.node_by_id[String(t.id || '')] : null;
    if (node && nodeMatchesActiveFocusScope(scene, node) && d <= Number(t.r || 0) && d < scopedCircleBest) {
      scopedCircleBest = d;
      scopedCircleFound = t;
    }
  }
  const found = scopeActive
    ? (scopedCircleFound || zoneFound)
    : (scopedCircleFound || circleFound || zoneFound);
  if (found) return found;
  const shellBoundary = Math.max(0, Number(scene.outer_shell_boundary || 0));
  if (shellBoundary > 0 && scene.system_target) {
    const d = Math.hypot(world.x - Number(center.x || 0), world.y - Number(center.y || 0));
    if (d > shellBoundary) {
      return scene.system_target;
    }
  }
  return null;
}

function distancePointToSegment(px, py, ax, ay, bx, by) {
  const abx = bx - ax;
  const aby = by - ay;
  const apx = px - ax;
  const apy = py - ay;
  const denom = (abx * abx) + (aby * aby);
  const t = denom > 0 ? clamp(((apx * abx) + (apy * aby)) / denom, 0, 1) : 0;
  const cx = ax + (abx * t);
  const cy = ay + (aby * t);
  return Math.hypot(px - cx, py - cy);
}

function hitTestLink(x, y, thresholdPx = 9) {
  const scene = state.scene;
  if (!scene) return null;
  const world = screenToWorld(x, y);
  const threshold = Math.max(2.5, Number(thresholdPx || 9) / Math.max(0.15, state.camera.zoom));
  const links = visibleLinksForScene(scene);
  let best = null;
  let bestDist = Infinity;
  for (const link of links) {
    if (!link || !link.p0 || !link.p3) continue;
    let prev = { x: Number(link.p0.x || 0), y: Number(link.p0.y || 0) };
    let localMin = Infinity;
    const steps = 20;
    for (let i = 1; i <= steps; i += 1) {
      const t = i / steps;
      const pt = bezierPoint(link, t);
      const d = distancePointToSegment(world.x, world.y, prev.x, prev.y, pt.x, pt.y);
      if (d < localMin) localMin = d;
      prev = pt;
    }
    if (localMin <= threshold && localMin < bestDist) {
      bestDist = localMin;
      best = link;
    }
  }
  if (!best) return null;
  const fromNode = scene.node_by_id ? scene.node_by_id[String(best.from_id || '')] : null;
  const toNode = scene.node_by_id ? scene.node_by_id[String(best.to_id || '')] : null;
  const fromName = String((fromNode && fromNode.name) || best.from_id || 'unknown');
  const toName = String((toNode && toNode.name) || best.to_id || 'unknown');
  const fromPath = String((fromNode && fromNode.rel) || best.from_id || '');
  const toPath = String((toNode && toNode.rel) || best.to_id || '');
  return {
    id: String(best.id || ''),
    kind: 'link',
    from_id: String(best.from_id || ''),
    to_id: String(best.to_id || ''),
    name: `${fromName} -> ${toName}`,
    path: `${fromPath} -> ${toPath}`,
    line2: `kind ${String(best.kind || 'flow')} | activity ${fmtNum(Number(best.activity || 0) * 100)}%`,
    link: best
  };
}

function linksInteractiveForSelection() {
  const scene = state.scene;
  if (!scene || !scene.node_by_id) return false;
  const sid = String(state.selected && state.selected.id || '').trim();
  if (sid) {
    const node = scene.node_by_id[sid];
    const type = String(node && node.type || '').toLowerCase();
    if (type === 'module' || type === 'submodule') return true;
  }
  const focusId = String(state.camera && state.camera.focus_target_id || '').trim();
  if (focusId) {
    const focusNode = scene.node_by_id[focusId];
    const focusType = String(focusNode && focusNode.type || '').toLowerCase();
    if (focusType === 'module' || focusType === 'submodule') return true;
  }
  const mapOwnerId = String(state.camera && state.camera.map_owner_id || '').trim();
  if (mapOwnerId) {
    const mapOwnerNode = scene.node_by_id[mapOwnerId];
    const ownerType = String(mapOwnerNode && mapOwnerNode.type || '').toLowerCase();
    if (ownerType === 'module' || ownerType === 'submodule') return true;
  }
  return false;
}

function isModuleDepthSelection(scene) {
  if (!scene || !scene.node_by_id) return false;
  const sid = String(state.selected && state.selected.id || '').trim();
  if (!sid) return false;
  const node = scene.node_by_id[sid];
  if (!node) return false;
  const type = String(node.type || '').toLowerCase();
  return type === 'module' || type === 'submodule';
}

function shouldPreferLinkSelection(scene, hit, linkHit) {
  if (!linkHit) return false;
  if (!hit) return true;
  if (!isModuleDepthSelection(scene)) return false;
  const targetNode = interactionNodeFromHit(scene, hit);
  const targetType = String((targetNode && targetNode.type) || '').toLowerCase();
  const hitKind = String(hit.kind || '').toLowerCase();
  if (hitKind === 'ring' || hitKind === 'shell_fill') return true;
  if (targetType === 'layer' || targetType === 'spine' || targetType === 'system') return true;
  return false;
}

function interactionNodeFromHit(scene, hit) {
  if (!scene || !scene.node_by_id || !hit) return null;
  return scene.node_by_id[String(hit.id || '')] || null;
}

function hitTargetFromNode(scene, nodeId, fallbackKind = 'circle') {
  if (!scene) return null;
  const id = String(nodeId || '').trim();
  if (!id) return null;
  if (scene.hit_target_by_id && scene.hit_target_by_id[id]) {
    const t = scene.hit_target_by_id[id];
    return {
      id: String(t.id || id),
      name: String(t.name || ''),
      path: String(t.path || ''),
      kind: String(t.kind || fallbackKind || 'circle')
    };
  }
  const node = scene.node_by_id && scene.node_by_id[id] ? scene.node_by_id[id] : null;
  if (!node) return null;
  return {
    id,
    name: String(node.name || id),
    path: String(node.rel || ''),
    kind: fallbackKind
  };
}

function layerIdForNode(scene, node) {
  if (!scene || !node) return '';
  const type = String(node.type || '').toLowerCase();
  if (type === 'layer' || type === 'spine') return String(node.id || '');
  if (type === 'module') return String(node.parent_id || '');
  if (type === 'submodule') {
    const parentModule = scene.node_by_id ? scene.node_by_id[String(node.parent_id || '')] : null;
    return String(parentModule && parentModule.parent_id || '');
  }
  return '';
}

function selectionInteractionContext(scene) {
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selectedRef && selectedRef.id || '').trim();
  const selectedNode = selectedId && scene && scene.node_by_id ? scene.node_by_id[selectedId] : null;
  const selectedTypeHint = String(selectedRef && selectedRef.type || '').toLowerCase();
  const selectedType = selectedTypeHint || String((selectedNode && selectedNode.type) || '').toLowerCase();
  const focusMode = Boolean(state.camera && state.camera.focus_mode);
  const focusId = focusMode ? String(state.camera.focus_target_id || '').trim() : '';
  const focusNode = focusId && scene && scene.node_by_id ? scene.node_by_id[focusId] : null;
  const focusType = focusMode ? String((focusNode && focusNode.type) || '').toLowerCase() : '';
  let interactionNode = selectedNode;
  let interactionType = selectedType;
  if (focusNode && focusType) {
    const selectedMatchesFocus = Boolean(
      selectedNode
      && selectedType
      && String(selectedNode.id || '') === focusId
      && selectedType === focusType
    );
    if (!selectedMatchesFocus) {
      interactionNode = focusNode;
      interactionType = focusType;
    }
  }
  return {
    selectedRef,
    selectedId,
    selectedNode,
    selectedType,
    focusMode,
    focusId,
    focusNode,
    focusType,
    interactionNode,
    interactionType
  };
}

function isLayerFocusZoomed(scene, layerId) {
  if (!scene || !scene.node_by_id) return false;
  const id = String(layerId || '').trim();
  if (!id || !state.camera.focus_mode) return false;
  const focusId = String(state.camera.focus_target_id || '').trim();
  if (!focusId || focusId !== id) return false;
  const focusNode = scene.node_by_id[focusId];
  return String((focusNode && focusNode.type) || '').toLowerCase() === 'layer';
}

function normalizeHitForCurrentLevel(scene, hit) {
  if (!scene || !hit) return hit;
  const hitKind = String(hit.kind || '').toLowerCase();
  if (hitKind === 'subfractal') return hit;
  const node = interactionNodeFromHit(scene, hit);
  if (!node) return hit;
  const interaction = selectionInteractionContext(scene);
  const selectedNode = interaction.interactionNode;
  const selectedType = interaction.interactionType;
  const nodeType = String(node.type || '').toLowerCase();

  const mapToLayer = () => {
    const lid = layerIdForNode(scene, node);
    if (!lid) return hit;
    return hitTargetFromNode(scene, lid, lid === SPINE_NODE_ID ? 'circle' : 'ring') || hit;
  };

  // Preferred shell selector level.
  if (!selectedType || selectedType === 'spine' || selectedType === 'system') {
    if (nodeType === 'module' || nodeType === 'submodule') return mapToLayer();
    if (nodeType === 'spine') return hitTargetFromNode(scene, SPINE_NODE_ID, 'circle') || hit;
    if (nodeType === 'system') return hitTargetFromNode(scene, SYSTEM_ROOT_ID, 'system') || hit;
    return hit;
  }

  // Preferred module selector level inside selected shell.
  if (selectedType === 'layer') {
    const selectedLayerId = String(selectedNode && selectedNode.id || '');
    if (!selectedLayerId) return hit;
    const shellZoomed = isLayerFocusZoomed(scene, selectedLayerId);
    if (!shellZoomed) {
      if (nodeType === 'module') {
        if (String(node.parent_id || '') === selectedLayerId) {
          return hitTargetFromNode(scene, String(node.id || ''), 'circle') || hit;
        }
        return mapToLayer();
      }
      if (nodeType === 'submodule') {
        const parentId = String(node.parent_id || '');
        const parentNode = scene.node_by_id ? scene.node_by_id[parentId] : null;
        if (parentNode && String(parentNode.parent_id || '') === selectedLayerId) {
          return hitTargetFromNode(scene, parentId, 'circle') || hit;
        }
        return mapToLayer();
      }
      return hit;
    }
    if (nodeType === 'module' && String(node.parent_id || '') !== selectedLayerId) {
      return mapToLayer();
    }
    if (nodeType === 'submodule') {
      const parentId = String(node.parent_id || '');
      const parentNode = scene.node_by_id ? scene.node_by_id[parentId] : null;
      if (parentNode && String(parentNode.parent_id || '') === String(selectedNode.id || '')) {
        return hitTargetFromNode(scene, String(node.id || ''), 'circle') || hit;
      }
      return mapToLayer();
    }
    return hit;
  }

  return hit;
}

function isHitAllowedForCurrentLevel(scene, hit) {
  if (!scene || !hit) return false;
  const targetNode = interactionNodeFromHit(scene, hit);
  const targetType = String((targetNode && targetNode.type) || '').toLowerCase();
  const hitKind = String(hit.kind || '').toLowerCase();
  if (hitKind === 'subfractal') {
    const sid = String(state.selected && state.selected.id || '').trim();
    const stype = String(state.selected && state.selected.type || '').toLowerCase();
    const focusId = String(state.camera && state.camera.focus_mode ? state.camera.focus_target_id || '' : '').trim();
    const focusNode = focusId && scene.node_by_id ? scene.node_by_id[focusId] : null;
    const focusType = String(focusNode && focusNode.type || '').toLowerCase();
    const targetId = String(hit.id || '').trim();
    if ((stype === 'submodule' && !!sid && sid === targetId)
      || (focusType === 'submodule' && !!focusId && focusId === targetId)) {
      return true;
    }
    const interaction = selectionInteractionContext(scene);
    const interactionNode = interaction.interactionNode;
    const interactionType = interaction.interactionType;
    if (!interactionNode || !targetNode) return false;
    if (interactionType === 'module') {
      return String(targetNode.parent_id || '') === String(interactionNode.id || '');
    }
    if (interactionType === 'layer' && isLayerFocusZoomed(scene, String(interactionNode.id || ''))) {
      const parentModule = scene.node_by_id ? scene.node_by_id[String(targetNode.parent_id || '')] : null;
      return String(parentModule && parentModule.parent_id || '') === String(interactionNode.id || '');
    }
    return false;
  }
  const isShellHit = hitKind === 'ring' || hitKind === 'shell_fill';
  const interaction = selectionInteractionContext(scene);
  const selectedNode = interaction.interactionNode;
  const selectedType = interaction.interactionType;
  if (targetType === 'system') return true;

  // Root level: only shell (layer ring) interaction.
  if (!selectedType) {
    if (targetType === 'spine') return true;
    return isShellHit && targetType === 'layer';
  }

  if (selectedType === 'spine') {
    if (targetType === 'spine') return true;
    if (targetType === 'system') return true;
    return isShellHit && targetType === 'layer';
  }

  if (selectedType === 'system') {
    if (targetType === 'spine') return true;
    return isShellHit && targetType === 'layer';
  }

  if (selectedType === 'layer') {
    const selectedLayerId = String(selectedNode && selectedNode.id || '');
    if (!selectedLayerId) return isShellHit && targetType === 'layer';
    const shellZoomed = isLayerFocusZoomed(scene, selectedLayerId);
    if (targetType === 'spine') return true;
    if (isShellHit && targetType === 'layer') return true;
    if (!shellZoomed) {
      return targetType === 'module' && String(targetNode.parent_id || '') === selectedLayerId;
    }
    if (targetType === 'module') {
      return String(targetNode.parent_id || '') === selectedLayerId;
    }
    if (targetType === 'submodule') {
      const parentModule = scene.node_by_id ? scene.node_by_id[String(targetNode.parent_id || '')] : null;
      return String(parentModule && parentModule.parent_id || '') === selectedLayerId;
    }
    return false;
  }

  if (selectedType === 'module') {
    const selectedLayerId = String(selectedNode && selectedNode.parent_id || '');
    if (targetType === 'spine') return true;
    if (isShellHit && targetType === 'layer') return true;
    if (targetType === 'module') return String(targetNode.parent_id || '') === selectedLayerId;
    if (targetType === 'submodule') return String(targetNode.parent_id || '') === String(selectedNode && selectedNode.id || '');
    return false;
  }

  if (selectedType === 'submodule') {
    const parentModuleId = String(selectedNode && selectedNode.parent_id || '');
    const parentModule = parentModuleId && scene.node_by_id ? scene.node_by_id[parentModuleId] : null;
    const layerId = String(parentModule && parentModule.parent_id || '');
    if (targetType === 'spine') return true;
    if (isShellHit && targetType === 'layer') return true;
    if (targetType === 'module') {
      return String(targetNode.parent_id || '') === layerId || String(targetNode.id || '') === parentModuleId;
    }
    if (targetType === 'submodule') return String(targetNode.parent_id || '') === parentModuleId;
    return false;
  }

  return isShellHit && targetType === 'layer';
}

function onCanvasClick(evt) {
  if (isShellIntroActive(performance.now())) return;
  if (state.camera.drag_px > 5) return;
  syncCameraTransition(performance.now());
  const rect = state.canvas.getBoundingClientRect();
  const x = evt.clientX - rect.left;
  const y = evt.clientY - rect.top;
  const scene = state.scene;
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedSnapshot = cloneSelectionRef(selectedRef);
  const selectedId = String(selectedRef && selectedRef.id || '').trim();
  const selectedNode = selectedId && scene && scene.node_by_id ? scene.node_by_id[selectedId] : null;
  const selectedTypeHint = String(selectedRef && selectedRef.type || '').toLowerCase();
  const selectedType = selectedTypeHint || String((selectedNode && selectedNode.type) || '').toLowerCase();
  const activeFocusId = String(state.camera.focus_target_id || '');
  const activeFocusNode = activeFocusId && scene && scene.node_by_id
    ? scene.node_by_id[activeFocusId]
    : null;
  const activeFocusType = String((activeFocusNode && activeFocusNode.type) || '').toLowerCase();
  const interactionType = activeFocusType || selectedType;
  const interactionNode = activeFocusNode || selectedNode;
  const rawHit = hitTest(x, y);
  const normalizedHit = rawHit ? normalizeHitForCurrentLevel(scene, rawHit) : null;
  let hit = normalizedHit && isHitAllowedForCurrentLevel(scene, normalizedHit) ? normalizedHit : null;
  const rawNode = rawHit ? interactionNodeFromHit(scene, rawHit) : null;
  const rawNodeType = String(rawNode && rawNode.type || '').toLowerCase();
  if (!hit && rawNode) {
    if (interactionType === 'layer' && interactionNode) {
      const selectedLayerId = String(interactionNode.id || '');
      const layerZoomed = isLayerFocusZoomed(scene, selectedLayerId);
      if (layerZoomed) {
        if (rawNodeType === 'module' && String(rawNode.parent_id || '') === selectedLayerId) {
          hit = hitTargetFromNode(scene, String(rawNode.id || ''), 'circle') || null;
        } else if (rawNodeType === 'submodule') {
          const parentModule = scene && scene.node_by_id
            ? scene.node_by_id[String(rawNode.parent_id || '')]
            : null;
          if (parentModule && String(parentModule.parent_id || '') === selectedLayerId) {
            hit = hitTargetFromNode(scene, String(rawNode.id || ''), 'circle') || null;
          }
        }
      }
    } else if (interactionType === 'module') {
      const selectedModuleId = String(interactionNode && interactionNode.id || '');
      if (rawNodeType === 'submodule' && String(rawNode.parent_id || '') === selectedModuleId) {
        hit = hitTargetFromNode(scene, String(rawNode.id || ''), 'circle') || null;
      }
    }
  }
  const linkHit = linksInteractiveForSelection() ? hitTestLink(x, y, 12) : null;
  const preferLine = linkHit ? (state.camera.map_mode || shouldPreferLinkSelection(scene, hit, linkHit)) : false;
  const rawHitKind = String(rawHit && rawHit.kind || '').toLowerCase();
  const rawLayerId = rawNode
    ? String(layerIdForNode(scene, rawNode) || '')
    : ((rawHitKind === 'ring' || rawHitKind === 'shell_fill') ? String(rawHit && rawHit.id || '') : '');
  const rawLayerNode = rawLayerId && scene && scene.node_by_id ? scene.node_by_id[rawLayerId] : null;
  const clickedOutsideFocusedShell = Boolean(
    state.camera.focus_mode
    && !preferLine
    && activeFocusType === 'layer'
    && rawLayerId
    && rawLayerId !== activeFocusId
    && String(rawLayerNode && rawLayerNode.type || '').toLowerCase() === 'layer'
  );
  if (clickedOutsideFocusedShell) {
    const systemTarget = scene && scene.system_target
      ? scene.system_target
      : hitTargetFromNode(scene, SYSTEM_ROOT_ID, 'system');
    const sid = String(systemTarget && systemTarget.id || SYSTEM_ROOT_ID);
    const snode = scene && scene.node_by_id ? scene.node_by_id[sid] : null;
    state.selected = {
      id: sid,
      name: String((systemTarget && systemTarget.name) || (snode && snode.name) || 'System Root'),
      path: String((systemTarget && systemTarget.path) || (snode && snode.rel) || WORKSPACE_ROOT_PATH),
      kind: String((systemTarget && systemTarget.kind) || 'system'),
      type: 'system'
    };
    state.selected_link = null;
    clearModuleDepthReturns();
    clearMapMode();
    zoomToSystemRoot();
    applySelectionFocus(true);
    renderSelectionTag();
    renderStats();
    return;
  }
  if (rawHit && !hit && !linkHit) {
    if (state.camera.focus_mode && activeFocusType === 'layer') {
      if (rawLayerId && rawLayerId !== activeFocusId) {
        restoreCameraFocus();
        const layerTarget = hitTargetFromNode(scene, rawLayerId, 'ring');
        if (layerTarget) {
          state.selected = {
            id: String(layerTarget.id || rawLayerId),
            name: String(layerTarget.name || rawLayerId),
            path: String(layerTarget.path || ''),
            kind: String(layerTarget.kind || 'ring'),
            type: 'layer'
          };
          state.selected_link = null;
          applySelectionFocus(true);
          renderSelectionTag();
          renderStats();
        }
      }
    }
    return;
  }
  const moduleDepthType = (selectedType === 'module' || selectedType === 'submodule')
    ? selectedType
    : ((activeFocusType === 'module' || activeFocusType === 'submodule') ? activeFocusType : '');
  const moduleDepthNode = moduleDepthType
    ? ((moduleDepthType === selectedType && selectedNode) ? selectedNode : activeFocusNode)
    : null;
  const moduleDepthId = String(moduleDepthNode && moduleDepthNode.id || '');
  const selectedSubfractal = validateSelectedSubfractal(scene);
  if (!state.camera.map_mode && !hit && !linkHit && selectedSubfractal && selectedType === 'submodule') {
    const submoduleSelectedId = String(selectedId || '');
    clearSelectedSubfractal();
    state.selected_link = null;
    if (submoduleSelectedId) zoomIntoSubmodule(submoduleSelectedId, null);
    applySelectionFocus(true);
    renderSelectionTag();
    renderStats();
    return;
  }
  if (!hit && !linkHit && moduleDepthType && moduleDepthNode) {
    const cam = state.camera;
    const submoduleReturnSelection = cloneSelectionRef(cam.submodule_return_selection);
    const hasSubmoduleReturn = (
      moduleDepthType === 'submodule'
      && cam.focus_mode
      && String(cam.focus_target_id || '') === moduleDepthId
      && submoduleReturnSelection
      && String(cam.submodule_return_selection_for || '') === moduleDepthId
    );
    if (hasSubmoduleReturn) {
      restoreCameraFocus();
      state.selected = submoduleReturnSelection;
      state.selected_link = null;
      clearModuleDepthReturns();
      applySelectionFocus(true);
      renderSelectionTag();
      renderStats();
      return;
    }
    const moduleReturnSelection = cloneSelectionRef(cam.module_return_selection);
    const hasModuleReturn = (
      moduleDepthType === 'module'
      && cam.focus_mode
      && String(cam.focus_target_id || '') === moduleDepthId
      && moduleReturnSelection
      && String(cam.module_return_selection_for || '') === moduleDepthId
    );
    if (hasModuleReturn) {
      restoreCameraFocus();
      state.selected = moduleReturnSelection;
      state.selected_link = null;
      clearModuleDepthReturns();
      applySelectionFocus(true);
      renderSelectionTag();
      renderStats();
      return;
    }
    let parentLayerId = '';
    if (moduleDepthType === 'module') {
      parentLayerId = String(moduleDepthNode && moduleDepthNode.parent_id || '');
    } else {
      const parentModuleId = String(moduleDepthNode && moduleDepthNode.parent_id || '');
      const parentModule = parentModuleId && scene && scene.node_by_id
        ? scene.node_by_id[parentModuleId]
        : null;
      parentLayerId = String(parentModule && parentModule.parent_id || '');
    }
    const layerNode = parentLayerId && scene && scene.node_by_id ? scene.node_by_id[parentLayerId] : null;
    if (layerNode && String(layerNode.type || '') === 'layer') {
      const layerTarget = hitTargetFromNode(scene, parentLayerId, 'ring');
      state.selected = {
        id: String(parentLayerId),
        name: String((layerTarget && layerTarget.name) || layerNode.name || parentLayerId),
        path: String((layerTarget && layerTarget.path) || layerNode.rel || ''),
        kind: String((layerTarget && layerTarget.kind) || 'ring'),
        type: 'layer'
      };
      state.selected_link = null;
      clearModuleDepthReturns();
      zoomIntoLayer(parentLayerId);
      applySelectionFocus(true);
      renderSelectionTag();
      renderStats();
      return;
    }
  }
  const hitId = String(hit && hit.id || '');
  const hitNode = hitId && scene && scene.node_by_id ? scene.node_by_id[hitId] : null;
  const hitType = String((hitNode && hitNode.type) || '').toLowerCase();
  const hitLayerId = hitNode ? String(layerIdForNode(scene, hitNode) || '') : '';
  const clickingFocusedModule = Boolean(
    hit && !preferLine
    && activeFocusId
    && hitId
    && activeFocusId === hitId
  );
  const clickedDifferentLayerWhileFocusedLayer = Boolean(
    hit && !preferLine
    && state.camera.focus_mode
    && activeFocusType === 'layer'
    && hitType === 'layer'
    && activeFocusId
    && hitId
    && activeFocusId !== hitId
  );
  const linePreviewClick = Boolean(preferLine && linkHit);
  const hitKind = String(hit && hit.kind || '').toLowerCase();
  const subfractalClick = Boolean(hit && !preferLine && hitKind === 'subfractal');
  if (subfractalClick && hitNode && hitType === 'submodule') {
    const parentId = String(hitNode.id || '').trim();
    const parentPath = String(hitNode.rel || '').trim();
    const selectedParent = state.selected
      && String(state.selected.id || '') === parentId
      && String(state.selected.type || '').toLowerCase() === 'submodule';
    if (!selectedParent) {
      state.selected = {
        id: parentId,
        name: String(hitNode.name || parentId),
        path: parentPath,
        kind: 'circle',
        type: 'submodule'
      };
    }
    const childRows = subfractalChildrenForNode(hitNode);
    const childId = String(hit && hit.subfractal_id || '').trim();
    const childIndexRaw = Math.floor(Number(hit && hit.subfractal_index || 0) || 0);
    const childIndex = clamp(childIndexRaw, 0, Math.max(0, childRows.length - 1));
    const child = childRows.find((row) => String(row.id || '') === childId) || childRows[childIndex] || null;
    const nextId = String((child && child.id) || childId || '').trim();
    const sameSubfractal = state.selected_subfractal
      && String(state.selected_subfractal.parent_id || '') === parentId
      && String(state.selected_subfractal.id || '') === nextId;
    if (sameSubfractal || !nextId) {
      clearSelectedSubfractal();
      zoomIntoSubmodule(parentId, null);
    } else {
      setSelectedSubfractal({
        id: nextId,
        parent_id: parentId,
        index: childIndex,
        count: childRows.length,
        name: String((child && child.name) || hit.subfractal_name || nextId),
        path: String((child && child.rel) || hit.subfractal_path || parentPath)
      });
      zoomIntoSubfractal(parentId, state.selected_subfractal);
    }
    state.selected_link = null;
    applySelectionFocus(true);
    renderSelectionTag();
    renderStats();
    return;
  }
  if (state.camera.map_mode && !linePreviewClick) {
    const restored = exitMapModeToCenteredSelection();
    if (restored) {
      const ownerId = String(state.camera.focus_target_id || '').trim();
      const ownerNode = ownerId && scene && scene.node_by_id ? scene.node_by_id[ownerId] : null;
      if (ownerNode) {
        state.selected = {
          id: ownerId,
          name: String(ownerNode.name || ownerId),
          path: String(ownerNode.rel || ''),
          kind: 'circle',
          type: String(ownerNode.type || '').toLowerCase()
        };
      }
      state.selected_link = null;
      applySelectionFocus(true);
      renderSelectionTag();
      renderStats();
      return;
    }
  }
  const keepFocusForHit = Boolean(
    hit && !preferLine && (
      (activeFocusId && hitId && activeFocusId === hitId)
      || (activeFocusType === 'layer'
        && (hitType === 'module' || hitType === 'submodule')
        && hitLayerId
        && hitLayerId === activeFocusId)
      || (activeFocusType === 'module'
        && hitType === 'submodule'
        && String(hitNode && hitNode.parent_id || '') === activeFocusId)
      || (activeFocusType === 'module'
        && hitType === 'module'
        && String(hitNode && hitNode.parent_id || '') === String(activeFocusNode && activeFocusNode.parent_id || ''))
      || (activeFocusType === 'submodule'
        && hitType === 'submodule'
        && String(hitNode && hitNode.parent_id || '') === String(activeFocusNode && activeFocusNode.parent_id || ''))
    )
  );
  if (state.camera.focus_mode
    && !linePreviewClick
    && !keepFocusForHit
    && (clickedDifferentLayerWhileFocusedLayer || !clickingFocusedModule)) {
    restoreCameraFocus();
  }
  const suppressAutoLayerZoom = clickedDifferentLayerWhileFocusedLayer;

  if (hit && !preferLine) {
    clearSelectedSubfractal();
    const node = scene && scene.node_by_id
      ? scene.node_by_id[String(hit.id || '')]
      : null;
    let targetId = String(hit.id || '');
    let targetName = String(hit.name || '');
    let targetPath = String(hit.path || '');
    let targetKind = String(hit.kind || '');

    // Fractal clicks map to parent module at shell-level, but remain direct at module/fractal level.
    if (node && String(node.type || '') === 'submodule') {
      const parentId = String(node.parent_id || '').trim();
      const parentNode = scene && scene.node_by_id ? scene.node_by_id[parentId] : null;
      const selectedSubmoduleParentId = interactionNode ? String(interactionNode.parent_id || '') : '';
      const selectedLayerId = interactionNode ? String(interactionNode.id || '') : '';
      const layerZoomed = interactionType === 'layer' && isLayerFocusZoomed(scene, selectedLayerId);
      const allowDirectSubmodule = (
        (interactionType === 'module' && String(interactionNode && interactionNode.id || '') === parentId)
        || (interactionType === 'submodule' && selectedSubmoduleParentId === parentId)
        || (layerZoomed
          && parentNode
          && String(parentNode.parent_id || '') === selectedLayerId)
      );
      if (!allowDirectSubmodule && parentNode) {
        targetId = parentId;
        targetName = String(parentNode.name || targetName);
        targetPath = String(parentNode.rel || targetPath);
        targetKind = 'circle';
      } else {
        targetId = String(node.id || targetId);
        targetName = String(node.name || targetName);
        targetPath = String(node.rel || targetPath);
      }
    }

    const targetNode = scene && scene.node_by_id ? scene.node_by_id[targetId] : null;
    const sameNode = state.selected && String(state.selected.id) === targetId;
    if (sameNode && targetNode) {
      const targetType = String(targetNode.type || '').toLowerCase();
      if (targetType === 'module') {
        state.selected = {
          id: targetId,
          name: targetName,
          path: targetPath,
          kind: targetKind,
          type: targetType
        };
      } else if (targetType === 'layer' || targetType === 'spine' || targetType === 'system') {
        state.selected = {
          id: targetId,
          name: targetName,
          path: targetPath,
          kind: targetKind,
          type: targetType
        };
        if (targetType === 'layer' && !suppressAutoLayerZoom) zoomIntoLayer(targetNode.id);
      } else if (targetType === 'submodule') {
        state.selected = {
          id: targetId,
          name: targetName,
          path: targetPath,
          kind: targetKind,
          type: targetType
        };
      } else {
        state.selected = null;
      }
    } else {
      state.selected = sameNode ? null : {
        id: targetId,
        name: targetName,
        path: targetPath,
        kind: targetKind,
        type: selectionTypeFrom(targetNode, targetKind)
      };
    }
    const selectedTypeFinal = String(state.selected && state.selected.type || '').toLowerCase();
    const cameFromModuleDepth = selectedType === 'module' || selectedType === 'submodule';
    if (selectedTypeFinal === 'module') {
      const originSelection = (selectedType === 'module' || selectedType === 'submodule')
        ? null
        : selectedSnapshot;
      zoomIntoModule(String(state.selected.id || ''), originSelection);
      state.camera.submodule_return_selection = null;
      state.camera.submodule_return_selection_for = '';
    } else if (selectedTypeFinal === 'submodule') {
      const originSelection = selectedType === 'module' ? selectedSnapshot : null;
      zoomIntoSubmodule(String(state.selected.id || ''), originSelection);
      state.camera.module_return_selection = null;
      state.camera.module_return_selection_for = '';
    } else if (selectedTypeFinal === 'layer') {
      clearModuleDepthReturns();
      if (cameFromModuleDepth) {
        zoomIntoLayer(String(state.selected.id || ''));
      }
      clearMapMode();
    } else if (selectedTypeFinal === 'system') {
      clearModuleDepthReturns();
      zoomToSystemRoot();
      clearMapMode();
    } else {
      clearModuleDepthReturns();
      clearMapMode();
    }
    state.selected_link = null;
  } else {
    if (linkHit) {
      clearSelectedSubfractal();
      const sameLink = state.selected_link && String(state.selected_link.id) === String(linkHit.id);
      state.selected_link = sameLink ? null : {
        id: linkHit.id,
        name: linkHit.name,
        path: linkHit.path || '',
        kind: 'link',
        from_id: linkHit.from_id,
        to_id: linkHit.to_id
      };
      const mapOwnerId = (selectedType === 'module' || selectedType === 'submodule')
        ? selectedId
        : ((activeFocusType === 'module' || activeFocusType === 'submodule')
          ? activeFocusId
          : '');
      if (mapOwnerId && (!state.selected || String(state.selected.id || '') !== mapOwnerId)) {
        const ownerNode = scene && scene.node_by_id ? scene.node_by_id[mapOwnerId] : null;
        if (ownerNode) {
          state.selected = {
            id: mapOwnerId,
            name: String(ownerNode.name || mapOwnerId),
            path: String(ownerNode.rel || ''),
            kind: 'circle',
            type: String(ownerNode.type || '').toLowerCase()
          };
        }
      }
      if (state.selected_link && mapOwnerId) {
        enterMapModeForSelection(mapOwnerId);
      }
    } else {
      state.selected = null;
      clearSelectedSubfractal();
      state.selected_link = null;
      clearModuleDepthReturns();
      clearMapMode();
    }
  }
  applySelectionFocus(true);
  renderSelectionTag();
  renderStats();
}

function updateHoverAtCanvasPoint(x, y) {
  if (isShellIntroActive(performance.now())) {
    state.hover = null;
    return;
  }
  syncCameraTransition(performance.now());
  const scene = state.scene;
  if (!scene) {
    state.hover = null;
    return;
  }
  const rawHit = hitTest(x, y);
  const normalizedHit = rawHit ? normalizeHitForCurrentLevel(scene, rawHit) : null;
  const hit = normalizedHit && isHitAllowedForCurrentLevel(scene, normalizedHit) ? normalizedHit : null;
  const linkHit = linksInteractiveForSelection() ? hitTestLink(x, y, 10) : null;
  const preferLine = shouldPreferLinkSelection(scene, hit, linkHit);
  if (hit && !preferLine) {
    if (String(hit.kind || '').toLowerCase() === 'subfractal') {
      state.hover = {
        id: String(hit.subfractal_id || `${hit.id || ''}:sub`),
        name: String(hit.subfractal_name || hit.name || 'Subfractal'),
        path: String(hit.subfractal_path || hit.path || ''),
        kind: 'subfractal',
        sx: Number(x || 0),
        sy: Number(y || 0)
      };
      return;
    }
    const node = scene.node_by_id ? scene.node_by_id[String(hit.id || '')] : null;
    state.hover = {
      id: String((node && node.id) || hit.id || ''),
      name: String((node && node.name) || hit.name || ''),
      path: String((node && node.rel) || hit.path || ''),
      kind: String(hit.kind || ''),
      sx: Number(x || 0),
      sy: Number(y || 0)
    };
    return;
  }
  if (!linkHit) {
    state.hover = null;
    return;
  }
  state.hover = {
    id: String(linkHit.id || ''),
    name: String(linkHit.name || ''),
    path: String(linkHit.path || ''),
    kind: 'link',
    line2: String(linkHit.line2 || ''),
    sx: Number(x || 0),
    sy: Number(y || 0)
  };
}

function animate(ts) {
  syncCameraTransition(ts);
  const hasFrameHistory = Number.isFinite(state.last_frame_ts) && state.last_frame_ts > 0;
  const dtMs = hasFrameHistory ? clamp(ts - state.last_frame_ts, 1, 80) : 16.7;
  state.last_frame_ts = ts;
  if (!state.motion_initialized) {
    state.motion_initialized = true;
    state.motion_frame_count = 1;
    state.motion_dt_smoothed = dtMs;
    state.motion_jitter_ema = 0;
    state.motion_smoothness_ema = 1;
  } else {
    state.motion_frame_count += 1;
    const warmupBlend = state.motion_frame_count < 18 ? 0.26 : 0.14;
    state.motion_dt_smoothed = (state.motion_dt_smoothed * (1 - warmupBlend)) + (dtMs * warmupBlend);
    const jitterSample = Math.abs(dtMs - state.motion_dt_smoothed) / Math.max(8, state.motion_dt_smoothed);
    state.motion_jitter_ema = (state.motion_jitter_ema * 0.9) + (clamp(jitterSample, 0, 2.5) * 0.1);
    const smoothFrame = clamp(1 - (state.motion_jitter_ema / 0.75), 0, 1);
    state.motion_smoothness_ema = (state.motion_smoothness_ema * 0.88) + (smoothFrame * 0.12);
  }
  state.fps = 1000 / dtMs;
  state.fps_smoothed = (state.fps_smoothed * 0.92) + (state.fps * 0.08);
  maybeAutoTuneQuality();
  drawScene(ts);
  renderStats();
  requestAnimationFrame(animate);
}

async function refreshNow(force = false) {
  if (!force && state.ws_connected) return;
  const hours = currentHours();
  const liveCfg = currentLiveConfig();
  state.live_mode = liveCfg.live_mode;
  state.live_minutes = liveCfg.live_minutes;
  try {
    const payload = await fetchPayload(hours, liveCfg);
    setPayload(payload);
  } catch (err) {
    byId('metaLine').textContent = `Load failed: ${String(err && err.message || err || 'unknown')}`;
  }
}

function startRefreshLoop() {
  if (state.refresh_timer) clearInterval(state.refresh_timer);
  state.refresh_timer = setInterval(() => {
    refreshNow(false);
  }, state.refresh_ms);
}

function requestRefresh() {
  if (!sendWs({ type: 'refresh' })) {
    refreshNow(true);
  }
}

function restoreCameraFocus(options = {}) {
  const cam = state.camera;
  if (!cam.focus_mode) return;
  const instant = Boolean(options && options.instant);
  const zoomTarget = clamp(cam.restore_zoom, cam.min_zoom, cam.max_zoom);
  const panXTarget = Number(cam.restore_pan_x || 0);
  const panYTarget = Number(cam.restore_pan_y || 0);
  startCameraTransition(zoomTarget, panXTarget, panYTarget, instant ? 1 : 170);
  cam.focus_mode = false;
  cam.focus_target_id = null;
  cam.map_mode = false;
  cam.map_owner_id = '';
}

function clearMapMode() {
  const cam = state.camera;
  cam.map_mode = false;
  cam.map_owner_id = '';
}

function clearModuleDepthReturns() {
  const cam = state.camera;
  cam.module_return_selection = null;
  cam.module_return_selection_for = '';
  cam.submodule_return_selection = null;
  cam.submodule_return_selection_for = '';
}

function centerOnSelectionNoZoom(nodeId) {
  const id = String(nodeId || '').trim();
  if (!id || !state.scene || !state.scene.node_by_id) return false;
  const node = state.scene.node_by_id[id];
  if (!node) return false;
  const cam = state.camera;
  syncCameraTransition(performance.now());
  if (!cam.focus_mode || String(cam.focus_target_id || '') !== id) {
    cam.restore_zoom = cam.zoom;
    cam.restore_pan_x = cam.pan_x;
    cam.restore_pan_y = cam.pan_y;
  }
  const z = clamp(Number(cam.zoom || 1), cam.min_zoom, cam.max_zoom);
  const panX = (state.width * 0.5) - (Number(node.x || 0) * z);
  const panY = (state.height * 0.5) - (Number(node.y || 0) * z);
  startCameraTransition(z, panX, panY, 145);
  cam.focus_mode = true;
  cam.focus_target_id = id;
  clearMapMode();
  return true;
}

function layerShellBounds(scene, layerId) {
  if (!scene || !layerId) return null;
  const id = String(layerId || '').trim();
  if (!id) return null;
  const layerNode = scene.node_by_id ? scene.node_by_id[id] : null;
  if (!layerNode || String(layerNode.type || '') !== 'layer') return null;
  const targets = Array.isArray(scene.hit_targets) ? scene.hit_targets : [];
  let inner = Infinity;
  let outer = 0;
  let cx = Number(layerNode.x || 0);
  let cy = Number(layerNode.y || 0);
  for (const t of targets) {
    if (!t || String(t.id || '') !== id) continue;
    const kind = String(t.kind || '').toLowerCase();
    if (kind !== 'ring' && kind !== 'shell_fill') continue;
    const tin = Number(t.inner || 0);
    const tout = Number(t.outer || 0);
    if (Number.isFinite(tin)) inner = Math.min(inner, Math.max(0, tin));
    if (Number.isFinite(tout)) outer = Math.max(outer, Math.max(0, tout));
    if (Number.isFinite(Number(t.cx))) cx = Number(t.cx);
    if (Number.isFinite(Number(t.cy))) cy = Number(t.cy);
  }
  if (!Number.isFinite(inner) || inner === Infinity) {
    inner = Math.max(0, Number(layerNode.radius || 0) - Math.max(8, Number(layerNode.ring_width || 12)));
  }
  if (!(outer > inner)) {
    outer = Math.max(inner + 1, Number(layerNode.radius || 0) + Math.max(16, Number(layerNode.ring_width || 12) * 1.6));
  }
  return { id, cx, cy, inner, outer };
}

function zoomIntoLayer(layerId) {
  const scene = state.scene;
  const bounds = layerShellBounds(scene, layerId);
  if (!bounds) return false;
  const cam = state.camera;
  syncCameraTransition(performance.now());
  const id = String(bounds.id || '');
  if (!cam.focus_mode || String(cam.focus_target_id || '') !== id) {
    cam.restore_zoom = cam.zoom;
    cam.restore_pan_x = cam.pan_x;
    cam.restore_pan_y = cam.pan_y;
  }
  const targetOuterScreenRadius = Math.max(150, Math.min(state.width, state.height) * 0.47);
  const shellOuter = Math.max(16, Number(bounds.outer || 0));
  const desiredZoom = targetOuterScreenRadius / shellOuter;
  const zoomTarget = clamp(Math.max(1.02, desiredZoom), cam.min_zoom, cam.max_zoom);
  const panX = (state.width * 0.5) - (Number(bounds.cx || 0) * zoomTarget);
  const panY = (state.height * 0.5) - (Number(bounds.cy || 0) * zoomTarget);
  startCameraTransition(zoomTarget, panX, panY, 175);
  cam.focus_mode = true;
  cam.focus_target_id = id;
  return true;
}

function zoomToSystemRoot() {
  const scene = state.scene;
  const bounds = sceneWorldBounds(scene);
  if (!scene || !bounds) return false;
  const cam = state.camera;
  syncCameraTransition(performance.now());
  const id = String(scene.system_id || SYSTEM_ROOT_ID);
  if (!cam.focus_mode || String(cam.focus_target_id || '') !== id) {
    cam.restore_zoom = cam.zoom;
    cam.restore_pan_x = cam.pan_x;
    cam.restore_pan_y = cam.pan_y;
  }
  const fill = 0.92;
  const zoomX = (Math.max(1, state.width) * fill) / Math.max(1, Number(bounds.width || 1));
  const zoomY = (Math.max(1, state.height) * fill) / Math.max(1, Number(bounds.height || 1));
  let zoomTarget = Math.min(zoomX, zoomY);
  if (!Number.isFinite(zoomTarget) || zoomTarget <= 0) return false;
  zoomTarget = clamp(zoomTarget, cam.min_zoom, cam.max_zoom);
  const panX = (state.width * 0.5) - (Number(bounds.cx || 0) * zoomTarget);
  const panY = (state.height * 0.5) - (Number(bounds.cy || 0) * zoomTarget);
  startCameraTransition(zoomTarget, panX, panY, 180);
  cam.focus_mode = true;
  cam.focus_target_id = id;
  clearMapMode();
  return true;
}

function selectionFocusBounds(scene, selectedId) {
  if (!scene || !scene.node_by_id || !selectedId) return null;
  const focus = computeSelectionFocus(scene, selectedId);
  const ids = focus && focus.nodes instanceof Set
    ? Array.from(focus.nodes)
    : [selectedId];
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  for (const rawId of ids) {
    const id = String(rawId || '').trim();
    if (!id) continue;
    const node = scene.node_by_id[id];
    if (!node) continue;
    const x = Number(node.x);
    const y = Number(node.y);
    if (!Number.isFinite(x) || !Number.isFinite(y)) continue;
    const type = String(node.type || '').toLowerCase();
    const radius = Math.max(2, Number(node.radius || 8));
    let pad = radius + 12;
    if (type === 'submodule') {
      pad = Math.max(pad, Number(node.shell_outer || (radius * 3.2)) + 8);
    } else if (type === 'layer') {
      pad = Math.max(pad, Number(node.ring_width || 14) + 24);
    } else if (type === 'module') {
      pad = Math.max(pad, radius + 16);
    } else if (type === 'spine') {
      pad = Math.max(pad, 22);
    }
    minX = Math.min(minX, x - pad);
    maxX = Math.max(maxX, x + pad);
    minY = Math.min(minY, y - pad);
    maxY = Math.max(maxY, y + pad);
  }
  if (!(maxX > minX) || !(maxY > minY)) return null;
  return {
    cx: (minX + maxX) * 0.5,
    cy: (minY + maxY) * 0.5,
    width: Math.max(24, maxX - minX),
    height: Math.max(24, maxY - minY)
  };
}

function zoomIntoSelectionMap(nodeId) {
  const id = String(nodeId || '').trim();
  if (!id || !state.scene || !state.scene.node_by_id) return false;
  const node = state.scene.node_by_id[id];
  if (!node) return false;
  const type = String(node.type || '').toLowerCase();
  if (type !== 'module' && type !== 'submodule') return false;
  const bounds = selectionFocusBounds(state.scene, id);
  if (!bounds) return false;

  const cam = state.camera;
  syncCameraTransition(performance.now());
  if (!cam.focus_mode || String(cam.focus_target_id || '') !== id) {
    cam.restore_zoom = cam.zoom;
    cam.restore_pan_x = cam.pan_x;
    cam.restore_pan_y = cam.pan_y;
  }

  const fill = 0.9;
  const zoomX = (Math.max(1, state.width) * fill) / Math.max(1, Number(bounds.width || 1));
  const zoomY = (Math.max(1, state.height) * fill) / Math.max(1, Number(bounds.height || 1));
  let zoomTarget = Math.min(zoomX, zoomY);
  if (!Number.isFinite(zoomTarget) || zoomTarget <= 0) return false;

  if (type === 'module') {
    zoomTarget = clamp(zoomTarget, Math.max(cam.min_zoom, 1.08), Math.min(cam.max_zoom, 2.3));
  } else {
    zoomTarget = clamp(zoomTarget, Math.max(cam.min_zoom, 1.15), Math.min(cam.max_zoom, 2.55));
  }
  const panX = (state.width * 0.5) - (Number(bounds.cx || 0) * zoomTarget);
  const panY = (state.height * 0.5) - (Number(bounds.cy || 0) * zoomTarget);
  startCameraTransition(zoomTarget, panX, panY, 165);
  cam.focus_mode = true;
  cam.focus_target_id = id;
  return true;
}

function enterMapModeForSelection(nodeId) {
  const id = String(nodeId || '').trim();
  if (!id || !state.scene || !state.scene.node_by_id) return false;
  const node = state.scene.node_by_id[id];
  if (!node) return false;
  const type = String(node.type || '').toLowerCase();
  if (type !== 'module' && type !== 'submodule') return false;
  const bounds = selectionFocusBounds(state.scene, id);
  if (!bounds) return false;

  const cam = state.camera;
  syncCameraTransition(performance.now());
  if (!cam.map_mode || String(cam.map_owner_id || '') !== id) {
    cam.map_return_zoom = cam.zoom;
    cam.map_return_pan_x = cam.pan_x;
    cam.map_return_pan_y = cam.pan_y;
  }

  const fill = 0.94;
  const fitZoomX = (Math.max(1, state.width) * fill) / Math.max(1, Number(bounds.width || 1));
  const fitZoomY = (Math.max(1, state.height) * fill) / Math.max(1, Number(bounds.height || 1));
  const fitZoom = Math.min(fitZoomX, fitZoomY);
  if (!Number.isFinite(fitZoom) || fitZoom <= 0) return false;

  const zoomTarget = clamp(
    Math.min(Number(cam.zoom || 1) * 0.95, fitZoom),
    cam.min_zoom,
    cam.max_zoom
  );
  const panX = (state.width * 0.5) - (Number(bounds.cx || 0) * zoomTarget);
  const panY = (state.height * 0.5) - (Number(bounds.cy || 0) * zoomTarget);
  startCameraTransition(zoomTarget, panX, panY, 150);
  cam.focus_mode = true;
  cam.focus_target_id = id;
  cam.map_mode = true;
  cam.map_owner_id = id;
  return true;
}

function exitMapModeToCenteredSelection() {
  const cam = state.camera;
  if (!cam.map_mode) return false;
  const ownerId = String(cam.map_owner_id || '').trim();
  if (!ownerId || !state.scene || !state.scene.node_by_id || !state.scene.node_by_id[ownerId]) {
    clearMapMode();
    return false;
  }
  const zoomTarget = clamp(Number(cam.map_return_zoom || cam.zoom), cam.min_zoom, cam.max_zoom);
  const panXTarget = Number(cam.map_return_pan_x || cam.pan_x);
  const panYTarget = Number(cam.map_return_pan_y || cam.pan_y);
  startCameraTransition(zoomTarget, panXTarget, panYTarget, 150);
  cam.focus_mode = true;
  cam.focus_target_id = ownerId;
  clearMapMode();
  return true;
}

function submoduleVisualExtent(node) {
  const n = node && typeof node === 'object' ? node : {};
  const inner = Math.max(1, Number(n.shell_inner || (Number(n.radius || 2) * 2.2)));
  const outer = Math.max(inner + 1, Number(n.shell_outer || (Number(n.radius || 2) * 3.1)));
  const start = Number(n.shell_start || 0);
  const end = Number(n.shell_end || (start + (Math.PI * 0.35)));
  const span = Math.max(0.05, Math.abs(end - start));
  const midR = (inner + outer) * 0.5;
  const arcLength = span * midR;
  const radialThickness = outer - inner;
  const nodeDiameter = Math.max(2, Number(n.radius || 2) * 2.4);
  return Math.max(3.5, nodeDiameter, radialThickness, arcLength);
}

function zoomIntoSubfractal(nodeId, selectedSubfractal) {
  const id = String(nodeId || '').trim();
  if (!id || !state.scene || !state.scene.node_by_id) return false;
  const node = state.scene.node_by_id[id];
  if (!node || String(node.type || '') !== 'submodule') return false;
  const extentInfo = subfractalSelectionExtent(node, selectedSubfractal);
  const anchor = subfractalAnchorForSelection(node, selectedSubfractal);
  if (!extentInfo || !anchor) return false;
  const cam = state.camera;
  const targetDiameter = Math.max(34, state.height * 0.10);
  const desiredZoom = targetDiameter / Math.max(1, Number(extentInfo.extent || 1));
  const zoomTarget = clamp(Math.max(1.2, desiredZoom), cam.min_zoom, cam.max_zoom);
  const panX = (state.width * 0.5) - (Number(anchor.x || 0) * zoomTarget);
  const panY = (state.height * 0.5) - (Number(anchor.y || 0) * zoomTarget);
  startCameraTransition(zoomTarget, panX, panY, 155, { allow_out_of_bounds: true });
  cam.focus_mode = true;
  cam.focus_target_id = id;
  clearMapMode();
  return true;
}

function zoomIntoSubmodule(nodeId, originSelection = null) {
  const id = String(nodeId || '').trim();
  if (!id || !state.scene || !state.scene.node_by_id) return false;
  const node = state.scene.node_by_id[id];
  if (!node || String(node.type || '') !== 'submodule') return false;
  const cam = state.camera;
  const origin = cloneSelectionRef(originSelection);
  if (!cam.focus_mode || String(cam.focus_target_id || '') !== id) {
    cam.restore_zoom = cam.zoom;
    cam.restore_pan_x = cam.pan_x;
    cam.restore_pan_y = cam.pan_y;
  }
  if (origin) {
    cam.submodule_return_selection = origin;
    cam.submodule_return_selection_for = id;
  } else if (cam.submodule_return_selection && String(cam.submodule_return_selection.id || '').trim()) {
    cam.submodule_return_selection_for = id;
  } else {
    cam.submodule_return_selection = null;
    cam.submodule_return_selection_for = '';
  }
  const fractalExtent = submoduleVisualExtent(node);
  const hasSubfractals = subfractalChildrenForNode(node).length > 0;
  const targetDiameter = hasSubfractals
    ? Math.max(34, state.height * 0.10)
    : Math.max(56, state.height * 0.2);
  const desiredZoom = targetDiameter / Math.max(1, fractalExtent);
  const zoomTarget = clamp(Math.max(1.08, desiredZoom), cam.min_zoom, cam.max_zoom);
  const panX = (state.width * 0.5) - (Number(node.x || 0) * zoomTarget);
  const panY = (state.height * 0.5) - (Number(node.y || 0) * zoomTarget);
  startCameraTransition(zoomTarget, panX, panY, 160, { allow_out_of_bounds: true });
  cam.focus_mode = true;
  cam.focus_target_id = id;
  clearMapMode();
  return true;
}

function zoomIntoModule(nodeId, originSelection = null) {
  const id = String(nodeId || '').trim();
  if (!id || !state.scene || !state.scene.node_by_id) return false;
  const node = state.scene.node_by_id[id];
  if (!node || String(node.type || '') !== 'module') return false;
  const cam = state.camera;
  const origin = cloneSelectionRef(originSelection);
  if (!cam.focus_mode || String(cam.focus_target_id || '') !== id) {
    cam.restore_zoom = cam.zoom;
    cam.restore_pan_x = cam.pan_x;
    cam.restore_pan_y = cam.pan_y;
  }
  if (origin) {
    cam.module_return_selection = origin;
    cam.module_return_selection_for = id;
  } else if (cam.module_return_selection && String(cam.module_return_selection.id || '').trim()) {
    cam.module_return_selection_for = id;
  } else {
    cam.module_return_selection = null;
    cam.module_return_selection_for = '';
  }
  const radius = Math.max(8, Number(node.radius || 12));
  const targetDiameter = Math.max(56, state.height * 0.2);
  const desiredZoom = targetDiameter / Math.max(2, radius * 2);
  const zoomTarget = clamp(Math.max(1.08, desiredZoom), cam.min_zoom, cam.max_zoom);
  const panX = (state.width * 0.5) - (Number(node.x || 0) * zoomTarget);
  const panY = (state.height * 0.5) - (Number(node.y || 0) * zoomTarget);
  startCameraTransition(zoomTarget, panX, panY, 160, { allow_out_of_bounds: true });
  cam.focus_mode = true;
  cam.focus_target_id = id;
  return true;
}

function onCanvasWheel(evt) {
  if (state.camera.focus_mode) restoreCameraFocus({ instant: true });
  evt.preventDefault();
  const rect = state.canvas.getBoundingClientRect();
  const sx = evt.clientX - rect.left;
  const sy = evt.clientY - rect.top;
  const factor = Math.exp(-evt.deltaY * 0.0012);
  setZoomAt(sx, sy, state.camera.zoom * factor);
}

function onCanvasMouseDown(evt) {
  const enablePan = evt.button === 0 || evt.button === 1 || evt.button === 2 || evt.shiftKey === true;
  if (!enablePan) return;
  evt.preventDefault();
  const cam = state.camera;
  cam.panning = true;
  cam.pan_started = false;
  cam.drag_px = 0;
  cam.last_x = evt.clientX;
  cam.last_y = evt.clientY;
  state.canvas.style.cursor = 'grab';
}

function onCanvasMouseMove(evt) {
  const cam = state.camera;
  if (cam.panning) {
    const dx = evt.clientX - cam.last_x;
    const dy = evt.clientY - cam.last_y;
    cam.last_x = evt.clientX;
    cam.last_y = evt.clientY;
    cam.drag_px += Math.abs(dx) + Math.abs(dy);
    if (!cam.pan_started && cam.drag_px > 5) {
      cam.pan_started = true;
      stopCameraTransition();
      if (cam.focus_mode) restoreCameraFocus({ instant: true });
      state.canvas.style.cursor = 'grabbing';
    }
    if (!cam.pan_started) return;
    cam.pan_x += dx;
    cam.pan_y += dy;
    clampCameraPanInPlace();
    state.hover = null;
    return;
  }
  const rect = state.canvas.getBoundingClientRect();
  const x = evt.clientX - rect.left;
  const y = evt.clientY - rect.top;
  updateHoverAtCanvasPoint(x, y);
}

function onCanvasMouseLeave() {
  state.hover = null;
  if (!state.camera.panning) state.canvas.style.cursor = 'grab';
}

function onCanvasMouseUp() {
  const cam = state.camera;
  cam.panning = false;
  cam.pan_started = false;
  state.canvas.style.cursor = 'grab';
  setTimeout(() => {
    cam.drag_px = 0;
  }, 0);
}

function boot() {
  state.canvas = byId('holoCanvas');
  state.ctx = state.canvas.getContext('2d', { alpha: true });
  state.canvas.style.cursor = 'grab';
  state.gpu = detectGpuTier();
  setQualityTier(state.gpu.tier);
  resizeCanvas();
  renderSelectionTag();
  renderCodegraphStatus();
  const liveModeEl = byId('liveMode');
  const liveMinutesEl = byId('liveMinutes');
  if (liveModeEl) liveModeEl.checked = state.live_mode !== false;
  if (liveMinutesEl) liveMinutesEl.value = String(state.live_minutes || 6);
  syncLiveControlState();
  const tabPreviewBtn = byId('tabPreview');
  const tabCodeBtn = byId('tabCode');
  const tabTerminalBtn = byId('tabTerminal');
  const statsGridEl = byId('statsGrid');
  const terminalInputEl = byId('terminalInput');
  const terminalRunBtn = byId('terminalRun');
  const terminalClearBtn = byId('terminalClear');
  const terminalAutoFollowEl = byId('terminalAutoFollow');
  if (tabPreviewBtn) {
    tabPreviewBtn.addEventListener('click', () => setPreviewTab('preview'));
  }
  if (tabCodeBtn) {
    tabCodeBtn.addEventListener('click', () => setPreviewTab('code'));
  }
  if (tabTerminalBtn) {
    tabTerminalBtn.addEventListener('click', () => setPreviewTab('terminal'));
  }
  if (statsGridEl) {
    statsGridEl.addEventListener('click', onStatsGridClick);
  }
  if (terminalInputEl) {
    terminalInputEl.addEventListener('keydown', (evt) => {
      if (evt.key !== 'Enter') return;
      evt.preventDefault();
      runTerminalCommand();
    });
  }
  if (terminalRunBtn) {
    terminalRunBtn.addEventListener('click', () => runTerminalCommand());
  }
  if (terminalClearBtn) {
    terminalClearBtn.addEventListener('click', () => clearTerminalOutput());
  }
  if (terminalAutoFollowEl) {
    terminalAutoFollowEl.checked = false;
    terminalAutoFollowEl.addEventListener('change', () => {
      setTerminalAutoFollow(terminalAutoFollowEl.checked === true);
    });
  }

  const queryInputEl = byId('queryInput');
  const queryRunEl = byId('queryRun');
  const queryReindexEl = byId('queryReindex');
  if (queryInputEl) {
    queryInputEl.addEventListener('keydown', (evt) => {
      if (evt.key !== 'Enter') return;
      evt.preventDefault();
      runCodegraphQuery();
    });
  }
  if (queryRunEl) {
    queryRunEl.addEventListener('click', () => runCodegraphQuery());
  }
  if (queryReindexEl) {
    queryReindexEl.addEventListener('click', () => reindexCodegraph());
  }

  byId('refresh').addEventListener('click', requestRefresh);
  byId('hours').addEventListener('change', () => {
    const hours = currentHours();
    const liveCfg = currentLiveConfig();
    state.live_mode = liveCfg.live_mode;
    state.live_minutes = liveCfg.live_minutes;
    if (!sendWs({ type: 'subscribe', hours, live_mode: liveCfg.live_mode, live_minutes: liveCfg.live_minutes })) {
      refreshNow(true);
    }
  });
  byId('liveMode').addEventListener('change', () => {
    syncLiveControlState();
    const hours = currentHours();
    const liveCfg = currentLiveConfig();
    state.live_mode = liveCfg.live_mode;
    state.live_minutes = liveCfg.live_minutes;
    if (!sendWs({ type: 'subscribe', hours, live_mode: liveCfg.live_mode, live_minutes: liveCfg.live_minutes })) {
      refreshNow(true);
    }
  });
  byId('liveMinutes').addEventListener('change', () => {
    const hours = currentHours();
    const liveCfg = currentLiveConfig();
    state.live_mode = liveCfg.live_mode;
    state.live_minutes = liveCfg.live_minutes;
    if (!sendWs({ type: 'subscribe', hours, live_mode: liveCfg.live_mode, live_minutes: liveCfg.live_minutes })) {
      refreshNow(true);
    }
  });
  state.canvas.addEventListener('click', onCanvasClick);
  state.canvas.addEventListener('wheel', onCanvasWheel, { passive: false });
  state.canvas.addEventListener('mousedown', onCanvasMouseDown);
  window.addEventListener('mousemove', onCanvasMouseMove);
  window.addEventListener('mouseup', onCanvasMouseUp);
  state.canvas.addEventListener('mouseleave', onCanvasMouseLeave);
  state.canvas.addEventListener('contextmenu', (evt) => evt.preventDefault());
  window.addEventListener('resize', () => {
    resizeCanvas();
    clampCameraPanInPlace();
    state.scene = buildScene(state.payload);
    clampCameraPanInPlace();
    applyCodegraphMatches(state.scene);
    applySelectionFocus(false);
    syncParticlePool(true);
    renderCodegraphStatus();
  });
  window.addEventListener('beforeunload', () => {
    if (state.ws_retry_timer) clearTimeout(state.ws_retry_timer);
    if (state.ws) {
      try { state.ws.close(); } catch {}
    }
  });

  refreshNow(true);
  connectWebSocket();
  startRefreshLoop();
  requestAnimationFrame(animate);
}

boot();
