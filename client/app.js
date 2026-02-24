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
    dpr_cap: 1
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
    dpr_cap: 1.35
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
    dpr_cap: 1.8
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
    dpr_cap: 2.2
  }
};

const BASE_ORANGE = [255, 159, 31];
const BASE_BLUE = [28, 140, 255];
const SPINE_NODE_ID = 'spine:core';
const SPINE_NODE_PATH = 'systems/spine';
const SYSTEM_ROOT_ID = 'system:root';
const WORKSPACE_ROOT_PATH = '/Users/jay/.openclaw/workspace';
const SHELL_ORDER_SWAP_COOLDOWN_MS = 10 * 60 * 1000;
const SHELL_INTRO_INITIAL_DELAY_MS = 110;
const SHELL_INTRO_LAYER_DELAY_MS = 70;
const SHELL_INTRO_LAYER_DURATION_MS = 240;

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
  selected_link: null,
  hover: null,
  refresh_ms: 6000,
  refresh_timer: null,
  last_frame_ts: 0,
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
  spine_event_count: 0,
  spine_event_top: '',
  spine_burst_until: 0,
  orbit_lock: {
    layer_id: '',
    runtime_by_layer: Object.create(null)
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
  particle_signature: '',
  camera: {
    zoom: 1,
    min_zoom: 0.72,
    max_zoom: 2.8,
    pan_x: 0,
    pan_y: 0,
    focus_mode: false,
    focus_target_id: null,
    restore_zoom: 1,
    restore_pan_x: 0,
    restore_pan_y: 0,
    panning: false,
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
  if (state.fps_smoothed < 23) {
    state.adaptive_downgrade_streak += 1;
    if (state.adaptive_downgrade_streak > 140) {
      const idx = QUALITY_LEVELS.indexOf(state.quality_tier);
      if (idx > 0) {
        setQualityTier(QUALITY_LEVELS[idx - 1]);
      }
      state.adaptive_downgrade_streak = 0;
    }
    return;
  }
  if (state.fps_smoothed > 52) {
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

function setZoomAt(screenX, screenY, nextZoom) {
  const cam = state.camera;
  const prevZoom = cam.zoom;
  const target = clamp(nextZoom, cam.min_zoom, cam.max_zoom);
  if (Math.abs(target - prevZoom) < 0.0005) return;
  const worldX = (screenX - cam.pan_x) / prevZoom;
  const worldY = (screenY - cam.pan_y) / prevZoom;
  cam.zoom = target;
  cam.pan_x = screenX - (worldX * target);
  cam.pan_y = screenY - (worldY * target);
}

async function fetchPayload(hours) {
  const h = Math.max(1, Number(hours || 24));
  const res = await fetch(`/api/holo?hours=${encodeURIComponent(String(h))}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`api_http_${res.status}`);
  return res.json();
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

function wsEndpoint(hours) {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${proto}//${window.location.host}/ws/holo?hours=${encodeURIComponent(String(Math.max(1, Number(hours || 24))))}`;
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
  if (summary && holo) {
    setPayload({
      ok: true,
      generated_at: msg.generated_at || new Date().toISOString(),
      summary,
      holo
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
  try {
    socket = new WebSocket(wsEndpoint(currentHours()));
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
    sendWs({ type: 'subscribe', hours: currentHours() });
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

function updateSceneMotion(scene, ts) {
  if (!scene || !Number.isFinite(ts)) return;
  const nodes = Array.isArray(scene.nodes) ? scene.nodes : [];
  const links = Array.isArray(scene.links) ? scene.links : [];
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
  let frozenLayerId = '';
  if (selectedType === 'layer') {
    frozenLayerId = String(selectedNode && selectedNode.id || '');
  } else if (selectedType === 'module') {
    frozenLayerId = String(selectedNode && selectedNode.parent_id || '');
  } else if (selectedType === 'submodule') {
    const parentModule = nodeById[String(selectedNode && selectedNode.parent_id || '')];
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
  const dtMs = clamp(ts - prevTs, 0, 80);
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
    if (!Number.isFinite(node.spin_runtime)) {
      node.spin_runtime = Number(node.spin_base || 0);
    }
    const spinSpeed = Number(node.spin_speed || 0);
    const freezeSpin = selectedId && selectedId === String(node.id || '');
    let spinAngle = Number(node.spin_runtime || node.spin_base || 0);
    if (freezeSpin) {
      node.spin_locked = true;
      node.spin_lock_angle = spinAngle;
    } else if (spinSpeed !== 0) {
      node.spin_locked = false;
      node.spin_runtime += dtMs * spinSpeed;
      if (Math.abs(node.spin_runtime) > 1e6) {
        node.spin_runtime = node.spin_runtime % (Math.PI * 2);
      }
      spinAngle = Number(node.spin_runtime || 0);
    }
    node.spin_angle = spinAngle;
    for (const childId of childIds) {
      const sub = nodeById[String(childId || '')];
      if (!sub) continue;
      const localStart = Number(sub.local_shell_start || 0);
      const localEnd = Number(sub.local_shell_end || 0);
      const shellMid = Number(sub.shell_mid || ((Number(sub.shell_inner || 0) + Number(sub.shell_outer || 0)) * 0.5));
      const start = orbitAngle + localStart + spinAngle;
      const end = orbitAngle + localEnd + spinAngle;
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

  for (const link of links) {
    const fromId = String(link && link.from_id || '');
    const toId = String(link && link.to_id || '');
    const from = nodeById[fromId];
    const to = nodeById[toId];
    if (!from || !to) continue;
    setLinkGeometry(link, from, to, center);
  }
}

function buildScene(payload) {
  const holo = payload && payload.holo && typeof payload.holo === 'object' ? payload.holo : null;
  if (!holo) return null;
  const summary = payload && payload.summary && typeof payload.summary === 'object' ? payload.summary : {};
  const errorSignal = computeErrorSignal(summary);
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
    error_hint: 0.1
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
    activity: 0.5
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
      const moduleBaseRadius = Math.max(10, Math.min(28, nominalRingStep * 0.62));
      const fractionScale = 1 + Math.min(0.62, fractalCount * 0.052);
      const moduleRadius = Math.max(10, Math.min(44, moduleBaseRadius * fractionScale));
      return {
        mod,
        subs,
        fractal_count: fractalCount,
        module_radius: moduleRadius
      };
    });
    const maxModuleRadius = preparedModules.reduce((acc, row) => Math.max(acc, Number(row.module_radius || 0)), 0);
    const requiredCirc = preparedModules.reduce(
      (acc, row) => acc + (Math.max(8, Number(row.module_radius || 0)) * 2) + 9,
      0
    );
    const minByCirc = requiredCirc > 0 ? (requiredCirc / (Math.PI * 2)) : 0;
    return {
      layer,
      index: li,
      prepared_modules: preparedModules,
      max_module_radius: maxModuleRadius,
      min_by_circ: minByCirc
    };
  });

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
    const layerOrbitDirection = (li % 2 === 0) ? 1 : -1;
    const layerOrbitSpeed = layerOrbitDirection * (0.000018 + (li * 0.000001));
    const layerNode = {
      id: layerId,
      type: 'layer',
      name: String(layer.name || layer.key || layerId),
      rel: layerRel,
      x: center.x,
      y: center.y,
      radius: layerRadius,
      ring_width: Math.max(13, nominalRingStep * 0.5),
      activity: clamp(layer.activity, 0, 1),
      error_hint: computeNodeErrorHint(layerId, layerRel, layer.name, layer.key)
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
      const moduleRadius = Number(modRow.module_radius || Math.max(10, Math.min(28, layerNode.ring_width * 0.62)));
      const x = center.x + (Math.cos(angle) * layerRadius);
      const y = center.y + (Math.sin(angle) * layerRadius);
      const modId = String(mod.id || `${layerId}/m${mi}`);
      const modRel = String(mod.rel || `${layerRel}/${String(mod.name || modId)}`);
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
        child_ids: [],
        activity: clamp(mod.activity, 0, 1),
        error_hint: computeNodeErrorHint(modId, modRel, mod.name, layerNode.name)
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
          activity: clamp(sub.activity, 0, 1),
          error_hint: computeNodeErrorHint(subId, subRel, sub.name, moduleNode.name)
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
      kind,
      arc_side: (stableHash(`${row.from}|${row.to}|${kind}`) % 2) ? 1 : -1
    };
    const fromHint = Number(from.error_hint || 0);
    const toHint = Number(to.error_hint || 0);
    const endpointHint = Math.max(fromHint, toHint);
    const kindHint = (kind === 'ingress' || kind === 'egress') ? 0.18 : 0;
    const channelHint = Math.max(endpointHint, kindHint);
    link.error_weight = channelHint > 0
      ? clamp(errorSignal * channelHint * (0.7 + (link.activity * 0.3)), 0, 1)
      : 0;
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
    error_signal: errorSignal,
    metrics: holo.metrics && typeof holo.metrics === 'object' ? holo.metrics : {}
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
    const p = {
      id: i,
      link_id: link.id,
      t: ((stableHash(`${link.id}:${i}`) % 1000) / 1000),
      speed: 0.045 + (link.activity * 0.12) + (((i % 7) * 0.004)),
      radius: 1.1 + ((i % 3) * 0.45),
      trail: []
    };
    particles.push(p);
  }
  state.particles = particles;
}

function visibleLinksForScene(scene) {
  if (!scene) return [];
  const linksAll = Array.isArray(scene.links) ? scene.links : [];
  const focusLinks = state.focus && state.focus.links instanceof Set ? state.focus.links : null;
  if (!focusLinks) return linksAll;
  return linksAll.filter((link) => focusLinks.has(link.id));
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
    for (const link of links) {
      const lid = String(link && link.id || '').trim();
      if (!lid) continue;
      focusLinks.add(lid);
      focusNodes.add(String(link.from_id || ''));
      focusNodes.add(String(link.to_id || ''));
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
    if (rebuild) syncParticlePool(true);
    return;
  }
  if (lid) {
    const link = state.scene.link_by_id && state.scene.link_by_id[lid]
      ? state.scene.link_by_id[lid]
      : null;
    if (link) {
      state.focus = {
        nodes: new Set([String(link.from_id || ''), String(link.to_id || '')]),
        links: new Set([String(link.id || '')])
      };
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
  state.payload = payload;
  state.scene = buildScene(payload);
  if (state.scene) primeShellIntro(state.scene, performance.now());
  applySelectionFocus(false);
  syncParticlePool(false);
  renderSelectionTag();
  renderStats();
}

function drawBackground(ts) {
  const ctx = state.ctx;
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
}

function drawLayerRing(layerNode, ts) {
  const ctx = state.ctx;
  const selected = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selected && selected.id || '');
  const selectedType = String(selected && selected.type || '').toLowerCase();
  const isSelected = selectedId
    && selectedId === String(layerNode.id || '')
    && (!selectedType || selectedType === 'layer');
  const introScale = clamp(Number(layerNode.intro_scale == null ? 1 : layerNode.intro_scale), 0, 1);
  const baseRadius = Number(layerNode.render_radius || layerNode.radius || 0);
  if (baseRadius <= 0.5) return;
  const jitter = Math.sin(ts * 0.00035 + layerNode.radius * 0.02) * 1.5 * introScale;
  const radius = Math.max(0.2, baseRadius + jitter);
  const alphaScale = 0.25 + (introScale * 0.75);
  const primaryAlpha = (0.2 + (layerNode.activity * 0.22)) * alphaScale;
  const secondaryAlpha = 0.11;
  const stroke = isSelected
    ? colorFromActivityBright(layerNode.activity, primaryAlpha + 0.09, 0.18)
    : colorFromActivity(layerNode.activity, primaryAlpha);
  ctx.strokeStyle = stroke;
  ctx.lineWidth = Math.max(0.8, layerNode.ring_width * 0.08 * (0.32 + (introScale * 0.68)));
  ctx.beginPath();
  ctx.arc(layerNode.x, layerNode.y, radius, 0, Math.PI * 2);
  ctx.stroke();

  ctx.strokeStyle = isSelected
    ? colorFromActivityBright(layerNode.activity, secondaryAlpha + 0.1, 0.2)
    : colorFromActivity(layerNode.activity, secondaryAlpha);
  ctx.lineWidth = Math.max(0.7, layerNode.ring_width * 0.03 * (0.35 + (introScale * 0.65)));
  for (let i = 0; i < 24; i += 1) {
    const a0 = (i / 24) * Math.PI * 2;
    const a1 = a0 + 0.12;
    ctx.beginPath();
    ctx.arc(layerNode.x, layerNode.y, radius + (layerNode.ring_width * 0.2 * (0.2 + (introScale * 0.8))), a0, a1);
    ctx.stroke();
  }
}

function drawSpineHub(scene, ts) {
  const ctx = state.ctx;
  const c = scene.center;
  const selected = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selected && selected.id || '');
  const selectedType = String(selected && selected.type || '').toLowerCase();
  const spineSelected = selectedId
    && selectedId === SPINE_NODE_ID
    && (!selectedType || selectedType === 'spine');
  const pulse = 1 + (Math.sin(ts * 0.00125) * 0.08);
  const drift = clamp(Number(scene.metrics && scene.metrics.drift_proxy || 0), 0, 1);
  const base = 13 * pulse;
  ctx.strokeStyle = spineSelected
    ? `rgba(${brightenChannel(110, 0.2)},${brightenChannel(203, 0.2)},${brightenChannel(255, 0.2)},0.86)`
    : 'rgba(110, 203, 255, 0.72)';
  ctx.lineWidth = 1.2;
  ctx.beginPath();
  ctx.arc(c.x, c.y, base, 0, Math.PI * 2);
  ctx.stroke();
  ctx.strokeStyle = spineSelected
    ? `rgba(${brightenChannel(110, 0.22)},${brightenChannel(203, 0.22)},${brightenChannel(255, 0.22)},0.5)`
    : 'rgba(110, 203, 255, 0.34)';
  ctx.beginPath();
  ctx.arc(c.x, c.y, base + 7, 0, Math.PI * 2);
  ctx.stroke();
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

function drawModuleNode(node, ts) {
  const ctx = state.ctx;
  const selected = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selected && selected.id || '');
  const selectedType = String(selected && selected.type || '').toLowerCase();
  const isSelected = selectedId
    && selectedId === String(node.id || '')
    && (!selectedType || selectedType === 'module');
  const introScale = nodeIntroScale(node);
  if (introScale <= 0.02) return;
  const glow = 0.17 + (node.activity * 0.35);
  const pulse = 0.88 + (Math.sin(ts * 0.001 + node.x * 0.01) * 0.1);
  const r = node.radius * pulse;
  const spinOffset = Number(node.fractal_count || 0) > 0 ? Number(node.spin_angle || 0) * 0.42 : 0;
  ctx.save();
  ctx.globalAlpha *= (0.2 + (introScale * 0.8));
  ctx.strokeStyle = isSelected
    ? colorFromActivityBright(node.activity, 0.82, 0.2)
    : colorFromActivity(node.activity, 0.65);
  ctx.lineWidth = 1.2;
  ctx.beginPath();
  ctx.arc(node.x, node.y, r, 0, Math.PI * 2);
  ctx.stroke();

  ctx.fillStyle = isSelected
    ? colorFromActivityBright(node.activity, glow * 0.35, 0.16)
    : colorFromActivity(node.activity, glow * 0.2);
  ctx.beginPath();
  ctx.arc(node.x, node.y, r * 0.72, 0, Math.PI * 2);
  ctx.fill();

  ctx.strokeStyle = isSelected
    ? colorFromActivityBright(node.activity, 0.56, 0.19)
    : colorFromActivity(node.activity, 0.36);
  ctx.lineWidth = 1;
  for (let i = 0; i < 3; i += 1) {
    const start = ((i / 3) * Math.PI * 2) + (ts * 0.00016) + (stableHash(node.id) % 10) * 0.07 + spinOffset;
    ctx.beginPath();
    ctx.arc(node.x, node.y, r * (0.82 + i * 0.09), start, start + 0.78);
    ctx.stroke();
  }
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
  const introScale = nodeIntroScale(node);
  if (introScale <= 0.02) return;
  ctx.save();
  ctx.globalAlpha *= (0.18 + (introScale * 0.82));
  if (state.quality_tier === 'low') {
    const s = Math.max(1.4, node.radius * 1.45);
    ctx.fillStyle = isSelected
      ? colorFromActivityBright(node.activity, 0.95, 0.18)
      : colorFromActivity(node.activity, 0.82);
    ctx.fillRect(node.x - s, node.y - s, s * 2, s * 2);
    ctx.restore();
    return;
  }
  const px = Number(node.parent_x || node.x);
  const py = Number(node.parent_y || node.y);
  const inner = Math.max(1, Number(node.shell_inner || (node.radius * 2.2)));
  const outer = Math.max(inner + 1, Number(node.shell_outer || (node.radius * 3.1)));
  const start = Number(node.shell_start || 0);
  const end = Number(node.shell_end || (Math.PI * 0.4));
  ctx.fillStyle = isSelected
    ? colorFromActivityBright(node.activity, 0.3, 0.15)
    : colorFromActivity(node.activity, 0.16 + (node.activity * 0.22));
  ctx.strokeStyle = isSelected
    ? colorFromActivityBright(node.activity, 0.86, 0.2)
    : colorFromActivity(node.activity, 0.68);
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.arc(px, py, outer, start, end);
  ctx.arc(px, py, inner, end, start, true);
  ctx.closePath();
  ctx.fill();
  ctx.stroke();

  if (state.quality_tier === 'high' || state.quality_tier === 'ultra') {
    const step = (end - start) / 3;
    ctx.strokeStyle = isSelected
      ? colorFromActivityBright(node.activity, 0.62, 0.18)
      : colorFromActivity(node.activity, 0.4);
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
  ctx.save();
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
  const profile = state.quality_profile;
  const focusLinks = state.focus && state.focus.links instanceof Set ? state.focus.links : null;
  const nodeById = scene.node_by_id && typeof scene.node_by_id === 'object'
    ? scene.node_by_id
    : Object.create(null);
  for (const link of scene.links) {
    if (focusLinks && !focusLinks.has(link.id)) continue;
    const fromNode = nodeById[String(link.from_id || '')];
    const toNode = nodeById[String(link.to_id || '')];
    const introScale = Math.min(nodeIntroScale(fromNode), nodeIntroScale(toNode));
    if (introScale <= 0.02) continue;
    const kind = String(link.kind || '').toLowerCase();
    if (kind === 'fractal') {
      const alpha = clamp((0.2 + (Number(link.activity || 0) * 0.35)) * (0.2 + introScale * 0.8), 0.12, 0.8);
      ctx.shadowBlur = 0;
      ctx.strokeStyle = `rgba(196,232,255,${alpha})`;
      ctx.lineWidth = (0.7 + (Number(link.activity || 0) * 0.95)) * (0.35 + (introScale * 0.65));
      ctx.beginPath();
      ctx.moveTo(link.p0.x, link.p0.y);
      ctx.bezierCurveTo(link.p1.x, link.p1.y, link.p2.x, link.p2.y, link.p3.x, link.p3.y);
      ctx.stroke();
      continue;
    }
    const errW = clamp(Number(link.error_weight || 0), 0, 1);
    const errActive = errW > 0.045;
    const alpha = (profile.tube_alpha + (link.activity * 0.12)) * (0.22 + (introScale * 0.78));
    if (state.quality_tier === 'high' || state.quality_tier === 'ultra') {
      if (errActive) {
        ctx.shadowColor = `rgba(255,78,78,${0.24 + (errW * 0.4)})`;
        ctx.shadowBlur = (8 + (errW * 22)) * (0.2 + (introScale * 0.8));
      } else {
        ctx.shadowColor = colorFromActivity(link.activity, 0.26);
        ctx.shadowBlur = (8 + (link.activity * 16)) * (0.2 + (introScale * 0.8));
      }
    } else {
      ctx.shadowBlur = 0;
    }
    if (errActive) {
      ctx.strokeStyle = `rgba(255,72,72,${clamp(0.14 + alpha + (errW * 0.55), 0.08, 0.96)})`;
      ctx.lineWidth = (0.95 + (link.activity * 1.1) + (errW * 2.2)) * (0.35 + (introScale * 0.65));
    } else {
      ctx.strokeStyle = colorFromActivity(link.activity, alpha);
      ctx.lineWidth = (0.8 + (link.activity * 1.8)) * (0.35 + (introScale * 0.65));
    }
    ctx.beginPath();
    ctx.moveTo(link.p0.x, link.p0.y);
    ctx.bezierCurveTo(link.p1.x, link.p1.y, link.p2.x, link.p2.y, link.p3.x, link.p3.y);
    ctx.stroke();
  }
  ctx.shadowBlur = 0;
}

function drawParticles(dt) {
  const scene = state.scene;
  if (!scene || !state.particles.length) return;
  const ctx = state.ctx;
  const trailLength = state.quality_profile.trail_length;
  const burstBoost = performance.now() < state.spine_burst_until ? 1.45 : 1;
  const focusLinks = state.focus && state.focus.links instanceof Set ? state.focus.links : null;
  const nodeById = scene.node_by_id && typeof scene.node_by_id === 'object'
    ? scene.node_by_id
    : Object.create(null);

  const linkById = {};
  for (const link of scene.links) linkById[link.id] = link;

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
    p.t += p.speed * Math.max(0.001, dt) * (0.5 + link.activity) * burstBoost;
    if (p.t > 1) p.t -= 1;
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
    if (trailLength <= 0 || p.trail.length < 2) continue;
    for (let i = 1; i < p.trail.length; i += 1) {
      const a = p.trail[i - 1];
      const b = p.trail[i];
      const alpha = (i / p.trail.length) * 0.3 * (0.2 + (introScale * 0.8));
      ctx.strokeStyle = `rgba(255,255,255,${alpha})`;
      ctx.lineWidth = 0.8;
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
    const pt = bezierPoint(link, p.t);
    ctx.fillStyle = `rgba(255,255,255,${(0.45 + (link.activity * 0.5)) * (0.25 + (introScale * 0.75))})`;
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, p.radius, 0, Math.PI * 2);
    ctx.fill();
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
  return 'Cross-module process path';
}

function renderStats() {
  const payload = state.payload || {};
  const summary = payload.summary && typeof payload.summary === 'object' ? payload.summary : {};
  const holoMetrics = payload.holo && payload.holo.metrics && typeof payload.holo.metrics === 'object'
    ? payload.holo.metrics
    : {};
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
  const titleEl = byId('statsTitle');
  let rows = [];

  if (selectedLink && scene && scene.node_by_id) {
    const fromNode = scene.node_by_id[String(selectedLink.from_id || '')];
    const toNode = scene.node_by_id[String(selectedLink.to_id || '')];
    const fromName = String((fromNode && fromNode.name) || selectedLink.from_id || 'unknown');
    const toName = String((toNode && toNode.name) || selectedLink.to_id || 'unknown');
    const fromPath = String((fromNode && fromNode.rel) || selectedLink.from_id || '');
    const toPath = String((toNode && toNode.rel) || selectedLink.to_id || '');
    const errWeight = clamp(Number(selectedLink.error_weight || 0), 0, 1);
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
      ['Activity', `${fmtNum(Number(selectedLink.activity || 0) * 100)}%`],
      ['Error Weight', `${fmtNum(errWeight * 100)}%`],
      ['Packet Count', fmtNum(selectedLink.count || 0)]
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
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'System Process Overview'],
      ['Scope', 'All shells + children'],
      ['Total Links', fmtNum(scene.links.length)],
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

    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'Shell Process Overview'],
      ['Shell', String(selectedNode.name || layerId)],
      ['Shell Path', String(selectedNode.rel || layerId)],
      ['Modules', fmtNum(moduleIds.length)],
      ['Fractals', fmtNum(submoduleIds.length)],
      ['Total Child Links', fmtNum(relevantLinks.length)],
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
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'Module Process Overview'],
      ['Module', String(selectedNode.name || moduleId)],
      ['Module Path', String(selectedNode.rel || moduleId)],
      ['Fractals', fmtNum(submoduleIds.length)],
      ['Total Child Links', fmtNum(relevantLinks.length)],
      ...processRows
    ];
    if (!processRows.length) rows.push(['Process/none', '0']);
  } else if (selectedType === 'submodule' && scene && Array.isArray(scene.nodes) && Array.isArray(scene.links)) {
    const submoduleId = String(selectedNode.id || '');
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
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'Fractal Process Overview'],
      ['Fractal', String(selectedNode.name || submoduleId)],
      ['Fractal Path', String(selectedNode.rel || submoduleId)],
      ['Module', String((moduleNode && moduleNode.name) || moduleId || 'n/a')],
      ['Shell', String((layerNode && layerNode.name) || layerId || 'n/a')],
      ['Direct Links', fmtNum(relevantLinks.length)],
      ...processRows
    ];
    if (!processRows.length) rows.push(['Process/none', '0']);
  } else {
    if (titleEl) titleEl.textContent = 'Preview Pane';
    rows = [
      ['Mode', 'System Overview'],
      ['GPU Tier', gpuLabel],
      ['FPS', fmtNum(state.fps_smoothed)],
      ['Run Events', fmtNum(summary.run_events)],
      ['Executed', fmtNum(summary.executed)],
      ['Shipped', fmtNum(summary.shipped)],
      ['Policy Holds', fmtNum(summary.policy_holds)],
      ['Error Signal', `${fmtNum(errorSignal * 100)}%`],
      ['Yield', `${fmtNum(Number(holoMetrics.yield_rate || 0) * 100)}%`],
      ['Drift Proxy', `${fmtNum(Number(holoMetrics.drift_proxy || 0) * 100)}%`],
      ['Layer Nodes', fmtNum(nodeCount)],
      ['Links', fmtNum(linkCount)]
    ];
  }
  byId('statsGrid').innerHTML = rows.map(([k, v]) => (
    `<div class="item"><div class="k">${k}</div><div class="v">${v}</div></div>`
  )).join('');
  const pulseSuffix = state.spine_event_top
    ? ` | spine ${fmtNum(state.spine_event_count)} evt (${state.spine_event_top})`
    : ` | spine ${fmtNum(state.spine_event_count)} evt`;
  const linkPreviewSuffix = selectedLink ? ` | preview: ${String(selectedLink.from_id || '?')} -> ${String(selectedLink.to_id || '?')}` : '';
  byId('metaLine').textContent = `Updated ${new Date(payload.generated_at || Date.now()).toLocaleString()} | ${state.transport} | zoom ${fmtNum(state.camera.zoom)}x | fallback ${Math.round(state.refresh_ms / 1000)}s${pulseSuffix}${linkPreviewSuffix}`;
}

function renderSelectionTag() {
  const selectedTag = byId('selectedTag');
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

function hitTest(x, y) {
  const scene = state.scene;
  if (!scene) return null;
  const world = screenToWorld(x, y);
  const center = scene.center && typeof scene.center === 'object'
    ? scene.center
    : { x: state.width * 0.52, y: state.height * 0.5 };
  let circleFound = null;
  let circleBest = Infinity;
  let zoneFound = null;
  let zoneBest = Infinity;
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
  }
  const found = circleFound || zoneFound;
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
  if (!sid) return false;
  const node = scene.node_by_id[sid];
  if (!node) return false;
  const type = String(node.type || '').toLowerCase();
  return type === 'module' || type === 'submodule';
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
  const node = interactionNodeFromHit(scene, hit);
  if (!node) return hit;
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const sid = String(selectedRef && selectedRef.id || '').trim();
  const selectedNode = sid && scene.node_by_id ? scene.node_by_id[sid] : null;
  const selectedTypeHint = String(selectedRef && selectedRef.type || '').toLowerCase();
  const selectedType = selectedTypeHint || String((selectedNode && selectedNode.type) || '').toLowerCase();
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
    const selectedLayerId = String(selectedNode.id || '');
    const shellZoomed = isLayerFocusZoomed(scene, selectedLayerId);
    if (!shellZoomed) {
      if (nodeType === 'module' || nodeType === 'submodule') return mapToLayer();
      return hit;
    }
    if (nodeType === 'module' && String(node.parent_id || '') !== selectedLayerId) {
      return mapToLayer();
    }
    if (nodeType === 'submodule') {
      const parentId = String(node.parent_id || '');
      const parentNode = scene.node_by_id ? scene.node_by_id[parentId] : null;
      if (parentNode && String(parentNode.parent_id || '') === String(selectedNode.id || '')) {
        return hitTargetFromNode(scene, parentId, 'circle') || hit;
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
  const isShellHit = hitKind === 'ring' || hitKind === 'shell_fill';
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const sid = String(selectedRef && selectedRef.id || '').trim();
  const selectedNode = sid && scene.node_by_id ? scene.node_by_id[sid] : null;
  const selectedTypeHint = String(selectedRef && selectedRef.type || '').toLowerCase();
  const selectedType = selectedTypeHint || String((selectedNode && selectedNode.type) || '').toLowerCase();
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
    const selectedLayerId = String(selectedNode.id || '');
    const shellZoomed = isLayerFocusZoomed(scene, selectedLayerId);
    if (targetType === 'spine') return true;
    if (isShellHit && targetType === 'layer') return true;
    if (!shellZoomed) return false;
    return targetType === 'module' && String(targetNode.parent_id || '') === selectedLayerId;
  }

  if (selectedType === 'module') {
    const selectedLayerId = String(selectedNode.parent_id || '');
    if (targetType === 'spine') return true;
    if (isShellHit && targetType === 'layer') return true;
    if (targetType === 'module') return String(targetNode.parent_id || '') === selectedLayerId;
    if (targetType === 'submodule') return String(targetNode.parent_id || '') === String(selectedNode.id || '');
    return false;
  }

  if (selectedType === 'submodule') {
    const parentModuleId = String(selectedNode.parent_id || '');
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
  const rect = state.canvas.getBoundingClientRect();
  const x = evt.clientX - rect.left;
  const y = evt.clientY - rect.top;
  const scene = state.scene;
  const selectedRef = state.selected && typeof state.selected === 'object' ? state.selected : null;
  const selectedId = String(selectedRef && selectedRef.id || '').trim();
  const selectedNode = selectedId && scene && scene.node_by_id ? scene.node_by_id[selectedId] : null;
  const selectedTypeHint = String(selectedRef && selectedRef.type || '').toLowerCase();
  const selectedType = selectedTypeHint || String((selectedNode && selectedNode.type) || '').toLowerCase();
  const rawHit = hitTest(x, y);
  const normalizedHit = rawHit ? normalizeHitForCurrentLevel(scene, rawHit) : null;
  const hit = normalizedHit && isHitAllowedForCurrentLevel(scene, normalizedHit) ? normalizedHit : null;
  const linkHit = linksInteractiveForSelection() ? hitTestLink(x, y, 12) : null;
  const preferLine = shouldPreferLinkSelection(scene, hit, linkHit);
  const activeFocusId = String(state.camera.focus_target_id || '');
  const activeFocusNode = activeFocusId && scene && scene.node_by_id
    ? scene.node_by_id[activeFocusId]
    : null;
  const activeFocusType = String((activeFocusNode && activeFocusNode.type) || '').toLowerCase();
  if (rawHit && !hit && !linkHit) {
    if (state.camera.focus_mode && activeFocusType === 'layer') {
      const rawNode = interactionNodeFromHit(scene, rawHit);
      const rawHitKind = String(rawHit.kind || '').toLowerCase();
      const rawLayerId = rawNode
        ? String(layerIdForNode(scene, rawNode) || '')
        : ((rawHitKind === 'ring' || rawHitKind === 'shell_fill') ? String(rawHit.id || '') : '');
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
  const hitId = String(hit && hit.id || '');
  const hitNode = hitId && scene && scene.node_by_id ? scene.node_by_id[hitId] : null;
  const hitType = String((hitNode && hitNode.type) || '').toLowerCase();
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
  if ((clickedDifferentLayerWhileFocusedLayer || (!clickingFocusedModule && !linePreviewClick)) && state.camera.focus_mode) {
    restoreCameraFocus();
  }
  const suppressAutoLayerZoom = clickedDifferentLayerWhileFocusedLayer;

  if (hit && !preferLine) {
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
      const selectedSubmoduleParentId = selectedNode ? String(selectedNode.parent_id || '') : '';
      const allowDirectSubmodule = (
        (selectedType === 'module' && selectedId === parentId)
        || (selectedType === 'submodule' && selectedSubmoduleParentId === parentId)
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
        zoomIntoModule(targetNode.id);
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
        zoomIntoSubmodule(targetNode.id);
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
    state.selected_link = null;
  } else {
    if (linkHit) {
      const sameLink = state.selected_link && String(state.selected_link.id) === String(linkHit.id);
      state.selected_link = sameLink ? null : {
        id: linkHit.id,
        name: linkHit.name,
        path: linkHit.path || '',
        kind: 'link',
        from_id: linkHit.from_id,
        to_id: linkHit.to_id
      };
    } else {
      state.selected = null;
      state.selected_link = null;
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
  if (!state.last_frame_ts) state.last_frame_ts = ts;
  const dtMs = Math.max(1, ts - state.last_frame_ts);
  state.last_frame_ts = ts;
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
  try {
    const payload = await fetchPayload(hours);
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

function restoreCameraFocus() {
  const cam = state.camera;
  if (!cam.focus_mode) return;
  cam.zoom = clamp(cam.restore_zoom, cam.min_zoom, cam.max_zoom);
  cam.pan_x = Number(cam.restore_pan_x || 0);
  cam.pan_y = Number(cam.restore_pan_y || 0);
  cam.focus_mode = false;
  cam.focus_target_id = null;
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
  cam.zoom = zoomTarget;
  cam.pan_x = (state.width * 0.5) - (Number(bounds.cx || 0) * zoomTarget);
  cam.pan_y = (state.height * 0.5) - (Number(bounds.cy || 0) * zoomTarget);
  cam.focus_mode = true;
  cam.focus_target_id = id;
  return true;
}

function zoomIntoSubmodule(nodeId) {
  const id = String(nodeId || '').trim();
  if (!id || !state.scene || !state.scene.node_by_id) return false;
  const node = state.scene.node_by_id[id];
  if (!node || String(node.type || '') !== 'submodule') return false;
  const cam = state.camera;
  if (!cam.focus_mode || String(cam.focus_target_id || '') !== id) {
    cam.restore_zoom = cam.zoom;
    cam.restore_pan_x = cam.pan_x;
    cam.restore_pan_y = cam.pan_y;
  }
  const shellOuter = Math.max(6, Number(node.shell_outer || (Number(node.radius || 4) * 3.2)));
  const zoomTarget = clamp(Math.max(2.1, 220 / (shellOuter * 2)), cam.min_zoom, cam.max_zoom);
  cam.zoom = zoomTarget;
  cam.pan_x = (state.width * 0.5) - (Number(node.x || 0) * zoomTarget);
  cam.pan_y = (state.height * 0.5) - (Number(node.y || 0) * zoomTarget);
  cam.focus_mode = true;
  cam.focus_target_id = id;
  return true;
}

function zoomIntoModule(nodeId) {
  const id = String(nodeId || '').trim();
  if (!id || !state.scene || !state.scene.node_by_id) return false;
  const node = state.scene.node_by_id[id];
  if (!node || String(node.type || '') !== 'module') return false;
  const cam = state.camera;
  if (!cam.focus_mode || String(cam.focus_target_id || '') !== id) {
    cam.restore_zoom = cam.zoom;
    cam.restore_pan_x = cam.pan_x;
    cam.restore_pan_y = cam.pan_y;
  }
  const radius = Math.max(8, Number(node.radius || 12));
  const zoomTarget = clamp(Math.max(1.85, 210 / (radius * 2)), cam.min_zoom, cam.max_zoom);
  cam.zoom = zoomTarget;
  cam.pan_x = (state.width * 0.5) - (Number(node.x || 0) * zoomTarget);
  cam.pan_y = (state.height * 0.5) - (Number(node.y || 0) * zoomTarget);
  cam.focus_mode = true;
  cam.focus_target_id = id;
  return true;
}

function onCanvasWheel(evt) {
  if (state.camera.focus_mode) restoreCameraFocus();
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
  if (state.camera.focus_mode) restoreCameraFocus();
  evt.preventDefault();
  const cam = state.camera;
  cam.panning = true;
  cam.drag_px = 0;
  cam.last_x = evt.clientX;
  cam.last_y = evt.clientY;
  state.canvas.style.cursor = 'grabbing';
}

function onCanvasMouseMove(evt) {
  const cam = state.camera;
  if (cam.panning) {
    const dx = evt.clientX - cam.last_x;
    const dy = evt.clientY - cam.last_y;
    cam.last_x = evt.clientX;
    cam.last_y = evt.clientY;
    cam.pan_x += dx;
    cam.pan_y += dy;
    cam.drag_px += Math.abs(dx) + Math.abs(dy);
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

  byId('refresh').addEventListener('click', requestRefresh);
  byId('hours').addEventListener('change', () => {
    const hours = currentHours();
    if (!sendWs({ type: 'subscribe', hours })) {
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
    state.scene = buildScene(state.payload);
    applySelectionFocus(false);
    syncParticlePool(true);
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
