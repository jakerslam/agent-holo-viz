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
  spine_burst_until: 0
};

function byId(id) {
  return document.getElementById(id);
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

function colorFromActivity(activity, alpha = 1) {
  const t = clamp(activity, 0, 1);
  const r = Math.round(mix(BASE_ORANGE[0], BASE_BLUE[0], t));
  const g = Math.round(mix(BASE_ORANGE[1], BASE_BLUE[1], t));
  const b = Math.round(mix(BASE_ORANGE[2], BASE_BLUE[2], t));
  return `rgba(${r},${g},${b},${clamp(alpha, 0, 1)})`;
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

function buildScene(payload) {
  const holo = payload && payload.holo && typeof payload.holo === 'object' ? payload.holo : null;
  if (!holo) return null;
  const profile = state.quality_profile;
  const layersRaw = Array.isArray(holo.layers) ? holo.layers : [];
  const layerRows = layersRaw.slice(0, profile.max_layers);
  const ioIn = Array.isArray(holo.io && holo.io.inputs) ? holo.io.inputs : [];
  const ioOut = Array.isArray(holo.io && holo.io.outputs) ? holo.io.outputs : [];

  const center = { x: state.width * 0.52, y: state.height * 0.5 };
  const outerRadius = Math.min(state.width, state.height) * 0.43;
  const minRing = Math.max(50, outerRadius * 0.2);
  const ringStep = (outerRadius - minRing) / Math.max(1, layerRows.length + 1);

  const nodes = [];
  const hitTargets = [];
  const posById = {};

  const addCircleHitTarget = (id, name, x, y, r) => {
    hitTargets.push({
      id,
      name,
      kind: 'circle',
      x,
      y,
      r
    });
  };

  const addRingHitTarget = (id, name, cx, cy, inner, outer) => {
    hitTargets.push({
      id,
      name,
      kind: 'ring',
      cx,
      cy,
      inner: Math.max(0, inner),
      outer: Math.max(0, outer)
    });
  };

  for (let li = 0; li < layerRows.length; li += 1) {
    const layer = layerRows[li];
    const layerRadius = outerRadius - (li * ringStep);
    const layerId = String(layer.id || `layer:${li}`);
    const layerNode = {
      id: layerId,
      type: 'layer',
      name: String(layer.name || layer.key || layerId),
      x: center.x,
      y: center.y,
      radius: layerRadius,
      ring_width: Math.max(14, ringStep * 0.44),
      activity: clamp(layer.activity, 0, 1)
    };
    nodes.push(layerNode);
    posById[layerNode.id] = { x: layerNode.x, y: layerNode.y, r: layerNode.radius * 0.85, activity: layerNode.activity };
    addRingHitTarget(
      layerNode.id,
      `Layer / ${layerNode.name}`,
      layerNode.x,
      layerNode.y,
      layerNode.radius - (layerNode.ring_width * 0.9),
      layerNode.radius + (layerNode.ring_width * 0.9)
    );

    const modules = Array.isArray(layer.modules) ? layer.modules.slice(0, profile.max_modules_per_layer) : [];
    const count = Math.max(1, modules.length);
    const phase = ((stableHash(layerId) % 1000) / 1000) * (Math.PI * 2);
    for (let mi = 0; mi < modules.length; mi += 1) {
      const mod = modules[mi];
      const angle = phase + ((mi / count) * Math.PI * 2);
      const moduleRadius = Math.max(11, Math.min(30, layerNode.ring_width * 0.65));
      const x = center.x + (Math.cos(angle) * layerRadius);
      const y = center.y + (Math.sin(angle) * layerRadius);
      const modId = String(mod.id || `${layerId}/m${mi}`);
      const moduleNode = {
        id: modId,
        parent_id: layerId,
        type: 'module',
        name: String(mod.name || modId),
        x,
        y,
        radius: moduleRadius,
        angle,
        activity: clamp(mod.activity, 0, 1)
      };
      nodes.push(moduleNode);
      addCircleHitTarget(moduleNode.id, `${layerNode.name} / ${moduleNode.name}`, moduleNode.x, moduleNode.y, moduleNode.radius + 5);
      posById[moduleNode.id] = { x: moduleNode.x, y: moduleNode.y, r: moduleNode.radius, activity: moduleNode.activity };

      const subs = Array.isArray(mod.submodules)
        ? mod.submodules.slice(0, profile.max_submodules_per_module)
        : [];
      const shellOuter = moduleRadius * 1.12;
      const shellInner = moduleRadius * 0.74;
      const subSpan = (Math.PI * 2) / Math.max(1, subs.length);
      for (let si = 0; si < subs.length; si += 1) {
        const sub = subs[si];
        const start = angle + (si * subSpan) + 0.06;
        const end = start + (subSpan * 0.68);
        const mid = (start + end) * 0.5;
        const shellMid = (shellInner + shellOuter) * 0.5;
        const sx = x + (Math.cos(mid) * shellMid);
        const sy = y + (Math.sin(mid) * shellMid);
        const subId = String(sub.id || `${modId}/s${si}`);
        const subNode = {
          id: subId,
          parent_id: modId,
          type: 'submodule',
          name: String(sub.name || subId),
          x: sx,
          y: sy,
          radius: Math.max(2, moduleRadius * 0.15),
          parent_x: x,
          parent_y: y,
          shell_inner: shellInner,
          shell_outer: shellOuter,
          shell_start: start,
          shell_end: end,
          activity: clamp(sub.activity, 0, 1)
        };
        nodes.push(subNode);
        posById[subNode.id] = { x: subNode.x, y: subNode.y, r: subNode.radius, activity: subNode.activity };
        addCircleHitTarget(
          subNode.id,
          `${layerNode.name} / ${moduleNode.name} / ${subNode.name}`,
          subNode.x,
          subNode.y,
          Math.max(4, subNode.radius + 3)
        );
      }
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
      x,
      y,
      radius: 7,
      angle,
      activity: clamp(row.activity, 0, 1),
      count: Number(row.count || 0)
    };
    ioNodes.push(node);
    posById[node.id] = { x: node.x, y: node.y, r: node.radius, activity: node.activity };
    addCircleHitTarget(node.id, `${row.io_type === 'input' ? 'Input' : 'Output'} / ${node.name}`, node.x, node.y, node.radius + 5);
  }

  const rawLinks = Array.isArray(holo.links) ? holo.links : [];
  const linkRows = [];
  for (const row of rawLinks) {
    const from = posById[String(row.from || '')];
    const to = posById[String(row.to || '')];
    if (!from || !to) continue;
    const dx = to.x - from.x;
    const dy = to.y - from.y;
    const dist = Math.max(1, Math.hypot(dx, dy));
    const bend = Math.min(160, Math.max(40, dist * 0.28));
    const nx = -dy / dist;
    const ny = dx / dist;
    const p0 = { x: from.x, y: from.y };
    const p3 = { x: to.x, y: to.y };
    const p1 = { x: from.x + (dx * 0.33) + (nx * bend), y: from.y + (dy * 0.33) + (ny * bend) };
    const p2 = { x: from.x + (dx * 0.66) + (nx * bend), y: from.y + (dy * 0.66) + (ny * bend) };
    linkRows.push({
      id: `${row.from}|${row.to}|${row.kind || 'flow'}`,
      from_id: String(row.from),
      to_id: String(row.to),
      p0, p1, p2, p3,
      activity: clamp(row.activity, 0, 1),
      count: Number(row.count || 0),
      kind: String(row.kind || 'flow')
    });
  }

  return {
    center,
    layers: layerRows,
    nodes,
    io_nodes: ioNodes,
    links: linkRows,
    hit_targets: hitTargets,
    metrics: holo.metrics && typeof holo.metrics === 'object' ? holo.metrics : {}
  };
}

function rebuildParticles() {
  const scene = state.scene;
  if (!scene) return;
  const profile = state.quality_profile;
  const links = scene.links || [];
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

function setPayload(payload) {
  state.payload = payload;
  state.scene = buildScene(payload);
  rebuildParticles();
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
  const jitter = Math.sin(ts * 0.00035 + layerNode.radius * 0.02) * 1.5;
  const radius = layerNode.radius + jitter;
  const stroke = colorFromActivity(layerNode.activity, 0.2 + (layerNode.activity * 0.22));
  ctx.strokeStyle = stroke;
  ctx.lineWidth = Math.max(1, layerNode.ring_width * 0.08);
  ctx.beginPath();
  ctx.arc(layerNode.x, layerNode.y, radius, 0, Math.PI * 2);
  ctx.stroke();

  ctx.strokeStyle = colorFromActivity(layerNode.activity, 0.11);
  ctx.lineWidth = Math.max(1, layerNode.ring_width * 0.03);
  for (let i = 0; i < 24; i += 1) {
    const a0 = (i / 24) * Math.PI * 2;
    const a1 = a0 + 0.12;
    ctx.beginPath();
    ctx.arc(layerNode.x, layerNode.y, radius + (layerNode.ring_width * 0.2), a0, a1);
    ctx.stroke();
  }
}

function drawModuleNode(node, ts) {
  const ctx = state.ctx;
  const glow = 0.17 + (node.activity * 0.35);
  const pulse = 0.88 + (Math.sin(ts * 0.001 + node.x * 0.01) * 0.1);
  const r = node.radius * pulse;
  ctx.strokeStyle = colorFromActivity(node.activity, 0.65);
  ctx.lineWidth = 1.2;
  ctx.beginPath();
  ctx.arc(node.x, node.y, r, 0, Math.PI * 2);
  ctx.stroke();

  ctx.fillStyle = colorFromActivity(node.activity, glow * 0.2);
  ctx.beginPath();
  ctx.arc(node.x, node.y, r * 0.72, 0, Math.PI * 2);
  ctx.fill();

  ctx.strokeStyle = colorFromActivity(node.activity, 0.36);
  ctx.lineWidth = 1;
  for (let i = 0; i < 3; i += 1) {
    const start = ((i / 3) * Math.PI * 2) + (ts * 0.00016) + (stableHash(node.id) % 10) * 0.07;
    ctx.beginPath();
    ctx.arc(node.x, node.y, r * (0.82 + i * 0.09), start, start + 0.78);
    ctx.stroke();
  }
}

function drawSubmoduleNode(node) {
  const ctx = state.ctx;
  if (state.quality_tier === 'low') {
    const s = Math.max(1.4, node.radius * 1.45);
    ctx.fillStyle = colorFromActivity(node.activity, 0.82);
    ctx.fillRect(node.x - s, node.y - s, s * 2, s * 2);
    return;
  }
  const px = Number(node.parent_x || node.x);
  const py = Number(node.parent_y || node.y);
  const inner = Math.max(1, Number(node.shell_inner || (node.radius * 2.2)));
  const outer = Math.max(inner + 1, Number(node.shell_outer || (node.radius * 3.1)));
  const start = Number(node.shell_start || 0);
  const end = Number(node.shell_end || (Math.PI * 0.4));
  ctx.fillStyle = colorFromActivity(node.activity, 0.16 + (node.activity * 0.22));
  ctx.strokeStyle = colorFromActivity(node.activity, 0.68);
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.arc(px, py, outer, start, end);
  ctx.arc(px, py, inner, end, start, true);
  ctx.closePath();
  ctx.fill();
  ctx.stroke();

  if (state.quality_tier === 'high' || state.quality_tier === 'ultra') {
    const step = (end - start) / 3;
    ctx.strokeStyle = colorFromActivity(node.activity, 0.4);
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
  for (const link of scene.links) {
    const alpha = profile.tube_alpha + (link.activity * 0.12);
    if (state.quality_tier === 'high' || state.quality_tier === 'ultra') {
      ctx.shadowColor = colorFromActivity(link.activity, 0.26);
      ctx.shadowBlur = 8 + (link.activity * 16);
    } else {
      ctx.shadowBlur = 0;
    }
    ctx.strokeStyle = colorFromActivity(link.activity, alpha);
    ctx.lineWidth = 0.8 + (link.activity * 1.8);
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

  const linkById = {};
  for (const link of scene.links) linkById[link.id] = link;

  for (const p of state.particles) {
    const link = linkById[p.link_id];
    if (!link) continue;
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
    if (trailLength <= 0 || p.trail.length < 2) continue;
    for (let i = 1; i < p.trail.length; i += 1) {
      const a = p.trail[i - 1];
      const b = p.trail[i];
      const alpha = (i / p.trail.length) * 0.3;
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
    const pt = bezierPoint(link, p.t);
    ctx.fillStyle = `rgba(255,255,255,${0.45 + (link.activity * 0.5)})`;
    ctx.beginPath();
    ctx.arc(pt.x, pt.y, p.radius, 0, Math.PI * 2);
    ctx.fill();
  }
}

function drawScene(ts) {
  const scene = state.scene;
  if (!scene) return;
  drawBackground(ts);

  const layerNodes = scene.nodes.filter((n) => n.type === 'layer');
  for (const layerNode of layerNodes) drawLayerRing(layerNode, ts);

  drawLinks(scene);
  drawParticles(1 / Math.max(1, state.fps_smoothed));

  for (const node of scene.nodes) {
    if (node.type === 'module') drawModuleNode(node, ts);
    else if (node.type === 'submodule') drawSubmoduleNode(node);
  }
  for (const node of scene.io_nodes || []) drawIoNode(node, ts);
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
  const gpuLabel = state.quality_profile.label;

  const rows = [
    ['GPU Tier', gpuLabel],
    ['FPS', fmtNum(state.fps_smoothed)],
    ['Run Events', fmtNum(summary.run_events)],
    ['Executed', fmtNum(summary.executed)],
    ['Shipped', fmtNum(summary.shipped)],
    ['Policy Holds', fmtNum(summary.policy_holds)],
    ['Yield', `${fmtNum(Number(holoMetrics.yield_rate || 0) * 100)}%`],
    ['Drift Proxy', `${fmtNum(Number(holoMetrics.drift_proxy || 0) * 100)}%`],
    ['Layer Nodes', fmtNum(nodeCount)],
    ['Links', fmtNum(linkCount)]
  ];
  byId('statsGrid').innerHTML = rows.map(([k, v]) => (
    `<div class="item"><div class="k">${k}</div><div class="v">${v}</div></div>`
  )).join('');
  const pulseSuffix = state.spine_event_top
    ? ` | spine ${fmtNum(state.spine_event_count)} evt (${state.spine_event_top})`
    : ` | spine ${fmtNum(state.spine_event_count)} evt`;
  byId('metaLine').textContent = `Updated ${new Date(payload.generated_at || Date.now()).toLocaleString()} | ${state.transport} | fallback ${Math.round(state.refresh_ms / 1000)}s${pulseSuffix}`;
}

function renderSelectionTag() {
  const selectedTag = byId('selectedTag');
  if (!state.selected) {
    selectedTag.classList.remove('show');
    selectedTag.textContent = '';
    return;
  }
  selectedTag.textContent = state.selected.name;
  selectedTag.classList.add('show');
}

function hitTest(x, y) {
  const scene = state.scene;
  if (!scene) return null;
  let circleFound = null;
  let circleBest = Infinity;
  let ringFound = null;
  let ringBest = Infinity;
  for (const t of scene.hit_targets || []) {
    if (t.kind === 'ring') {
      const dx = x - Number(t.cx || 0);
      const dy = y - Number(t.cy || 0);
      const d = Math.hypot(dx, dy);
      const inner = Number(t.inner || 0);
      const outer = Number(t.outer || 0);
      if (d >= inner && d <= outer) {
        const score = Math.abs(d - ((inner + outer) * 0.5));
        if (score < ringBest) {
          ringBest = score;
          ringFound = t;
        }
      }
      continue;
    }
    const dx = x - Number(t.x || 0);
    const dy = y - Number(t.y || 0);
    const d = Math.hypot(dx, dy);
    if (d <= Number(t.r || 0) && d < circleBest) {
      circleBest = d;
      circleFound = t;
    }
  }
  return circleFound || ringFound;
}

function onCanvasClick(evt) {
  const rect = state.canvas.getBoundingClientRect();
  const x = evt.clientX - rect.left;
  const y = evt.clientY - rect.top;
  const hit = hitTest(x, y);
  state.selected = hit ? { id: hit.id, name: hit.name } : null;
  renderSelectionTag();
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

function boot() {
  state.canvas = byId('holoCanvas');
  state.ctx = state.canvas.getContext('2d', { alpha: true });
  state.gpu = detectGpuTier();
  setQualityTier(state.gpu.tier);
  resizeCanvas();

  byId('refresh').addEventListener('click', requestRefresh);
  byId('hours').addEventListener('change', () => {
    const hours = currentHours();
    if (!sendWs({ type: 'subscribe', hours })) {
      refreshNow(true);
    }
  });
  state.canvas.addEventListener('click', onCanvasClick);
  window.addEventListener('resize', () => {
    resizeCanvas();
    state.scene = buildScene(state.payload);
    rebuildParticles();
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
