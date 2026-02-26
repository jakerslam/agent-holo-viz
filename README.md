# Agent Holo Viz

Sidecar repository for the full-screen holographic system visualizer.

## What it does

- Renders layered module topology (adaptive/systems/memory/habits/lib/config/state).
- Streams live updates over WebSocket from autonomy + spine run events.
- Falls back to HTTP polling if WebSocket disconnects.
- Auto-scales rendering tier based on detected GPU capacity.

## Run

```bash
cd /Users/jay/.openclaw/workspace/agent-holo-viz
npm install
npm run start
```

Open:

- `http://127.0.0.1:8787`

## Wiring

- API snapshot: `GET /api/holo?hours=24&live_mode=1&live_minutes=6`
- Live stream: `ws://127.0.0.1:8787/ws/holo?hours=24&live_mode=1&live_minutes=6`

`live_mode=1` enables strict runtime liveness (tight recent-window checks); `live_mode=0` uses broader history-window behavior.

## Workspace compatibility

The workspace entrypoint `systems/ops/system_visualizer_server.js` is a launcher that delegates to this sidecar server.
