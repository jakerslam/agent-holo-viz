# agent-holo-viz — Ultron Interface for 160k LOC Agentic OS

The definitive holographic visualizer for the most stable autonomous operating system on Earth.

- 6-month runtime visualized with live 3% drift fractures
- Golden JARVIS-style OS Kernel at center
- Hierarchical subsystems → thousands of agents
- Real-time particle message flows
- Gesture + voice ready (MediaPipe + Web Speech)
- Pepper's Ghost physical hologram ready

Built for the creator of the immortal 3%-drift OS.

## Install & Run

```bash
git clone https://github.com/jakerslam/agent-holo-viz.git
cd agent-holo-viz
npm install
npm run dev
```

Open http://localhost:3000 — you're standing in your OS.

## Connect Your OS

Update `src/lib/os-websocket.ts` with your telemetry endpoint:

```typescript
const socket = io('http://localhost:8000'); // Your OS endpoint
```

## Structure

- `src/components/` — 3D visualization components
- `src/lib/os-websocket.ts` — WebSocket bridge to your OS
- `src/shaders/` — Holographic GLSL shaders
- `public/` — Assets (optional HDR environment)

## License

MIT — Built with React Three Fiber, Three.js, and 🔥
