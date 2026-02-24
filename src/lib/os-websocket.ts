import { create } from 'zustand';
import { io } from 'socket.io-client';

interface Subsystem {
  id: string;
  name: string;
  activeAgents: number;
  health: number;
  position: [number, number, number];
}

interface OSState {
  drift: number;
  subsystems: Subsystem[];
  setDrift: (d: number) => void;
  updateSubsystem: (id: string, data: Partial<Subsystem>) => void;
}

export const useOSStore = create<OSState>((set) => ({
  drift: 3.0,
  subsystems: [
    {
      id: 'memory',
      name: 'Long-Term Memory',
      activeAgents: 42,
      health: 98,
      position: [-12, 8, -5]
    },
    {
      id: 'planner',
      name: 'Strategic Planner',
      activeAgents: 27,
      health: 97,
      position: [12, 8, -5]
    },
    {
      id: 'executor',
      name: 'Execution Engine',
      activeAgents: 68,
      health: 96,
      position: [0, 2, -15]
    },
    {
      id: 'sensory',
      name: 'Sensory Layer',
      activeAgents: 11,
      health: 95,
      position: [-8, 10, 8]
    },
    {
      id: 'autonomy',
      name: 'Autonomy Core',
      activeAgents: 8,
      health: 92,
      position: [8, 6, -8]
    }
  ],
  setDrift: (d) => set({ drift: d }),
  updateSubsystem: (id, data) => set((state) => ({
    subsystems: state.subsystems.map((s) =>
      s.id === id ? { ...s, ...data } : s
    ),
  })),
}));

// Connect to your real OS (replace URL with your FastAPI / SSE endpoint)
// const socket = io('http://localhost:8000');
// 
// socket.on('os_telemetry', (data: { drift: number; subsystems: any[] }) => {
//   useOSStore.getState().setDrift(data.drift);
//   data.subsystems.forEach((s) => {
//     useOSStore.getState().updateSubsystem(s.id, s);
//   });
// });

// For demo: Simulate live drift changes
if (typeof window !== 'undefined') {
  setInterval(() => {
    const current = useOSStore.getState().drift;
    const variation = (Math.random() - 0.5) * 0.2;
    useOSStore.getState().setDrift(Math.max(1, Math.min(25, current + variation)));
  }, 3000);
}
