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

// Connect to live Protheus OS telemetry
let lastDrift = 3.0;

if (typeof window !== 'undefined') {
  const socket = io('http://localhost:8000');

  socket.on('connect', () => {
    console.log('🔌 Connected to Protheus OS telemetry');
  });

  socket.on('os_telemetry', (data: { drift: number; subsystems: any[]; timestamp: string }) => {
    console.log('📊 Received telemetry:', data);
    
    // Update drift
    useOSStore.getState().setDrift(data.drift);
    lastDrift = data.drift;
    
    // Update subsystems with real data
    if (data.subsystems) {
      data.subsystems.forEach((s) => {
        useOSStore.getState().updateSubsystem(s.id, {
          activeAgents: s.activeAgents,
          health: s.health
        });
      });
    }
  });

  socket.on('disconnect', () => {
    console.log('🔌 Disconnected from telemetry');
  });

  // Fallback: subtle animation on top of real data
  setInterval(() => {
    const current = useOSStore.getState().drift;
    // Small organic fluctuation around real value
    const variation = (Math.random() - 0.5) * 0.05;
    const blended = (current * 0.95) + (lastDrift * 0.05) + variation;
    useOSStore.getState().setDrift(Math.max(1, Math.min(25, blended)));
  }, 500);
}
