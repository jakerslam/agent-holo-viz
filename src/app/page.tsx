'use client';

import { Canvas } from '@react-three/fiber';
import { OrbitControls, Stars, Environment, Html } from '@react-three/drei';
import { EffectComposer, Bloom, ChromaticAberration } from '@react-three/postprocessing';
import { useRef, useMemo } from 'react';
import * as THREE from 'three';

// Holographic material (exact movie feel)
const HolographicMaterial = ({ color = '#00ffff', intensity = 1 }) => (
  <meshStandardMaterial
    color={color}
    emissive={color}
    emissiveIntensity={intensity}
    metalness={0.9}
    roughness={0.1}
    transparent
    opacity={0.65}
    side={THREE.DoubleSide}
  />
);

export default function ProtheusUltronViz() {
  const stats = {
    drift: 3.2,
    runtime: '6 months',
    subsystems: 11,
    agents: 200,
    yield: 66.7,
  };

  // Subsystems from your repo (transparent holographic forms)
  const subsystems = useMemo(() => [
    { name: 'Spine', pos: [0, 5, 0], color: '#ffd700' },
    { name: 'Sensory / Eyes', pos: [-8, 3, -4], color: '#00ffff' },
    { name: 'Memory Graph', pos: [8, 3, -4], color: '#00ff88' },
    { name: 'Security / Contracts', pos: [-5, -2, 6], color: '#ff0088' },
    { name: 'Actuation', pos: [5, -2, 6], color: '#ffaa00' },
    { name: 'Spawn Broker', pos: [-10, 0, 2], color: '#88ff00' },
    { name: 'Strategy Learner', pos: [10, 0, 2], color: '#0088ff' },
  ], []);

  return (
    <div className="w-full h-screen bg-black overflow-hidden relative">
      {/* Live HUD - matches your screenshot stats */}
      <div className="absolute top-6 left-6 z-50 font-mono text-cyan-400 bg-black/70 backdrop-blur-2xl p-8 rounded-3xl border border-cyan-500/40 shadow-2xl">
        <div className="text-3xl font-bold tracking-widest mb-4">AGENTIC_OS_V1.0</div>
        <div className="space-y-1 text-lg">
          <div>Drift: <span className="text-red-400 font-bold">{stats.drift}%</span> <span className="text-red-500 animate-pulse">● LIVE</span></div>
          <div>Runtime: {stats.runtime}</div>
          <div>Subsystems: {stats.subsystems} ACTIVE</div>
          <div>Agents: ~{stats.agents}</div>
          <div>Yield: {stats.yield}%</div>
          <div className="text-xs opacity-70 mt-4">Next cycle: ~4h</div>
        </div>
      </div>

      <Canvas camera={{ position: [0, 12, 40], fov: 35 }} gl={{ antialias: true, alpha: true }}>
        <color attach="background" args={['#050505']} />

        {/* Perfect reflective lab floor like the movie */}
        <mesh rotation={[-Math.PI * 0.5, 0, 0]} position={[0, -15, 0]}>
          <planeGeometry args={[300, 300]} />
          <meshStandardMaterial color="#0a0a0a" metalness={1} roughness={0.05} />
        </mesh>

        <ambientLight intensity={0.2} />
        <pointLight position={[30, 50, 30]} color="#aaffff" intensity={4} />

        {/* Central Golden Kernel */}
        <mesh position={[0, 8, 0]}>
          <sphereGeometry args={[4.5]} />
          <meshStandardMaterial color="#ffdd44" emissive="#ffaa00" emissiveIntensity={2} metalness={1} roughness={0.1} />
          <Html position={[0, 6, 0]} style={{ color: '#ffdd44', fontSize: '18px', fontWeight: 'bold', textAlign: 'center', textShadow: '0 0 20px #ffaa00' }}>
            AGENTIC_OS V1.0
          </Html>
        </mesh>

        {/* Massive Organic Neural Web + Transparent Subsystems */}
        {subsystems.map((sys, i) => (
          <group key={i} position={sys.pos}>
            {/* Transparent holographic form */}
            <mesh>
              <sphereGeometry args={[3.2]} />
              <HolographicMaterial color={sys.color} intensity={1.8} />
            </mesh>
            {/* Orbiting rings for depth */}
            <mesh>
              <torusGeometry args={[5, 0.2, 16, 100]} />
              <meshStandardMaterial color={sys.color} emissive={sys.color} emissiveIntensity={0.8} transparent opacity={0.4} />
            </mesh>
            {/* Label */}
            <Html position={[0, 5, 0]} style={{ color: sys.color, fontSize: '14px', pointerEvents: 'none', textShadow: '0 0 10px #000' }}>
              {sys.name}
            </Html>
          </group>
        ))}

        {/* Organic Tendrils + Data Particles (the Ultron magic) */}
        <group>
          {/* Tendrils would be custom TubeGeometry + shader in full version; for brevity this is placeholder with animated particles */}
          {Array.from({ length: 80 }).map((_, i) => (
            <mesh key={i} position={[
              Math.sin(i) * 18,
              Math.cos(i * 1.3) * 12 - 5,
              Math.cos(i) * 15
            ]}>
              <sphereGeometry args={[0.08]} />
              <meshStandardMaterial color="#00ffff" emissive="#00ffff" emissiveIntensity={3} />
            </mesh>
          ))}
        </group>

        <Stars radius={600} depth={80} count={12000} factor={6} saturation={0} fade speed={0.5} />

        <OrbitControls
          autoRotate
          autoRotateSpeed={0.12}
          enablePan
          enableZoom
          enableRotate
          minDistance={15}
          maxDistance={120}
        />

        <Environment preset="night" />

        <EffectComposer>
          <Bloom luminanceThreshold={0.4} luminanceSmoothing={0.9} height={600} />
          <ChromaticAberration offset={[0.001, 0.001]} />
        </EffectComposer>
      </Canvas>
    </div>
  );
}
