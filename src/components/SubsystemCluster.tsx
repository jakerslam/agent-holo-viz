'use client';

import { useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import { Sphere, Html } from '@react-three/drei';
import * as THREE from 'three';
import AgentNode from './AgentNode';

interface SubsystemClusterProps {
  id: string;
  name: string;
  activeAgents: number;
  health: number;
  position: [number, number, number];
  index: number;
}

export default function SubsystemCluster({ 
  id, 
  name, 
  activeAgents, 
  health, 
  position,
  index 
}: SubsystemClusterProps) {
  const groupRef = useRef<THREE.Group>(null);
  const orbitRadius = 8 + index * 2;
  const orbitSpeed = 0.1 + index * 0.02;

  useFrame((state) => {
    if (groupRef.current) {
      const t = state.clock.elapsedTime * orbitSpeed;
      groupRef.current.position.x = position[0] + Math.cos(t) * orbitRadius * 0.3;
      groupRef.current.position.z = position[2] + Math.sin(t) * orbitRadius * 0.3;
      groupRef.current.position.y = position[1] + Math.sin(t * 2) * 0.5;
      groupRef.current.rotation.y += 0.005;
    }
  });

  // Generate agent nodes
  const agents = Array.from({ length: Math.min(activeAgents, 20) }, (_, i) => ({
    id: `${id}-agent-${i}`,
    position: [
      (Math.random() - 0.5) * 4,
      (Math.random() - 0.5) * 4,
      (Math.random() - 0.5) * 4
    ] as [number, number, number],
    health: 90 + Math.random() * 10
  }));

  return (
    <group ref={groupRef} position={position}>
      {/* Subsystem orb */}
      <Sphere args={[1.5, 32, 32]}>
        <meshStandardMaterial
          color="#00d4ff"
          emissive="#0088cc"
          emissiveIntensity={health > 90 ? 0.6 : 0.3}
          metalness={0.9}
          roughness={0.1}
          transparent
          opacity={0.9}
        />
      </Sphere>

      {/* Health ring */}
      <group rotation={[Math.PI / 2, 0, 0]}>
        <mesh>
          <ringGeometry args={[1.8, 2, 64]} />
          <meshBasicMaterial
            color={health > 90 ? '#00ff88' : health > 70 ? '#ffaa00' : '#ff3333'}
            transparent
            opacity={0.4}
            side={THREE.DoubleSide}
          />
        </mesh>
      </group>

      {/* Agent nodes */}
      {agents.map((agent) => (
        <AgentNode key={agent.id} {...agent} />
      ))}

      {/* Label */}
      <Html distanceFactor={10}>
        <div className="text-cyan-400 text-xs font-mono whitespace-nowrap pointer-events-none">
          {name}
          <div className="text-cyan-600 text-[10px]">{activeAgents} agents</div>
        </div>
      </Html>
    </group>
  );
}
