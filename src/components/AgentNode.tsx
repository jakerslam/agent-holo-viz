'use client';

import { useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import { Sphere } from '@react-three/drei';
import * as THREE from 'three';

interface AgentNodeProps {
  id: string;
  position: [number, number, number];
  health: number;
}

export default function AgentNode({ id, position, health }: AgentNodeProps) {
  const meshRef = useRef<THREE.Mesh>(null);

  useFrame((state) => {
    if (meshRef.current) {
      const t = state.clock.elapsedTime * 2 + parseInt(id.split('-').pop() || '0');
      meshRef.current.position.x = position[0] + Math.cos(t) * 0.3;
      meshRef.current.position.y = position[1] + Math.sin(t) * 0.3;
      meshRef.current.position.z = position[2] + Math.sin(t * 0.5) * 0.3;
    }
  });

  return (
    <Sphere ref={meshRef} args={[0.15, 8, 8]} position={position}>
      <meshBasicMaterial
        color={health > 95 ? '#ffffff' : health > 80 ? '#aaffff' : '#ff6666'}
        transparent
        opacity={0.8}
      />
    </Sphere>
  );
}
