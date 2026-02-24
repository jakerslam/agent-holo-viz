'use client';

import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import { Line } from '@react-three/drei';
import * as THREE from 'three';

interface DriftFracturesProps {
  drift: number;
}

export default function DriftFractures({ drift }: DriftFracturesProps) {
  const groupRef = useRef<THREE.Group>(null);
  
  // Number of fractures based on drift (3% baseline = few, >10% = many)
  const fractureCount = Math.max(0, Math.floor((drift - 1) * 5));
  
  const fractures = useMemo(() => {
    return Array.from({ length: Math.min(fractureCount, 20) }, (_, i) => ({
      id: i,
      start: [
        (Math.random() - 0.5) * 10,
        Math.random() * 10,
        (Math.random() - 0.5) * 10
      ] as [number, number, number],
      end: [
        (Math.random() - 0.5) * 20,
        -10 + Math.random() * 5,
        (Math.random() - 0.5) * 20
      ] as [number, number, number],
      thickness: 0.02 + Math.random() * 0.03,
      // Store the THREE.Vector3 points
      points: [
        new THREE.Vector3(
          (Math.random() - 0.5) * 10,
          Math.random() * 10,
          (Math.random() - 0.5) * 10
        ),
        new THREE.Vector3(
          (Math.random() - 0.5) * 20,
          -10 + Math.random() * 5,
          (Math.random() - 0.5) * 20
        )
      ] as [THREE.Vector3, THREE.Vector3]
    }));
  }, [fractureCount]);

  useFrame((state) => {
    if (groupRef.current) {
      // Pulse opacity based on time and drift
      const pulse = Math.sin(state.clock.elapsedTime * 3) * 0.3 + 0.7;
      groupRef.current.children.forEach((child, i) => {
        if (child instanceof THREE.Line) {
          (child.material as THREE.LineBasicMaterial).opacity = 
            (0.4 + Math.sin(state.clock.elapsedTime * 2 + i) * 0.2) * pulse * (drift / 5);
        }
      });
    }
  });

  if (fractures.length === 0) return null;

  return (
    <group ref={groupRef}>
      {fractures.map((fracture) => (
        <Line
          key={fracture.id}
          points={fracture.points}
          color="#ff3333"
          lineWidth={fracture.thickness * 100}
          transparent
          opacity={0.4}
        />
      ))}
    </group>
  );
}
