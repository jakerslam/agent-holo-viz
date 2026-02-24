'use client';

import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import { Points, PointMaterial } from '@react-three/drei';
import * as THREE from 'three';
import { useOSStore } from '../lib/os-websocket';

export default function ParticleFlows() {
  const pointsRef = useRef<THREE.Points>(null);
  const { subsystems } = useOSStore();
  
  const particleCount = 200;
  const particles = useMemo(() => {
    const positions = new Float32Array(particleCount * 3);
    const colors = new Float32Array(particleCount * 3);
    const velocities: { x: number; y: number; z: number }[] = [];
    
    for (let i = 0; i < particleCount; i++) {
      const angle = Math.random() * Math.PI * 2;
      const radius = 5 + Math.random() * 20;
      positions[i * 3] = Math.cos(angle) * radius;
      positions[i * 3 + 1] = (Math.random() - 0.5) * 10;
      positions[i * 3 + 2] = Math.sin(angle) * radius;
      
      // Golden/cyan color mix
      const isSubsystem = Math.random() > 0.5;
      colors[i * 3] = isSubsystem ? 1.0 : 0.0; // R
      colors[i * 3 + 1] = 0.8 + Math.random() * 0.2; // G
      colors[i * 3 + 2] = 1.0; // B
      
      velocities.push({
        x: (Math.random() - 0.5) * 0.1,
        y: Math.random() * 0.05,
        z: (Math.random() - 0.5) * 0.1
      });
    }
    
    return { positions, colors, velocities };
  }, []);

  useFrame((state, delta) => {
    if (!pointsRef.current) return;
    
    const positions = pointsRef.current.geometry.attributes.position.array as Float32Array;
    
    for (let i = 0; i < particleCount; i++) {
      const i3 = i * 3;
      
      // Orbit around center
      const x = positions[i3];
      const z = positions[i3 + 2];
      const distance = Math.sqrt(x * x + z * z);
      const angle = Math.atan2(z, x) + 0.005;
      
      positions[i3] = Math.cos(angle) * distance;
      positions[i3 + 2] = Math.sin(angle) * distance;
      positions[i3 + 1] += particles.velocities[i].y;
      
      // Reset if too high
      if (positions[i3 + 1] > 10) {
        positions[i3 + 1] = -5;
      }
    }
    
    pointsRef.current.geometry.attributes.position.needsUpdate = true;
  });

  return (
    <Points ref={pointsRef}>
      <bufferGeometry>
        <bufferAttribute
          attach="attributes-position"
          count={particleCount}
          array={particles.positions}
          itemSize={3}
        />
        <bufferAttribute
          attach="attributes-color"
          count={particleCount}
          array={particles.colors}
          itemSize={3}
        />
      </bufferGeometry>
      <PointMaterial
        size={0.15}
        vertexColors
        transparent
        opacity={0.8}
        sizeAttenuation
        blending={THREE.AdditiveBlending}
      />
    </Points>
  );
}
