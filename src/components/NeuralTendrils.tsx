'use client';

import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import { Line } from '@react-three/drei';
import * as THREE from 'three';

export default function NeuralTendrils() {
  const linesRef = useRef<THREE.Group>(null);
  
  const tendrils = useMemo(() => {
    return Array.from({ length: 50 }, (_, i) => {
      const points: THREE.Vector3[] = [];
      const startAngle = (i / 50) * Math.PI * 2;
      
      for (let j = 0; j < 20; j++) {
        const t = j / 20;
        const radius = 4 + t * 15;
        const angle = startAngle + t * Math.PI * 4;
        const y = 4 - t * 8 + Math.sin(t * Math.PI) * 3;
        
        points.push(new THREE.Vector3(
          Math.cos(angle) * radius,
          y,
          Math.sin(angle) * radius
        ));
      }
      
      return {
        id: i,
        points,
        opacity: 0.15 + Math.sin(i * 0.5) * 0.1
      };
    });
  }, []);

  useFrame((state) => {
    if (linesRef.current) {
      linesRef.current.rotation.y = state.clock.elapsedTime * 0.05;
    }
  });

  return (
    <group ref={linesRef}>
      {tendrils.map((tendril) => (
        <Line
          key={tendril.id}
          points={tendril.points}
          color="#00d4ff"
          lineWidth={0.5}
          transparent
          opacity={tendril.opacity}
        />
      ))}
    </group>
  );
}
