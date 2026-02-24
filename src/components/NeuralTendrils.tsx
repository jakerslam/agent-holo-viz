'use client';

import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';

export default function NeuralTendrils() {
  const linesRef = useRef<THREE.Group>(null);
  
  const tendrils = useMemo(() => {
    return Array.from({ length: 50 }, (_, i) => {
      const points = [];
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
      
      return points;
    });
  }, []);

  useFrame((state) => {
    if (linesRef.current) {
      linesRef.current.rotation.y = state.clock.elapsedTime * 0.05;
    }
  });

  return (
    <group ref={linesRef}>
      {tendrils.map((points, i) => (
        <line key={i}>
          <geometry>
            {<> { new THREE.BufferGeometry().setFromPoints(points) } >}
          </geometry>
          <lineBasicMaterial
            color="#00d4ff"
            transparent
            opacity={0.15 + Math.sin(i * 0.5) * 0.1}
          />
        </line>
      ))}
    </group>
  );
}
