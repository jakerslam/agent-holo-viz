'use client';

import { useRef, useMemo } from 'react';
import { useFrame } from '@react-three/fiber';
import { Sphere, MeshDistortionMaterial } from '@react-three/drei';
import * as THREE from 'three';

interface OSKernelProps {
  position: [number, number, number];
}

export default function OSKernel({ position }: OSKernelProps) {
  const meshRef = useRef<THREE.Mesh>(null);
  const glowRef = useRef<THREE.Mesh>(null);

  useFrame((state) => {
    if (meshRef.current) {
      meshRef.current.rotation.y = state.clock.elapsedTime * 0.1;
    }
    if (glowRef.current) {
      const scale = 1 + Math.sin(state.clock.elapsedTime * 2) * 0.05;
      glowRef.current.scale.setScalar(scale);
    }
  });

  const shaderData = useMemo(() => ({
    uniforms: {
      time: { value: 0 },
      color: { value: new THREE.Color('#ffd700') }
    },
    vertexShader: `
      varying vec2 vUv;
      varying vec3 vPosition;
      uniform float time;
      
      void main() {
        vUv = uv;
        vPosition = position;
        vec3 pos = position;
        pos += normal * sin(time * 2.0 + position.x * 3.0) * 0.1;
        gl_Position = projectionMatrix * modelViewMatrix * vec4(pos, 1.0);
      }
    `,
    fragmentShader: `
      varying vec2 vUv;
      varying vec3 vPosition;
      uniform float time;
      uniform vec3 color;
      
      void main() {
        float pulse = sin(time * 3.0 + vPosition.y * 5.0) * 0.5 + 0.5;
        vec3 glow = color * (0.8 + pulse * 0.4);
        float alpha = 0.9 + pulse * 0.1;
        gl_FragColor = vec4(glow, alpha);
      }
    `
  }), []);

  return (
    <group position={position}>
      {/* Golden JARVIS core */}
      <Sphere args={[3, 64, 64]} ref={meshRef}>
        <meshStandardMaterial
          color="#ffd700"
          emissive="#ff9500"
          emissiveIntensity={0.5}
          metalness={1.0}
          roughness={0.1}
        />
      </Sphere>
      
      {/* Outer glow */}
      <Sphere args={[3.5, 32, 32]} ref={glowRef}>
        <meshBasicMaterial
          color="#ffaa00"
          transparent
          opacity={0.15}
          side={THREE.BackSide}
        />
      </Sphere>
      
      {/* Inner core */}
      <Sphere args={[2, 32, 32]}>
        <meshBasicMaterial color="#ffffff" opacity={0.8} transparent />
      </Sphere>
    </group>
  );
}
