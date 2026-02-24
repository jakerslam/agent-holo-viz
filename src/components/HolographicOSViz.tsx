'use client';

import { Canvas } from '@react-three/fiber';
import { OrbitControls, Stars, Environment } from '@react-three/drei';
import { EffectComposer, Bloom, ChromaticAberration } from '@react-three/postprocessing';
import { Suspense } from 'react';
import OSKernel from './OSKernel';
import SubsystemCluster from './SubsystemCluster';
import NeuralTendrils from './NeuralTendrils';
import ParticleFlows from './ParticleFlows';
import DriftFractures from './DriftFractures';
import HUD from './HUD';
import { useOSStore } from '../lib/os-websocket';

export default function HolographicOSViz() {
  const { drift, subsystems } = useOSStore();

  return (
    <>
      <HUD drift={drift} />
      <Canvas 
        camera={{ position: [0, 12, 35], fov: 40 }}
        gl={{ preserveDrawingBuffer: true }}
      >
        <color attach="background" args={['#000000']} />
        
        {/* Reflective lab floor */}
        <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, -12, 0]}>
          <planeGeometry args={[200, 200]} />
          <meshStandardMaterial color="#0a0a0a" metalness={0.95} roughness={0.05} />
        </mesh>

        <ambientLight intensity={0.15} />
        <pointLight position={[20, 30, 20]} color="#aaffff" intensity={3} />

        <Suspense fallback={null}>
          {/* Central immortal OS Kernel */}
          <OSKernel position={[0, 4, 0]} />
          
          {/* Subsystems orbiting kernel */}
          {subsystems.map((sys, i) => (
            <SubsystemCluster key={sys.id} {...sys} index={i} />
          ))}
          
          <NeuralTendrils />
          <ParticleFlows />
          <DriftFractures drift={drift} />
        </Suspense>

        <Stars radius={500} depth={60} count={8000} factor={7} saturation={0} fade />
        
        <OrbitControls 
          enablePan={true} 
          enableZoom={true} 
          enableRotate={true}
          autoRotate={true}
          autoRotateSpeed={0.15}
          minDistance={8}
          maxDistance={80}
        />
        
        <Environment preset="warehouse" />
        
        <EffectComposer>
          <Bloom luminanceThreshold={0.6} luminanceSmoothing={0.9} height={400} />
          <ChromaticAberration offset={[0.0008, 0.0008]} />
        </EffectComposer>
      </Canvas>
    </>
  );
}
