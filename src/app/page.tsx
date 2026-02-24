'use client';

import { Canvas, useFrame, useThree } from '@react-three/fiber';
import { OrbitControls, Stars, Environment, Html, Sphere } from '@react-three/drei';
import { EffectComposer, Bloom, ChromaticAberration } from '@react-three/postprocessing';
import { useRef, useMemo, useState, useEffect } from 'react';
import * as THREE from 'three';

// GPU Detection
function useGPUStats() {
  const [gpuTier, setGpuTier] = useState<'low' | 'medium' | 'high'>('medium');
  
  useEffect(() => {
    const canvas = document.createElement('canvas');
    const gl = canvas.getContext('webgl2') || canvas.getContext('webgl');
    if (!gl) {
      setGpuTier('low');
      return;
    }
    
    const debugInfo = (gl as any).getExtension('WEBGL_debug_renderer_info');
    if (debugInfo) {
      const renderer = gl.getParameter(debugInfo.UNMASKED_RENDERER_WEBGL);
      if (renderer.includes('Apple') || renderer.includes('Intel')) {
        setGpuTier('medium');
      } else if (renderer.includes('M1') || renderer.includes('M2') || renderer.includes('M3')) {
        setGpuTier('high');
      } else if (renderer.includes('NVIDIA') || renderer.includes('AMD')) {
        setGpuTier('high');
      }
    }
    
    if ('deviceMemory' in navigator && (navigator as any).deviceMemory < 8) {
      setGpuTier('low');
    }
  }, []);
  
  return gpuTier;
}

// COLORS based on GPU tier
const getColors = (gpuTier: 'low' | 'medium' | 'high') => {
  if (gpuTier === 'low') {
    return {
      modules: '#0066ff',      // Blue modules for low spec
      packets: '#ffffff',      // White packets
      tendrils: '#4488ff',      // Light blue connections
      kernel: '#ffaa00',       // Keep kernel gold
      accent: '#00ccff'        // Cyan accent
    };
  }
  return {
    modules: '#ff8800',        // Orange for high spec
    packets: '#ffcc00',        // Gold packets
    tendrils: '#ff6600',       // Orange tendrils
    kernel: '#ffdd44',         // Golden kernel
    accent: '#ffaa00'
  };
};

const holographicVertex = `
  varying vec3 vNormal;
  varying vec3 vPosition;
  void main() {
    vNormal = normalize(normalMatrix * normal);
    vPosition = position;
    gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
  }
`;

const holographicFragment = `
  uniform float time;
  uniform vec3 color;
  varying vec3 vNormal;
  varying vec3 vPosition;
  void main() {
    vec3 viewDir = normalize(cameraPosition - vPosition);
    float fresnel = pow(1.0 - dot(vNormal, viewDir), 2.8);
    float scan = sin(vPosition.y * 70.0 + time * 15.0) * 0.03 + 0.97;
    vec3 holo = color * (fresnel * 2.0 + 0.55);
    gl_FragColor = vec4(holo * scan, 0.82 * (fresnel + 0.45));
  }
`;

// Data Packet - white for low spec, gold for high
function DataPacket({ 
  curve, 
  lane, 
  speed = 1, 
  color 
}: { 
  curve: THREE.CatmullRomCurve3; 
  lane: number; 
  speed: number; 
  color: string;
}) {
  const meshRef = useRef<THREE.Mesh>(null);
  const progress = useRef(Math.random());
  
  useFrame((_, delta) => {
    if (!meshRef.current) return;
    progress.current += delta * speed * 0.2;
    if (progress.current > 1) progress.current = 0;
    
    const pos = curve.getPointAt(progress.current);
    meshRef.current.position.copy(pos);
    meshRef.current.position.x += lane * 0.3;
  });
  
  return (
    <mesh ref={meshRef}>
      <boxGeometry args={[0.12, 0.08, 0.25]} />
      <meshBasicMaterial color={color} />
    </mesh>
  );
}

// Conduit with packets
function Conduit({ 
  start, 
  end, 
  packets, 
  gpuTier,
  colors
}: { 
  start: [number, number, number]; 
  end: [number, number, number]; 
  packets: number;
  gpuTier: 'low' | 'medium' | 'high';
  colors: any;
}) {
  const lanes = gpuTier === 'low' ? 1 : gpuTier === 'medium' ? 2 : 3;
  
  const curve = useMemo(() => {
    const a = new THREE.Vector3(...start);
    const b = new THREE.Vector3(...end);
    const mid1 = a.clone().lerp(b, 0.33).add(new THREE.Vector3((Math.random()-0.5)*6, 2, (Math.random()-0.5)*6));
    const mid2 = a.clone().lerp(b, 0.66).add(new THREE.Vector3((Math.random()-0.5)*6, -1, (Math.random()-0.5)*6));
    return new THREE.CatmullRomCurve3([a, mid1, mid2, b]);
  }, [start, end]);
  
  const tubePoints = useMemo(() => curve.getPoints(40), [curve]);
  
  return (
    <group>
      <line>
        <bufferGeometry>
          <bufferAttribute
            attach="attributes-position"
            count={tubePoints.length}
            array={new Float32Array(tubePoints.flatMap(p => [p.x, p.y, p.z]))}
            itemSize={3}
          />
        </bufferGeometry>
        <lineBasicMaterial color={colors.tendrils} transparent opacity={0.35} linewidth={2} />
      </line>
      
      {Array.from({ length: lanes }).map((_, lane) => 
        Array.from({ length: packets }).map((_, i) => (
          <DataPacket
            key={`${lane}-${i}`}
            curve={curve}
            lane={lane - lanes / 2 + 0.5}
            speed={0.5 + Math.random()}
            color={colors.packets}
          />
        ))
      )}
    </group>
  );
}

// Module Node - adaptive geometry based on GPU
function ModuleNode({
  position,
  name,
  onClick,
  isSelected,
  gpuTier,
  colors
}: {
  position: [number, number, number];
  name: string;
  onClick: () => void;
  isSelected: boolean;
  gpuTier: 'low' | 'medium' | 'high';
  colors: any;
}) {
  const [hovered, setHovered] = useState(false);
  const meshRef = useRef<THREE.Mesh>(null);
  
  useFrame((state) => {
    if (meshRef.current && gpuTier !== 'low') {
      meshRef.current.rotation.y = state.clock.elapsedTime * 0.3;
    }
  });
  
  // LOW SPEC: Simple sphere, no shader
  if (gpuTier === 'low') {
    return (
      <group position={position}>
        <mesh
          ref={meshRef}
          onClick={onClick}
          onPointerOver={() => setHovered(true)}
          onPointerOut={() => setHovered(false)}
        >
          <sphereGeometry args={[3]} />
          <meshStandardMaterial
            color={colors.modules}
            emissive={colors.modules}
            emissiveIntensity={isSelected ? 1.5 : 0.8}
            metalness={0.8}
            roughness={0.2}
          />
        </mesh>
        
        {isSelected && (
          <Html position={[0, 5, 0]} center>
            <div style={{
              background: 'rgba(0,50,100,0.9)',
              border: `1px solid ${colors.modules}`,
              color: '#ffffff',
              padding: '8px 16px',
              fontFamily: 'monospace',
              fontSize: '13px',
              borderRadius: '4px',
              whiteSpace: 'nowrap'
            }}>
              {name}
            </div>
          </Html>
        )}
        
        {hovered && !isSelected && (
          <Html position={[0, -5, 0]} center>
            <div style={{ color: colors.modules, fontSize: '11px', opacity: 0.7 }}>[CLICK]</div>
          </Html>
        )}
      </group>
    );
  }
  
  // HIGH SPEC: Holographic shader
  const meshRef2 = useRef<THREE.Mesh>(null);
  const materialRef = useRef<THREE.ShaderMaterial>(null);

  useFrame((state) => {
    if (materialRef.current) materialRef.current.uniforms.time.value = state.clock.getElapsedTime();
  });

  return (
    <group position={position}>
      <mesh ref={meshRef2} onClick={onClick} onPointerOver={() => setHovered(true)} onPointerOut={() => setHovered(false)}>
        <sphereGeometry args={[3.8]} />
        <shaderMaterial
          ref={materialRef}
          vertexShader={holographicVertex}
          fragmentShader={holographicFragment}
          uniforms={{
            time: { value: 0 },
            color: { value: new THREE.Color(colors.modules) },
          }}
          transparent
          side={THREE.DoubleSide}
        />
      </mesh>
      
      {isSelected && (
        <Html position={[0, 6.5, 0]} center>
          <div style={{ color: colors.modules, fontSize: '15px', fontWeight: 700, textShadow: '0 0 15px #000' }}>
            {name}
          </div>
        </Html>
      )}
      
      {hovered && !isSelected && (
        <Html position={[0, -6.5, 0]} center>
          <div style={{ color: colors.modules, fontSize: '12px', opacity: 0.7 }}>[CLICK]</div>
        </Html>
      )}
    </group>
  );
}

// I/O Node
function IONode({
  position,
  targetModule,
  isInput,
  colors
}: {
  position: [number, number, number];
  targetModule: [number, number, number];
  isInput: boolean;
  colors: any;
}) {
  const ioColor = isInput ? '#00ff88' : '#ff0088';
  
  return (
    <group position={position}>
      <mesh>
        <tetrahedronGeometry args={[1.2, 0]} />
        <meshBasicMaterial color={ioColor} wireframe />
      </mesh>
      <line>
        <bufferGeometry>
          <bufferAttribute
            attach="attributes-position"
            count={2}
            array={new Float32Array([...position, ...targetModule])}
            itemSize={3}
          />
        </bufferGeometry>
        <lineBasicMaterial color={ioColor} transparent opacity={0.3} />
      </line>
    </group>
  );
}

// Main
export default function ProtheusAdaptiveViz() {
  const gpuTier = useGPUStats();
  const colors = getColors(gpuTier);
  const [selectedModule, setSelectedModule] = useState<string | null>(null);
  
  const orbitRadius = gpuTier === 'low' ? 16 : gpuTier === 'medium' ? 20 : 26;
  
  const modules = [
    { id: 'spine', name: 'SPINE', pos: [0, 8, 0] as [number, number, number] },
    { id: 'sensory', name: 'SENSORY_EYE', pos: [-orbitRadius, 4, -orbitRadius * 0.5] as [number, number, number] },
    { id: 'memory', name: 'MEMORY_GRAPH', pos: [orbitRadius, 4, -orbitRadius * 0.5] as [number, number, number] },
    { id: 'security', name: 'SECURITY_IO', pos: [-orbitRadius * 0.7, -4, orbitRadius * 0.8] as [number, number, number] },
    { id: 'actuation', name: 'ACTUATION', pos: [orbitRadius * 0.7, -4, orbitRadius * 0.8] as [number, number, number] },
    { id: 'spawn', name: 'SPAWN_BROKER', pos: [-orbitRadius * 1.1, 2, orbitRadius * 0.3] as [number, number, number] },
    { id: 'strategy', name: 'STRATEGY_LEARN', pos: [orbitRadius * 1.1, 2, orbitRadius * 0.3] as [number, number, number] },
  ];
  
  const connections = useMemo(() => {
    const conns = [];
    for (let i = 0; i < modules.length; i++) {
      for (let j = i + 1; j < modules.length; j++) {
        conns.push({
          from: modules[i].pos,
          to: modules[j].pos,
          packets: gpuTier === 'low' ? 1 : Math.floor(Math.random() * 2) + 1
        });
      }
    }
    return conns;
  }, [modules, gpuTier]);
  
  const ioNodes = useMemo(() => {
    const nodes = [];
    for (let i = 0; i < 6; i++) {
      const angle = (i / 6) * Math.PI * 2;
      const r = orbitRadius * 1.8;
      const target = modules[i % modules.length];
      nodes.push({
        pos: [Math.cos(angle) * r, Math.sin(i) * 3, Math.sin(angle) * r] as [number, number, number],
        target: target.pos,
        isInput: i % 2 === 0
      });
    }
    return nodes;
  }, [modules, orbitRadius]);
  
  const stats = {
    drift: 3.2,
    runtime: '6 months',
    subsystems: 11,
    agents: 200,
    yield: 66.7,
  };

  return (
    <div className="w-full h-screen bg-black overflow-hidden relative">
      <div className="absolute top-6 left-6 z-50 font-mono">
        <div className="text-2xl mb-2 tracking-widest" style={{ color: colors.modules }}>
          PROTHEUS_OS_V1
        </div>
        <div className="space-y-1 text-sm opacity-80 text-white">
          <div>GPU: {gpuTier.toUpperCase()}</div>
          <div>Drift: <span className="text-red-400">{stats.drift}%</span></div>
          <div>MODULES: {modules.length}</div>
          <div>CONDUITS: {connections.length}</div>
          <div>VIEW: {selectedModule || 'NONE'}</div>
        </div>
      </div>
      
      <Canvas
        camera={{ position: [0, 30, 60], fov: 45 }}
        gl={{ antialias: gpuTier !== 'low' }}
        dpr={gpuTier === 'low' ? 1 : gpuTier === 'medium' ? 1.5 : 2}
      >
        <color attach="background" args={['#000510']} />
        <ambientLight intensity={gpuTier === 'low' ? 0.3 : 0.2} />
        <pointLight position={[50, 50, 50]} color={colors.modules} intensity={gpuTier === 'low' ? 3 : 2} />
        <pointLight position={[-50, 30, -50]} color={colors.packets} intensity={1} />
        
        {/* Floor grid */}
        <group rotation={[-Math.PI / 2, 0, 0]} position={[0, -20, 0]}>
          <gridHelper args={[400, 40, colors.modules, '#111122']} />
          <mesh position={[0, -0.1, 0]}>
            <planeGeometry args={[400, 400]} />
            <meshBasicMaterial color="#010205" />
          </mesh>
        </group>
        
        {/* Kernel */}
        <mesh position={[0, 10, 0]}>
          <sphereGeometry args={[4.5]} />
          <meshStandardMaterial
            color={colors.kernel}
            emissive={colors.kernel}
            emissiveIntensity={3}
            metalness={1}
          />
        </mesh>
        
        {/* Connections */}
        {connections.map((conn, i) => (
          <Conduit
            key={i}
            start={conn.from}
            end={conn.to}
            packets={conn.packets}
            gpuTier={gpuTier}
            colors={colors}
          />
        ))}
        
        {/* Modules */}
        {modules.map((mod) => (
          <ModuleNode
            key={mod.id}
            position={mod.pos}
            name={mod.name}
            onClick={() => setSelectedModule(mod.id === selectedModule ? null : mod.id)}
            isSelected={selectedModule === mod.id}
            gpuTier={gpuTier}
            colors={colors}
          />
        ))}
        
        {/* I/O */}
        {ioNodes.map((io, i) => (
          <IONode
            key={i}
            position={io.pos}
            targetModule={io.target}
            isInput={io.isInput}
            colors={colors}
          />
        ))}
        
        {gpuTier === 'high' && <Stars radius={400} depth={50} count={6000} />}
        
        <OrbitControls autoRotate autoRotateSpeed={0.1} enablePan enableZoom />
        <Environment preset="night" />
        
        {gpuTier !== 'low' && (
          <EffectComposer>
            <Bloom luminanceThreshold={0.35} luminanceSmoothing={0.9} height={500} />
            <ChromaticAberration offset={[0.0008, 0.0008]} />
          </EffectComposer>
        )}
      </Canvas>
    </div>
  );
}
