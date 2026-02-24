'use client';

import { Canvas, useFrame } from '@react-three/fiber';
import { OrbitControls, Stars, Environment, Html } from '@react-three/drei';
import { EffectComposer, Bloom, ChromaticAberration } from '@react-three/postprocessing';
import { useRef, useMemo } from 'react';
import * as THREE from 'three';

const ORANGE = '#ff8800';

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

function HolographicSphere({ pos, name }: { pos: [number, number, number]; name: string }) {
  const meshRef = useRef<THREE.Mesh>(null!);
  const materialRef = useRef<THREE.ShaderMaterial>(null!);

  useFrame((state) => {
    if (materialRef.current) materialRef.current.uniforms.time.value = state.clock.getElapsedTime();
  });

  return (
    <group position={pos}>
      <mesh ref={meshRef}>
        <sphereGeometry args={[3.8]} />
        <shaderMaterial
          ref={materialRef}
          vertexShader={holographicVertex}
          fragmentShader={holographicFragment}
          uniforms={{
            time: { value: 0 },
            color: { value: new THREE.Color(ORANGE) },
          }}
          transparent
          side={THREE.DoubleSide}
        />
      </mesh>
      <Html position={[0, 6.5, 0]} style={{ color: ORANGE, fontSize: '15px', fontWeight: 700, textShadow: '0 0 15px #000' }}>
        {name}
      </Html>
    </group>
  );
}

function FlowingParticles({ subsystems }: { subsystems: any[] }) {
  const pointsRef = useRef<THREE.Points>(null!);
  const particleCount = 960;

  const { positions, colors } = useMemo(() => {
    const pos = new Float32Array(particleCount * 3);
    const col = new Float32Array(particleCount * 3);
    for (let i = 0; i < particleCount; i++) {
      const from = subsystems[Math.floor(Math.random() * subsystems.length)];
      const to = subsystems[Math.floor(Math.random() * subsystems.length)];
      const start = new THREE.Vector3(...from.pos);
      const end = new THREE.Vector3(...to.pos);
      const point = start.lerp(end, Math.random());
      pos[i * 3] = point.x + (Math.random() - 0.5) * 5;
      pos[i * 3 + 1] = point.y + (Math.random() - 0.5) * 5;
      pos[i * 3 + 2] = point.z + (Math.random() - 0.5) * 5;
      const c = new THREE.Color(ORANGE);
      col[i * 3] = c.r;
      col[i * 3 + 1] = c.g;
      col[i * 3 + 2] = c.b;
    }
    return { positions: pos, colors: col };
  }, [subsystems]);

  useFrame(() => {
    if (!pointsRef.current) return;
    const prog = pointsRef.current.geometry.attributes.progress.array as Float32Array;
    for (let i = 0; i < particleCount; i++) {
      prog[i] = (prog[i] + 0.008) % 1;
    }
    pointsRef.current.geometry.attributes.progress.needsUpdate = true;
  });

  const geometry = useMemo(() => {
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
    geo.setAttribute('progress', new THREE.BufferAttribute(new Float32Array(particleCount).map(() => Math.random()), 1));
    return geo;
  }, [positions, colors]);

  return (
    <points ref={pointsRef} geometry={geometry}>
      <shaderMaterial
        vertexShader={`
          attribute float progress;
          varying float vProgress;
          void main() {
            vProgress = progress;
            gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
            gl_PointSize = 3.2;
          }
        `}
        fragmentShader={`
          varying float vProgress;
          void main() {
            float a = sin(vProgress * 3.1416) * 0.95;
            gl_FragColor = vec4(1.0, 0.65, 0.25, a);
          }
        `}
        transparent
        depthTest={false}
        blending={THREE.AdditiveBlending}
      />
    </points>
  );
}

function DynamicTendrils({ subsystems }: { subsystems: any[] }) {
  return (
    <>
      {subsystems.flatMap((a, i) =>
        subsystems.slice(i + 1).map((b, j) => (
          <line key={`${i}-${j}`}>
            <bufferGeometry>
              <bufferAttribute
                attach="attributes-position"
                count={4}
                array={new Float32Array([
                  ...a.pos,
                  ...a.pos.map((v: number, k: number) => v + (b.pos[k] - v) * 0.35),
                  ...b.pos.map((v: number, k: number) => v + (a.pos[k] - v) * 0.35),
                  ...b.pos,
                ])}
                itemSize={3}
              />
            </bufferGeometry>
            <lineBasicMaterial color={ORANGE} transparent opacity={0.35} linewidth={2} />
          </line>
        ))
      )}
    </>
  );
}

export default function ProtheusUltronViz() {
  const stats = {
    drift: 3.2,
    runtime: '6 months',
    subsystems: 11,
    agents: 200,
    yield: 66.7,
  };

  const subsystems = [
    { name: 'Spine', pos: [0, 8, 0] as [number, number, number] },
    { name: 'Sensory / Eyes', pos: [-12, 4, -6] as [number, number, number] },
    { name: 'Memory Graph', pos: [12, 4, -6] as [number, number, number] },
    { name: 'Security / Contracts', pos: [-9, -3, 8] as [number, number, number] },
    { name: 'Actuation', pos: [9, -3, 8] as [number, number, number] },
    { name: 'Spawn Broker', pos: [-14, 1, 3] as [number, number, number] },
    { name: 'Strategy Learner', pos: [14, 1, 3] as [number, number, number] },
  ];

  return (
    <div className="w-full h-screen bg-black overflow-hidden relative">
      <div className="absolute top-6 left-6 z-50 font-mono text-orange-400 bg-black/80 backdrop-blur-3xl p-8 rounded-3xl border border-orange-500/50 text-lg">
        <div className="text-3xl font-bold tracking-widest mb-4">AGENTIC_OS_V1.0</div>
        <div className="space-y-1 text-lg">
          <div>Drift: <span className="text-red-400 font-bold">{stats.drift}%</span> <span className="text-red-500 animate-pulse">● LIVE</span></div>
          <div>Runtime: {stats.runtime}</div>
          <div>Subsystems: {stats.subsystems} ACTIVE</div>
          <div>Agents: ~{stats.agents}</div>
          <div>Yield: {stats.yield}%</div>
        </div>
      </div>

      <Canvas
        camera={{ position: [0, 18, 55], fov: 32 }}
        gl={{ antialias: true, alpha: true, powerPreference: 'default', preserveDrawingBuffer: false }}
        dpr={[1, 1.5]}
      >
        <color attach="background" args={['#020207']} />
        <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, -18, 0]}>
          <planeGeometry args={[400, 400]} />
          <meshStandardMaterial color="#0a0a0a" metalness={0.98} roughness={0.05} />
        </mesh>
        <ambientLight intensity={0.12} />
        <pointLight position={[40, 60, 40]} color="#ffaa44" intensity={4} />

        <mesh position={[0, 10, 0]}>
          <sphereGeometry args={[5]} />
          <meshStandardMaterial color="#ffdd44" emissive="#ffaa00" emissiveIntensity={3} metalness={1} />
        </mesh>

        {subsystems.map((s, i) => (
          <HolographicSphere key={i} pos={s.pos} name={s.name} />
        ))}

        <DynamicTendrils subsystems={subsystems} />
        <FlowingParticles subsystems={subsystems} />
        <Stars radius={800} depth={90} count={12000} factor={4} fade />
        <OrbitControls autoRotate autoRotateSpeed={0.07} enablePan enableZoom enableRotate minDistance={20} maxDistance={140} />
        <Environment preset="night" />
        <EffectComposer>
          <Bloom luminanceThreshold={0.35} luminanceSmoothing={0.9} height={500} />
          <ChromaticAberration offset={[0.0008, 0.0008]} />
        </EffectComposer>
      </Canvas>
    </div>
  );
}
