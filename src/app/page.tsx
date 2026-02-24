'use client';

import { Canvas, useFrame, useThree } from '@react-three/fiber';
import { OrbitControls, Stars, Environment, Html } from '@react-three/drei';
import { EffectComposer, Bloom, ChromaticAberration } from '@react-three/postprocessing';
import { useRef, useMemo } from 'react';
import * as THREE from 'three';

// ====================== HOLOGRAPHIC SHADER (perfect movie transparency) ======================
const holographicVertexShader = `
  varying vec3 vNormal;
  varying vec3 vPosition;
  void main() {
    vNormal = normalize(normalMatrix * normal);
    vPosition = position;
    gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
  }
`;

const holographicFragmentShader = `
  uniform float time;
  uniform vec3 color;
  uniform float opacity;
  varying vec3 vNormal;
  varying vec3 vPosition;
  void main() {
    vec3 viewDir = normalize(cameraPosition - vPosition);
    float fresnel = pow(1.0 - dot(vNormal, viewDir), 2.5);
    // Scanlines
    float scan = sin(vPosition.y * 60.0 + time * 12.0) * 0.035 + 0.965;
    // Subtle noise + glitch
    float noise = fract(sin(dot(vPosition.xy, vec2(12.9898, 78.233))) * 43758.5453 + time);
    float glitch = step(0.985, noise) * 0.12;
    vec3 holo = color * (fresnel * 1.8 + 0.6);
    gl_FragColor = vec4(holo * scan + glitch, opacity * (fresnel + 0.4));
  }
`;

function HolographicMaterial({ color = '#ff8800', opacity = 0.75 }) {
  const materialRef = useRef<THREE.ShaderMaterial>(null);

  useFrame((state) => {
    if (materialRef.current) {
      materialRef.current.uniforms.time.value = state.clock.elapsedTime;
    }
  });

  return (
    <shaderMaterial
      ref={materialRef}
      vertexShader={holographicVertexShader}
      fragmentShader={holographicFragmentShader}
      uniforms={{
        time: { value: 0 },
        color: { value: new THREE.Color(color) },
        opacity: { value: opacity }
      }}
      transparent
      side={THREE.DoubleSide}
    />
  );
}

// ====================== TRAVELING PARTICLES ALONG TENDRILS ======================
function FlowingParticles({ subsystems }: { subsystems: any[] }) {
  const pointsRef = useRef<THREE.Points>(null!);
  const particleCount = 2400;

  const { positions, colors, progresses } = useMemo(() => {
    const pos = new Float32Array(particleCount * 3);
    const col = new Float32Array(particleCount * 3);
    const prog = new Float32Array(particleCount);
    for (let i = 0; i < particleCount; i++) {
      const from = subsystems[Math.floor(Math.random() * subsystems.length)];
      const to = subsystems[Math.floor(Math.random() * subsystems.length)];
      if (from === to) continue;
      const start = new THREE.Vector3(...from.pos);
      const end = new THREE.Vector3(...to.pos);
      const mid = start.clone().lerp(end, Math.random());
      pos[i * 3] = mid.x + (Math.random() - 0.5) * 4;
      pos[i * 3 + 1] = mid.y + (Math.random() - 0.5) * 4;
      pos[i * 3 + 2] = mid.z + (Math.random() - 0.5) * 4;
      const c = new THREE.Color('#ffaa44');
      col[i * 3] = c.r;
      col[i * 3 + 1] = c.g;
      col[i * 3 + 2] = c.b;
      prog[i] = Math.random();
    }
    return { positions: pos, colors: col, progresses: prog };
  }, [subsystems]);

  useFrame((_, delta) => {
    if (!pointsRef.current) return;
    const progressAttr = pointsRef.current.geometry.attributes.progress.array as Float32Array;
    const positions = pointsRef.current.geometry.attributes.position.array as Float32Array;
    for (let i = 0; i < particleCount; i++) {
      progressAttr[i] = (progressAttr[i] + delta * 0.6) % 1.0;
      // Simple drift animation
      positions[i * 3 + 1] += Math.sin(Date.now() * 0.001 + i) * 0.02;
    }
    pointsRef.current.geometry.attributes.progress.needsUpdate = true;
    pointsRef.current.geometry.attributes.position.needsUpdate = true;
  });

  const geometry = useMemo(() => {
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    geo.setAttribute('color', new THREE.BufferAttribute(colors, 3));
    geo.setAttribute('progress', new THREE.BufferAttribute(progresses, 1));
    return geo;
  }, [positions, colors, progresses]);

  const material = useMemo(() => new THREE.ShaderMaterial({
    uniforms: { time: { value: 0 } },
    vertexShader: `
      attribute float progress;
      varying float vProgress;
      void main() {
        vProgress = progress;
        gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
        gl_PointSize = 3.5;
      }
    `,
    fragmentShader: `
      varying float vProgress;
      void main() {
        float alpha = sin(vProgress * 3.14) * 0.9 + 0.1;
        gl_FragColor = vec4(1.0, 0.65, 0.25, alpha);
      }
    `,
    transparent: true,
    depthTest: false,
    blending: THREE.AdditiveBlending
  }), []);

  return <points ref={pointsRef} geometry={geometry} material={material} />;
}

// ====================== DYNAMIC TENDRILS ======================
function DynamicTendrils({ subsystems }: { subsystems: any[] }) {
  const groupRef = useRef<THREE.Group>(null!);
  const curves = useMemo(() => {
    const c: THREE.CatmullRomCurve3[] = [];
    for (let i = 0; i < subsystems.length; i++) {
      for (let j = i + 1; j < subsystems.length; j++) {
        const a = new THREE.Vector3(...subsystems[i].pos);
        const b = new THREE.Vector3(...subsystems[j].pos);
        const mid1 = a.clone().lerp(b, 0.3).add(new THREE.Vector3(
          (Math.random() - 0.5) * 6,
          (Math.random() - 0.5) * 8,
          (Math.random() - 0.5) * 6
        ));
        const mid2 = a.clone().lerp(b, 0.7).add(new THREE.Vector3(
          (Math.random() - 0.5) * 6,
          (Math.random() - 0.5) * 8,
          (Math.random() - 0.5) * 6
        ));
        c.push(new THREE.CatmullRomCurve3([a, mid1, mid2, b], false, 'catmullrom', 0.6));
      }
    }
    return c;
  }, [subsystems]);

  return (
    <group ref={groupRef}>
      {curves.map((curve, i) => (
        <mesh key={i}>
          <tubeGeometry args={[curve, 64, 0.18, 8, false]} />
          <meshStandardMaterial
            color="#ff6600"
            emissive="#ff4400"
            emissiveIntensity={0.6}
            transparent
            opacity={0.35}
          />
        </mesh>
      ))}
    </group>
  );
}

// ====================== MAIN COMPONENT ======================
export default function ProtheusUltronViz() {
  const stats = {
    drift: 3.2,
    runtime: '6 months',
    subsystems: 11,
    agents: 200,
    yield: 66.7,
  };

  // All subsystems unified in ORANGE
  const subsystems = useMemo(() => [
    { name: 'Spine', pos: [0, 8, 0] as [number, number, number] },
    { name: 'Sensory / Eyes', pos: [-12, 4, -6] as [number, number, number] },
    { name: 'Memory Graph', pos: [12, 4, -6] as [number, number, number] },
    { name: 'Security / Contracts', pos: [-9, -3, 8] as [number, number, number] },
    { name: 'Actuation', pos: [9, -3, 8] as [number, number, number] },
    { name: 'Spawn Broker', pos: [-14, 1, 3] as [number, number, number] },
    { name: 'Strategy Learner', pos: [14, 1, 3] as [number, number, number] },
  ], []);

  const ORANGE_COLOR = '#ff8800';

  return (
    <div className="w-full h-screen bg-black overflow-hidden relative">
      {/* Live Protheus HUD */}
      <div className="absolute top-6 left-6 z-50 font-mono text-orange-400 bg-black/80 backdrop-blur-3xl p-8 rounded-3xl border border-orange-500/50 shadow-2xl text-lg">
        <div className="text-3xl font-bold tracking-widest mb-4">AGENTIC_OS_V1.0</div>
        <div className="space-y-1 text-lg">
          <div>Drift: <span className="text-red-400 font-bold">{stats.drift}%</span> <span className="text-red-500 animate-pulse">● LIVE</span></div>
          <div>Runtime: {stats.runtime}</div>
          <div>Subsystems: {stats.subsystems} ACTIVE</div>
          <div>Agents: ~{stats.agents}</div>
          <div>Yield: {stats.yield}%</div>
          <div className="text-xs opacity-70 mt-4">Next cycle: ~4h</div>
        </div>
      </div>

      <Canvas camera={{ position: [0, 18, 55], fov: 32 }} gl={{ antialias: true, alpha: true }}>
        <color attach="background" args={['#020207']} />

        {/* Reflective lab floor */}
        <mesh rotation={[-Math.PI * 0.5, 0, 0]} position={[0, -18, 0]}>
          <planeGeometry args={[400, 400]} />
          <meshStandardMaterial color="#111111" metalness={0.98} roughness={0.05} />
        </mesh>

        <ambientLight intensity={0.15} />
        <pointLight position={[40, 60, 40]} color="#ffaa44" intensity={5} />

        {/* Central Golden Kernel */}
        <mesh position={[0, 10, 0]}>
          <sphereGeometry args={[5]} />
          <meshStandardMaterial
            color="#ffdd44"
            emissive="#ffaa00"
            emissiveIntensity={3}
            metalness={1}
            roughness={0.05}
          />
        </mesh>

        {/* Subsystem Holographic Nodes - ALL ORANGE */}
        {subsystems.map((sys, i) => (
          <group key={i} position={sys.pos}>
            <mesh>
              <sphereGeometry args={[3.8]} />
              <HolographicMaterial color={ORANGE_COLOR} opacity={0.78} />
            </mesh>
            <Html position={[0, 6, 0]} style={{ color: ORANGE_COLOR, fontSize: '15px', fontWeight: 700, textShadow: '0 0 15px #000' }}>
              {sys.name}
            </Html>
          </group>
        ))}

        <DynamicTendrils subsystems={subsystems} />
        <FlowingParticles subsystems={subsystems} />
        <Stars radius={800} depth={90} count={18000} factor={5} saturation={0} fade />
        <OrbitControls autoRotate autoRotateSpeed={0.08} enablePan enableZoom enableRotate minDistance={20} maxDistance={140} />
        <Environment preset="night" />
        <EffectComposer>
          <Bloom luminanceThreshold={0.3} luminanceSmoothing={0.85} height={800} />
          <ChromaticAberration offset={[0.0012, 0.0012]} />
        </EffectComposer>
      </Canvas>
    </div>
  );
}
