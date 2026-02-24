varying vec2 vUv;
varying vec3 vPosition;
varying vec3 vNormal;

uniform float time;
uniform vec3 color;
uniform float opacity;

void main() {
  // Pulsing holographic effect
  float pulse = sin(time * 3.0 + vPosition.y * 5.0) * 0.5 + 0.5;
  
  // Fresnel effect for holographic edge glow
  vec3 viewDir = normalize(cameraPosition - vPosition);
  float fresnel = 1.0 - abs(dot(viewDir, vNormal));
  fresnel = pow(fresnel, 2.0);
  
  // Scanline effect
  float scanline = step(0.8, fract(vUv.y * 100.0 + time));
  
  // Combine effects
  vec3 glow = color * (0.6 + pulse * 0.4);
  glow += vec3(1.0, 1.0, 1.0) * fresnel * 0.3;
  
  float alpha = opacity * (0.7 + pulse * 0.3) * (0.9 + fresnel * 0.2);
  alpha *= (0.95 + scanline * 0.05);
  
  gl_FragColor = vec4(glow, alpha);
}
