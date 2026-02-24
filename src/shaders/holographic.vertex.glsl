varying vec2 vUv;
varying vec3 vPosition;
varying vec3 vNormal;

uniform float time;

void main() {
  vUv = uv;
  vPosition = position;
  vNormal = normal;
  
  // Subtle vertex displacement
  vec3 pos = position;
  float displacement = sin(time * 2.0 + position.x * 3.0) * 0.02;
  pos += normal * displacement;
  
  gl_Position = projectionMatrix * modelViewMatrix * vec4(pos, 1.0);
}
