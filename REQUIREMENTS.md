# Protheus Visualizer IDE Requirements

## Purpose

Build the visualizer into a full IDE-grade control and observability surface for Protheus:

- Real-time system state and dataflow visibility
- Deterministic incident detection and debugging
- Safe operator controls with policy-aware guardrails

---

## Product Objectives

1. Make core autonomy state legible in under 10 seconds.
2. Make any blocker root-cause discoverable in under 60 seconds.
3. Make safe operator intervention possible without shell access.
4. Keep all rendered telemetry tied to real runtime events, not synthetic placeholders.

---

## Non-Negotiable Constraints

- No control action bypasses existing security/integrity gates.
- Visual state must be sourced from canonical logs/state files/APIs.
- Core views must degrade cleanly on low-spec GPU profiles.
- Error and incident states must not rely on color alone.

---

## Version Plan

## V1.0 - Operator Visibility Core

### Required Views

1. Goal/Directive DAG
- objective -> directive -> proposal -> execution -> outcome chain
- highlight incomplete edges and blockers

2. Queue Timeline
- queued, running, blocked, dropped lanes
- age and SLA-risk annotations

3. Gate/Mode State Machine
- autonomy mode and gate pass/fail status
- explicit fail reason per gate

4. Routing/Provider Health
- model availability, latency, timeout/failure counts
- fallback/circuit state

5. Incident Rail
- integrity, safety, risky-toggle, startup-attestation incidents
- severity, start time, top affected files/modules

### Acceptance Criteria

- Operator can identify why spine/autonomy is blocked from UI only.
- Operator can identify top 3 degraded providers from UI only.
- Incident banner + panel + graph cues stay consistent for same event.

---

## V1.1 - Engineering Visibility

### Required Views

1. Change Impact Overlay
- changed modules/submodules
- before/after metric deltas (yield/drift/safety)

2. Change Lifecycle Overlay
- mutating, dirty, staged, pending_push, just_pushed
- push transition clears pending state and shows short success ripple

3. Resource Pressure Plane
- token budget pressure, queue depth, projected saturation

4. Agent Lifecycle Map
- spawn, active, idle, dormant, terminated
- handoff/event links between agents

### Acceptance Criteria

- Operator can determine whether current regressions correlate with active code changes.
- Pending-push indicators clear automatically after successful push transition.

---

## V2.0 - Debugging and Causality

### Required Views

1. Time Travel Replay
- scrubber over event history
- deterministic frame reconstruction for decisions

2. Data Lineage Graph
- input source -> transforms -> output destination path
- selectable packet provenance

3. Experiment Board
- active experiments, confidence, winner/loser, promotion status

4. Failure Forensics
- MTTD/MTTR timeline
- remediation chain and verification state

### Acceptance Criteria

- Operator can replay any critical incident and trace causal chain.
- Operator can explain one failed proposal path end-to-end from UI only.

---

## V3.0 - Full IDE Control Plane

### Required Capabilities

1. Visual Policy Editor
- policy diffs and simulation gate before apply

2. Live Runbook Controls
- pause lane, reroute, retry class, budget dial
- all actions policy-gated and auditable

3. What-If Sandbox
- branch simulation vs production side-by-side

4. Event Bus Inspector
- schema visibility, drift detection, contract violations

### Acceptance Criteria

- All control actions are auditable and reversible.
- Simulation predicts policy/control impact before live application.

---

## Cross-Cutting UX Rules

1. Selection Context
- clicking any element must update preview pane with:
  - status
  - related errors/incidents
  - change state
  - key counters

2. Severity Encoding
- combine color + motion + text labels (not color only)

3. Performance
- profile-based rendering tiers must remain stable at target FPS per tier

4. Data Fidelity
- no fake “healthy” placeholders when source data is unavailable
- show `unknown` with reason when telemetry cannot be resolved

---

## Immediate Backlog (Next 2 Sprints)

1. Add Goal/Directive DAG panel and selection bridge to preview pane.
2. Add Queue Timeline with blocker reason chips.
3. Add Gate/Mode state machine panel from spine/autonomy events.
4. Add Provider health cards with live latency/failure sparklines.
5. Add Incident rail history list with filter by severity/type.
6. Add change-impact metric delta rows per selected module.

