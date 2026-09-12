# agent-control-plane

Agent Execution Control Plane.

Deterministic, durable scheduling and bounded execution authority for agent workers.

```text
Design Plane
    ↓ approved ExecutionPacket
Agent Execution Control Plane
    ↓ bounded deterministic execution
Worker
```

Status: bootstrap. See the `m0/bootstrap-contract` branch for the M0
durable-foundation implementation.
