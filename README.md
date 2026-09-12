# agent-control-plane

Agent Execution Control Plane — deterministic, durable scheduling and bounded
execution authority for agent workers.

```text
Design Plane
    ↓ approved ExecutionPacket
Agent Execution Control Plane
    ↓ bounded deterministic execution
Worker
```

This repository exists to move work that a human currently does by hand —
deciding which agent runs where, on which repository, and whether the work
actually landed — onto machine control that fails closed.

## What this is not

- Not a design authority. Semantic authority stays in the canonical design source.
- Not an autonomous inventor of work. Every execution needs an approved packet.
- Not worker-driven. A worker never schedules itself and never decides that it is done.

## Milestone status

**M0 — Bootstrap & Contract.** This milestone builds the durable foundation
only: identity, schema, state domains, transactional store, append-only event
history, validation, tests, CI and documentation.

M0 deliberately contains no worker launcher, no Git push, no GitHub or Notion
mutation, no lease renewal or takeover, and no scheduler loop. Those are later
milestones and are not authorised by the completion of this one.

## Layout

| Path | Contents |
| --- | --- |
| `internal/ids` | Stable identifiers (CC10). One distinct Go type per identifier kind. |
| `internal/state` | Separated state domains (CC6): packet, task run, worker attempt, publication. |
| `internal/domain` | Durable entities, the ExecutionPacket authority rules, publish binding, evidence rules, event model. |
| `internal/store` | SQLite backend: fail-closed open, WAL, versioned migrations, transactional primitives. |
| `internal/store/migrations` | Versioned, forward-only SQL. |
| `docs/architecture.md` | Entities, authority model, state domains, invariants. |
| `docs/threat-boundary.md` | Trust boundaries, worker isolation contract, publication and evidence threats. |
| `docs/repository-guardrail-baseline.md` | Observed repository governance baseline at M0. |

## Central invariants

The four that shape everything else:

**Worker exit is not completion.** There is no function anywhere in this
module that maps a process exit code, or a worker's self-report, onto a task
status. `state.TaskCompleted` is reachable only through
`domain.DeriveTaskCompletion`, which requires an externally observed effect
bound to the right attempt, epoch, repository and workspace. Modifying work
needs an applied or observed publication; read-only work needs review evidence
bound to a fixed reviewed commit and an artifact digest, never a generic
branch-head observation.

**A worker has no remote mutation authority.** Workers edit, build, test and
commit locally inside their own workspace. Remote mutation is scheduler-owned
and binds an exact immutable `source_commit_sha` — publishing "the current
branch head" is forbidden by construction.

**An approved packet is a closed world.** Explicitly allowed is allowed;
everything else, including anything merely absent from `forbidden_scope`, is
denied. A source-binding mismatch is a stop, not a warning. An approved packet
is also immutable: every authority-bearing field is frozen at approval, and its
status only ever moves away from authority, never back towards it.

**Unknown state fails closed.** A missing, foreign, corrupt or
newer-than-supported database is an error. The control plane never starts over
with a fresh database to get itself unstuck.

## Development

```sh
go build ./...
go vet ./...
go test ./...
```

The SQLite driver is pure Go, so no C toolchain is required:

```sh
CGO_ENABLED=0 go test ./...
```

Migrations are embedded and forward-only. A migration that has been applied is
never edited: its checksum is recorded, and a mismatch fails the next open.
