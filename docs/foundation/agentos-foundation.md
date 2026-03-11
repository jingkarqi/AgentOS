# AgentOS Foundation

## Subtitle

A minimal, durable, extensible foundation for the operating system of the agent era.

## Status

Draft v0.1

## Intent

This document defines the foundational design of **AgentOS**: a long-lived runtime substrate for agents.

It is not a product spec, a UI spec, or a model benchmark paper.
It is the ground layer: the constraints, abstractions, interfaces, and invariants that should remain stable even as models, tools, protocols, and products change.

The purpose of this document is to answer one question:

**What is the smallest possible foundation that can support the next decade of agent systems without becoming a trap?**

---

# 1. Thesis

The agent era will not be won by the single most impressive demo.
It will be won by the runtime that makes agents:

- durable
- observable
- controllable
- composable
- secure
- portable
- cheap to evolve

The foundation must therefore be designed like an operating system, not like a prompt wrapper.

A model is not the system.
A tool list is not the system.
A chat transcript is not the system.

The system is the thing that:

- manages execution
- governs state
- controls capabilities
- persists progress
- restores failure
- records evidence
- coordinates humans, models, and machines

That system is AgentOS.

---

# 2. Design Objective

Build the smallest possible core that can support:

1. short interactive agents
2. long-running agents
3. multi-agent workloads
4. human-in-the-loop workflows
5. secure enterprise deployment
6. future model and protocol changes

The foundation must be:

- **minimal** enough that it stays understandable
- **strict** enough that it stays reliable
- **modular** enough that it stays extensible
- **boring** enough that it survives fashion cycles

---

# 3. Primary Design Principles

## 3.1 Core small, world large

The kernel must do very little.
Everything else belongs in layers above it.

## 3.2 State first, transcript second

Conversation is not the source of truth.
Conversation is one projection of system state.

## 3.3 Durable by default

Any meaningful task may outlive a process, container, machine, or model session.
Execution must therefore be restartable.

## 3.4 Capabilities are explicit

Agents do not get ambient authority.
Every side effect must pass through explicit capability boundaries.

## 3.5 Events over black boxes

All important execution steps must be externally observable through structured events.

## 3.6 Human override is native

Human steering, approval, intervention, and takeover are first-class control flows, not hacks.

## 3.7 Models are replaceable

The architecture must assume that today’s best model will not remain the best model.
No foundational abstraction may depend on one vendor’s quirks.

## 3.8 Failure is normal

Timeouts, hallucinations, rate limits, bad tool output, partial writes, stale memory, and policy denials are normal operating conditions.
The system must be shaped around recovery, not optimism.

## 3.9 Semantic portability matters

The same task, state, and capability graph should be movable across runtimes, products, and model providers with minimal rewrite.

## 3.10 Stable abstractions beat feature urgency

If a feature complicates the core abstraction model, it should remain outside the core until proven foundational.

---

# 3.11 Scope of adequacy

The kernel primitives are **not** intended as a total ontology of human existence.
They are intended as the smallest stable substrate for **organized, durable, accountable action**.

AgentOS therefore claims a narrower and more defensible scope:

**Any human or machine activity that must be coordinated across time, actors, resources, permissions, and consequences should be representable as a composition of the kernel primitives.**

This includes commerce, administration, war, law, science, engineering, logistics, governance, care operations, and most collaborative creative work.

It does **not** imply that every dimension of human life is naturally or exhaustively reducible to the kernel.
Love, grief, contemplation, play, ritual, aesthetic experience, and private consciousness may enter the system only when they become organized, expressed, recorded, delegated, governed, or acted upon.

The kernel is therefore judged not by whether it captures all of humanity, but by whether it can faithfully carry all civilization-scale organized activity without conceptual distortion.

# 4. Non-Goals

AgentOS is **not**:

- a chatbot product
- a single-agent personality framework
- a prompt-engineering toolkit
- a monolithic multi-agent ideology
- a vendor-specific API wrapper
- a giant default tool bundle
- an autonomous decision-making religion

AgentOS does not attempt to define the ideal UI, the ideal orchestration pattern, or the ideal model.
It defines the ground layer on which many such things may exist.

---

# 5. The Minimal Kernel

The AgentOS kernel is the smallest runtime that guarantees the following primitives:

1. **Principal**
2. **Task**
3. **State**
4. **Event**
5. **Capability Call**
6. **Checkpoint**
7. **Policy Authority**
8. **Control Signal**

These eight primitives are intentionally chosen to cover the four enduring problems of all scalable systems:

- who acts
- what is being pursued
- what is true and what changed
- who may do what, under whose authority

`Resume` remains a first-class kernel operation, but not a foundational primitive.
It is a continuation mechanism derived from Task, State, Event, and Checkpoint.

If a candidate feature does not strengthen one of these eight primitives, it should not enter the kernel.

Note on cognition:

AgentOS deliberately does **not** make cognition a kernel primitive.
Cognitive workers and strategies live above the kernel (Layer 5) and are implementation-defined.
The kernel coordinates cognition only through the primitives listed above, and cognition is externally visible only via state, events, capability calls, checkpoints, policy authority, and control signals.

---

# 6. Core Abstractions

## 6.1 Principal

A Principal is the accountable source of authority within the system.

A principal may be:

- a human user
- an organization
- a service identity
- a tenant-scoped operator
- a policy-controlled automated worker
- a delegated runtime identity

A Principal answers the question: **who is acting, on whose behalf, with what rights, under what accountability?**

Without Principals, ownership, delegation, budget, policy, and audit all become ambiguous.

## 6.2 Task

A Task is the smallest durable unit of purposeful execution.

A Task has:

- `task_id`
- `goal`
- `status`
- `owner_principal`
- `budget`
- `priority`
- `policy_context`
- `working_state_ref`
- `history_ref` (transcript projection; non-authoritative)
- `checkpoint_ref`
- `result_ref`

A Task is not a chat session.
A Task may contain many turns, many models, many tools, many pauses, and many handoffs.

## 6.3 State

State is the canonical substrate of execution.

State must be structured, addressable, versioned, and partially replayable.

At minimum, AgentOS distinguishes four classes of state:

### Working State

The active, short-horizon information needed for the next step.

### Durable State

The persisted operational state required to restore execution.

### Semantic Memory

Longer-lived facts, knowledge, preferences, and validated conclusions.

### Evidence State

Artifacts, citations, tool outputs, diffs, logs, and proofs that justify decisions.

## 6.4 Event

Every meaningful execution change emits a structured event.

Examples:

- task started
- model turn started
- model output streamed
- capability requested
- capability approved
- capability denied
- capability result received
- checkpoint written
- policy escalation triggered
- control signal received
- task paused
- task resumed
- task completed
- task failed

Events are append-only.
Events are observable.
Events support replay, audit, monitoring, and debugging.

## 6.5 Capability Call

A Capability Call is the only legal path for producing side effects or reading privileged resources.

Every capability call must include:

- caller principal
- target capability
- input payload
- declared effect class
- expected cost class
- policy context
- correlation id

Every capability result must include:

- outcome
- structured payload
- evidence metadata
- side-effect summary
- retry safety information

## 6.6 Checkpoint

A Checkpoint is a persisted execution boundary from which a task can safely continue.

A checkpoint must be:

- serializable
- restorable
- versioned
- cheap enough to create often
- precise enough to prevent ambiguous recovery

`Resume` is the kernel operation that continues execution from a known checkpoint and event boundary.
It is mandatory in the runtime, but it is not elevated to a separate primitive because it derives its meaning from existing kernel objects.

## 6.7 Policy Authority

Policy Authority is the machine-enforceable source of constraint, permission, escalation, and institutional boundary.

Every sensitive action passes through policy evaluation.
Policy may allow, deny, constrain, defer, or escalate.

The primitive here is not a single decision record but the existence of governing authority that can issue such decisions consistently.

## 6.8 Control Signal

A Control Signal is a higher-authority intervention into task execution.

Control signals may originate from:

- a human principal
- an operator console
- a governance workflow
- a supervisory service
- a safety subsystem

A control signal may:

- approve
- deny
- steer
- pause
- resume
- modify budget
- modify scope
- take over
- reassign
- terminate

This preserves a crucial invariant: the runtime must always remain interruptible by a higher authority.

---

# 7. Kernel Responsibilities

The kernel is responsible for exactly these categories:

## 7.1 Execution lifecycle

Start, pause, resume, cancel, fail, and complete tasks.

## 7.2 State coordination

Load, update, snapshot, and reference the state needed for execution.

## 7.3 Event emission

Emit structured events for all execution-significant changes.

## 7.4 Capability dispatch boundary

Route capability calls through a strict invocation interface.

## 7.5 Checkpointing

Persist execution boundaries in a recoverable form.

## 7.6 Policy enforcement hook

Require policy evaluation at defined control points.

## 7.7 Human control integration

Accept and apply human signals as runtime control operations.

The kernel is **not** responsible for:

- specific tool ecosystems
- model-specific prompt templates
- UI formatting
- memory ranking heuristics
- domain workflows
- business logic
- product opinion

---

# 8. Layered Architecture

## Layer 0: Kernel

Task lifecycle, events, state boundary, checkpoint/resume, policy hooks, control signals.

## Layer 1: Runtime Services

Schedulers, queues, retries, budget managers, distributed coordination, storage, logging, observability.

## Layer 2: Capability Bus

Tools, APIs, databases, file systems, browsers, sandboxes, code runners, search systems, approval systems.

## Layer 3: Memory Plane

Working memory assembly, semantic retrieval, summarization, compression, evidence indexing, state packing.

## Layer 4: Policy Plane

Identity, permissions, tenant isolation, budget policy, compliance, escalation rules, trust zones.

## Layer 5: Cognitive Plane

Model routing, prompt assembly, planning strategies, reflection loops, verifier patterns, handoff logic.

## Layer 6: Product Plane

Coding agents, research agents, customer support agents, enterprise workflow agents, copilots, IDE agents, CLI agents.

The lower the layer, the more conservative its abstraction velocity must be.

---

# 9. The Execution Model

The execution model must remain conceptually simple.

At the highest level:

1. Load task
2. Assemble working state
3. Select next execution step
4. If capability needed, request capability through the bus
5. Apply result into state and evidence
6. Emit events
7. Write checkpoint at safe boundaries
8. Continue, pause, escalate, or complete

This is the permanent loop.
Everything else is policy, memory, or product logic.

---

# 10. The Agent Model

AgentOS does not require one universal concept of “agent.”
Instead, an agent is defined operationally as:

**A task-executing cognitive worker that reads state, reasons over bounded context, proposes or takes actions through capabilities, and advances a task toward completion under policy.**

This definition is intentionally plain.
It allows:

- a single-step assistant
- a long-running autonomous worker
- a verifier agent
- a planning agent
- a specialist tool-using agent
- a human-guided semi-agent

AgentOS does not hardcode a single ideology such as “all problems should use multi-agent graphs.”
That belongs above the kernel.

---

# 11. State Model

## 11.1 State must be typed

Free-form transcript blobs are not enough.
The runtime must support typed references for task, memory, evidence, budget, policy, and control state.

## 11.2 State must be compressible

Long-running tasks cannot depend on infinitely growing context.
State must support compaction, distillation, and tiered retention.

## 11.3 State must be inspectable

Operators must be able to inspect why a task is where it is.
No hidden state should be required for basic diagnosis.

## 11.4 State must be replay-aware

Not all state requires replay, but replay-relevant state must be marked.

## 11.5 State must support semantic projection

The same underlying state may be projected as:

- chat history
- UI summary
- checkpoint bundle
- audit report
- model context pack
- execution timeline

State is primary. Projection is secondary.

---

# 12. Memory Model

AgentOS memory is not “just store more messages.”
It is a managed subsystem.

## 12.1 Working Memory

Small, hot, task-local, rapidly changing.

## 12.2 Episodic Memory

Chronological records of prior executions and meaningful moments.

## 12.3 Semantic Memory

Abstracted facts and validated conclusions extracted from prior work.

## 12.4 Procedural Memory

Reusable methods, strategies, playbooks, tool-use patterns, and workflow recipes.

## 12.5 Evidence Memory

Verifiable outputs, artifacts, citations, logs, tests, diffs, and provenance records.

A future-proof system must keep these concepts distinct even if they share storage infrastructure.

---

# 13. Capability Model

Capabilities are the equivalent of system calls plus drivers.

A capability must declare:

- identity
- version
- required permissions
- input schema
- output schema
- effect class
- idempotency behavior
- timeout class
- cost class
- trust level
- audit requirements

Capabilities should be composable, but composition belongs above the raw capability contract.

A capability may be implemented by:

- local code
- remote API
- sandboxed process
- browser automation
- database query layer
- human approval interface
- another agent system

The kernel does not care how a capability is implemented.
It cares that the contract is explicit.

---

# 14. Policy Model

AgentOS assumes that unconstrained agency is unacceptable in real deployments.
Therefore policy is not optional.

Policy governs:

- who may start tasks
- what a task may access
- which capabilities it may invoke
- when it must escalate
- how much budget it may spend
- which outputs require review
- which actions require human approval
- which trust zones it may cross

Policy outcomes:

- allow
- deny
- allow with constraints
- require approval
- require stronger identity
- require human takeover
- pause task
- terminate task

Policy must be machine-enforceable and human-auditable.

---

# 15. Human-in-the-Loop Model

Human participation is not a fallback for system failure.
It is one normal operating mode.

AgentOS must support:

- approval gates
- inline steering
- branch review
- counterfactual review
- scoped delegation
- partial takeover
- full takeover
- post-hoc audit

The runtime must preserve the continuity of task state when humans intervene.
Human override must not destroy machine progress.

---

# 16. Observability Model

No serious agent substrate can survive without deep observability.

AgentOS observability must support:

- event stream inspection
- task timeline reconstruction
- capability call traces
- policy decision logs
- cost and latency accounting
- checkpoint lineage
- memory assembly trace
- model routing trace
- failure cluster analysis

The system must answer:

- What happened?
- Why did it happen?
- What changed?
- What did it cost?
- What evidence supports it?
- Where did it fail?
- Can it resume?

If a system cannot answer these, it is not a foundation.

---

# 17. Compatibility Principles

To survive for many years, the foundation must be conservative about compatibility.

## 17.1 Stable kernel contract

The kernel primitives should change rarely and reluctantly.

## 17.2 Versioned schemas everywhere

Tasks, state packs, events, checkpoints, and capability contracts must all be versioned.

## 17.3 Forward-compatible extension points

Unknown fields and future capability attributes should not break older runtimes when safely ignorable.

## 17.4 Protocol plurality

The foundation should support multiple capability protocols and model providers.
No single external protocol should become the architecture.

## 17.5 Replaceability over lock-in

A working deployment should be able to replace:

- model provider
- capability implementation
- storage backend
- policy engine
- UI layer

without rewriting the conceptual system.

---

# 18. Extension Model

Extensions must exist, but they must not infect the kernel.

## 18.1 Classes of extension

### Capability Extensions

New tools, APIs, drivers, sandbox connectors.

### Cognitive Extensions

Planning modules, routers, verifiers, debate strategies, decomposition modules.

### Memory Extensions

Retrieval, ranking, summarization, state compaction, evidence indexing.

### Policy Extensions

Compliance packs, approval logic, tenant rules, budget rules.

### Product Extensions

IDE integration, CRM integration, research UI, operations console.

## 18.2 Extension requirements

Every extension must declare:

- API version compatibility
- required permissions
- side-effect class
- failure semantics
- observability surface
- rollback expectations

## 18.3 Extension discipline

An extension may enrich the runtime.
It may not violate kernel invariants.

---

# 19. Failure and Recovery

AgentOS assumes failure as a baseline condition.

The runtime must explicitly handle:

- model timeout
- model degradation
- malformed tool output
- non-deterministic partial execution
- policy denial
- storage failure
- network partition
- budget exhaustion
- stale memory projection
- human interruption
- duplicate event delivery
- duplicate side-effect attempt

Recovery design principles:

- fail closed where side effects are dangerous
- fail open only where policy allows
- separate retryable from non-retryable failure
- checkpoint before expensive or risky transitions
- preserve evidence for diagnosis
- surface recovery affordances to operators

---

# 20. Security Assumptions

The foundation must assume adversarial conditions.

Threat categories include:

- prompt injection
- capability abuse
- privilege escalation
- cross-tenant leakage
- memory poisoning
- tool output deception
- compromised extension packages
- replay abuse
- audit tampering

Therefore AgentOS must support:

- least privilege
- explicit capability grants
- sandboxing
- trust zone separation
- signed extensions
- immutable audit streams
- policy-gated side effects
- provenance tracking

Security is not a plugin.
Security is part of the substrate.

---

# 21. Economic Model

A real foundation must respect economic constraints.

AgentOS must make costs visible at the runtime level:

- model cost
- tool cost
- latency cost
- storage cost
- retry cost
- human review cost

Budgets should apply to:

- per task
- per tenant
- per capability class
- per model class
- per time window

A runtime that cannot govern cost will not survive contact with reality.

---

# 22. What Must Stay Out of the Kernel

To preserve long-term clarity, the following should remain outside the kernel unless proven foundational:

- specific prompt templates
- specific planning algorithms
- agent personalities
- UI chat rendering logic
- domain workflows
- vendor-specific protocols
- default memory ranking heuristics
- product-specific approval UX
- marketplace logic
- monetization logic

The kernel should feel slightly under-featured.
That is a sign of health.

---

# 23. The Foundational APIs

At minimum, AgentOS should stabilize the following API families:

## 23.1 Task API

Create, inspect, schedule, pause, resume, cancel, complete, fail.

## 23.2 State API

Read typed state, write typed state, diff state, snapshot state, project state.

## 23.3 Event API

Subscribe, append, query timeline, filter by correlation id, replay.

## 23.4 Capability API

Register capability, declare contract, invoke capability, return result, surface evidence.

## 23.5 Checkpoint API

Create checkpoint, inspect checkpoint, restore checkpoint, list lineage.

## 23.6 Policy Authority API

Evaluate action, escalate, approve, deny, attach constraints.

## 23.7 Control Signal API

Steer, pause, resume, approve, deny, terminate, take over.

## 23.8 Observability API

Trace task, inspect cost, inspect failures, inspect capability graph, inspect state lineage.

These APIs are more important than any particular implementation language.

---

# 24. The First Implementation Rule

The first implementation of AgentOS must prove the abstraction model without overbuilding.

Version 1 should aim to demonstrate:

- a tiny kernel
- durable task execution
- explicit capability invocation
- checkpoint and resume
- human steering
- policy hooks
- full event traceability

Version 1 should avoid pretending to solve every advanced problem.
The first victory is clarity.
The second is reliability.
Scale comes later.

---

# 25. Language and Runtime Philosophy

The conceptual architecture must not be fused to one language.

The likely future shape is:

- high-iteration product layers in flexible languages
- strong service layers in reliability-oriented languages
- hotspot components in systems languages

The foundation is therefore defined by contracts and invariants, not by implementation fashion.

If the abstractions are good, the runtime can evolve.
If the abstractions are bad, a faster language only produces faster confusion.

---

# 26. Governance Rules for the Foundation

Any proposal to change the foundation should answer:

1. Which kernel primitive does this strengthen?
2. Why can this not live in a higher layer?
3. What long-term invariant does this preserve?
4. What complexity does this add?
5. How does it fail?
6. How is it observed?
7. How is it versioned?
8. How can it be replaced later?

Any proposal that cannot survive these questions should not enter the ground layer.

---

# 27. The Litmus Test

A candidate AgentOS foundation is healthy only if all of the following are true:

- It can run a tiny interactive agent without feeling heavy.
- It can run a long-duration task without pretending memory is infinite.
- It can survive a process crash without losing the task.
- It can explain every important action through events and evidence.
- It can pause for human input without losing continuity.
- It can deny dangerous side effects through policy.
- It can replace models and tools without conceptual collapse.
- It can grow upward without bloating downward.

---

# 28. Final Position

The future foundation of the agent era should not be a giant magical framework.
It should be a strict, durable, extensible substrate.

Its core should be smaller than people expect.
Its interfaces should be clearer than people expect.
Its recovery model should be stronger than people expect.
Its extension surface should be wider than people expect.

The right foundation will feel almost disappointingly simple.
That is exactly what gives it the chance to last.
