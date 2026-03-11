# AgentOS Kernel Spec v0.1

## Subtitle
Normative kernel specification for the minimal durable runtime of the agent era.

## Status
Draft v0.1

## Relationship to the Foundation Document
This document derives from the AgentOS Foundation and translates the constitutional layer into kernel-level normative requirements.

If the Foundation defines **what must exist**, this specification defines **how the kernel must behave**.

This document is intentionally narrower than the Foundation.
It defines only the kernel boundary and its required semantics.
It does not define product UX, model strategy, memory ranking policy, or extension marketplace behavior.

---

# 1. Purpose

The purpose of this specification is to define a minimal, stable, implementable kernel contract for AgentOS.

The kernel exists to support organized, durable, accountable task execution under explicit authority, observable state transitions, capability boundaries, and higher-order control.

This specification is designed to support:

- interactive agents
- long-running agents
- human-in-the-loop workflows
- multi-actor orchestration
- crash recovery
- policy-governed execution
- future compatibility across implementations

---

# 2. Normative Language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** in this document are to be interpreted as normative requirements.

- **MUST / MUST NOT** indicate a mandatory requirement for conformance.
- **SHOULD / SHOULD NOT** indicate a strong recommendation that may be violated only with a clearly justified reason.
- **MAY** indicates an optional behavior that remains within spec.

## 2.1 Canonical field names and equivalence
Some documents in the v0.1 set use different field names for the same semantic concept (for example, retry/idempotency hints, or budget references).

Where this specification allows “or equivalent”, a conforming implementation MUST provide a deterministic mapping to canonical field names as defined in:

- `docs/specs/field-alignment-v0.1.md`

If an implementation exposes a public API with different names, it MUST still be able to present a stable canonical projection for conformance and test harnesses.

---

# 3. Scope

This specification defines the kernel semantics for the following primitives:

1. Principal
2. Task
3. State
4. Event
5. Capability Call
6. Checkpoint
7. Policy Authority
8. Control Signal

This specification also defines the kernel operations that coordinate them, including:

- task lifecycle transitions
- capability dispatch
- checkpoint creation
- checkpoint restore
- policy evaluation points
- control signal injection
- failure handling
- observability requirements

This specification does **not** define:

- a specific model API
- a specific storage engine
- a specific UI
- a specific planner or reasoning strategy
- a specific extension protocol
- a specific identity provider
- a specific memory retrieval algorithm

---

# 4. Kernel Model

The kernel is a durable task execution runtime.

At a minimum, the kernel MUST:

1. hold a durable representation of tasks
2. attach each task to one or more principals
3. maintain structured state references for active execution
4. emit append-only execution-significant events
5. route all privileged action through capability calls
6. evaluate policy at defined control points
7. permit higher-authority control signals
8. persist checkpoints and restore from them

The kernel MUST remain agnostic to:

- specific model vendors
- specific cognitive strategies
- specific application domains

The kernel MUST expose a clean boundary between:

- execution
- state
- history
- authority
- side effects
- interruption

---

# 5. Global Invariants

Any conforming implementation MUST preserve the following invariants.

## 5.1 No ambient authority
No task, model, or extension may perform privileged reads or side effects except through a declared capability boundary.

## 5.2 Append-only event history
Events that are emitted as part of task execution MUST NOT be mutated in place.
Corrections MUST be represented through subsequent events.

## 5.3 State is not identical to transcript
The kernel MUST treat structured state as primary and transcript projections as derived views.

## 5.4 Tasks are durable objects
A task MUST be representable independently of any single process, session, or model invocation.

## 5.5 Control remains possible
The runtime MUST preserve the ability for higher-authority intervention while a task is active, unless the task has already reached a terminal state.

## 5.6 Policy precedes sensitive execution
Any capability call or state transition classified as sensitive by policy MUST be evaluated before execution is committed.

## 5.7 Checkpoints are authoritative recovery boundaries
A restored task MUST resume from a checkpoint boundary, not from an ambiguous reconstruction of prior transcript.

## 5.8 Conformance over convenience
Implementations MAY add behavior, but MUST NOT weaken the guarantees established in this specification.

---

# 6. Principal

## 6.1 Definition
A Principal is the accountable source of authority within the system.

A Principal MAY represent:

- a human user
- an operator
- an organization
- a service account
- an automated worker identity
- a tenant-scoped system identity

## 6.2 Required fields
A conforming Principal object MUST contain at least:

- `principal_id`: globally unique within the runtime domain
- `principal_type`: implementation-defined classification
- `display_name` or equivalent human-readable identifier
- `authority_scope_ref`: reference to the permissions or governing policy context
- `status`: active, suspended, revoked, or equivalent
- `created_at`

## 6.3 Semantics
A Principal MUST be attachable to:

- task ownership
- capability invocation identity
- policy evaluation context
- budget or quota context
- audit trails
- control signals

A Principal MUST NOT be inferred only from UI session state or a raw model turn.

## 6.4 Constraints
A revoked or suspended Principal MUST NOT initiate new privileged actions unless policy explicitly permits limited recovery or administrative operations.

A task MAY reference multiple principals, but the kernel MUST be able to distinguish at minimum:

- owning principal
- acting principal
- approving or overriding principal

---

# 7. Task

## 7.1 Definition
A Task is the smallest durable unit of purposeful execution.

## 7.2 Required fields
A conforming Task object MUST contain at least:

- `task_id`
- `goal`
- `status`
- `owner_principal_id`
- `created_at`
- `updated_at`
- `policy_context_ref`
- `working_state_ref`
- `history_ref`
- `checkpoint_ref` or equivalent nullable reference
- `budget_context_ref` or explicit budget fields
- `priority`
- `result_ref` or equivalent nullable reference

`history_ref` is a transcript projection reference (non-authoritative); see Section 8.6.

## 7.3 Minimum status model
A conforming implementation MUST support at least the following task states:

- `created`
- `ready`
- `running`
- `waiting_on_capability`
- `waiting_on_policy`
- `waiting_on_control`
- `paused`
- `completed`
- `failed`
- `cancelled`

Implementations MAY define additional non-terminal states, but MUST map them safely to this minimum model.

## 7.4 Terminal states
The following states are terminal:

- `completed`
- `failed`
- `cancelled`

Once a task enters a terminal state, it MUST NOT re-enter a non-terminal execution state.
If continuation is required after terminality, a new task MUST be created or a new explicit branch mechanism MUST be used.

## 7.5 Legal state transitions
At minimum, the kernel MUST support the following transitions:

- `created -> ready`
- `ready -> running`
- `running -> waiting_on_capability`
- `waiting_on_capability -> running`
- `running -> waiting_on_policy`
- `waiting_on_policy -> running`
- `running -> waiting_on_control`
- `waiting_on_control -> running`
- `running -> paused`
- `paused -> ready` or `paused -> running`
- `running -> completed`
- `running -> failed`
- `running -> cancelled`
- `waiting_on_capability -> failed`
- `waiting_on_policy -> failed`
- `waiting_on_control -> failed`
- `paused -> cancelled`

Illegal state transitions MUST be rejected and MUST emit a failure-class event.

## 7.6 Task ownership
Every task MUST have an owning principal.
A task MAY also have:

- an acting principal
- a supervising principal
- a delegated principal
- an approving principal

For v0.1 Core conformance, the kernel MUST ensure these roles are not “lost in prose”:

- The owning principal MUST be explicitly recorded as `owner_principal_id`.
- The acting principal for any privileged action MUST be explicitly recorded on the relevant record and events (for capability calls, this is `caller_principal_id`).
- The approving or overriding principal MUST be explicitly recorded on control signals and their corresponding control events.

Implementations MAY denormalize role fields onto the Task object for convenience, but MUST keep them consistent with the event stream.

## 7.7 Task continuity
A task MUST remain semantically stable across:

- process restart
- worker reassignment
- checkpoint restore
- model replacement
- human steering

A task is not equivalent to a chat thread.
A task MAY project into chat, but chat MUST NOT be the sole durable carrier of task identity.

## 7.8 Budget context
If a Task uses `budget_context_ref`, that reference MUST resolve to a budget context object with a stable, machine-readable shape suitable for policy and observability.

For v0.1, the minimum required shape is defined in:

- `docs/specs/budget/budget-context-spec-v0.1.md`

---

# 8. State

## 8.1 Definition
State is the canonical structured substrate of execution.

## 8.2 State classes
A conforming implementation MUST distinguish at minimum:

- `working_state`
- `durable_state`
- `semantic_memory_ref` or equivalent typed reference
- `evidence_state_ref` or equivalent typed reference

These MAY share storage backends, but MUST remain semantically distinguishable.

## 8.3 Required properties
State MUST be:

- addressable
- versioned or version-observable
- inspectable
- serializable where required for checkpointing
- partially replayable where needed for recovery

## 8.4 State writes
All kernel-visible state mutations MUST either:

- emit a corresponding event, or
- be derivable from an emitted event and an associated state version change

Silent mutation of task-relevant state is non-conforming.

## 8.5 State consistency
The kernel SHOULD preserve atomicity between:

- critical state transition
- associated event emission
- checkpoint updates when applicable

If exact atomicity is not possible in the implementation substrate, the runtime MUST define and document a recovery-safe write order.

## 8.5.1 Recommended durability pattern \(non-normative\)
For v1 implementations, the recommended default is a transactional or quasi-transactional pattern such as:

- durable state write plus outbox intent in one local transaction where possible
- asynchronous event publication from the outbox
- checkpoint creation at explicit post-commit recovery boundaries

If a full transactional outbox is not feasible, the implementation SHOULD still define a single authoritative durability source and a replay procedure that can deterministically reconcile:

- state version
- pending event publication
- checkpoint lineage

The goal is not perfect distributed atomicity.
The goal is bounded inconsistency with deterministic recovery.

## 8.6 Transcript projection (history_ref)
A transcript is a projection: a derived view computed from authoritative kernel data.

A transcript MAY be generated from task state and events.
If a Task includes `history_ref`, it MUST refer to such a transcript projection (canonical name: `transcript_projection_ref`; see `docs/specs/field-alignment-v0.1.md`).

A transcript MUST NOT be treated as authoritative for recovery, policy enforcement, or audit.
Authoritative recovery and audit depend on checkpoints, state, and append-only events.

If a transcript is persisted, it MUST be:

- regeneratable from state + events, **or**
- verifiable against state + events (for example, by referencing an event_id/sequence_number range or a hash over the referenced events)

---

# 9. Event

## 9.1 Definition
An Event is an append-only record of an execution-significant transition.

## 9.2 Required fields
Every event MUST contain at least:

- `event_id`
- `task_id`
- `event_type`
- `occurred_at`
- `emitted_by` or equivalent actor reference
- `correlation_id` where applicable
- `sequence_number` or equivalent monotonic ordering field within task scope
- `payload`

## 9.3 Required properties
Events MUST be:

- append-only
- ordered within task scope
- queryable by task
- attributable to a principal or kernel component
- durable enough to support recovery and audit

## 9.4 Minimum required event families
A conforming kernel MUST support events for at least:

- task lifecycle changes
- capability request start
- capability result received
- capability denied
- policy evaluation start/result
- checkpoint created
- checkpoint restore start/result
- control signal received
- task failure
- task completion

Implementations MAY emit more granular events, including streaming deltas, but MUST preserve the minimum required families.

## 9.5 Event correction
If an event payload is incorrect, the runtime MUST NOT rewrite prior event history.
It MUST instead emit a correction or superseding event.

## 9.6 Idempotent replay
The kernel SHOULD make event replay idempotent where practical.
If perfect idempotence is impossible, replay hazards MUST be documented and bounded.

---

# 10. Capability Call

## 10.1 Definition
A Capability Call is the only legal boundary for privileged reads and side effects.

## 10.2 Required fields
Every capability invocation MUST contain at least:

- `call_id`
- `task_id`
- `caller_principal_id`
- `capability_id`
- `capability_version` or resolvable contract reference
- `input_payload`
- `effect_class`
- `cost_class` (may be resolved from capability registration)
- `requested_at`
- `policy_context_ref`
- `budget_context_ref`
- `idempotency_expectation` (alias: `idempotency_hint`)
- `correlation_id` (default: `call_id`)

## 10.3 Required result fields
Every capability result MUST contain at least:

- `call_id`
- `capability_id`
- `outcome`
- `completed_at` or equivalent
- `result_payload` or equivalent structured response
- `evidence_ref` or inline evidence metadata if applicable
- `side_effect_summary`
- `retry_safety`

## 10.4 Outcomes
At minimum, the kernel MUST support these outcomes:

- `succeeded`
- `failed`
- `denied`
- `timed_out`
- `cancelled`

Implementations MAY define additional subcodes.

## 10.5 Invocation rule
A task, model, or extension MUST NOT directly access privileged resources outside the capability boundary.
Any such behavior is non-conforming.

## 10.6 Dispatch semantics
Capability invocation MUST emit an event when:

- invocation begins
- policy blocks invocation
- invocation completes or fails

## 10.7 Idempotency and retries
Every capability MUST declare retry expectations.
The kernel MUST preserve enough metadata to distinguish:

- safe retry
- unsafe retry
- unknown retry safety

For side-effecting capabilities, the kernel SHOULD checkpoint before risky transitions when practical.

---

# 11. Checkpoint

## 11.1 Definition
A Checkpoint is an explicit persisted recovery boundary for a task.

## 11.2 Required contents
A checkpoint MUST contain or reference enough information to restore at minimum:

- task identity and status
- relevant state references
- event boundary or sequence position
- current policy context reference
- current budget context reference
- in-flight capability status if any
- creation timestamp
- checkpoint version

## 11.3 Validity
A checkpoint MUST represent a coherent recovery point.
A partially written or unverifiable checkpoint MUST NOT be treated as a valid restore source.

## 11.4 Creation points
The kernel MUST support checkpoint creation at least:

- before or after major side-effect boundaries
- before pausing a task
- after meaningful state convergence
- before worker handoff
- at operator request if policy permits

Implementations MAY add automatic checkpoint heuristics.

## 11.5 Restore semantics
Restore MUST resume from a known checkpoint boundary, not from an approximate reconstruction of transcript or raw memory.

Restore MUST emit events indicating:

- restore requested
- restore started
- restore succeeded or failed

## 11.6 Branching
An implementation MAY support branching from a checkpoint.
If branching is supported, the resulting branch MUST have:

- distinct task identity or explicit branch identity
- visible lineage
- isolated future event sequence

A Kernel Core implementation SHOULD NOT enable checkpoint branching by default in v0.1 unless it also defines, at minimum:

- branch budget inheritance or isolation semantics
- principal and delegation inheritance semantics
- policy-context inheritance semantics
- failure isolation semantics between parent and branch
- completion and result lineage semantics

If these are not explicitly defined, branching MUST remain disabled or experimental.

---

# 12. Policy Authority

## 12.1 Definition
Policy Authority is the governing source of machine-enforceable permission, constraint, escalation, and institutional boundary.

## 12.2 Required evaluation points
The kernel MUST permit policy evaluation at minimum before:

- privileged capability execution
- trust-zone boundary crossing
- budget escalation
- external side effects
- checkpoint restore if identity or environment has materially changed
- control actions that alter authority, scope, or delegation

### 12.2.1 Trust zones (v0.1 minimal)
In v0.1, the trust-zone model MAY be implementation-defined, but it MUST be explicit.

- A trust zone is an isolation partition for resources and data (tenant boundary, network boundary, environment boundary, compliance boundary).
- A trust-zone boundary crossing is any operation that would read, write, or disclose data across zones without an explicit policy decision.
- Trust zones are distinct from capability `trust_level`: trust_level describes *the provider/execution environment*, while trust zones describe *the resource/data domain being touched*.

Policy evaluation MUST have access to enough information to decide whether a given capability call crosses a trust-zone boundary.
If the requested zone surface is unknown, policy MUST treat it conservatively.

## 12.3 Policy outcomes
At minimum, policy evaluation MUST support the following outcomes:

- `allow`
- `deny`
- `allow_with_constraints`
- `require_approval`
- `require_stronger_authority`
- `pause`
- `terminate`

## 12.3.1 Approval gating (canonical v0.1 projection)
`require_approval` is a policy outcome that blocks an action until higher authority intervenes.

If policy returns `require_approval` for a privileged capability call, the kernel MUST:

1. NOT dispatch the privileged capability call
2. emit a `policy.result.require_approval` event correlated to the blocked call (for example via `call_id` and/or an `approval_gate_id`)
3. transition the task into a waiting state that represents awaiting authority (recommended: `waiting_on_control`)

To continue, a control signal MUST resolve the gate:

- `approve` MUST permit the previously blocked call to proceed (subject to any additional constraints)
- `deny` MUST prevent it from proceeding and MUST leave an auditable record of the denial

The runtime MAY use an `approval.request` capability (effect_class=`authority_mediated`) to create an approval workflow artifact, but that capability’s success MUST NOT be interpreted as final approval.
Final approval or denial MUST be represented as a control signal so it is a uniform, high-authority, replay-safe intervention in the kernel semantics.

## 12.4 Constraint semantics
If policy returns `allow_with_constraints`, the kernel MUST bind the resulting execution to those constraints or reject execution.
Ignoring applied constraints is non-conforming.

## 12.5 Explainability
Policy results SHOULD include a machine-readable reason code.
They MAY include a human-readable explanation.

## 12.6 Separation of concerns
The kernel MUST provide a policy hook.
The kernel MAY embed a default policy engine, but MUST NOT require one specific policy implementation model for conformance.

---

# 13. Control Signal

## 13.1 Definition
A Control Signal is a higher-authority intervention into task execution.

## 13.2 Allowed issuers
A control signal MUST be attributable to a principal or trusted kernel authority.
Anonymous high-authority intervention is non-conforming.

## 13.3 Minimum control actions
The kernel MUST support at least:

- `approve`
- `deny`
- `steer`
- `pause`
- `resume`
- `modify_budget`
- `modify_scope`
- `take_over`
- `reassign`
- `terminate`

## 13.4 Injection semantics
A control signal MUST:

- be recorded as an event
- indicate issuing authority
- indicate intended target task
- indicate requested effect
- produce either an applied outcome or explicit rejection

## 13.5 Priority
Control signals SHOULD be processed ahead of ordinary non-critical execution steps once the runtime reaches a safe interruption boundary.

If the runtime supports immediate interruption, the implementation MUST define which operations are interruptible and which are only deferrably interruptible.

## 13.5.1 Safe interruption boundary
A safe interruption boundary is a point at which the task can be paused, redirected, or terminated without:

- corrupting durable state
- losing event attribution
- duplicating or ambiguously committing a side effect
- breaking checkpoint recoverability

At minimum, a conforming implementation MUST document whether the following are safe interruption boundaries:

- before capability dispatch
- after capability result commit
- before checkpoint creation
- after checkpoint creation
- during model output streaming
- during policy evaluation

If model output streaming is interruptible, the implementation MUST define whether partially streamed output is:

- discarded
- preserved as non-authoritative output
- committed as authoritative task state

Interruptibility during non-idempotent side-effect execution SHOULD be treated as unsafe unless the implementation provides a stronger transactional guarantee.

## 13.6 Safety
If applying a control signal would create state corruption, the kernel MAY defer application until a safe boundary, but MUST emit an event recording the deferment.

---

# 14. Kernel Operations

## 14.1 Create Task
The kernel MUST provide an operation to create a task from:

- owning principal
- goal
- initial policy context
- initial budget context
- initial working state

Task creation MUST emit a task-created event.

## 14.2 Start Task
A task in `created` or `ready` state MAY be started if policy permits.
Task start MUST emit a task-running transition event.

## 14.3 Advance Task
The kernel MUST support advancing a task by one or more execution steps.
An execution step MAY include:

- state assembly
- model invocation
- capability dispatch
- policy evaluation
- checkpoint creation
- terminal completion evaluation

The internal cognitive mechanism is implementation-defined, but externally visible consequences MUST still conform to kernel invariants.

## 14.4 Pause Task
Pausing a task MUST:

- update task status
- emit an event
- preserve enough state to resume later
- optionally write a checkpoint if policy or runtime settings require it

## 14.5 Resume Task
Resume is a kernel operation, not a foundational primitive.

A conforming resume operation MUST:

- target a non-terminal task or a valid checkpoint lineage
- restore or reattach the required state references
- establish a clear recovery boundary
- emit restore and resume events
- reject ambiguous continuation

## 14.6 Cancel Task
Cancellation MUST move the task to a terminal state and emit a cancellation event.
In-flight capabilities SHOULD be cancelled when safe and supported.

## 14.7 Complete Task
Completion MUST move the task to `completed`, persist result references if any, and emit a completion event.

## 14.8 Fail Task
Failure MUST move the task to `failed`, preserve failure evidence where available, and emit a failure event.

Failure SHOULD distinguish between:

- recoverable failure
- non-recoverable failure
- policy-induced termination
- operator-induced termination

---

# 15. Failure Model

## 15.1 Failure classes
A conforming kernel SHOULD distinguish at least the following failure classes:

- policy failure
- capability failure
- state consistency failure
- checkpoint failure
- control application failure
- task logic failure
- infrastructure failure

## 15.2 Failure recording
Every terminal or materially execution-affecting failure MUST emit an event and SHOULD preserve structured diagnostic context.

## 15.3 Recovery behavior
If recovery is possible, the kernel MAY pause a task instead of failing it terminally.
If recovery is not possible, the kernel MUST fail or cancel the task explicitly.
Silent abandonment is non-conforming.

## 15.4 Duplicate execution defense
The runtime SHOULD provide safeguards against duplicate side-effect execution during retries, replays, or restores.

---

# 16. Observability Requirements

## 16.1 Minimum introspection
A conforming implementation MUST provide enough introspection to answer, for any task:

- current status
- owning principal
- latest checkpoint
- latest event sequence position
- active or last capability call
- current policy context reference
- current budget context reference
- terminal result or failure cause if task is terminal

## 16.2 Timeline reconstruction
A conforming implementation MUST support reconstruction of the ordered task timeline from stored events.

## 16.3 State lineage visibility
The implementation SHOULD expose state version lineage sufficient for debugging and recovery analysis.

## 16.4 Cost visibility
The implementation SHOULD expose model, capability, and task-level cost surfaces where available.

---

# 17. Conformance Levels

## 17.1 Kernel Core Conformance
An implementation is Kernel Core conformant only if it satisfies the MUST requirements of Sections 4 through 16.

## 17.2 Extended Conformance
An implementation MAY claim Extended Conformance if it additionally supports:

- checkpoint branching
- distributed worker failover
- multi-principal approval chains
- richer event replay semantics
- stronger cost and budget telemetry
- stronger trust-zone enforcement

Extended behavior MUST NOT break Kernel Core behavior.

---

# 18. Minimal Conformance Test Themes

A future conformance suite SHOULD test at minimum:

1. illegal task state transition rejection
2. event append-only behavior
3. policy denial before privileged capability call
4. checkpoint creation and restore correctness
5. control signal pause and resume semantics
6. duplicate capability retry safety metadata presence
7. terminal state immutability
8. principal attribution on capability calls and control signals
9. failure event emission
10. task continuity across process restart

---

# 19. Implementation Guidance (Non-Normative)

The first implementation SHOULD optimize for clarity, not breadth.

Recommended early constraints:

- single-node runtime
- explicit persistent event log
- explicit state store
- small number of built-in capabilities
- synchronous or semi-synchronous policy hook
- explicit checkpoint store
- operator-visible task inspector
- documented recovery-safe write order for state, event publication, and checkpoint creation

For most v1 systems, a transactional outbox or equivalent recovery-safe publication pattern is strongly recommended.

The first implementation SHOULD avoid:

- massive extension surface before invariants are proven
- hidden state derived only from prompts
- overloading chat threads as task identity
- implicit side-effect channels
- automatic autonomy without control injection points

---

# 20. Open Questions for v0.2

The following topics are intentionally left open for future versions:

- multi-task transactions
- cross-task dependency semantics
- distributed checkpoint coordination
- capability sandbox attestation
- branch merge semantics
- branch budget and authority inheritance semantics
- delegation chains and sub-principal structures
- formal trust-zone taxonomy and attestation model
- richer standard budget schema extensions
- formal evidence schema
- deterministic replay guarantees
- safe interruption boundary taxonomy

These are important, but they are not required for Kernel Spec v0.1.

---

# 21. Final Statement

Kernel Spec v0.1 is designed to be small enough to implement, strict enough to govern, and abstract enough to survive shifts in model vendors, tool protocols, and product fashions.

Its purpose is not to predict every future feature.
Its purpose is to prevent the future from rotting the core.
