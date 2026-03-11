# AgentOS Event Schema Spec v0.1

## Subtitle
Normative event schema and lineage rules for durable, auditable task execution.

## Status
Draft v0.1

## Relationship to Other Documents
This specification derives from:

- AgentOS Foundation
- AgentOS Kernel Spec v0.1

If the Kernel Spec defines that events MUST exist and remain append-only, this document defines the minimum schema, semantics, ordering, and lineage requirements for those events.

---

# 1. Purpose

The purpose of this specification is to define a stable event model for AgentOS.

Events are the historical spine of the runtime.
They are the mechanism by which the system preserves:

- time-ordered execution history
- attribution
- causality
- recovery boundaries
- policy review
- cost and latency accounting
- auditability
- replay support

A conforming implementation may vary in storage, transport, and indexing strategy, but MUST preserve the event semantics defined here.

---

# 2. Normative Language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative.

---

# 3. Scope

This specification defines:

- the event envelope
- required fields
- ordering and lineage rules
- event families
- correction rules
- replay expectations
- minimal privacy and redaction semantics
- minimum conformance requirements

This specification does **not** define:

- a particular event transport system
- a particular database or queue
- a full evidence schema
- a UI timeline format
- vendor-specific tracing integrations

---

# 4. Event Model

An Event is an append-only record of an execution-significant occurrence within the lifetime of a task.

An Event MUST represent one of the following:

- a task lifecycle transition
- a capability lifecycle transition
- a policy lifecycle transition
- a checkpoint lifecycle transition
- a control lifecycle transition
- a kernel-detected failure or correction
- an implementation-defined execution-significant state transition

Events are not arbitrary logs.
They are structured records that participate in lineage, replay, diagnosis, and audit.

---

# 5. Event Envelope

## 5.1 Required top-level fields
Every event MUST contain at least:

- `schema_version`
- `event_id`
- `event_type`
- `event_family`
- `task_id`
- `sequence_number`
- `occurred_at`
- `recorded_at`
- `emitted_by`
- `payload`

## 5.2 Recommended top-level fields
A conforming implementation SHOULD include:

- `correlation_id`
- `causation_event_id`
- `principal_id`
- `principal_role`
- `component_id`
- `policy_context_ref`
- `budget_context_ref`
- `checkpoint_ref`
- `state_version_ref`
- `evidence_ref`
- `trace_ref`
- `tenant_id` or equivalent isolation identifier

## 5.3 Field semantics

### `schema_version`
MUST identify the schema version of the event envelope and payload interpretation.

### `event_id`
MUST be globally unique within the runtime domain.
Event IDs MUST NOT be reused.

### `event_type`
MUST identify the specific event kind.
Examples: `task.started`, `capability.result.succeeded`, `policy.result.denied`.

### `event_family`
MUST identify the broader event family.
Allowed minimum families are defined in Section 7.

### `task_id`
MUST identify the task to which the event belongs.

### `sequence_number`
MUST be monotonic within task scope.
Sequence numbers MUST NOT be reused within a task.
For v0.1 Core conformance, gaps MUST NOT occur.
Extended implementations MAY allow gaps only if they document the gap semantics and still preserve deterministic ordering.

### `occurred_at`
MUST represent the time at which the underlying event occurred or was deemed to have occurred by the kernel.

### `recorded_at`
MUST represent the time at which the event was durably recorded.

### `emitted_by`
MUST identify the emitting kernel component, trusted subsystem, or attributed principal pathway.

### `principal_id` (recommended)
If the event is attributable to a principal action, `principal_id` SHOULD identify the principal on whose behalf the event occurred.
For capability calls, this SHOULD match `caller_principal_id`.
For control signals, this SHOULD match the issuing principal.

### `principal_role` (recommended)
If `principal_id` is present, `principal_role` SHOULD indicate the principal’s role for this event (for example: `owning`, `acting`, `approving`, `supervising`, `delegated`).

### `policy_context_ref` (recommended)
If present, this SHOULD reference the governing policy context in effect for the event.

### `budget_context_ref` (recommended)
If present, this SHOULD reference the budget context in effect for the event.
For v0.1, the minimum budget context shape is defined in `docs/specs/budget/budget-context-spec-v0.1.md`.

### `payload`
MUST be a structured object.
Unstructured free text alone is non-conforming.

---

# 6. Event Identity, Ordering, and Lineage

## 6.1 Global identity
Every event MUST have a unique `event_id`.

## 6.2 Task-local ordering
Every event MUST have a `sequence_number` that provides a stable ordering within the task.
The implementation MUST be able to reconstruct a total order of events within a task.
For v0.1 Core conformance, the total order SHOULD be representable as a contiguous sequence with no gaps.

## 6.3 Cross-task ordering
Cross-task global ordering MAY be approximate.
A conforming implementation is NOT required to provide a globally total order across all tasks.

## 6.4 Causation
If one event directly results from another, the derived event SHOULD include `causation_event_id`.

Examples:

- a capability result event caused by a capability start event
- a policy denial event caused by a capability request event
- a task failure event caused by a checkpoint restore failure event

## 6.5 Correlation
Events that belong to one logical execution episode SHOULD share a `correlation_id`.

Examples:

- one capability request and its result events
- one restore request and restore completion
- one approval workflow and its eventual resolution

## 6.6 State lineage
If the event changes task-relevant state, the event SHOULD include a `state_version_ref` or equivalent lineage reference.

## 6.7 Checkpoint lineage
Events associated with checkpoint creation, restore, or branch operations SHOULD include `checkpoint_ref`.

---

# 7. Minimum Event Families

A conforming implementation MUST support at least the following `event_family` values:

- `task`
- `capability`
- `policy`
- `checkpoint`
- `control`
- `failure`
- `kernel`

Implementations MAY add additional families, but MUST NOT overload these names with incompatible semantics.

## 7.1 Task family
The task family covers lifecycle events such as:

- `task.created`
- `task.ready`
- `task.started`
- `task.paused`
- `task.resumed`
- `task.completed`
- `task.failed`
- `task.cancelled`

## 7.2 Capability family
The capability family covers capability lifecycle events such as:

- `capability.requested`
- `capability.policy_blocked`
- `capability.dispatched`
- `capability.result.succeeded`
- `capability.result.failed`
- `capability.result.denied`
- `capability.result.timed_out`
- `capability.result.cancelled`

## 7.3 Policy family
The policy family covers policy evaluation events such as:

- `policy.evaluation.requested`
- `policy.evaluation.completed`
- `policy.result.allow`
- `policy.result.deny`
- `policy.result.allow_with_constraints`
- `policy.result.require_approval`
- `policy.result.pause`
- `policy.result.terminate`

## 7.4 Checkpoint family
The checkpoint family covers checkpoint lifecycle events such as:

- `checkpoint.create.requested`
- `checkpoint.created`
- `checkpoint.restore.requested`
- `checkpoint.restore.started`
- `checkpoint.restore.succeeded`
- `checkpoint.restore.failed`
- `checkpoint.branch.created`

## 7.5 Control family
The control family covers higher-authority intervention events such as:

- `control.signal.received`
- `control.signal.deferred`
- `control.signal.applied`
- `control.signal.rejected`

## 7.6 Failure family
The failure family covers failures not already fully expressed by the above families, such as:

- `failure.state_consistency`
- `failure.infrastructure`
- `failure.replay`
- `failure.kernel`

## 7.7 Kernel family
The kernel family covers implementation-significant events that affect runtime semantics but do not fit the other families cleanly.
Kernel events SHOULD be used sparingly.

---

# 8. Required Payload Conventions

## 8.1 Common payload fields
Event payloads SHOULD include, where applicable:

- `status_before`
- `status_after`
- `reason_code`
- `summary`
- `applied_constraints`
- `effect_class`
- `retry_safety`
- `cost_snapshot`
- `latency_ms`
- `references`

## 8.2 Structured payload requirement
Payloads MUST be machine-readable.
Human-readable text MAY be included, but MUST NOT be the only meaningful representation.

## 8.3 Bounded payload size
Implementations SHOULD keep event payloads bounded.
Large artifacts SHOULD be referenced indirectly through `evidence_ref`, `state_version_ref`, or equivalent references.

## 8.4 Sensitive data
Events SHOULD avoid embedding unnecessary sensitive data directly in payloads.
Sensitive content SHOULD be referenced rather than duplicated when possible.

## 8.5 Approval and authority gating conventions
If policy blocks execution pending approval (`policy.result.require_approval`), the event payload SHOULD include stable linkage to the blocked action, such as:

- `approval_gate_id` (a stable identifier for the approval gate)
- `blocked_call_id` (the blocked capability `call_id`, if applicable)

If a control signal resolves an approval gate (for example, `approve` or `deny`), the control event payload SHOULD include:

- `action`
- `issuer_principal_id` (or equivalent, if not already provided via top-level `principal_id`)
- `approval_gate_id` and/or `blocked_call_id`

---

# 9. Event Write Semantics

## 9.1 Append-only rule
Once durably recorded, an event MUST NOT be mutated in place.

## 9.2 Write durability
An implementation MUST define when an event is considered durably recorded.
This boundary MUST be stable enough for replay and audit.

## 9.3 Recovery-safe ordering
If state mutation, event recording, and checkpoint creation are not fully atomic, the implementation MUST define a recovery-safe ordering.

At minimum, the implementation MUST document how it reconciles:

- state version
- event sequence position
- pending event publication
- checkpoint lineage

## 9.4 Event publication versus event durability
Transport publication MAY be delayed.
Durable recording semantics MUST remain distinguishable from downstream publication semantics.

---

# 10. Correction and Supersession

## 10.1 Correction rule
Incorrect events MUST NOT be rewritten.
Corrections MUST be represented through subsequent events.

## 10.2 Minimum correction fields
A correction or supersession event SHOULD include:

- `corrects_event_id`
- `reason_code`
- `corrected_fields` or semantic equivalent
- corrected payload or reference

## 10.3 Historical preservation
Original erroneous events MUST remain queryable for audit unless policy or law requires restricted visibility.

---

# 11. Replay Semantics

## 11.1 Replay purpose
A conforming implementation MUST support replay for at least one of the following purposes:

- audit reconstruction
- failure diagnosis
- checkpoint validation
- task timeline reconstruction

## 11.2 Replay boundary
Replay MUST respect:

- task-local sequence order
- correction events
- terminal state semantics
- checkpoint boundaries when applicable

## 11.3 Side-effect safety
Replaying event history MUST NOT itself re-trigger side effects unless the implementation is explicitly running in a controlled recovery mode that documents such behavior.

## 11.4 Idempotence guidance
The implementation SHOULD strive to make replay interpretation idempotent.
When that is impossible, replay hazards MUST be documented.

---

# 12. Redaction, Privacy, and Restricted Visibility

## 12.1 Visibility layers
A conforming implementation MAY support multiple visibility layers over the same event stream, including:

- full internal visibility
- operator visibility
- tenant visibility
- audit visibility
- redacted external visibility

## 12.2 Redaction rule
If redaction is applied for a viewer, the underlying event identity and sequence SHOULD remain stable.
Redaction SHOULD be a projection-layer concern rather than destructive mutation of core event history.

## 12.3 Sensitive payload policy
If policy forbids raw payload exposure, the runtime MAY replace sensitive fields with:

- redacted markers
- cryptographic digests
- evidence references
- policy reason codes

---

# 13. Event Schema Versioning

## 13.1 Envelope versioning
The event envelope MUST be versioned via `schema_version`.

## 13.2 Payload evolution
Payload schemas MAY evolve between versions, but implementations MUST preserve enough compatibility information to interpret historical events.

## 13.3 Forward compatibility
Unknown fields SHOULD be ignored when safely ignorable.
Unknown mandatory semantics MUST cause explicit rejection or quarantine rather than silent corruption.

---

# 14. Minimum Conformance Requirements

An implementation is Event Schema Core conformant only if it:

1. emits append-only structured events
2. provides stable task-local event ordering
3. supports the minimum required event families
4. preserves unique event identity
5. supports correction without in-place mutation
6. provides durable recording semantics
7. supports ordered task timeline reconstruction
8. distinguishes event durability from downstream publication

---

# 15. Non-Normative Implementation Guidance

Recommended v1 choices:

- use a durable local event store as the source of truth
- publish to external buses asynchronously when needed
- keep event payloads small and structured
- reference large artifacts indirectly
- define a compact reason code taxonomy early
- avoid overloading kernel events for application chatter

Common failure smell:

- using free-form logs as substitute events
- mutating stored events to “fix” history
- relying on wall-clock time alone for task ordering
- publishing events externally before they are durably recorded
- embedding giant artifacts directly in payloads

---

# 16. Open Questions for v0.2

- standard reason code taxonomy
- stronger cross-task causality model
- event signature and tamper-evidence model
- standard redaction envelope
- stronger replay certification semantics
- branch lineage event conventions
- event compaction and archival policy

---

# 17. Final Statement

The event schema is not auxiliary plumbing.
It is the accountable memory of the runtime.

If state tells the system where it stands, events tell the system how it got there.
A durable agent substrate without durable event discipline is just a forgetful machine wearing enterprise clothing.
