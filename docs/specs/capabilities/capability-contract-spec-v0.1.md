# AgentOS Capability Contract Spec v0.1

## Subtitle
Normative contract model for governed capability invocation in AgentOS.

## Status
Draft v0.1

## Relationship to Other Documents
This specification derives from:

- AgentOS Foundation
- AgentOS Kernel Spec v0.1
- AgentOS Event Schema Spec v0.1

If the Kernel Spec defines that all privileged reads and side effects MUST cross a capability boundary, this document defines the minimum structure, semantics, and governance contract for that boundary.

---

# 1. Purpose

The purpose of this specification is to define a stable capability contract for AgentOS.

A capability contract is the runtime equivalent of a system call contract plus a governed driver interface.
It exists to make privileged action:

- explicit
- governable
- attributable
- typed
- auditable
- retry-aware
- cost-visible
- portable across implementations

A capability is not merely a tool description for a model.
It is a controlled interface between cognition and consequence.

---

# 2. Normative Language

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative.

---

# 3. Scope

This specification defines:

- capability identity and registration
- capability metadata
- invocation contract
- result contract
- effect classification
- idempotency and retry semantics
- policy integration
- evidence and observability requirements
- minimum conformance requirements

This specification does **not** define:

- one specific transport protocol
- one specific packaging format
- one specific extension marketplace model
- one specific sandbox implementation
- one specific auth provider

---

# 4. Capability Model

A Capability is a governed interface that permits a task to read privileged resources, produce side effects, or request authority-mediated operations.

Examples include:

- reading files in a protected workspace
- executing a command in a sandbox
- querying a private database
- calling an external API
- writing to a repository
- invoking a browser automation environment
- requesting human approval
- calling another trusted subsystem

A capability MAY be implemented by:

- local code
- remote service
- sandboxed process
- brokered system API
- human approval interface
- delegated runtime worker

The kernel does not require one implementation style.
It requires one explicit contract boundary.

---

# 5. Capability Identity and Registration

## 5.1 Required identity fields
Every registered capability MUST define at least:

- `capability_id`
- `capability_name`
- `capability_version`
- `contract_version`
- `description`
- `provider_id` or equivalent implementation owner reference
- `status`

## 5.2 Required registration metadata
Every registered capability MUST define at least:

- `input_schema`
- `output_schema`
- `effect_class`
- `idempotency_class`
- `timeout_class`
- `cost_class`
- `trust_level`
- `required_authority_scope`
- `observability_class`

## 5.3 Optional metadata
A capability SHOULD also declare, where applicable:

- `streaming_support`
- `supports_cancellation`
- `supports_partial_result`
- `supports_checkpoint_safe_resume`
- `side_effect_surface`
- `trust_zone_surface`
- `external_dependencies`
- `sandbox_profile`
- `evidence_profile`
- `rate_limit_class`
- `tenant_restrictions`

## 5.4 Registration semantics
A capability MUST be registered before privileged invocation.
Ad hoc privileged access outside registered capability contracts is non-conforming.

## 5.5 Status values
A capability MUST support at least these statuses:

- `active`
- `disabled`
- `deprecated`
- `revoked`

A revoked capability MUST NOT accept new invocations.
A deprecated capability MAY continue to function if policy permits.

---

# 6. Capability Taxonomy

## 6.1 Effect class
Every capability MUST declare one `effect_class`.
At minimum, a conforming implementation MUST support:

- `read_only`
- `state_mutating_internal`
- `external_side_effect`
- `authority_mediated`
- `mixed`

### `read_only`
The capability reads data without intentionally mutating external or durable state.

### `state_mutating_internal`
The capability mutates runtime-controlled or tenant-controlled state without immediate external side effects.

### `external_side_effect`
The capability may cause effects outside the runtime boundary.
Examples: sending messages, writing to third-party systems, launching jobs.

#### v0.1 classification guidance: filesystem and workspace
If a capability writes only to a runtime-managed tenant workspace (a sandboxed, runtime/tenant-controlled storage namespace), it MUST be classified as `state_mutating_internal`.
If it can write outside that workspace (host filesystem, operator machine, third-party systems, or uncontrolled mounts), it MUST be classified as `external_side_effect`.

Implementations SHOULD split “workspace write” and “host/system write” into separate capabilities rather than using one capability whose `effect_class` depends on deployment.

### `authority_mediated`
The capability itself is an authority boundary, such as human approval or policy escalation.
If an authority-mediated capability creates an approval workflow artifact (for example, `approval.request`), its success MUST NOT be interpreted as final approval.
Final approval or denial MUST be represented as a control signal in the kernel so it remains auditable and replay-safe.

### `mixed`
The capability combines more than one effect class and therefore SHOULD be used cautiously.
A mixed capability SHOULD document its side-effect surface clearly.

## 6.2 Idempotency class
Every capability MUST declare one `idempotency_class`.
At minimum, a conforming implementation MUST support:

- `idempotent`
- `conditionally_idempotent`
- `non_idempotent`
- `unknown`

## 6.3 Timeout class
Every capability MUST declare one `timeout_class`.
At minimum, a conforming implementation MUST support:

- `short`
- `medium`
- `long`
- `operator_defined`

## 6.4 Cost class
Every capability MUST declare one `cost_class`.
At minimum, a conforming implementation MUST support:

- `low`
- `moderate`
- `high`
- `variable`

## 6.5 Trust level
Every capability MUST declare one `trust_level`.
At minimum, a conforming implementation MUST support:

- `kernel_trusted`
- `runtime_trusted`
- `sandboxed`
- `external_untrusted`
- `human_authority`

Trust level MUST influence policy and observability decisions.

---

# 7. Input and Output Schema Requirements

## 7.1 Typed inputs
The capability input contract MUST be machine-readable and typed.
JSON Schema or an equivalent typed schema model MAY be used.

## 7.2 Typed outputs
The capability output contract MUST be machine-readable and typed.
Human-readable text MAY be included, but MUST NOT be the only meaningful representation.

## 7.3 Schema versioning
Input and output schemas MUST be versioned or version-observable through `contract_version` or equivalent.

## 7.4 Unknown fields
Unknown input fields SHOULD be rejected or ignored deterministically according to the declared contract.
Silent, inconsistent handling is non-conforming.

## 7.5 Validation
The runtime MUST validate capability invocation inputs against the registered input schema before dispatch, unless policy explicitly delegates validation to a trusted provider.

---

# 8. Invocation Contract

## 8.1 Required invocation fields
Every capability invocation MUST contain at least:

- `call_id`
- `task_id`
- `caller_principal_id`
- `capability_id`
- `capability_version` or resolvable contract reference
- `input_payload`
- `requested_at`
- `policy_context_ref`
- `budget_context_ref`
- `effect_class`
- `idempotency_expectation`
- `correlation_id`

`budget_context_ref` MUST resolve to a stable, machine-readable budget context object.
For v0.1, the minimum required shape is defined in `docs/specs/budget/budget-context-spec-v0.1.md`.

## 8.2 Optional invocation fields
An invocation SHOULD also include, where applicable:

- `checkpoint_ref`
- `state_version_ref`
- `timeout_override`
- `cancellation_token`
- `sandbox_request`
- `evidence_request`
- `reason_code`
- `trace_ref`

## 8.3 Invocation attribution
Every invocation MUST be attributable to:

- the task
- the caller principal
- the invoked capability
- the governing policy context

## 8.4 Invocation preconditions
Before dispatching a privileged capability, the runtime MUST ensure:

- task is in a valid non-terminal execution state
- capability is active
- caller principal is valid for the requested authority scope
- policy evaluation has occurred where required
- input validation has succeeded
- budget constraints have not already disqualified execution

## 8.5 Dispatch event requirements
Dispatch MUST emit capability lifecycle events consistent with the Event Schema specification.

---

# 9. Result Contract

## 9.1 Required result fields
Every capability result MUST contain at least:

- `call_id`
- `capability_id`
- `outcome`
- `completed_at`
- `result_payload`
- `side_effect_summary`
- `retry_safety`

## 9.2 Recommended result fields
A result SHOULD also include, where applicable:

- `evidence_ref`
- `latency_ms`
- `cost_snapshot`
- `provider_metadata`
- `partial_result_flag`
- `warnings`
- `reason_code`
- `state_mutation_ref`

## 9.3 Outcomes
A conforming implementation MUST support at least these outcomes:

- `succeeded`
- `failed`
- `denied`
- `timed_out`
- `cancelled`

Implementations MAY support richer subcodes such as `validation_failed`, `rate_limited`, or `provider_unavailable`.

## 9.4 Side-effect summary
If a capability may cause mutation or external effect, the result MUST summarize whether an effect was:

- not attempted
- attempted but not committed
- committed
- committed with unknown finality
- unknown

This requirement exists to support safe recovery and retry behavior.

## 9.5 Retry safety
Every result MUST state whether retry is:

- `safe`
- `unsafe`
- `requires_operator_decision`
- `unknown`

---

# 10. Retry and Idempotency Semantics

## 10.1 Registration rule
Declared `idempotency_class` at registration time MUST describe the capability’s general behavior.

## 10.2 Call-specific retry safety
Per-call `retry_safety` in the result MAY be stricter than the registered idempotency class.
The runtime MUST respect the stricter interpretation.

## 10.3 Non-idempotent capability handling
For non-idempotent or unknown-idempotency capabilities, the runtime SHOULD:

- checkpoint before dispatch where practical
- preserve explicit side-effect outcome metadata
- avoid automatic retry unless policy explicitly permits it

## 10.4 Duplicate defense
The runtime SHOULD preserve enough metadata to detect or reduce duplicate side-effect risk across:

- retry
- replay
- restore
- operator reissue

## 10.5 Idempotency keys
Implementations MAY support explicit idempotency keys.
If supported, their semantics MUST be documented and stable.

---

# 11. Policy Integration

## 11.1 Mandatory policy compatibility
Capability contracts MUST be compatible with policy evaluation.
A capability that cannot be policy-governed is non-conforming for privileged use.

## 11.2 Policy-relevant registration fields
At minimum, the following fields MUST be available for policy use:

- `capability_id`
- `effect_class`
- `cost_class`
- `trust_level`
- `trust_zone_surface` if declared
- `required_authority_scope`
- `tenant_restrictions` if applicable

## 11.3 Policy outcomes
A capability invocation MAY be:

- allowed
- denied
- allowed with constraints
- paused pending approval
- escalated to stronger authority

## 11.4 Constraint binding
If policy attaches constraints to a capability call, the runtime MUST bind those constraints to dispatch and result interpretation.

---

# 12. Cancellation and Interruption

## 12.1 Cancellation declaration
A capability SHOULD declare whether it supports cancellation.

## 12.2 Unsafe interruption
If a capability is non-idempotent and does not support safe cancellation, the runtime SHOULD treat mid-execution interruption as unsafe.

## 12.3 Deferred control
If a control signal cannot be safely applied during an in-flight capability call, the runtime MUST:

- record the deferment as an event
- preserve the invocation lineage
- apply the control signal at the next safe interruption boundary if still valid

## 12.4 Partial results
If partial results are possible, the capability SHOULD declare whether partial results are:

- advisory only
- durable but non-authoritative
- authoritative under specific conditions

---

# 13. Evidence Requirements

## 13.1 Evidence surface
A capability SHOULD declare an `evidence_profile` describing what proof or trace can be produced.

## 13.2 Minimum evidence expectation
For privileged or side-effecting capabilities, the runtime SHOULD preserve enough evidence to support:

- audit
- diagnosis
- side-effect verification
- post-hoc review

## 13.3 Evidence forms
Evidence MAY include:

- structured result metadata
- output artifacts
- logs
- diffs
- receipts
- external identifiers
- hashes
- approval records

---

# 14. Observability Requirements

## 14.1 Capability lifecycle visibility
A conforming runtime MUST be able to inspect, for any call:

- who requested it
- which task requested it
- which capability was invoked
- current or final outcome
- whether policy intervened
- whether side effects were committed
- whether retry is safe

## 14.2 Cost visibility
Where measurable, capability execution SHOULD expose cost and latency surfaces.

## 14.3 Provider opacity rule
A provider MAY keep internal implementation details private, but MUST still satisfy the registered contract and observability obligations.

---

# 15. Sandboxing and Trust Guidance

## 15.1 Trust-aware dispatch
The runtime SHOULD vary policy, evidence, and observability requirements according to declared `trust_level`.

## 15.2 Sandboxed capabilities
If a capability declares `sandboxed`, the implementation SHOULD document:

- filesystem scope
- network scope
- process scope
- secret access model
- output capture model

## 15.3 External untrusted capabilities
Capabilities marked `external_untrusted` SHOULD default to stricter policy review, stronger evidence requirements, and conservative retry behavior.

---

# 16. Capability Evolution and Compatibility

## 16.1 Versioned contracts
A capability contract MUST be versioned.
Breaking changes MUST produce a new contract version.

## 16.2 Backward compatibility
Minor compatible additions MAY be introduced without changing capability identity, but SHOULD remain detectable through version metadata.

## 16.3 Revocation
If a capability version is revoked, the runtime MUST preserve historical interpretability of prior events and results.
Revocation MUST NOT erase history.

---

# 17. Minimum Conformance Requirements

An implementation is Capability Contract Core conformant only if it:

1. requires capability registration before privileged invocation
2. defines typed input and output contracts
3. classifies effect, idempotency, timeout, cost, and trust
4. attributes every invocation to task and principal
5. integrates with policy evaluation
6. records capability lifecycle events
7. records outcome and retry safety
8. preserves side-effect summary semantics

---

# 18. Non-Normative Implementation Guidance

Recommended v1 choices:

- start with a very small built-in capability set
- ban mixed-effect capabilities unless strongly justified
- keep read-only and side-effecting capabilities separate where possible
- classify trust level conservatively
- record side-effect finality explicitly even when outcome is failure
- prefer declarative schemas over prose-only contracts
- make human approval a capability rather than an informal side channel
- represent final approval/denial as a kernel control signal

Common failure smell:

- giant “do anything” capability with vague semantics
- no idempotency declaration
- hidden side effects in supposedly read-only capabilities
- provider-specific blobs with no structured result shape
- retries without side-effect finality metadata
- policy wrappers that operate only on capability names, not semantics

---

# 19. Open Questions for v0.2

- standard capability packaging format
- standard sandbox attestation fields
- stronger rate-limit contract model
- streaming result schema
- standardized idempotency key semantics
- standard evidence profile taxonomy
- multi-step capability session model
- delegated capability chains

---

# 20. Final Statement

A capability contract is where intention crosses into consequence.
If that crossing is vague, the runtime will become unsafe, unauditable, and impossible to reason about.

The point of this specification is not to make capability invocation bureaucratic for its own sake.
It is to ensure that power enters the system through a governed door rather than through cracks in the wall.
