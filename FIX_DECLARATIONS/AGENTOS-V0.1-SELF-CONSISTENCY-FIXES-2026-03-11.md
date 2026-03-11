# AgentOS v0.1 Self-Consistency Fixes (2026-03-11)

This declaration records a set of v0.1 documentation fixes applied to reduce implementation-splitting ambiguities and make Conformance Suite interpretations more deterministic.

## Scope
This change set targets the v0.1 specification family (Foundation / Kernel / Event / Capability / CTS / MRR) and introduces two new normative support artifacts:

- a canonical field alignment page
- a minimal budget context spec + JSON schema

## Fixes applied

### 1) Cognition is not a kernel primitive
- Clarified that `cognition` in the irreducible kernel sentence refers to implementation-defined cognitive-plane components, not a ninth primitive.
- Added minimal vocabulary definition for Cognition.

### 2) `history_ref` is a transcript projection (non-authoritative)
- Defined `history_ref`/transcript as a projection derived from state + events; it MUST NOT be treated as authoritative for recovery or policy enforcement.
- Added canonical alias guidance (`transcript_projection_ref`) via the field alignment page.

### 3) Principal roles have an authoritative, comparable record
- Tightened Kernel language so owning/acting/approving principals are not “implementation folklore”:
  - owning: `owner_principal_id`
  - acting (privileged actions): `caller_principal_id`
  - approving/overriding: control signal issuer attribution
- Added `principal_role` as a recommended event envelope field and documented semantics.

### 4) Canonical field naming and mapping for v0.1
- Added a single mapping page for canonical field names and allowed aliases to prevent “or equivalent” from fragmenting CTS harness logic.
- Aligned Kernel capability invocation fields with Capability Contract expectations (budget/idempotency/correlation/version terminology).

### 5) `effect_class` no longer depends on “what the filesystem means”
- Standardized v0.1 guidance: runtime-managed tenant workspace writes are `state_mutating_internal`; writes outside that workspace are `external_side_effect`.
- Updated MRR built-in capability list accordingly.

### 6) Trust Zone vs Trust Level: minimal definitions + relationship
- Added seed vocabulary definitions to distinguish:
  - trust zones (resource/data isolation partitions)
  - trust levels (provider/execution environment classification)
- Added Kernel policy text requiring trust-zone boundary crossings to be explicitly policy-decidable.

### 7) Approval path is projected into one canonical kernel semantics
- Defined `require_approval` as a policy gate that MUST be resolved by control signals (`approve`/`deny`).
- Clarified `approval.request` capability semantics: it may create workflow artifacts but MUST NOT be interpreted as final approval.
- Added CTS case to lock the ordering/blocked-dispatch invariant.

### 8) v0.1 Core event ordering forbids sequence gaps
- Tightened Event Schema: Core conformance MUST NOT have `sequence_number` gaps.
- Updated CTS assertions accordingly.

### 9) Budget context now has a minimum shared shape
- Added a minimal Budget Context Spec and a canonical JSON Schema.
- Required Kernel/Capability references to resolve to that shape for policy + observability comparability.

## Primary files touched
- `docs/foundation/agentos-foundation.md`
- `docs/foundation/appendices/appendix-a-irreducible-kernel-sentence.md`
- `docs/foundation/appendices/appendix-b-seed-vocabulary.md`
- `docs/specs/kernel/kernel-spec-v0.1.md`
- `docs/specs/events/event-schema-spec-v0.1.md`
- `docs/specs/capabilities/capability-contract-spec-v0.1.md`
- `docs/reference-runtime/minimal-reference-runtime-v0.1.md`
- `docs/conformance/conformance-test-suite-v0.1.md`
- `docs/specs/field-alignment-v0.1.md` (new)
- `docs/specs/budget/budget-context-spec-v0.1.md` (new)
- `schemas/budgets/budget-context.schema.json` (new)

## Compatibility notes
- Existing implementations may keep legacy field names, but MUST provide a canonical projection per `docs/specs/field-alignment-v0.1.md`.
- `history_ref` remains valid in v0.1, but its semantics are now explicitly non-authoritative transcript projection.
