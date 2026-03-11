# AgentOS Field Alignment v0.1

## Status
Draft v0.1

## Purpose
The v0.1 document set is intentionally small, but some documents use different field names for the same semantic concept.
This page defines canonical names and allowed aliases so that:

- independent implementations can be compared
- the Conformance Test Suite can make deterministic assertions
- “or equivalent” does not become an interoperability escape hatch

Conformance rule:
If an implementation uses aliases internally, it MUST still be able to present a stable canonical projection using the names below.

---

# Canonical field mapping

## Task

| Canonical field | Allowed aliases | Notes |
| --- | --- | --- |
| `owner_principal_id` | `owner_principal` | Owning principal of the task. |
| `policy_context_ref` | `policy_context` | Reference to the governing policy context. |
| `budget_context_ref` | `budget_ref`, `budget` | Reference to the budget context object. |
| `history_ref` | `transcript_ref`, `transcript_projection_ref` | Non-authoritative transcript projection reference. |

## Capability invocation

| Canonical field | Allowed aliases | Notes |
| --- | --- | --- |
| `idempotency_expectation` | `idempotency_hint` | Call-level retry / duplicate-execution expectation. |
| `budget_context_ref` | `budget_ref` | Budget context reference used for policy and accounting. |
| `correlation_id` | (none) | Recommended default is `call_id`. |

## Event envelope

| Canonical field | Allowed aliases | Notes |
| --- | --- | --- |
| `principal_id` | (none) | Principal attributable for the event, when applicable. |
| `principal_role` | (none) | `owning` / `acting` / `approving` / etc, when applicable. |
