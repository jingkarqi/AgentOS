# AgentOS Budget Context Spec v0.1

## Status
Draft v0.1

## Purpose
Budgets are policy-relevant and observability-relevant, but v0.1 previously lacked a minimum shared shape.
This specification defines a minimal, implementation-agnostic budget context object so that different runtimes can expose comparable budget semantics.

A budget context is referenced by:

- Task `budget_context_ref` (Kernel Spec v0.1)
- capability invocation `budget_context_ref` (Capability Contract Spec v0.1)
- event envelope `budget_context_ref` (Event Schema Spec v0.1, recommended)

---

# 1. Budget context object

## 1.1 Required fields
A conforming budget context object MUST include:

- `budget_context_id`: stable identifier within the runtime domain
- `unit`: accounting unit (for example `credits`)
- `limit.amount`: non-negative number
- `spent.amount`: non-negative number
- `scope`: attribution scope for ownership and accountability (see 1.2)
- `updated_at`: last update timestamp for the budget context

## 1.2 Scope (attribution)
`scope` MUST include at least one of:

- `tenant_id`
- `principal_id`
- `task_id`

The runtime SHOULD include `principal_id` when a budget is owned by or charged to a principal.
The runtime SHOULD include `task_id` when a budget is task-scoped or when spend is primarily explained at the task level.

## 1.3 Semantics

- `spent.amount` MUST be monotonic non-decreasing for a given `budget_context_id`.
- `unit` MUST be stable for the lifetime of a budget context.
- Policy decisions MAY be expressed in terms of `remaining = limit.amount - spent.amount` (with optional soft limits if the implementation provides them).

## 1.4 Optional windowing
Implementations MAY attach a `window` to express time-bounded budgets (rolling or calendar windows).
If `window` is present, the meaning of resets MUST be explicit and stable.

---

# 2. Canonical JSON Schema

The canonical JSON Schema for this object is provided at:

- `schemas/budgets/budget-context.schema.json`

Implementations MAY extend the object with additional fields, but MUST preserve the required fields and their semantics.

---

# 3. Example

```json
{
  "budget_context_id": "BUDGET-001",
  "unit": "credits",
  "limit": { "amount": 1000 },
  "spent": { "amount": 125, "as_of": "2026-03-11T12:00:00Z" },
  "scope": { "tenant_id": "TENANT-1", "principal_id": "P1", "task_id": "T1" },
  "window": { "kind": "lifetime" },
  "updated_at": "2026-03-11T12:00:00Z",
  "metadata": { "notes": "example only" }
}
```
