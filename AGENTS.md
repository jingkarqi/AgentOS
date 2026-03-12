# Repository Guidelines

## Project Structure & Module Organization
AgentOS is still spec-first, but it now also contains a committed Rust reference runtime. Put normative design docs in `docs/`: foundation material in `docs/foundation/`, domain specs under `docs/specs/` such as `kernel/`, `events/`, `budget/`, and `capabilities/`, conformance rules in `docs/conformance/`, reference-runtime notes in `docs/reference-runtime/`, and architecture or decision records in `docs/architecture/` and `docs/decisions/`. Use `docs/architecture/repository-map.md` as the quickest orientation aid when you need to confirm where a repo concern lives before editing. Store machine-readable contracts in `schemas/` by domain, including `budgets/`, `capabilities/`, `checkpoints/`, `controls/`, `events/`, and `tasks/`. Keep executable assets under `runtime/`; the current implementation lives in `runtime/mrr/` with Rust sources in `src/`, SQL migrations in `migrations/`, and runtime conformance coverage in `tests/runtime_conformance.rs` plus `tests/capability_conformance.rs`. Capability dispatch and contract enforcement currently live alongside task, event, control-signal, checkpoint, and state logic in the Rust runtime. Use `tests/conformance/` for spec-facing cases and `tests/fixtures/` for sample payloads or replay data. `FIX_DECLARATIONS/` is for targeted repo-wide fix notes, not durable normative specs.

## Build, Test, and Development Commands
Use small, non-interactive validation commands that match the area you changed:

- `git status --short` to confirm the workspace state before and after edits.
- `rg --files docs schemas tests runtime` to inspect the active working set quickly.
- `python -m json.tool schemas/<domain>/<file>.schema.json` to verify schema JSON syntax.
- `cargo test` from `runtime/mrr` to run the Rust runtime test suite.
- `cargo test --test runtime_conformance` from `runtime/mrr` when touching conformance-sensitive runtime behavior.
- `cargo test --test capability_conformance` from `runtime/mrr` when touching capability registration, invocation, durable dispatch, or approval-sensitive behavior.
- `cargo fmt --check` from `runtime/mrr` to enforce Rust formatting before commit.
- `git diff -- docs schemas tests runtime AGENTS.md` to review only repository content relevant to the change.

When you add new automation, document it in the nearest `README.md` and keep commands reproducible and non-interactive.

## Coding Style & Naming Conventions
Use Markdown with short sections, explicit headings, and direct normative language. Preserve versioned, kebab-case filenames such as `kernel-spec-v0.1.md`, `event-schema-spec-v0.1.md`, and `conformance-test-suite-v0.1.md`. Keep terminology aligned across specs; when a field is canonicalized or renamed, update `docs/specs/field-alignment-v0.1.md` in the same change. Format JSON schemas with two-space indentation, double-quoted keys, explicit `required` lists, and `additionalProperties: false` where the contract is closed.

For `runtime/mrr`, follow standard Rust conventions: snake_case modules, focused types, clear error propagation, and small functions around task, event, principal, checkpoint, control-signal, capability, and state semantics. Keep persistence changes paired with SQL migrations and avoid mixing unrelated refactors with normative behavior changes.

## Testing Guidelines
Treat `docs/conformance/conformance-test-suite-v0.1.md` as the test oracle. Every new `MUST`-level rule should be backed by a conformance case, fixture, or runtime test update. Reuse suite case IDs in filenames or test names when possible, for example `CTS-01-task-lifecycle.json` or `cts_07_sequence_numbers_are_monotonic_and_gapless`. When a schema changes, verify at least one matching fixture and the affected spec section in the same change. When runtime behavior changes, run at minimum the focused `cargo test` target that covers the affected module, and prefer `runtime_conformance.rs` or `capability_conformance.rs` coverage for changes tied to spec guarantees.

## Commit & Pull Request Guidelines
Follow the existing scoped subject style: `<scope>: <concise summary>`, with scopes such as `docs`, `schemas`, `tests`, `runtime`, or `chore`. Keep commits narrowly scoped to one logical change. Pull requests should state which spec areas or runtime modules changed, whether terminology or compatibility shifted, what tests were run, and which fixtures or conformance cases were updated. Link the relevant issue or decision record when one exists. Screenshots are only useful for rendered diagrams or tables.

## Security & Configuration Tips
Do not commit secrets, vendor credentials, or environment-specific tokens. Use synthetic IDs and sample data in schemas, migrations, and fixtures. Keep vendor- or runtime-specific behavior clearly labeled as non-normative unless the spec explicitly standardizes it. Do not commit Rust build artifacts from `runtime/mrr/target/` or Windows ADS files such as `*:Zone.Identifier`; those are already ignored and should stay out of reviews.
