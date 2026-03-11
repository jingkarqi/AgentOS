# Repository Guidelines

## Project Structure & Module Organization
AgentOS is a spec-first repository. Put normative design docs in `docs/`: foundation material in `docs/foundation/`, versioned specs in `docs/specs/`, conformance rules in `docs/conformance/`, reference-runtime notes in `docs/reference-runtime/`, and architecture or decision records alongside them. Store machine-readable contracts in `schemas/` by domain, for example `schemas/budgets/budget-context.schema.json`. Keep executable or future runtime assets under `runtime/`. Use `tests/conformance/` for conformance cases and `tests/fixtures/` for sample payloads or replay data. `FIX_DECLARATIONS/` is for targeted repository-wide fix notes, not long-term specs.

## Build, Test, and Development Commands
There is no committed `npm`, `make`, or compiled build workflow yet. Use lightweight validation commands while editing:

- `rg --files docs schemas tests runtime` to inspect the working set.
- `python -m json.tool schemas/budgets/budget-context.schema.json` to verify JSON syntax before commit.
- `git diff -- docs schemas tests runtime` to review only repository content relevant to a spec change.

If you add automation later, document it in the relevant directory `README.md` and keep commands non-interactive.

## Coding Style & Naming Conventions
Use Markdown with short sections, explicit headings, and direct normative language. Preserve versioned, kebab-case filenames such as `kernel-spec-v0.1.md` and `conformance-test-suite-v0.1.md`. Keep terminology aligned across specs; when a field is canonicalized, update `docs/specs/field-alignment-v0.1.md`. Format JSON schemas with two-space indentation, double-quoted keys, explicit `required` lists, and `additionalProperties: false` where the contract is closed.

## Testing Guidelines
Treat `docs/conformance/conformance-test-suite-v0.1.md` as the test oracle. Every new `MUST`-level rule should be backed by a conformance case or fixture update. Reuse suite case IDs in filenames or headings when possible, for example `CTS-01-task-lifecycle.json`. When a schema changes, verify at least one matching fixture and the affected spec section in the same change.

## Commit & Pull Request Guidelines
Current history uses scoped subjects such as `docs: 统一 v0.1 规范术语并添加预算上下文定义`. Follow the same pattern: `<scope>: <concise summary>`, with scopes like `docs`, `schemas`, `tests`, or `runtime`. Pull requests should state which spec areas changed, whether terminology or compatibility shifted, and what tests or fixtures were updated. Link the relevant issue or decision record when one exists. Screenshots are only useful for rendered diagrams or tables.

## Security & Configuration Tips
Do not commit secrets, vendor credentials, or environment-specific tokens. Use synthetic IDs and sample data in schemas and fixtures. Keep vendor- or runtime-specific behavior clearly labeled as non-normative unless the spec explicitly standardizes it.
