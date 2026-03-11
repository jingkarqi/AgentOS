# Appendix B: Seed Vocabulary

To avoid conceptual drift, the following words should remain sharply defined:

- Principal
- Task
- State
- Event
- Capability
- Checkpoint
- Resume
- Policy Authority
- Evidence
- Memory
- Projection
- Transcript
- Budget
- Control Signal
- Handoff
- Trust Zone
- Trust Level
- Cognition
- Operator
- Tenant

The health of the system depends on vocabulary discipline.

---

# v0.1 minimal definitions (non-exhaustive)

## Cognition
Cognition is the implementation-defined cognitive-plane mechanism that reads state, reasons over bounded context, and proposes or takes actions through capability calls under policy.
Cognition is not a kernel primitive and must not expand the kernel object model by default.

## Projection
A Projection is a derived view computed from authoritative kernel data (state, events, checkpoints) for a specific audience or interface.
Projections are secondary: they MUST NOT be treated as authoritative recovery or policy truth unless explicitly stated.

## Transcript
A Transcript is a projection that renders execution history into a conversational or human-readable narrative.
In v0.1, transcripts are non-authoritative and are expected to be derivable from state + events (or at least verifiable against them).

## Trust Zone
A Trust Zone is an implementation-defined isolation partition for resources and data (tenant boundary, network boundary, environment boundary, compliance boundary).
Trust zones are used by policy to reason about boundary crossings and data movement.

## Trust Level
Trust Level classifies the *capability provider / execution environment* (e.g., kernel-trusted vs sandboxed vs external untrusted).
Trust level is orthogonal to trust zone: a highly trusted provider may still touch a low-trust zone, and vice versa.
