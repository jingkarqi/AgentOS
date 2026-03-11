# Appendix A: The Irreducible Kernel Sentence

**AgentOS is a durable runtime that advances tasks by coordinating principals, state, cognition, capabilities, policy authority, checkpoints, events, and control signals.**

Clarification (v0.1):

- `cognition` in this sentence refers to implementation-defined cognitive-plane execution components (models, planners, verifiers, human steering loops) that the kernel may coordinate.
- `cognition` is **not** a kernel primitive, and conformance does not require a dedicated kernel object model for it.
- Cognition is externally visible only through the existing primitives (state references, events, capability calls, policy outcomes, checkpoints, and control signals).

If a future change preserves this sentence, it may belong.
If it breaks this sentence, it probably does not.
