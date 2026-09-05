---
agent: ask
description: Review the current file against Durable Functions determinism and binding rules
---

Review ${file} against these rules. Report every violation as a table:
line | rule broken | why it matters | fix.

1. Orchestrator determinism — no `DateTime.Now`/`UtcNow`, `Guid.NewGuid()`, `Random`,
   `Environment.*`, file/HTTP/database I/O inside an `[OrchestrationTrigger]` function.
2. Orchestrators await only durable APIs.
3. Logging inside orchestrators is guarded by `context.IsReplaying`.
4. Activity functions take a single input parameter and are idempotent.
5. No reflection over SDK internals to set broker-owned properties.
6. No `.Result`, `.Wait()` or `Thread.Sleep` anywhere.

Do not change any code. If the file contains no orchestrator or activity functions,
say so and stop.