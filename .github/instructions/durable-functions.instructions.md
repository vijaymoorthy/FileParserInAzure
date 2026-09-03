---
applyTo: "**/AzureFunctionDurable*/**/*.cs"
---

# Durable Functions rules

## Orchestrator functions (`[OrchestrationTrigger]`) must be deterministic
- Never use `DateTime.Now`/`UtcNow`, `Guid.NewGuid()`, `Random`, `Environment.*`,
  HTTP calls, file or database I/O, or any non-deterministic API inside an orchestrator.
- Use `context.CurrentUtcDateTime` for time and `context.NewGuid()` for identifiers.
- Do all I/O in activity functions called via `context.CallActivityAsync<T>`.
- Never `await` anything except durable APIs (`CallActivityAsync`, `CreateTimer`,
  `WaitForExternalEvent`, `Task.WhenAll`/`WhenAny` over durable tasks).
- Guard logging with `context.IsReplaying` so replays don't duplicate log lines.

## Activity functions (`[ActivityTrigger]`)
- Take exactly one input parameter; wrap multiple values in a DTO from `SharedModel`.
- Keep them idempotent — they can be retried.

## Bindings
- Prefer declarative bindings over constructing SDK clients by hand.
- Never reflect over SDK internals to set broker-owned properties.