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

## Logging inside orchestrators
Deterministic *values* are not enough: the log statement itself re-executes on every
replay, so unguarded logging emits duplicates.

- **Preferred (in-process model):** create one replay-safe logger at the top of the
  orchestrator and use it for every log call in the method:
```csharp
  ILogger replaySafeLogger = context.CreateReplaySafeLogger(log);
```
  `DurableContextExtensions.CreateReplaySafeLogger(IDurableOrchestrationContext, ILogger)`,
  available since Durable Functions 2.0. In the isolated worker model, use
  `TaskOrchestrationContext.CreateReplaySafeLogger`.
- Do **not** scatter `if (!context.IsReplaying)` guards at each call site — that is the
  manual form of a problem the SDK already solves, and it rots as log statements are added.
- Declare the replay-safe logger **once**, inside the orchestrator method, never at class
  scope and never inside a loop.
- **Always use message templates with named parameters — never `$"..."` interpolation.**
  Interpolation flattens values into the message string, so Application Insights cannot
  index them.
```csharp
  // correct
  replaySafeLogger.LogInformation("Orchestrator executing at {ExecutionTime:O}", context.CurrentUtcDateTime);
  // wrong
  log.LogInformation($"Orchestrator executing at {context.CurrentUtcDateTime:O}");
```

## Activity functions (`[ActivityTrigger]`)
- Take exactly one input parameter; wrap multiple values in a DTO from `SharedModel`.
- Keep them idempotent — they can be retried.

## Bindings
- Prefer declarative bindings over constructing SDK clients by hand.
- Never reflect over SDK internals to set broker-owned properties.