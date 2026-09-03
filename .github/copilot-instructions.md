# FileParserInAzure — Copilot instructions

## What this is
An Azure Durable Functions pipeline. The publisher reads files from an Azure File Share,
splits them by line count, and sends each chunk to Service Bus. The subscriber receives
chunks, pattern-matches each line, and reports non-matching files for deletion.
Shared DTOs live in `SharedModel`.

## Hard constraints — never violate
- Targets **net6.0** using the **in-process** Azure Functions model (`Microsoft.NET.Sdk.Functions`).
- **C# 10 only.** Never use `required` members, primary constructors, collection expressions,
  raw string literals, or any other C# 11+ feature.
- DTOs in `SharedModel` are deserialized by **Newtonsoft.Json** through Durable Functions.
  They need a public parameterless constructor and settable properties.
- Do not add, remove or upgrade NuGet packages unless I explicitly ask.
- Do not reformat or "tidy" code I did not ask you to change.

## Conventions for new or changed code
- Fix nullable warnings by initialising strings to `string.Empty` and collections to a new
  instance. Use nullable types only where an absent value is meaningful.
- Prefer `async Task` and flow a `CancellationToken`. Never use `.Result`, `.Wait()` or `Thread.Sleep`.
- Log via `ILogger` with structured message templates, not string interpolation.
- Read configuration through `IConfiguration` or typed options, not `Environment.GetEnvironmentVariable`.
  (Existing code does the latter — do not copy that pattern into new code.)

## Modernisation
The agreed migration order is in `docs/copilot-inventory.md`. When modernising, follow that
order, change one project at a time, and build after each step.

## When unsure
State your assumption before generating code, and prefer the smallest change that compiles.