## Code-health inventory

| File | Line | Smell | Severity (High/Med/Low) | Modern replacement |
|---|---:|---|---|---|
| `FilesSendToAzureBus.csproj:3` | 3 | In-process Azure Functions model targeting `net6.0` through WebJobs packages | High | Migrate to the isolated worker model using `Microsoft.Azure.Functions.Worker`, a supported .NET LTS target, and isolated-worker bindings |
| `AzureFunctionDurableSubscriber.csproj:3` | 3 | In-process Azure Functions model targeting `net6.0` through WebJobs packages | High | Migrate to the isolated worker model and supported .NET LTS |
| `FilesSendToAzureBus.csproj:11` | 11 | Preview `Microsoft.NET.Sdk.Functions` package (`4.0.0-preview2`) | High | Use the stable Functions SDK appropriate for the selected isolated-worker/.NET target |
| `FilesSendToAzureBus.csproj:8` | 8 | Deprecated `Microsoft.Azure.ServiceBus` package | High | Replace with `Azure.Messaging.ServiceBus` and the current Azure Functions Service Bus extension |
| `AzureFunctionDurableSubscriber.csproj:7` | 7 | Deprecated `Microsoft.Azure.ServiceBus` package | High | Replace with `Azure.Messaging.ServiceBus` and isolated-worker Service Bus bindings |
| `SendDataToMessageBus.cs:36` | 36 | Reflection used to construct and mutate `Message.SystemProperties` | High | Stop setting service-owned system properties; create an `Azure.Messaging.ServiceBus.ServiceBusMessage` and use application properties for caller-defined metadata |
| `SendDataToMessageBus.cs:47` | 47 | Reflection used to set the message’s internal `SystemProperties` property | High | Use the supported Service Bus message API; sequence number and enqueue time should be assigned by the broker |
| `SendDataToMessageBus.cs:64` | 64 | `Thread.Sleep` blocks a Functions worker thread for 15 seconds | Med | Use `await Task.Delay(..., cancellationToken)` or remove the artificial delay |
| `FileProcessingOrch.cs:62` | 62 | `.Result` performs synchronous blocking after asynchronous work | High | Use `var results = await Task.WhenAll(parallelActivities)` and aggregate the returned results |
| `SendDataToMessageBus.cs:76` | 76 | Direct `Environment.GetEnvironmentVariable` calls scatter configuration access and provide no validation | Med | Inject `IConfiguration` or validated options through DI; fail clearly for missing settings |
| `SendDataToMessageBus.cs:125` | 125 | Storage connection configuration is read directly from the environment at call time | Med | Use injected `ShareServiceClient`/options, with the connection handled by configuration and DI |
| `ParsingFileAndDelete.cs:22` | 22 | Pattern configuration is read directly from the environment and is not validated | Med | Inject validated options/configuration and handle a missing or malformed pattern explicitly |
| `FileProcessingOrch.cs:19` | 19 | Static function class prevents constructor injection and encourages hidden dependencies | Med | Use an instance function class with constructor-injected services in the isolated worker model |
| `SendDataToMessageBus.cs:25` | 25 | Static function methods prevent service injection and make the publisher difficult to unit test | Med | Inject a Service Bus sender, configuration/options, and logging dependencies |
| `ReceiveFileFromMessageBus.cs:15` | 15 | Static function class prevents DI and isolates orchestration logic from testable services | Med | Convert to an instance function class and inject orchestration/application services |
| `SendDataToMessageBus.cs:27` | 27 | Function signature has no `CancellationToken` for cooperative shutdown | Med | Add `CancellationToken cancellationToken` and pass it to asynchronous SDK operations |
| `SendDataToMessageBus.cs:70` | 70 | Async activity has no cancellation path and uses a dummy `Task.Delay(1)` | Low | Add and honor a cancellation token; remove the artificial delay if it has no functional purpose |
| `ReceiveFileFromMessageBus.cs:39` | 39 | Service Bus-triggered function has no `CancellationToken` | Med | Add a cancellation token and pass it through orchestration or downstream work |
| `FileProcessingOrch.cs:77` | 77 | HTTP-triggered function has no cancellation token | Low | Accept a cancellation token where supported and propagate it to startup and downstream operations |
| `FileProcessingOrch.cs:13` | 13 | Both `Microsoft.WindowsAzure.Storage` and `Azure.Storage.Files.Shares` SDK generations are present | High | Standardize on `Azure.Storage.Files.Shares`; remove legacy WindowsAzure Storage references and namespaces |
| `FileProcessingOrch.cs:13` | 13 | Legacy storage namespace is imported but the implementation uses the newer Share SDK | Low | Remove the unused legacy namespace and any transitive dependency that keeps the old SDK alive |
| `SendDataToMessageBus.cs:146` | 146 | Unused variable `i` | Low | Remove it; use the directory enumeration index only if it is required |
| `ReceiveFileFromMessageBus.cs:20` | 20 | Unused `parallelActivities` variable; its processing loop is commented out | Low | Remove the dead variable and commented implementation, or restore the intended fan-out behavior |
| `TransferFileInfo.cs:4` | 4 | Nullable reference types are enabled, but required string properties are non-nullable and lack initialization | Med | Mark values nullable where valid, or use `required`/constructors and enforce invariants |
| `ParsingFileAndDelete.cs:38` | 38 | `ParseFiles` declares non-nullable `string` return type but returns `null` | Med | Return `string?`, use an explicit result type, or return an empty/non-null value according to the binding contract |
| `SendDataToMessageBus.cs:125` | 125 | Environment-variable result can be null but is passed as a storage connection string | Med | Validate configuration during startup and inject a non-null connection/options object |
| `AzureFuncSendFileToAzureBus.sln:5` | n/a | No test project or test source files are included in the solution | High | Add unit tests for parsing, splitting, message construction, and configuration validation, plus integration tests for trigger workflows |
| `FileProcessingOrch.cs:75` | 75 | `ToDo` comment records an unimplemented trigger change | Low | Resolve the decision and track remaining work in an issue or architecture document |
| `ParsingFileAndDelete.cs:17` | 17 | `Todo` comment identifies parser and Service Bus responsibilities that remain coupled | Med | Split parsing from message-posting/orchestration responsibilities and track the refactor explicitly |

`get_errors` reported no active diagnostics for the workspace, so the nullable entries are code-level risks and likely warning sources rather than currently surfaced compiler errors.

## Recommended migration order

1. Move both Function Apps from the in-process WebJobs model and `net6.0` to the isolated worker model on a supported .NET LTS version.
2. Replace `Microsoft.Azure.ServiceBus` with `Azure.Messaging.ServiceBus`, including trigger/output binding types and message construction.
3. Remove reflection-based system-property mutation; rely on broker-assigned system properties and supported application properties.
4. Consolidate storage access on `Azure.Storage.Files.Shares` and remove legacy `Microsoft.WindowsAzure.Storage` usage.
5. Introduce DI with instance function classes, typed configuration, and startup validation.
6. Add cancellation-token propagation and remove blocking `.Result` and `Thread.Sleep` calls.
7. Resolve nullable contracts and dead code/comments.
8. Add focused unit and integration tests before further behavioral changes.
