# Copilot instructions for Dredis

## Build and test

- Use the .NET SDK pinned in `global.json` (`10.0.202`).
- CI restores and builds the solution with:
  - `dotnet restore Dredis.slnx`
  - `dotnet build Dredis.slnx --configuration Release --no-restore`
- Run the test suite with:
  - `dotnet test Dredis.Tests\Dredis.Tests.csproj --configuration Release`
- Run a single xUnit test with a filter such as:
  - `dotnet test Dredis.Tests\Dredis.Tests.csproj --configuration Release --filter "FullyQualifiedName~Dredis.Tests.DredisServerOptionsTests.FromConfiguration_BindsSectionValues"`
- `Dredis.slnx` includes a sibling project at `..\Dredis.Extensions\Storage\Dredis.Extensions.Storage.SqlServer\Dredis.Extensions.Storage.SqlServer.csproj`. If that sibling checkout is unavailable, work from `Dredis.csproj` and `Dredis.Tests\Dredis.Tests.csproj` instead of the solution.

## High-level architecture

- `DredisServer` is the network host. It owns the DotNetty bootstrap/event loops, installs the Redis decoder/aggregator/encoder pipeline, and creates one `DredisCommandHandler` per connection.
- `DredisCommandHandler` is the main behavior surface. It accepts RESP arrays and inline commands, implements the built-in Redis-compatible command set, and maps those commands onto `IKeyValueStore`.
- JSON support lives in the partial class `DredisCommandHandler.Json.cs`, but it uses the same `_store`, reply helpers, and transaction notifications as the main handler.
- Extensibility is layered in a fixed order for unknown commands:
  1. built-in command handling in `DredisCommandHandler`
  2. `ICustomDataTypeStore.TryExecuteCustomDataTypeAsync(...)` for storage-backed extensions that need full RESP response shapes
  3. registered `ICommand` handlers for simpler string-in/string-out custom commands
- `Dredis.Abstractions.Storage` is the core contract package. `Dredis.Abstractions.Command` and `Dredis.Abstractions.Auth` are separate contract-only packages for custom commands and auth integration.
- Pub/sub and transaction state are held by static manager instances inside `DredisCommandHandler`, so some behavior is intentionally shared across handler instances/connections.
- Tests follow the same layering: `DredisCommandHandlerTests` exercises most command behavior with `EmbeddedChannel`, while `DredisServerTests` and `DredisNRedisCompatibilityTests` cover real socket behavior and client compatibility.

## Key conventions

- Registered `ICommand` handlers must not override built-in Redis commands; built-ins win dispatch first.
- When adding a mutating command, follow existing write paths and call `Transactions.NotifyKeyModified(key, _store)` so `WATCH`/`EXEC` behavior stays consistent.
- Preserve unknown-command dispatch order: `ICustomDataTypeStore` first, then registered `ICommand`, then the standard unknown-command error.
- RESP reply shape is part of the contract. Reuse the existing reply helpers (`WriteSimpleString`, `WriteBulkString`, `WriteNullBulkString`, `WriteInteger`, `WriteArray`, `WriteError`) and match existing Redis-style arity/error text.
- Keep JSON command work in `DredisCommandHandler.Json.cs` rather than folding it back into the main command file.
- Reuse the in-memory test store and RESP helper methods already defined in `Dredis.Tests\DredisCommandHandlerTests.cs` when adding command coverage; that file is the central test harness for handler-level behavior.
