# AGENTS.md

Compact instruction file for OpenCode sessions working with the MassTransit codebase.

## Project Overview

This is **MassTransit** - a distributed application framework for .NET supporting multiple message brokers and persistence engines. The solution contains 50+ projects organized into categories:

- `src/MassTransit/` - Core library
- `src/MassTransit.Abstractions/` - Base abstractions
- `src/Transports/` - RabbitMQ, Azure Service Bus, ActiveMQ, AmazonSQS, SQL, Kafka, EventHub
- `src/Persistence/` - Saga persistence (EF, EF Core, Marten, MongoDB, Redis, NHibernate, Dapper, Cosmos, DynamoDB, etc.)
- `src/Scheduling/` - Quartz and Hangfire integrations
- `tests/` - Corresponding test projects (27+ test suites)

## Build & Test Commands

```bash
# Build entire solution
dotnet build

# Build release
dotnet build -c Release

# Restore packages
dotnet restore

# Run specific test project (most common pattern)
dotnet test -c Release -f net9.0 --logger GitHubActions --filter Category!=Flaky
```

### Running Single Test Projects

Tests are in `tests/<ProjectName>.Tests/` directories. To run a specific test suite:

```bash
# Core unit tests
dotnet test -c Release -f net9.0 --filter Category!=Flaky tests/MassTransit.Tests

# Transport-specific (may need docker-compose)
dotnet test -c Release tests/MassTransit.RabbitMqTransport.Tests
dotnet test -c Release tests/MassTransit.AmazonSqsTransport.Tests
```

### Tests Requiring Infrastructure

Many integration tests need Docker services. Check for `docker-compose.yml` in the test directory:

```bash
# Start services first
cd tests/MassTransit.KafkaIntegration.Tests
docker compose up -d

# Run tests
dotnet test -c Release --filter Category!=Flaky

# Clean up
docker compose down
```

Tests that commonly need Docker:
- Kafka, EventHub, MongoDB, Redis, Azure Table
- SQL Transport tests use GitHub Actions services (postgres/mssql)

Some tests are **disabled in CI** (check `.github/workflows/build.yml` for `if: false`):
- Azure Service Bus (requires secrets, too flaky)

## Project Structure & Dependencies

### Multi-Targeting

- **Source projects**: `netstandard2.0;net8.0;net9.0;net10.0` (+ `net472` on Windows)
- **Test projects**: `net9.0` (+ `net472` on Windows)
- Check `<TargetFrameworks>` in `.csproj` files before making assumptions

### Central Package Management

Uses **Directory.Packages.props** for version management:
- All package versions defined centrally
- Conditional versions based on target framework (e.g., different EF versions for net9 vs net10)
- Don't add `Version=` attributes to `<PackageReference>` in individual projects

### Signing

All source projects import `signing.props` and are strong-named with `MassTransit.snk`.

## Code Style & Conventions

From `.editorconfig`:
- **Indent**: 4 spaces (except 2 for config/xml/json/yml/proto)
- **End of line**: LF for `.cs` and `.sh`, CRLF for `.cmd`/`.bat`
- **Warnings suppressed**: CS1591 (missing XML comments), CS1998 (async without await)
- C# LangVersion: 12

## Git Workflow

**CRITICAL**: Before committing, disable autocrlf:

```bash
git config core.autocrlf false
```

Line endings are managed by `.editorconfig` and `.gitattributes`, not git config.

## CI/GitHub Actions

`.github/workflows/build.yml` runs:
1. **Compile** job (ubuntu + windows)
2. **Test jobs** in parallel for each transport/persistence/scheduler
3. **Version calculation** based on branch
4. **Publish** to NuGet (master/develop only)

CI uses:
- .NET 10 SDK (`dotnet-version: '10.0.x'`)
- `--logger GitHubActions` for test output
- `--filter Category!=Flaky` to skip flaky tests
- Docker services for many tests
- Secrets for Azure resources (ASB, Cosmos, Storage)

## Test Categorization

Tests use NUnit `[Category]` attributes:
- `Category=Flaky` - excluded from CI runs
- `Category=Integration` - may be excluded for specific suites

## Common Gotchas

1. **Windows-only targets**: `net472` is only built on Windows (`IsWindows` condition)
2. **SQL tests need env var**: `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false` (otherwise SQL client fails)
3. **Transport tests are sequential**: Each needs its own Docker services on specific ports
4. **Version sourcing**: Version is set via env var `MASSTRANSIT_VERSION` in workflows, not in props files
5. **Disabled tests exist**: Some integration tests are commented out or conditionally disabled in CI

## Documentation

Official docs: https://masstransit-project.com/

## Issue Reporting

Do NOT open GitHub issues for questions. Use:
- **GitHub Discussions** for questions/ideas
- **Discord** for live help: https://discord.gg/rNpQgYn
- **GitHub Issues** only for actual bugs
