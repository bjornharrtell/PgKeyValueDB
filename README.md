# PgKeyValueDB

## Introduction

A simple as possible document database abstraction and implementation using PostgreSQL.

It uses [POCO mapping](https://www.npgsql.org/doc/types/json.html#poco-mapping) to
allow use of any C# instance that can be (de)serialized from/to a JSON object as a value.

## Usage

It's recommended to use [.NET Dependency Injection](https://www.nuget.org/packages/Npgsql.DependencyInjection#readme-body-tab)
and setup via your Startup class like this:

```csharp
services.AddPgKeyValueDB(connectionString);
```

You can then inject `PgKeyValueDB` as constructor parameter where you need it.

The `PgKeyValueDB` instance offers basic API to do `Create`, `Update`, `Upsert`, `Get` and `Remove` operations with optional partition key and expiration.

## FAQ

### What about distributed/concurrent usage?

Should be no different from standard PostgreSQL behaviour i.e https://www.postgresql.org/docs/current/transaction-iso.html.
Note that default isolation level can be set in connection string with fx `Options=-c default_transaction_isolation=serializable`.
