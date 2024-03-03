# Npgsql.DocumentDB

## Introduction

A simple as possible Document DB on PostgreSQL.

## Usage

The intended usage of Npgsql.DocumentDB is via Dependency Injection.

In your Startup class or similar setup services like this:

```csharp
services.AddNpgsqlDataSource(connectionString, a => a.EnableDynamicJson());
services.AddSingleton<NpgsqlDocumentDB>();
```

This will then allow you to inject `NpgsqlDocumentDB` where you need it.

The `NpgsqlDocumentDB` instance simply allows you to do `Set`, `Get` and `Remove`. As values use any C# instance that can be (de)serialized from/to a JSON object.
